//! HTML本文抽出パイプライン。
//!
//! CETD (Composite Text Density) ライクなスコアでDOMの「本文ノード」を選び、
//! 整形してMarkdownに変換、併せてリンクとコードブロックを抽出する。
//!
//! 流れ: `parse` → `annotate` (統計付与) → `select` (最大スコア) →
//! `render` (ノイズ/低品質ノードを除外しながらMarkdownへ直接書き出し)
//! + `extract_links` + `extract_code_blocks`。

use scraper::{ElementRef, Html, Node as ScraperNode, Selector};
use serde::Serialize;
use std::collections::HashSet;
use std::sync::LazyLock;
use tracing::debug;
use url::Url;

// ----------------------------------------------------------------
// 1. 設定 & タグ判定
// ----------------------------------------------------------------

/// 主要HTML5タグの列挙。未知タグは `Other(String)` で吸収する。
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tag {
    Html,
    Body,
    Head,
    Div,
    Article,
    Section,
    Main,
    P,
    H1,
    H2,
    H3,
    H4,
    H5,
    H6,
    Ul,
    Ol,
    Li,
    Dl,
    Dt,
    Dd,
    Pre,
    Code,
    Blockquote,
    Table,
    Thead,
    Tbody,
    Tr,
    Th,
    Td,
    Caption,
    Figure,
    Figcaption,
    Hr,
    Br,
    A,
    Img,
    Strong,
    B,
    Em,
    I,
    Span,
    // ノイズ (削除対象)
    Script,
    Style,
    Noscript,
    Iframe,
    Svg,
    Header,
    Footer,
    Nav,
    Aside,
    Form,
    Button,
    Other(String),
}

impl Tag {
    /// `scraper` から取れるタグ名 (html5ever で小文字化済み) を enum に写す。
    fn from_name(name: &str) -> Self {
        use Tag::*;
        match name {
            "html" => Html,
            "body" => Body,
            "head" => Head,
            "div" => Div,
            "article" => Article,
            "section" => Section,
            "main" => Main,
            "p" => P,
            "h1" => H1,
            "h2" => H2,
            "h3" => H3,
            "h4" => H4,
            "h5" => H5,
            "h6" => H6,
            "ul" => Ul,
            "ol" => Ol,
            "li" => Li,
            "dl" => Dl,
            "dt" => Dt,
            "dd" => Dd,
            "pre" => Pre,
            "code" => Code,
            "blockquote" => Blockquote,
            "table" => Table,
            "thead" => Thead,
            "tbody" => Tbody,
            "tr" => Tr,
            "th" => Th,
            "td" => Td,
            "caption" => Caption,
            "figure" => Figure,
            "figcaption" => Figcaption,
            "hr" => Hr,
            "br" => Br,
            "a" => A,
            "img" => Img,
            "strong" => Strong,
            "b" => B,
            "em" => Em,
            "i" => I,
            "span" => Span,
            "script" => Script,
            "style" => Style,
            "noscript" => Noscript,
            "iframe" => Iframe,
            "svg" => Svg,
            "header" => Header,
            "footer" => Footer,
            "nav" => Nav,
            "aside" => Aside,
            "form" => Form,
            "button" => Button,
            _ => Other(name.to_string()),
        }
    }

    /// 統計計算/出力対象から外すタグ。
    fn is_noise(&self) -> bool {
        use Tag::*;
        matches!(
            self,
            Script | Style | Noscript | Iframe | Svg | Header | Footer | Nav | Aside | Form | Button
        )
    }

    /// 文字数足切りから守るタグ (見出し・コード・リスト要素など)。
    fn is_protected(&self) -> bool {
        use Tag::*;
        matches!(
            self,
            H1 | H2
                | H3
                | H4
                | H5
                | H6
                | Code
                | Pre
                | Blockquote
                | Li
                | Dt
                | Dd
                | Th
                | Td
                | Caption
                | Figcaption
        )
    }

    /// 子要素が空でも消さないタグ。
    fn is_void(&self) -> bool {
        matches!(self, Tag::Img | Tag::Br | Tag::Hr)
    }
}

/// リンク抽出時に除外するドメイン (SNS・寄付サービスなど)。
static IGNORE_DOMAINS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "paypal.me",
        "ko-fi.com",
        "www.ko-fi.com",
        "twitter.com",
        "x.com",
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "pinterest.com",
        "reddit.com",
        "hatenabookmark.com",
        "b.hatena.ne.jp",
    ]
    .into_iter()
    .collect()
});

/// href文字列に含まれていたら除外するパターン (`javascript:`, `mailto:` など)。
static IGNORE_HREF_PATTERNS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "/cdn-cgi/l/email-protection",
        "javascript:",
        "mailto:",
        "tel:",
    ]
    .into_iter()
    .collect()
});

static BODY_SEL: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("body").expect("valid selector"));
static HTML_SEL: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("html").expect("valid selector"));
static TITLE_SEL: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("title").expect("valid selector"));

// ----------------------------------------------------------------
// 2. 内部表現
// ----------------------------------------------------------------

/// クレンジング/出力に必要な属性だけ持つコンパクト表現。
#[derive(Debug, Clone, Default)]
struct Attrs {
    href: Option<String>,
    src: Option<String>,
    alt: Option<String>,
    title: Option<String>,
}

impl Attrs {
    fn from_iter<'a>(iter: impl Iterator<Item = (&'a str, &'a str)>) -> Self {
        let mut a = Attrs::default();
        for (k, v) in iter {
            match k {
                "href" => a.href = Some(v.to_string()),
                "src" => a.src = Some(v.to_string()),
                "alt" => a.alt = Some(v.to_string()),
                "title" => a.title = Some(v.to_string()),
                _ => {}
            }
        }
        a
    }
}

/// CETDスコア算出に使う、サブツリーから集計した統計量。
#[derive(Debug, Clone, Default)]
struct Stats {
    char_count: usize,
    tag_count: usize,
    link_char_count: usize,
}

/// 注釈付きDOMノード。要素には統計とスコアが埋め込まれる。
#[derive(Debug, Clone)]
enum Node {
    Text(String),
    Element {
        tag: Tag,
        attrs: Attrs,
        children: Vec<Node>,
        stats: Stats,
        score: f64,
    },
}

/// 抽出された外向きリンク (テキストと絶対URL)。
#[derive(Debug, Clone, Serialize)]
pub struct Link {
    pub text: String,
    pub href: String,
}

/// `parse_html` の戻り値。`is_spa` は本文がほぼ空 (SPA等で要JS) を示す。
#[derive(Debug, Serialize)]
pub struct ParsedHtml {
    pub title: String,
    pub url: String,
    pub content: String,
    pub length: usize,
    pub links: Vec<Link>,
    pub code_blocks: Vec<String>,
    pub is_spa: bool,
}

// ----------------------------------------------------------------
// 3. 統計情報の計算 (Bottom-up Annotation)
// ----------------------------------------------------------------

/// CETD ライクなノード単体のスコア。
/// テキスト密度 (文字数/タグ数) × リンク密度ペナルティ × 文字数の対数ブースト。
fn calculate_score(stats: &Stats) -> f64 {
    if stats.char_count == 0 || stats.tag_count == 0 {
        return 0.0;
    }
    let text_density = stats.char_count as f64 / stats.tag_count.max(1) as f64;
    let link_density = stats.link_char_count as f64 / stats.char_count.max(1) as f64;
    let link_penalty = if link_density > 0.5 {
        0.05
    } else {
        1.0 - link_density
    };
    let log_boost = ((stats.char_count + 1) as f64).log10();
    text_density * link_penalty * log_boost
}

/// `scraper` のDOMを再帰的に走査して `Node` を構築し、
/// 各要素に統計量と「自身の密度 + 子合計の0.4倍」の伝播スコアを付与する。
fn convert_element(el_ref: ElementRef) -> Node {
    let el = el_ref.value();
    let tag = Tag::from_name(el.name());

    if tag.is_noise() {
        return Node::Element {
            tag,
            attrs: Attrs::default(),
            children: vec![],
            stats: Stats {
                char_count: 0,
                tag_count: 1,
                link_char_count: 0,
            },
            score: 0.0,
        };
    }

    let attrs = Attrs::from_iter(el.attrs());

    let mut children: Vec<Node> = Vec::new();
    for child in el_ref.children() {
        match child.value() {
            ScraperNode::Text(t) => children.push(Node::Text(t.text.to_string())),
            ScraperNode::Element(_) => {
                if let Some(child_el) = ElementRef::wrap(child) {
                    children.push(convert_element(child_el));
                }
            }
            _ => {}
        }
    }

    let mut sum_char = 0usize;
    let mut sum_tag = 0usize;
    let mut sum_link = 0usize;
    let mut sum_children_score = 0.0f64;

    for c in &children {
        match c {
            Node::Text(t) => sum_char += t.trim().chars().count(),
            Node::Element { stats, score, .. } => {
                sum_char += stats.char_count;
                sum_tag += stats.tag_count;
                sum_link += stats.link_char_count;
                sum_children_score += score;
            }
        }
    }

    let is_a = matches!(tag, Tag::A);
    let final_link_char = if is_a { sum_char } else { sum_link };
    let stats = Stats {
        char_count: sum_char,
        tag_count: sum_tag + 1,
        link_char_count: final_link_char,
    };
    let native_score = calculate_score(&stats);
    // 最終スコア = 自身の密度 + (子要素の合計スコア * 減衰係数 0.4)
    let final_score = native_score + sum_children_score * 0.4;

    Node::Element {
        tag,
        attrs,
        children,
        stats,
        score: final_score,
    }
}

// ----------------------------------------------------------------
// 4. 本文ノード選択
// ----------------------------------------------------------------

/// ツリー全体から `score × √char_count` が最大のノードを返す。
/// √文字数を掛けることで、短い `<p>` より本文を包む `<div>` が選ばれやすくなる。
fn find_best_node(root: &Node) -> &Node {
    let mut best = root;
    let mut best_value = f64::NEG_INFINITY;

    let mut stack: Vec<&Node> = vec![root];
    while let Some(n) = stack.pop() {
        if let Node::Element {
            score,
            stats,
            children,
            ..
        } = n
        {
            let value = score * (stats.char_count as f64).sqrt();
            if value > best_value {
                best_value = value;
                best = n;
            }
            for c in children {
                stack.push(c);
            }
        }
    }
    best
}

// ----------------------------------------------------------------
// 5. Markdown 書き出し
// ----------------------------------------------------------------

/// `out` 末尾の連続改行数を `want` 以上に揃える (それ以上は足さない)。
/// ブロック境界の重複改行を発生時点で抑え、後段の正規化処理を不要にする。
fn ensure_newlines(out: &mut String, want: usize) {
    let trailing = out.as_bytes().iter().rev().take_while(|&&b| b == b'\n').count();
    for _ in trailing..want {
        out.push('\n');
    }
}

/// 1ノードをMarkdownとして `out` に追記する。
/// `is_protected` が `true` の祖先 (pre/code/見出し等) の配下では
/// 文字数足切りもテキストtrimも行わない。
fn render(node: &Node, out: &mut String, is_protected: bool) {
    match node {
        Node::Text(s) => {
            if is_protected {
                out.push_str(s);
            } else {
                let t = s.trim();
                if !t.is_empty() {
                    out.push_str(t);
                }
            }
        }
        Node::Element {
            tag,
            attrs,
            children,
            stats,
            ..
        } => {
            if tag.is_noise() {
                return;
            }
            let p = is_protected || tag.is_protected();

            // 足切り: 保護されていない & 文字数 < 10 & 何らかのタグを持つ
            if !p && stats.char_count < 10 && stats.tag_count > 0 {
                return;
            }
            // 子なし & non-void は描画しない
            if children.is_empty() && !tag.is_void() {
                return;
            }

            render_tag(tag, attrs, children, out, p);
        }
    }
}

fn render_children(children: &[Node], out: &mut String, is_protected: bool) {
    for c in children {
        render(c, out, is_protected);
    }
}

fn render_children_string(children: &[Node], is_protected: bool) -> String {
    let mut s = String::new();
    render_children(children, &mut s, is_protected);
    s
}

/// 「前置プレフィックス + 子のtrim結果」を空行で挟むブロック (見出し用)。
fn write_block_prefix(out: &mut String, prefix: &str, children: &[Node], is_protected: bool) {
    let inner = render_children_string(children, is_protected);
    let t = inner.trim();
    if !t.is_empty() {
        ensure_newlines(out, 2);
        out.push_str(prefix);
        out.push_str(t);
        ensure_newlines(out, 2);
    }
}

/// 子のtrim結果を空行で挟むブロック (p/ul/ol用)。
fn write_block(out: &mut String, children: &[Node], is_protected: bool) {
    let inner = render_children_string(children, is_protected);
    let t = inner.trim();
    if !t.is_empty() {
        ensure_newlines(out, 2);
        out.push_str(t);
        ensure_newlines(out, 2);
    }
}

fn render_tag(tag: &Tag, attrs: &Attrs, children: &[Node], out: &mut String, p: bool) {
    use Tag::*;
    match tag {
        H1 => write_block_prefix(out, "# ", children, p),
        H2 => write_block_prefix(out, "## ", children, p),
        H3 => write_block_prefix(out, "### ", children, p),
        H4 => write_block_prefix(out, "#### ", children, p),
        H5 => write_block_prefix(out, "##### ", children, p),
        H6 => write_block_prefix(out, "###### ", children, p),
        P | Ul | Ol => write_block(out, children, p),
        Li => {
            let inner = render_children_string(children, p);
            let t = inner.trim();
            if !t.is_empty() {
                out.push_str("* ");
                out.push_str(t);
                out.push('\n');
            }
        }
        Hr => {
            ensure_newlines(out, 2);
            out.push_str("---");
            ensure_newlines(out, 2);
        }
        Div | Article | Section | Main => {
            let inner = render_children_string(children, p);
            let t = inner.trim();
            if !t.is_empty() {
                ensure_newlines(out, 1);
                out.push_str(t);
                ensure_newlines(out, 1);
            }
        }
        Strong | B => {
            let inner = render_children_string(children, p);
            if !inner.is_empty() {
                out.push_str("**");
                out.push_str(&inner);
                out.push_str("**");
            }
        }
        Em | I => {
            let inner = render_children_string(children, p);
            if !inner.is_empty() {
                out.push('*');
                out.push_str(&inner);
                out.push('*');
            }
        }
        A => {
            let inner = render_children_string(children, p);
            match (&attrs.href, inner.is_empty()) {
                (Some(href), false) => {
                    out.push('[');
                    out.push_str(&inner);
                    out.push_str("](");
                    out.push_str(href);
                    out.push(')');
                }
                _ => out.push_str(&inner),
            }
        }
        Img => {
            if let Some(src) = &attrs.src {
                let alt = attrs.alt.as_deref().unwrap_or("");
                out.push_str("![");
                out.push_str(alt);
                out.push_str("](");
                out.push_str(src);
                out.push(')');
            }
        }
        Pre => {
            // pre 配下は強制 protected = trim せずインデント保持
            let inner = render_children_string(children, true);
            ensure_newlines(out, 2);
            out.push_str("```\n");
            out.push_str(&inner);
            ensure_newlines(out, 1);
            out.push_str("```");
            ensure_newlines(out, 2);
        }
        Code => render_children(children, out, p),
        Br => out.push_str("  \n"),
        _ => render_children(children, out, p),
    }
}

// ----------------------------------------------------------------
// 6. テキスト/リンク/コードブロック抽出
// ----------------------------------------------------------------

/// ノード配下の生テキストを連結する (リンクラベル・コードブロック抽出に使用)。
fn collect_text(node: &Node) -> String {
    match node {
        Node::Text(s) => s.clone(),
        Node::Element { children, .. } => children.iter().map(collect_text).collect(),
    }
}

/// ベースURLと相対パスを結合して絶対URLを返す。失敗時は `href` をそのまま返す。
fn resolve_url(base: &str, href: &str) -> String {
    match Url::parse(base).and_then(|b| b.join(href)) {
        Ok(u) => u.to_string(),
        Err(_) => href.to_string(),
    }
}

/// テキスト/href の空判定と、除外パターン・除外ドメイン・ページ内リンクを弾くフィルタ。
fn is_valid_link(link: &Link) -> bool {
    let lower = link.href.to_lowercase();
    let trimmed = link.text.trim();
    !trimmed.is_empty()
        && !link.href.is_empty()
        && !IGNORE_HREF_PATTERNS.iter().any(|p| lower.contains(p))
        && !IGNORE_DOMAINS.iter().any(|d| lower.contains(d))
        && !link.href.starts_with('#')
}

/// `<a>` を全走査し、絶対URL化・フィルタ・重複排除した結果を返す。
fn extract_links(root: &Node, base_url: &str) -> Vec<Link> {
    let mut links: Vec<Link> = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut stack: Vec<&Node> = vec![root];

    while let Some(n) = stack.pop() {
        if let Node::Element {
            tag,
            attrs,
            children,
            ..
        } = n
        {
            if matches!(tag, Tag::A) {
                if let Some(raw_href) = attrs.href.as_ref() {
                    if !raw_href.is_empty() && !raw_href.starts_with('#') {
                        let text = collect_text(n)
                            .replace('\u{200B}', "")
                            .trim()
                            .to_string();
                        if !text.is_empty() {
                            let abs = resolve_url(base_url, raw_href);
                            let link = Link { text, href: abs };
                            if is_valid_link(&link) {
                                let key = (link.text.clone(), link.href.clone());
                                if seen.insert(key) {
                                    links.push(link);
                                }
                            }
                        }
                    }
                }
            }
            for c in children {
                stack.push(c);
            }
        }
    }
    links
}

/// `<pre>` 要素を全走査し、配下の生テキストをコードブロック1件として収集する。
/// ネストした `<pre>` には再帰しない (外側を1単位として扱う)。
fn extract_code_blocks(root: &Node) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut stack: Vec<&Node> = vec![root];

    while let Some(n) = stack.pop() {
        if let Node::Element { tag, children, .. } = n {
            if matches!(tag, Tag::Pre) {
                let text = collect_text(n);
                let trimmed = text.trim_matches('\n');
                if !trimmed.trim().is_empty() {
                    blocks.push(trimmed.to_string());
                }
                continue;
            }
            for c in children {
                stack.push(c);
            }
        }
    }
    blocks
}

// ----------------------------------------------------------------
// 7. メイン
// ----------------------------------------------------------------

/// `<title>` 要素のテキストを取り出す。無い・空なら `None`。
fn extract_title(html: &Html) -> Option<String> {
    let el = html.select(&TITLE_SEL).next()?;
    let text: String = el.text().collect();
    let t = text.trim();
    if t.is_empty() { None } else { Some(t.to_string()) }
}

/// HTML本文を解析し、本文Markdown・タイトル・リンク・コードブロックを抽出する。
///
/// `url` は相対URLを絶対化するためのベースURLとして使う。
/// `<body>` が見つからなければ `<html>` をフォールバックとし、いずれも無ければ
/// `is_spa = true` の空結果を返す。
pub fn parse_html(html_body: &str, url: &str) -> ParsedHtml {
    let document = Html::parse_document(html_body);
    let title = extract_title(&document).unwrap_or_default();

    let root_ref: Option<ElementRef> = document
        .select(&BODY_SEL)
        .next()
        .or_else(|| document.select(&HTML_SEL).next());

    let Some(root_ref) = root_ref else {
        return ParsedHtml {
            title,
            url: url.to_string(),
            content: String::new(),
            length: 0,
            links: vec![],
            code_blocks: vec![],
            is_spa: true,
        };
    };

    let root_node = convert_element(root_ref);
    let best = find_best_node(&root_node);

    if let Node::Element { tag, score, stats, .. } = best {
        debug!(
            ?tag,
            score = *score,
            chars = stats.char_count,
            "selected body node"
        );
    }

    let mut buf = String::new();
    render(best, &mut buf, false);
    let content = buf.trim().to_string();

    let links = extract_links(best, url);
    let code_blocks = extract_code_blocks(best);
    let length = content.chars().count();
    let is_spa = content.is_empty();

    ParsedHtml {
        title,
        url: url.to_string(),
        content,
        length,
        links,
        code_blocks,
        is_spa,
    }
}
