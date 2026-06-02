use gloo_net::http::Request;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::{Document, Element, HtmlInputElement, HtmlSelectElement, HtmlTextAreaElement};

// ----------------------------------------------------------------
// API response types
// ----------------------------------------------------------------

#[derive(Deserialize)]
struct SearchHit {
    statement: String,
    keywords: Vec<String>,
    score: f32,
}

#[derive(Serialize)]
struct ChatReq<'a> {
    namespace: &'a str,
    session_id: &'a str,
    message: &'a str,
}

#[derive(Deserialize)]
struct ChatResp {
    answer: String,
}

#[derive(Serialize)]
struct ClearReq<'a> {
    session_id: &'a str,
}

#[derive(Serialize)]
struct IngestReq {
    namespace: String,
    urls: Vec<String>,
}

#[derive(Deserialize)]
struct IngestResp {
    ingested: usize,
    skipped: usize,
}

#[derive(Serialize)]
struct CrawlReq {
    depth: usize,
    seeds: Vec<String>,
}

#[derive(Deserialize)]
struct CrawlResp {
    urls: Vec<String>,
}

#[derive(Serialize)]
struct RefreshReq {
    namespace: String,
}

#[derive(Deserialize)]
struct TopStory {
    fetched_at: String,
    count: usize,
}

// ----------------------------------------------------------------
// DOM helpers
// ----------------------------------------------------------------

fn doc() -> Document {
    web_sys::window().unwrap().document().unwrap()
}

fn get_input(id: &str) -> String {
    doc()
        .get_element_by_id(id)
        .and_then(|el| el.dyn_into::<HtmlInputElement>().ok())
        .map(|el| el.value())
        .unwrap_or_default()
}

fn get_select(id: &str) -> String {
    doc()
        .get_element_by_id(id)
        .and_then(|el| el.dyn_into::<HtmlSelectElement>().ok())
        .map(|el| el.value())
        .unwrap_or_default()
}

fn get_textarea(id: &str) -> String {
    doc()
        .get_element_by_id(id)
        .and_then(|el| el.dyn_into::<HtmlTextAreaElement>().ok())
        .map(|el| el.value())
        .unwrap_or_default()
}

fn set_status(id: &str, msg: &str, is_error: bool) {
    if let Some(el) = doc().get_element_by_id(id) {
        el.set_text_content(Some(msg));
        let _ = el.class_list().remove_2("ok", "error");
        let _ = el.class_list().add_1(if is_error { "error" } else { "ok" });
    }
}

fn clear_list(id: &str) {
    if let Some(el) = doc().get_element_by_id(id) {
        el.set_inner_html("");
    }
}

fn append_li(list_id: &str, html: String) {
    if let Some(ul) = doc().get_element_by_id(list_id) {
        let li = doc().create_element("li").unwrap();
        li.set_inner_html(&html);
        let _ = ul.append_child(&li);
    }
}

fn append_chat_msg(role: &str, text: &str) {
    if let Some(history) = doc().get_element_by_id("chat-history") {
        let div = doc().create_element("div").unwrap();
        let _ = div.class_list().add_2("chat-msg", role);
        let role_el = doc().create_element("div").unwrap();
        let _ = role_el.class_list().add_1("role");
        role_el.set_text_content(Some(if role == "user" { "You" } else { "Assistant" }));
        let body_el = doc().create_element("div").unwrap();
        let _ = body_el.class_list().add_1("body");
        body_el.set_text_content(Some(text));
        let _ = div.append_child(&role_el);
        let _ = div.append_child(&body_el);
        let _ = history.append_child(&div);
        history.set_scroll_top(history.scroll_height());
    }
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn on_click(id: &str, f: impl Fn() + 'static) {
    if let Some(el) = doc().get_element_by_id(id) {
        let cb = Closure::<dyn FnMut(_)>::new(move |_: web_sys::MouseEvent| f());
        let _ = el.add_event_listener_with_callback("click", cb.as_ref().unchecked_ref());
        cb.forget();
    }
}

fn on_enter(id: &str, f: impl Fn() + 'static) {
    if let Some(el) = doc().get_element_by_id(id) {
        let cb = Closure::<dyn FnMut(_)>::new(move |e: web_sys::KeyboardEvent| {
            if e.key() == "Enter" {
                f();
            }
        });
        let _ = el.add_event_listener_with_callback("keydown", cb.as_ref().unchecked_ref());
        cb.forget();
    }
}

// ----------------------------------------------------------------
// Tab switching
// ----------------------------------------------------------------

fn setup_tabs() {
    let document = doc();
    let tab_btns = document.query_selector_all(".tab-btn").unwrap();
    for i in 0..tab_btns.length() {
        let btn: Element = tab_btns.item(i).unwrap().dyn_into().unwrap();
        let tab_id = btn.get_attribute("data-tab").unwrap_or_default();
        let cb = Closure::<dyn FnMut(_)>::new(move |_: web_sys::MouseEvent| {
            let d = doc();
            let btns = d.query_selector_all(".tab-btn").unwrap();
            for j in 0..btns.length() {
                let b: Element = btns.item(j).unwrap().dyn_into().unwrap();
                let _ = b.class_list().remove_1("active");
            }
            let panes = d.query_selector_all(".tab-pane").unwrap();
            for j in 0..panes.length() {
                let p: Element = panes.item(j).unwrap().dyn_into().unwrap();
                let _ = p.class_list().remove_1("active");
            }
            if let Some(clicked) = d.query_selector(&format!("[data-tab='{}']", tab_id)).ok().flatten() {
                let _ = clicked.class_list().add_1("active");
            }
            if let Some(pane) = d.get_element_by_id(&format!("tab-{}", tab_id)) {
                let _ = pane.class_list().add_1("active");
            }
        });
        let _ = btn.add_event_listener_with_callback("click", cb.as_ref().unchecked_ref());
        cb.forget();
    }
}

// ----------------------------------------------------------------
// Search
// ----------------------------------------------------------------

fn do_search() {
    spawn_local(async move {
        let namespace = get_select("search-namespace");
        let search_type = get_select("search-type");
        let q = get_input("search-query");
        let limit = get_input("search-limit");

        if q.trim().is_empty() {
            set_status("search-status", "クエリを入力してください", true);
            return;
        }

        set_status("search-status", "検索中...", false);
        clear_list("search-results");

        let url = format!(
            "/api/search/{}?namespace={}&q={}&limit={}",
            search_type,
            js_sys::encode_uri_component(&namespace),
            js_sys::encode_uri_component(&q),
            limit
        );

        match Request::get(&url).send().await {
            Ok(resp) if resp.ok() => {
                match resp.json::<Vec<SearchHit>>().await {
                    Ok(hits) if hits.is_empty() => {
                        set_status("search-status", "ヒットなし", false);
                    }
                    Ok(hits) => {
                        set_status(
                            "search-status",
                            &format!("{} 件ヒット", hits.len()),
                            false,
                        );
                        for h in &hits {
                            let kw = h.keywords.join(", ");
                            append_li(
                                "search-results",
                                format!(
                                    "<span class='hit-score'>{:.3}</span>{}<div class='hit-keywords'>{}</div>",
                                    h.score,
                                    escape(&h.statement),
                                    escape(&kw)
                                ),
                            );
                        }
                    }
                    Err(e) => set_status("search-status", &format!("parse error: {e}"), true),
                }
            }
            Ok(resp) => {
                let msg = resp.text().await.unwrap_or_default();
                set_status("search-status", &format!("error: {msg}"), true);
            }
            Err(e) => set_status("search-status", &format!("fetch error: {e}"), true),
        }
    });
}

// ----------------------------------------------------------------
// Chat
// ----------------------------------------------------------------

fn do_chat_send() {
    spawn_local(async move {
        let namespace = get_select("chat-namespace");
        let session_id = get_input("chat-session");
        let message = get_input("chat-input");

        if message.trim().is_empty() {
            return;
        }

        // clear input
        if let Some(el) = doc().get_element_by_id("chat-input") {
            if let Ok(input) = el.dyn_into::<HtmlInputElement>() {
                input.set_value("");
            }
        }

        append_chat_msg("user", &message);
        set_status("chat-status", "応答中...", false);

        let body = serde_json::to_string(&ChatReq {
            namespace: &namespace,
            session_id: &session_id,
            message: &message,
        })
        .unwrap();

        match Request::post("/api/chat")
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap()
            .send()
            .await
        {
            Ok(resp) if resp.ok() => match resp.json::<ChatResp>().await {
                Ok(r) => {
                    append_chat_msg("assistant", &r.answer);
                    set_status("chat-status", "", false);
                }
                Err(e) => set_status("chat-status", &format!("parse error: {e}"), true),
            },
            Ok(resp) => {
                let msg = resp.text().await.unwrap_or_default();
                set_status("chat-status", &format!("error: {msg}"), true);
            }
            Err(e) => set_status("chat-status", &format!("fetch error: {e}"), true),
        }
    });
}

fn do_chat_clear() {
    spawn_local(async move {
        let session_id = get_input("chat-session");
        let body = serde_json::to_string(&ClearReq { session_id: &session_id }).unwrap();
        match Request::post("/api/chat/clear")
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap()
            .send()
            .await
        {
            Ok(_) => {
                if let Some(h) = doc().get_element_by_id("chat-history") {
                    h.set_inner_html("");
                }
                set_status("chat-status", "履歴を削除しました", false);
            }
            Err(e) => set_status("chat-status", &format!("error: {e}"), true),
        }
    });
}

// ----------------------------------------------------------------
// Admin: Ingest
// ----------------------------------------------------------------

fn do_ingest() {
    spawn_local(async move {
        let namespace = get_select("ingest-namespace");
        let raw = get_textarea("ingest-urls");
        let urls: Vec<String> = raw
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        if urls.is_empty() {
            set_status("ingest-status", "URLを入力してください", true);
            return;
        }

        set_status("ingest-status", &format!("{} 件を送信中...", urls.len()), false);

        let body = serde_json::to_string(&IngestReq { namespace, urls }).unwrap();
        match Request::post("/api/ingest")
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap()
            .send()
            .await
        {
            Ok(resp) if resp.ok() => match resp.json::<IngestResp>().await {
                Ok(r) => set_status(
                    "ingest-status",
                    &format!("完了: {} 件 ingest, {} 件スキップ", r.ingested, r.skipped),
                    false,
                ),
                Err(e) => set_status("ingest-status", &format!("parse error: {e}"), true),
            },
            Ok(resp) => {
                let msg = resp.text().await.unwrap_or_default();
                set_status("ingest-status", &format!("error: {msg}"), true);
            }
            Err(e) => set_status("ingest-status", &format!("fetch error: {e}"), true),
        }
    });
}

// ----------------------------------------------------------------
// Admin: Crawl
// ----------------------------------------------------------------

fn do_crawl() {
    spawn_local(async move {
        let depth_str = get_input("crawl-depth");
        let depth = depth_str.parse::<usize>().unwrap_or(2);
        let raw = get_textarea("crawl-seeds");
        let seeds: Vec<String> = raw
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        if seeds.is_empty() {
            set_status("crawl-status", "Seed URLを入力してください", true);
            return;
        }

        set_status("crawl-status", "クロール中...", false);
        clear_list("crawl-results");

        let body = serde_json::to_string(&CrawlReq { depth, seeds }).unwrap();
        match Request::post("/api/crawl")
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap()
            .send()
            .await
        {
            Ok(resp) if resp.ok() => match resp.json::<CrawlResp>().await {
                Ok(r) => {
                    set_status(
                        "crawl-status",
                        &format!("{} 件のURLを発見", r.urls.len()),
                        false,
                    );
                    for url in &r.urls {
                        append_li("crawl-results", escape(url));
                    }
                }
                Err(e) => set_status("crawl-status", &format!("parse error: {e}"), true),
            },
            Ok(resp) => {
                let msg = resp.text().await.unwrap_or_default();
                set_status("crawl-status", &format!("error: {msg}"), true);
            }
            Err(e) => set_status("crawl-status", &format!("fetch error: {e}"), true),
        }
    });
}

// ----------------------------------------------------------------
// Admin: Refresh Indexes
// ----------------------------------------------------------------

fn do_refresh() {
    spawn_local(async move {
        let namespace = get_select("refresh-namespace");
        set_status("refresh-status", "インデックス再構築中...", false);
        let body = serde_json::to_string(&RefreshReq { namespace }).unwrap();
        match Request::post("/api/refresh")
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap()
            .send()
            .await
        {
            Ok(resp) if resp.ok() => set_status("refresh-status", "完了", false),
            Ok(resp) => {
                let msg = resp.text().await.unwrap_or_default();
                set_status("refresh-status", &format!("error: {msg}"), true);
            }
            Err(e) => set_status("refresh-status", &format!("fetch error: {e}"), true),
        }
    });
}

// ----------------------------------------------------------------
// Admin: Top Stories
// ----------------------------------------------------------------

fn do_top_stories() {
    spawn_local(async move {
        let namespace = get_select("stories-namespace");
        set_status("stories-status", "取得中...", false);
        clear_list("stories-results");

        let url = format!(
            "/api/top-stories?namespace={}",
            js_sys::encode_uri_component(&namespace)
        );
        match Request::get(&url).send().await {
            Ok(resp) if resp.ok() => match resp.json::<Vec<TopStory>>().await {
                Ok(stories) => {
                    set_status(
                        "stories-status",
                        &format!("{} スナップショット", stories.len()),
                        false,
                    );
                    for s in &stories {
                        append_li(
                            "stories-results",
                            format!(
                                "<span class='hit-score'>{} 件</span>{}",
                                s.count,
                                escape(&s.fetched_at)
                            ),
                        );
                    }
                }
                Err(e) => set_status("stories-status", &format!("parse error: {e}"), true),
            },
            Ok(resp) => {
                let msg = resp.text().await.unwrap_or_default();
                set_status("stories-status", &format!("error: {msg}"), true);
            }
            Err(e) => set_status("stories-status", &format!("fetch error: {e}"), true),
        }
    });
}

// ----------------------------------------------------------------
// Namespace loader
// ----------------------------------------------------------------

fn load_namespaces() {
    spawn_local(async move {
        let Ok(resp) = Request::get("/api/namespaces").send().await else { return };
        let Ok(names) = resp.json::<Vec<String>>().await else { return };
        let d = doc();
        let selects = d.query_selector_all(".ns-select").unwrap();
        for i in 0..selects.length() {
            let sel: web_sys::HtmlSelectElement =
                selects.item(i).unwrap().dyn_into().unwrap();
            sel.set_inner_html("");
            for name in &names {
                let opt = d.create_element("option").unwrap();
                opt.set_text_content(Some(name));
                opt.set_attribute("value", name).unwrap();
                sel.append_child(&opt).unwrap();
            }
        }
    });
}

// ----------------------------------------------------------------
// Entry point
// ----------------------------------------------------------------

#[wasm_bindgen(start)]
pub fn main() {
    load_namespaces();
    setup_tabs();

    // Search
    on_click("search-btn", do_search);
    on_enter("search-query", do_search);

    // Chat
    on_click("chat-send-btn", do_chat_send);
    on_enter("chat-input", do_chat_send);
    on_click("chat-clear-btn", do_chat_clear);

    // Admin
    on_click("ingest-btn", do_ingest);
    on_click("crawl-btn", do_crawl);
    on_click("refresh-btn", do_refresh);
    on_click("stories-btn", do_top_stories);
}
