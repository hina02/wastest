use anyhow::Result;
use std::sync::LazyLock;
use tiktoken_rs::{CoreBPE, o200k_base};

static TOKENIZER: LazyLock<CoreBPE> =
    LazyLock::new(|| o200k_base().expect("failed to load o200k_base tokenizer"));

pub fn count_tokens(text: &str) -> usize {
    TOKENIZER.encode_with_special_tokens(text).len()
}

pub fn split_text(text: &str, max_tokens: usize) -> Result<Vec<String>> {
    assert!(max_tokens > 0, "max_tokens must be > 0");
    let tokens = TOKENIZER.encode_with_special_tokens(text);
    let mut chunks = Vec::with_capacity(tokens.len().div_ceil(max_tokens));
    for chunk in tokens.chunks(max_tokens) {
        chunks.push(TOKENIZER.decode(chunk)?);
    }
    Ok(chunks)
}
