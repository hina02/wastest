use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn greet(name: &str) -> String {
    format!("Hello, {}!", name)
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[wasm_bindgen]
pub fn hello_wasm() {
    log("Hello from Rust!");
}


// 数値
#[wasm_bindgen]
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

// 文字列
#[wasm_bindgen]
pub fn reverse_string(s: &str) -> String {
    s.chars().rev().collect()
}

// 配列（Uint8Array）
#[wasm_bindgen]
pub fn sum_array(arr: &[u8]) -> u32 {
    arr.iter().map(|&x| x as u32).sum()
}