import init, { greet, hello_wasm } from "./wasm/pkg/wastest.js";

await init();
hello_wasm();
export { greet };