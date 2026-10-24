//! Cheap content sniffing helpers used by validation.

use std::path::Path;

/// Media type of a source path as `deno_ast` classifies it (`.ts`, `.tsx`, `.jsx`, `.mjs`, …).
pub fn media_type_of(path: &str) -> deno_ast::MediaType {
    deno_ast::MediaType::from_path(Path::new(path))
}

/// True for a WebAssembly **component** (not a core module).
pub fn is_wasm_component(bytes: &[u8]) -> bool {
    wasmparser::Parser::is_component(bytes)
}

/// True for a WebAssembly core module.
pub fn is_wasm_core_module(bytes: &[u8]) -> bool {
    wasmparser::Parser::is_core_wasm(bytes)
}
