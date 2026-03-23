//! Swift/Kotlin binding generator for taskchampion-ffi.
//!
//! Usage:
//!   cargo run --bin uniffi-bindgen -- generate --library \
//!       target/release/libtaskchampion_ffi.dylib \
//!       --language swift --out-dir out/
//!
//! Requires the `uniffi-bindgen` crate. Add it to the workspace or run via
//! `cargo install uniffi-bindgen` and invoke directly.
fn main() {
    eprintln!(
        "Run binding generation via `uniffi-bindgen generate --library <path> \
         --language swift --out-dir out/`"
    );
    eprintln!("Install: cargo install uniffi-bindgen");
    std::process::exit(1);
}
