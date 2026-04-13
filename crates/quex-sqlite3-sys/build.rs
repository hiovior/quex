use std::env;
use std::path::PathBuf;

fn main() {
    let library = pkg_config::Config::new()
        .probe("sqlite3")
        .expect("failed to find sqlite3 via pkg-config; install sqlite3 development headers");

    let include_dir = library
        .include_paths
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("/usr/include"));

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}", include_dir.display()))
        .allowlist_type("sqlite3")
        .allowlist_type("sqlite3_stmt")
        .allowlist_type("sqlite3_value")
        .allowlist_var("SQLITE_.*")
        .allowlist_function("sqlite3_.*")
        .generate()
        .expect("failed to generate SQLite bindings");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("failed to write generated bindings");
}
