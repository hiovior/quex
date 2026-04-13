use std::env;
use std::path::PathBuf;

fn main() {
    let library = pkg_config::Config::new()
        .probe("mariadb")
        .expect("failed to find MariaDB Connector/C via pkg-config; install libmariadb-dev");

    let include_dir = library
        .include_paths
        .first()
        .cloned()
        .expect("pkg-config returned no include path for mariadb");

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}", include_dir.display()))
        .allowlist_type("MYSQL.*")
        .allowlist_type("MARIADB_.*")
        .allowlist_type("NET")
        .allowlist_type("my_bool")
        .allowlist_type("my_socket")
        .allowlist_type("enum_mysql_option")
        .allowlist_type("enum_field_types")
        .allowlist_type("enum_mysql_set_option")
        .allowlist_var("MYSQL_.*")
        .allowlist_var("MARIADB_.*")
        .allowlist_var("CLIENT_.*")
        .allowlist_function("mysql_.*")
        .allowlist_function("mariadb_.*")
        .generate()
        .expect("failed to generate MariaDB bindings");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("failed to write generated bindings");
}
