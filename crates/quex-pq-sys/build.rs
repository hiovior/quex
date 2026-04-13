use std::env;
use std::path::PathBuf;

fn main() {
    let library = pkg_config::Config::new()
        .probe("libpq")
        .expect("failed to find libpq via pkg-config; install libpq development headers");

    let include_dir = library
        .include_paths
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("/usr/include"));

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}", include_dir.display()))
        .allowlist_type("PGconn")
        .allowlist_type("PGresult")
        .allowlist_type("PostgresPollingStatusType")
        .allowlist_type("ExecStatusType")
        .allowlist_type("ConnStatusType")
        .allowlist_var("PGRES_.*")
        .allowlist_var("CONNECTION_.*")
        .allowlist_var("PG_DIAG_.*")
        .allowlist_function("PQconnectStartParams")
        .allowlist_function("PQconnectPoll")
        .allowlist_function("PQsetnonblocking")
        .allowlist_function("PQsocket")
        .allowlist_function("PQstatus")
        .allowlist_function("PQfinish")
        .allowlist_function("PQerrorMessage")
        .allowlist_function("PQsendQuery")
        .allowlist_function("PQsendQueryParams")
        .allowlist_function("PQsendPrepare")
        .allowlist_function("PQsendQueryPrepared")
        .allowlist_function("PQflush")
        .allowlist_function("PQconsumeInput")
        .allowlist_function("PQisBusy")
        .allowlist_function("PQgetResult")
        .allowlist_function("PQresultStatus")
        .allowlist_function("PQresultErrorMessage")
        .allowlist_function("PQresultErrorField")
        .allowlist_function("PQclear")
        .allowlist_function("PQntuples")
        .allowlist_function("PQnfields")
        .allowlist_function("PQcmdTuples")
        .allowlist_function("PQfname")
        .allowlist_function("PQftype")
        .allowlist_function("PQfformat")
        .allowlist_function("PQgetisnull")
        .allowlist_function("PQgetlength")
        .allowlist_function("PQgetvalue")
        .generate()
        .expect("failed to generate libpq bindings");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("failed to write generated bindings");
}
