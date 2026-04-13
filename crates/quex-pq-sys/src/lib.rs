//! Raw bindgen bindings to libpq.
//!
//! This crate is published so `quex-driver` can be built from crates.io. It is
//! not intended to be a hand-written safe wrapper.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
