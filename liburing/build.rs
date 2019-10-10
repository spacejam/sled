extern crate bindgen;

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    Command::new("make")
        .arg("liburing.a")
        .current_dir("lib/src")
        .env("CFLAGS", "-fPIC")
        .status()
        .expect("failed to build liburing.a");

    // Tell cargo to tell rustc to link the system liburing
    // shared library.
    println!("cargo:rustc-link-lib=static=uring");
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rustc-link-search=native=lib/src");

    // Generate bindings
    let bindings = bindgen::Builder::default()
        .whitelist_function("io_uring.*")
        .whitelist_var("IORING.*")
        .whitelist_type("io_uring.*")
        .header("wrapper.h")
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(out_dir);
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
