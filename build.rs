use std::{env, path::PathBuf};

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["protos/data.proto", "protos/meta.proto"], &["protos"])
        .unwrap()
}
