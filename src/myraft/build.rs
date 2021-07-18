use std::{env, path::PathBuf};

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["proto/raftpb.proto"], &["proto"])
        .unwrap()
}
