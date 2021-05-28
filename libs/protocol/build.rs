fn main() {
    tonic_build::configure()
        .out_dir("src")
        .compile(&["proto/yql.proto"], &["proto"])
        .unwrap();
    println!("cargo:rerun-if-changed=proto");
}
