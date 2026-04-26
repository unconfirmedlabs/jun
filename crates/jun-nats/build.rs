// Compile jun.proto into Rust types via prost-build. We use a vendored protoc
// binary so the OSS repo doesn't require a system protoc install.

fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("protoc-bin-vendored should resolve a binary for this platform");
    std::env::set_var("PROTOC", protoc);

    prost_build::compile_protos(&["proto/jun.proto"], &["proto/"])
        .expect("compile jun.proto");

    println!("cargo:rerun-if-changed=proto/jun.proto");
}
