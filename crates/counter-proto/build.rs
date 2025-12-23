fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = &["../../proto/counter.proto", "../../proto/replication.proto"];

    let proto_include_dirs = &["../../proto"];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(proto_files, proto_include_dirs)?;

    // Re-run build if proto files change
    for file in proto_files {
        println!("cargo:rerun-if-changed={}", file);
    }

    Ok(())
}
