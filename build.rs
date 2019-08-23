fn main() {
    #[cfg(feature = "proximo")]
    {
        tower_grpc_build::Config::new()
            .enable_client(true)
            .build(&["proto/proximo/proximo.proto"], &["proto/proximo"])
            .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
        println!("cargo:rerun-if-changed=proto/proximo/proximo.proto");
    }
}
