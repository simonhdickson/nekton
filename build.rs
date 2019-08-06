fn main() {
    #[cfg(feature = "proximo")]
    {   
        protoc_grpcio::compile_grpc_protos(
            &["proximo.proto"],
            &["proto/proximo"],
            &"src/proximo/generated",
            None
        ).expect("Failed to compile gRPC definitions!");
        println!("cargo:rerun-if-changed=proto/proximo/proximo.proto");
    }
}
