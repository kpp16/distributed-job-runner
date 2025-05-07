fn main() {
    tonic_build::compile_protos("proto/task.proto").unwrap_or_else(|e| panic!("Failed to compile protof: {}", e));
}
