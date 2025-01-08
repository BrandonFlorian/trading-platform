use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from("src/generated");

    println!("cargo:rerun-if-changed=proto/wallet.proto");

    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .out_dir(&out)
        .compile_protos(&["proto/wallet.proto"], &["proto"])?;

    Ok(())
}
