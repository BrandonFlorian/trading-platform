use std::env;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let script_name = env::args().nth(1).expect("Expected script name");

    let metadata = cargo_metadata::MetadataCommand::new().exec()?;

    let workspace = metadata.workspace_metadata;
    let scripts = workspace
        .get("scripts")
        .and_then(|s| s.as_object())
        .expect("No scripts found in workspace metadata");

    let script = scripts
        .get(&script_name)
        .and_then(|s| s.as_str())
        .expect("Script not found");

    // Windows-specific script handling
    let script = if cfg!(target_os = "windows") {
        script.replace("bash -c", "powershell").replace(
            "/dev/tcp",
            "Test-NetConnection -ComputerName localhost -Port",
        )
    } else {
        script.to_string()
    };

    let status = if cfg!(target_os = "windows") {
        Command::new("cmd").args(["/C", &script]).status()?
    } else {
        Command::new("sh").arg("-c").arg(&script).status()?
    };

    std::process::exit(status.code().unwrap_or(1));
}
