use std::process::Command;

#[test]
fn state_directory_is_explicit_and_help_is_available_without_it() {
    let bin = env!("CARGO_BIN_EXE_codex-wechat");
    let result = Command::new(bin).arg("status").output().unwrap();
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("--state-dir <directory> is required")
    );
    let help = Command::new(bin).arg("--help").output().unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("required; no default"));
}

#[test]
fn explicit_state_directory_before_or_after_subcommand_is_supported() {
    let bin = env!("CARGO_BIN_EXE_codex-wechat");
    let dir = tempfile::tempdir().unwrap();
    for args in [
        vec!["--state-dir", dir.path().to_str().unwrap(), "status"],
        vec!["status", "--state-dir", dir.path().to_str().unwrap()],
    ] {
        let result = Command::new(bin).args(args).output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stdout).contains("Not signed in"));
    }
}
