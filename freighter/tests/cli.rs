//! Exit codes and offline behaviour of the `freighter` binary. Every test
//! here runs the real executable; none needs credentials or a network.
#![allow(clippy::unwrap_used)]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

const MANIFEST_FIXTURE: &str = include_str!("../fixtures/freighter-lookup-manifest-example.json");
const SOURCE_FIXTURE: &str = include_str!("../fixtures/freighter-source-record-example.json");

fn freighter(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_freighter"))
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .expect("run freighter")
}

fn freighter_stdin(args: &[&str], stdin: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_freighter"))
        .args(args)
        .env("RUST_LOG", "error")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn freighter");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().expect("wait for freighter")
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freighter-cli-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn no_arguments_exits_2_and_help_exits_0() {
    // The exit code is a contract: an image smoke test runs the bare binary
    // and expects 2.
    assert_eq!(freighter(&[]).status.code(), Some(2));
    let help = freighter(&["--help"]);
    assert_eq!(help.status.code(), Some(0));
    let text = String::from_utf8_lossy(&help.stdout);
    for cmd in ["prepare", "plan", "move", "verify", "validate"] {
        assert!(text.contains(cmd), "help names {cmd}");
    }
    assert_eq!(freighter(&["--no-such-flag"]).status.code(), Some(2));
}

#[test]
fn validate_accepts_the_fixture_on_stdin_silently() {
    let out = freighter_stdin(&["validate", "-"], MANIFEST_FIXTURE);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert!(out.stdout.is_empty());
    assert!(out.stderr.is_empty());
}

#[test]
fn validate_refuses_a_bad_manifest_with_the_mover_s_message() {
    let bad = MANIFEST_FIXTURE.replace("\"flush_cells\": 8000", "\"flush_cells\": 95000");
    let out = freighter_stdin(&["validate", "-"], &bad);
    assert_eq!(out.status.code(), Some(2));
    assert!(
        stderr(&out).contains("manifest: flush_cells 95000 outside"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn validate_reads_a_file_uri_and_refuses_gs() {
    let dir = scratch_dir("validate");
    let path = dir.join("manifest.json");
    std::fs::write(&path, MANIFEST_FIXTURE).unwrap();
    let uri = format!("file://{}", path.display());
    assert_eq!(freighter(&["validate", &uri]).status.code(), Some(0));
    let out = freighter(&["validate", "gs://bucket/manifest.json"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("file://"), "{}", stderr(&out));
    let out = freighter(&["validate", "file:///no/such/manifest.json"]);
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn the_bare_uri_form_and_move_both_parse_and_fail_at_fetch() {
    // Both spellings reach the manifest fetch: the directory exists but the
    // manifest does not, so the run fails at `store.get` and exits 2 as the
    // bare form always has. A clap usage error would also exit 2 but would not say
    // "fetch manifest".
    let dir = scratch_dir("bare");
    let uri = format!("file://{}", dir.join("manifest.json").display());
    for args in [vec![uri.as_str()], vec!["move", uri.as_str()]] {
        let out = freighter(&args);
        assert_eq!(out.status.code(), Some(2), "{}", stderr(&out));
        assert!(stderr(&out).contains("fetch manifest"), "{}", stderr(&out));
    }
}

#[test]
fn prepare_needs_exactly_one_query_source() {
    let base = ["prepare", "--table", "p.d.t"];
    let neither = freighter(&base);
    assert_eq!(neither.status.code(), Some(2));
    let both = freighter(&[
        "prepare",
        "--table",
        "p.d.t",
        "--query",
        "SELECT 1",
        "--query-file",
        "q.sql",
    ]);
    assert_eq!(both.status.code(), Some(2));
    let bad_table = freighter(&["prepare", "--table", "not-a-table", "--query", "SELECT 1"]);
    assert_eq!(bad_table.status.code(), Some(2));
    assert!(
        stderr(&bad_table).contains("project.dataset.table"),
        "{}",
        stderr(&bad_table)
    );
}

#[test]
fn plan_runs_offline_and_its_output_validates() {
    let dir = scratch_dir("plan");
    let source = dir.join("source.json");
    std::fs::write(&source, SOURCE_FIXTURE).unwrap();
    let out_dir = dir.join("out");
    let common = [
        "plan",
        "--source",
        source.to_str().unwrap(),
        "--lanes",
        "1",
        "--memory",
        "2Gi",
        "--bt-project",
        "example-project",
        "--bt-instance",
        "example-instance",
        "--destination-table",
        "example_table",
        "--app-profile",
        "bulk-write",
        "--cell-timestamp-micros",
        "1789776000123000",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ];
    let out = freighter(&common);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let manifest = out_dir.join("manifest.json");
    let uri = format!("file://{}", manifest.display());
    assert_eq!(freighter(&["validate", &uri]).status.code(), Some(0));
    let text = std::fs::read_to_string(&manifest).unwrap();
    assert!(text.contains("\"version\": 4"));
    assert!(text.contains("\"cell_timestamp_micros\": 1789776000123000"));

    // A second plan into the same directory is refused: the manifest's
    // directory is where its verdicts land.
    let again = freighter(&common);
    assert_eq!(again.status.code(), Some(2));
    assert!(
        stderr(&again).contains("already exists"),
        "{}",
        stderr(&again)
    );

    let ramp_dir = dir.join("ramp");
    let mut ramped = common.to_vec();
    let last = ramped.len() - 1;
    ramped[last] = ramp_dir.to_str().unwrap();
    ramped.extend(["--ramp", "0.05,0.10,0.85"]);
    let out = freighter(&ramped);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    for step in 0..3 {
        let m = ramp_dir.join(format!("step-{step}/manifest.json"));
        let uri = format!("file://{}", m.display());
        assert_eq!(
            freighter(&["validate", &uri]).status.code(),
            Some(0),
            "step {step}"
        );
    }

    let mut bad = common.to_vec();
    bad[6] = "24G";
    let out = freighter(&bad);
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("Gi or Mi"), "{}", stderr(&out));

    let mut secs = common.to_vec();
    let at = secs.iter().position(|a| *a == "1789776000123000").unwrap();
    secs[at] = "1792003349";
    let out = freighter(&secs);
    assert_eq!(out.status.code(), Some(2));
    assert!(
        stderr(&out).contains("not microseconds"),
        "{}",
        stderr(&out)
    );

    let mut zero = common.to_vec();
    zero.extend(["--read-streams", "0"]);
    assert_eq!(freighter(&zero).status.code(), Some(2));
}
