//! freighter prepare | plan | move | verify | validate
//!
//! Parses the command line (`cli.rs`), installs the TLS provider and the
//! logger, and dispatches into the library. Everything else is the
//! library's job. Exit codes: 0 ok; 1 it broke; 2 the user must fix
//! something (a refused manifest, a table that exists, a bad flag).

use std::io::Read;
use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::SystemTime;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

mod cli;

use freighter::bqjobs::BqJobs;
use freighter::error::Error;
use freighter::manifest::MoveManifest;
use freighter::source::SourceRecord;

/// Binary-only allocator swap (see Cargo.toml note on `mimalloc`).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn worker_threads() -> usize {
    // requests == limits pins the pod; match tokio to the quota
    // (oversubscribing threads inside a CFS quota buys throttling).
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// A command that failed: its exit code and the one line that reports it.
/// `main` prints it; nothing else does.
struct Fail(i32, String);

impl From<Error> for Fail {
    fn from(e: Error) -> Self {
        Fail(exit_code(&e), e.to_string())
    }
}

/// Exit 2: what the user fixes.
fn refused(message: impl Into<String>) -> Fail {
    Fail(2, message.into())
}

/// Splits "gs://bucket/a/b/manifest.json" into a store rooted at the
/// prefix and the object name. file:// maps to a LocalFileSystem root for
/// tests.
fn store_for(uri: &str) -> Result<(Arc<dyn ObjectStore>, Path), Fail> {
    if let Some(rest) = uri.strip_prefix("gs://") {
        let (bucket, key) = rest
            .split_once('/')
            .ok_or_else(|| refused("gs uri needs bucket/key"))?;
        let (prefix, name) = key.rsplit_once('/').unwrap_or(("", key));
        // from_env, NOT new(): new() ignores GOOGLE_APPLICATION_CREDENTIALS,
        // which may be the pod's only credential path.
        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| refused(format!("gcs store: {e}")))?;
        let store: Arc<dyn ObjectStore> = if prefix.is_empty() {
            Arc::new(store)
        } else {
            Arc::new(object_store::prefix::PrefixStore::new(
                store,
                Path::from(prefix),
            ))
        };
        Ok((store, Path::from(name)))
    } else if let Some(rest) = uri.strip_prefix("file://") {
        let dir = std::path::Path::new(rest)
            .parent()
            .ok_or_else(|| refused("file uri needs a parent dir"))?;
        let name = std::path::Path::new(rest)
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| refused("file uri needs a file name"))?;
        let store = object_store::local::LocalFileSystem::new_with_prefix(dir)
            .map_err(|e| refused(format!("local store: {e}")))?;
        Ok((Arc::new(store), Path::from(name)))
    } else {
        Err(refused(format!(
            "manifest uri {uri:?} must be gs:// or file://"
        )))
    }
}

/// 2 for what the user fixes, 1 for what broke. `verify` overrides this:
/// any error there is "the check could not run", 2.
fn exit_code(e: &Error) -> i32 {
    match e {
        Error::Refused(_) | Error::Manifest(_) | Error::Source(_) => 2,
        _ => 1,
    }
}

/// The manifest document from a URI, through the store rooted at its
/// directory (verdicts land and are read there).
///
/// Every failure here is 2: a manifest that cannot be fetched is the
/// caller's to fix, and a Job's restart policy should not retry it.
async fn fetch_manifest(uri: &str) -> Result<(Arc<dyn ObjectStore>, MoveManifest), Fail> {
    let (store, name) = store_for(uri)?;
    let doc = store
        .get(&name)
        .await
        .map_err(|e| refused(format!("fetch manifest {uri}: {e}")))?
        .bytes()
        .await
        .map_err(|e| refused(format!("read manifest {uri}: {e}")))?;
    let text = std::str::from_utf8(&doc).map_err(|e| refused(e.to_string()))?;
    let manifest = MoveManifest::from_json(text).map_err(|e| refused(e.to_string()))?;
    Ok((store, manifest))
}

/// `validate`'s document: stdin or a local file. No network, so gs:// is
/// refused with the usage line.
fn read_local(manifest: &str) -> Result<String, Fail> {
    if manifest == "-" {
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(|e| refused(format!("read stdin: {e}")))?;
        return Ok(s);
    }
    let Some(path) = manifest.strip_prefix("file://") else {
        return Err(refused(format!(
            "validate reads file:// or - (stdin), got {manifest:?}"
        )));
    };
    std::fs::read_to_string(path).map_err(|e| refused(format!("read {path}: {e}")))
}

/// Exit 1 on failure; `verify` makes it 2 with the rest of its failures.
fn write_out(out: Option<&FsPath>, text: &str) -> Result<(), Fail> {
    match out {
        Some(path) => std::fs::write(path, format!("{text}\n"))
            .map_err(|e| Fail(1, format!("write {}: {e}", path.display()))),
        None => {
            println!("{text}");
            Ok(())
        }
    }
}

async fn run_prepare(args: cli::PrepareArgs) -> Result<i32, Fail> {
    let out = args.out.clone();
    let cfg = args.into_config()?;
    if let Some(out) = &out {
        if !out
            .parent()
            .is_none_or(|p| p.as_os_str().is_empty() || p.is_dir())
        {
            return Err(refused(format!(
                "--out {}: its directory does not exist",
                out.display()
            )));
        }
    }
    let jobs = BqJobs::connect().await?;
    let record = freighter::prepare::run(&jobs, &cfg).await?;
    let text = record.to_json().map_err(|e| Fail(1, e.to_string()))?;
    write_out(out.as_deref(), &text).inspect_err(|_| {
        // The record cost a full scan and a CTAS; a rerun refuses the built table, so never let a bad --out swallow it.
        println!("{text}");
    })?;
    Ok(0)
}

fn run_plan(args: cli::PlanArgs) -> Result<i32, Fail> {
    let source_path = args.source.clone();
    let out_dir = args.out_dir.clone();
    let cfg = args.into_config(SystemTime::now());
    let text = std::fs::read_to_string(&source_path)
        .map_err(|e| Error::Refused(format!("read {}: {e}", source_path.display())))?;
    let source = SourceRecord::from_json(&text)?;
    let manifests = freighter::plan::plan(&source, &cfg)?;
    for p in freighter::plan::write_manifests(&out_dir, &manifests)? {
        log::info!("wrote {}", p.display());
    }
    Ok(0)
}

async fn run_move(manifest_uri: &str) -> Result<i32, Fail> {
    let (store, manifest) = fetch_manifest(manifest_uri).await?;
    freighter::pipeline::run_move(manifest, store)
        .await
        .map_err(|e| Fail(1, format!("move failed: {e}")))?;
    Ok(0)
}

/// 0 when the verdicts match the source, 1 when they do not. Any failure
/// is 2, set by the dispatch in `main`.
async fn run_verify(args: cli::VerifyArgs) -> Result<i32, Fail> {
    let (store, manifest) = fetch_manifest(&args.manifest_uri).await?;
    let billing_project = args
        .billing_project
        .unwrap_or_else(|| manifest.billing_project.clone());
    let could_not_run = |e: Error| Fail(2, format!("verify could not run: {e}"));
    let jobs = BqJobs::connect().await.map_err(could_not_run)?;
    let report = freighter::verify::run(&manifest, store, &jobs, &billing_project)
        .await
        .map_err(could_not_run)?;
    let text =
        serde_json::to_string_pretty(&report).map_err(|e| Fail(2, format!("report json: {e}")))?;
    write_out(args.out.as_deref(), &text)?;
    Ok(if report.ok { 0 } else { 1 })
}

fn run_validate(manifest: &str) -> Result<i32, Fail> {
    MoveManifest::from_json(&read_local(manifest)?)?;
    Ok(0)
}

fn main() {
    let command = cli::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    // reqwest (object_store's and bqjobs') is built without a crypto
    // provider (Cargo.toml), so rustls needs a process default before the
    // first HTTPS request.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = rt.block_on(async {
        let result = match command {
            cli::Command::Prepare(args) => run_prepare(args).await,
            cli::Command::Plan(args) => run_plan(args),
            cli::Command::Move { manifest_uri } => run_move(&manifest_uri).await,
            cli::Command::Verify(args) => run_verify(args)
                .await
                .map_err(|Fail(_, message)| Fail(2, message)),
            cli::Command::Validate { manifest } => {
                // validate's refusal is its whole output: bare on stderr,
                // without the logger's prefix.
                return run_validate(&manifest).unwrap_or_else(|Fail(code, message)| {
                    eprintln!("{message}");
                    code
                });
            }
        };
        result.unwrap_or_else(|Fail(code, message)| {
            log::error!("{message}");
            code
        })
    });
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::store_for;

    #[test]
    fn a_gs_manifest_uri_builds_a_store() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (_, name) = store_for("gs://bucket/loads/1/manifest.json")
            .unwrap_or_else(|f| panic!("gs:// store: {}", f.1));
        assert_eq!(name.as_ref(), "manifest.json");
    }
}
