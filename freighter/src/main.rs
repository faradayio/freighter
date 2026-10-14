//! freighter <manifest-uri>
//!
//! Fetches the MoveManifest JSON from the URI (gs:// or file://), runs the
//! move, exits non-zero on error. Everything else is the library's job.

use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

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

/// Splits "gs://bucket/a/b/manifest.json" into a store rooted at the
/// prefix and the object name. file:// maps to a LocalFileSystem root for
/// tests.
fn store_for(uri: &str) -> Result<(Arc<dyn ObjectStore>, Path), String> {
    if let Some(rest) = uri.strip_prefix("gs://") {
        let (bucket, key) = rest.split_once('/').ok_or("gs uri needs bucket/key")?;
        let (prefix, name) = key.rsplit_once('/').unwrap_or(("", key));
        // from_env, NOT new(): new() ignores GOOGLE_APPLICATION_CREDENTIALS,
        // which may be the pod's only credential path.
        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| format!("gcs store: {e}"))?;
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
            .ok_or("file uri needs a parent dir")?;
        let name = std::path::Path::new(rest)
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or("file uri needs a file name")?;
        let store = object_store::local::LocalFileSystem::new_with_prefix(dir)
            .map_err(|e| format!("local store: {e}"))?;
        Ok((Arc::new(store), Path::from(name)))
    } else {
        Err(format!("manifest uri {uri:?} must be gs:// or file://"))
    }
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let uri = match std::env::args().nth(1) {
        Some(u) => u,
        None => {
            eprintln!("usage: freighter <manifest-uri>");
            std::process::exit(2);
        }
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = rt.block_on(async {
        let (store, name) = match store_for(&uri) {
            Ok(x) => x,
            Err(e) => {
                log::error!("{e}");
                return 2;
            }
        };
        let doc = match store.get(&name).await {
            Ok(r) => match r.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    log::error!("read manifest {uri}: {e}");
                    return 2;
                }
            },
            Err(e) => {
                log::error!("fetch manifest {uri}: {e}");
                return 2;
            }
        };
        let manifest = match std::str::from_utf8(&doc)
            .map_err(|e| e.to_string())
            .and_then(|s| {
                freighter::manifest::MoveManifest::from_json(s).map_err(|e| e.to_string())
            }) {
            Ok(m) => m,
            Err(e) => {
                log::error!("manifest: {e}");
                return 2;
            }
        };
        match freighter::pipeline::run_move(manifest, store).await {
            Ok(()) => 0,
            Err(e) => {
                log::error!("move failed: {e}");
                1
            }
        }
    });
    std::process::exit(code);
}
