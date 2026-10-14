use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("manifest: {0}")]
    Manifest(String),
    #[error("bq read: {0}")]
    BqRead(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("write: {0}")]
    Write(String),
    #[error("storage: {0}")]
    Storage(String),
    /// An empty-input no-op that CAN happen silently WILL. Raised when a shard reads zero rows without `expect_empty`.
    #[error("empty shard: {0}")]
    EmptyShard(String),
    #[error("bigtable: {0}")]
    Bigtable(String),
    /// A Bigtable RPC that failed with a gRPC status, or a transport /
    /// deadline failure recorded as one. `code` is the wire-level
    /// google.rpc.Code (the same representation `EntryStatus::Failed.code`
    /// uses). The client reports the code; retry policy lives in retry.rs.
    #[error("bigtable rpc {code}: {message}")]
    BigtableRpc { code: i32, message: String },
}

pub type Result<T> = std::result::Result<T, Error>;
