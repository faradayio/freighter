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
    /// A BigQuery jobs-API call that failed for a reason other than a status:
    /// transport, token, or a result freighter could not use.
    #[error("bq job: {0}")]
    BqJob(String),
    /// A BigQuery REST call answered with an error status: the HTTP code,
    /// BigQuery's `reason` (`notFound`, `invalidQuery`, `rateLimitExceeded`, …)
    /// and its message. Kept structured so callers classify by `reason`,
    /// never by parsing the text.
    #[error("bq job: {code}{}: {message}", reason.as_deref().map(|r| format!(" {r}")).unwrap_or_default())]
    BqStatus {
        code: u16,
        reason: Option<String>,
        message: String,
    },
    /// A source record (the `prepare` -> `plan` contract) that fails to
    /// parse or validate.
    #[error("source record: {0}")]
    Source(String),
    /// A precondition the user must fix before anything runs: the table
    /// already exists, the query yields no rows, a flag is out of range.
    /// `main` maps it to exit 2. Displayed bare — the message is the whole
    /// line.
    #[error("{0}")]
    Refused(String),
    /// `plan` could not render a manifest, or rendered one the mover's
    /// validator refuses — a bug in `plan`, exit 1.
    #[error("plan: {0}")]
    Plan(String),
}

pub type Result<T> = std::result::Result<T, Error>;
