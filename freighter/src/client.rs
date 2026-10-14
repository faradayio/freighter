//! The Bigtable client wrapper — **the swap boundary**.
//!
//! This module is the ONLY file in `src/` that may name `bigtable_rs` or
//! Bigtable proto types (`BigTableConnection`, `BigTable`, `RowCell`,
//! `ReadRowsRequest`, `RowSet`, …). Everything it hands out is crate-local
//! (`RawRow` / `RawCell`), so replacing the client crate costs exactly this
//! file. See "Swap boundary" in `libs/rust/README.md`.
//!
//! The one deliberate exception is the `RowFilter` proto accepted by
//! [`BtClient::read_rows`]: callers (the emulator test harnesses' read-back
//! helpers) build those protos, so the filter type crosses the boundary by
//! design.

use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use bigtable_rs::bigtable::BigTableConnection;
use futures::StreamExt;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{
    DeleteFromRow, Mutation as MutationOneOf, SetCell,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    mutate_rows_request, MutateRowsRequest, Mutation, ReadRowsRequest, RowFilter, RowSet,
};

use crate::error::{Error, Result};

/// Per-RPC deadline handed to bigtable_rs (`Endpoint::timeout`): bounds the
/// response HEADERS only — tonic applies no `grpc-timeout` and a streaming
/// body can stall past it. `write_rows` therefore also wraps its whole call
/// in [`WRITE_RPC_TIMEOUT`].
const TIMEOUT: Duration = Duration::from_secs(60);

/// Whole-call bound on one MutateRows (headers AND body drain). A stalled
/// response body otherwise hangs the flush — and its permits — forever;
/// measured 2026-08-21 that BigQuery read streams stall ~13 min silently,
/// and the write side has the same class of hazard. Elapsed surfaces as
/// `BigtableRpc { code: GRPC_DEADLINE_EXCEEDED }`, which the mover retries.
const WRITE_RPC_TIMEOUT: Duration = Duration::from_secs(120);

/// Bound on one `BigTableConnection::new` dial (`connect()` and every
/// `rotate()` redial): the ADC walk inside it (`gcp_auth::provider()`) has
/// no timeout of its own, so a silent metadata server would otherwise hang
/// the dial forever — and inside the rotation ticker, that hang would stop
/// every future rotation too.
const DIAL_TIMEOUT: Duration = Duration::from_secs(60);

/// google.rpc.Code values the mapper names explicitly (the rest come off
/// the status as-is).
const GRPC_DEADLINE_EXCEEDED: i32 = 4;
const GRPC_UNAVAILABLE: i32 = 14;

/// Folds a bigtable_rs failure into the crate error, keeping the gRPC code
/// where there is one so the mover can classify it: statuses keep their
/// code; transport, timeout, and auth-side failures (a metadata-server blip
/// mid-run) are recorded as UNAVAILABLE / DEADLINE_EXCEEDED / UNAVAILABLE
/// so they are retryable; everything else stays a plain `Bigtable` error.
fn rpc_error(ctx: &str, e: bigtable_rs::bigtable::Error) -> Error {
    use bigtable_rs::bigtable::Error as Bt;
    match e {
        Bt::RpcError(st) => Error::BigtableRpc {
            code: st.code() as i32,
            message: format!("{ctx}: {st}"),
        },
        Bt::TimeoutError(_) => Error::BigtableRpc {
            code: GRPC_DEADLINE_EXCEEDED,
            message: format!("{ctx}: {e}"),
        },
        Bt::TransportError(_) | Bt::AccessTokenError(_) | Bt::GCPAuthError(_) => {
            Error::BigtableRpc {
                code: GRPC_UNAVAILABLE,
                message: format!("{ctx}: {e}"),
            }
        }
        other => Error::Bigtable(format!("{ctx}: {other}")),
    }
}

/// Runs `f` — one MutateRows call, including its full response-stream drain
/// — under [`WRITE_RPC_TIMEOUT`]. A stalled response body otherwise hangs
/// the flush, and its permits, forever; elapsed surfaces as
/// `BigtableRpc { code: GRPC_DEADLINE_EXCEEDED }`, which the mover retries.
async fn under_write_deadline<T>(
    ctx: &str,
    f: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(WRITE_RPC_TIMEOUT, f).await {
        Ok(r) => r,
        Err(_elapsed) => Err(Error::BigtableRpc {
            code: GRPC_DEADLINE_EXCEEDED,
            message: format!(
                "{ctx}: no response within {}s (whole-call bound)",
                WRITE_RPC_TIMEOUT.as_secs()
            ),
        }),
    }
}

/// One Bigtable cell, stripped of client-crate types. `value` is the raw
/// stored bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCell {
    pub family: String,
    pub qualifier: Vec<u8>,
    pub timestamp_micros: i64,
    pub value: Vec<u8>,
}

/// One Bigtable row: its key plus every cell the filter admitted, in the
/// order the server emitted them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRow {
    pub rowkey: Vec<u8>,
    pub cells: Vec<RawCell>,
}

/// A connected Bigtable data-plane client.
///
/// The connection sits behind a `RwLock` so [`BtClient::rotate`] can swap in
/// a fresh pool while requests in flight keep the old one alive through
/// their own `BigTable` clone (Google frontends close every connection at
/// ~60 min of age; rotating every 30 min means no request starts on one that
/// can reach that wall). The lock is never held across an await. Cloning
/// `BtClient` is cheap and every clone sees the swap.
#[derive(Clone)]
pub struct BtClient {
    connection: Arc<RwLock<BigTableConnection>>,
    project: String,
    instance: String,
    channel_size: usize,
    app_profile: String,
}

impl BtClient {
    /// Connects to `project`/`instance`, reading `app_profile` for every
    /// request.
    ///
    /// `channel_size` is the gRPC connection pool size. `bigtable_rs`
    /// recommends starting at 1 when load is low, which is what the emulator
    /// harness passes. Heavier writers (the mover) pass more.
    ///
    /// Honors `BIGTABLE_EMULATOR_HOST`: when it is set, `bigtable_rs` swaps
    /// to a plaintext channel against that endpoint and skips auth entirely.
    /// Otherwise it walks the ADC chain (`GOOGLE_APPLICATION_CREDENTIALS`,
    /// gcloud user creds, GKE metadata server / workload identity).
    pub async fn connect(
        project: &str,
        instance: &str,
        app_profile: &str,
        channel_size: usize,
    ) -> Result<Self> {
        let connection = Self::dial(project, instance, channel_size).await?;
        Ok(Self {
            connection: Arc::new(RwLock::new(connection)),
            project: project.to_owned(),
            instance: instance.to_owned(),
            channel_size,
            app_profile: app_profile.to_owned(),
        })
    }

    async fn dial(
        project: &str,
        instance: &str,
        channel_size: usize,
    ) -> Result<BigTableConnection> {
        match tokio::time::timeout(
            DIAL_TIMEOUT,
            BigTableConnection::new(project, instance, false, channel_size, Some(TIMEOUT)),
        )
        .await
        {
            Ok(r) => r.map_err(|e| {
                Error::Bigtable(format!("connect to bigtable {project}/{instance}: {e}"))
            }),
            Err(_elapsed) => Err(Error::Bigtable(format!(
                "connect to bigtable {project}/{instance}: no connection within {}s",
                DIAL_TIMEOUT.as_secs()
            ))),
        }
    }

    /// Builds a fresh connection pool exactly as `connect` did and swaps it
    /// in. On failure the old pool stays in place and the error is returned
    /// for the caller to log — rotation never takes the client down, and the
    /// reactive retries remain the backstop until the next tick.
    pub async fn rotate(&self) -> Result<()> {
        let fresh = Self::dial(&self.project, &self.instance, self.channel_size).await?;
        *self
            .connection
            .write()
            .unwrap_or_else(PoisonError::into_inner) = fresh;
        Ok(())
    }

    /// The current connection, cloned under a read lock that is released
    /// before any await.
    fn connection(&self) -> BigTableConnection {
        self.connection
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Reads `rowkeys` from `table` through `filter`.
    ///
    /// TWO PROPERTIES CALLERS MUST NOT ASSUME AWAY:
    ///
    /// 1. **Absent rowkeys are omitted.** A key with no row — or whose row
    ///    the filter emptied — simply does not appear in the result. The
    ///    result is NOT parallel to `rowkeys` and may be shorter.
    /// 2. **Result order is Bigtable's, not the caller's.** Rows come back in
    ///    row-key lexicographic order, NOT the order `rowkeys` was given in.
    ///
    /// This issues a single `ReadRows` RPC; splitting a large key list into
    /// batches is the caller's job.
    ///
    /// Rows are handed to `on_row` ONE AT A TIME as the stream delivers them
    /// and never collected here. A materialized chunk costs ~190 bytes per
    /// cell (three allocations per cell plus the row vectors), and the
    /// in-flight set is `concurrency x chunk_size` keys: measured 2026-08-22,
    /// a 250k-person extract held ~9 GB resident on exactly that shape,
    /// 100% of it raw cells the decoder was about to discard. The caller
    /// reduces each row to what it keeps and lets the raw one drop.
    pub async fn read_rows(
        &self,
        table: &str,
        rowkeys: &[Vec<u8>],
        filter: RowFilter,
        mut on_row: impl FnMut(RawRow) -> Result<()>,
    ) -> Result<()> {
        let mut client = self.connection().client();
        let table_name = client.get_full_table_name(table);
        let request = ReadRowsRequest {
            table_name,
            app_profile_id: self.app_profile.clone(),
            rows: Some(RowSet {
                row_keys: rowkeys.to_vec(),
                row_ranges: vec![],
            }),
            filter: Some(filter),
            // 0 means "no limit": the RowSet already bounds the scan.
            rows_limit: 0,
            ..Default::default()
        };
        let stream = client
            .stream_rows(request)
            .await
            .map_err(|e| Error::Bigtable(format!("read_rows on table {table:?}: {e}")))?;
        futures::pin_mut!(stream);

        while let Some(row) = stream.next().await {
            let (rowkey, cells) =
                row.map_err(|e| Error::Bigtable(format!("read_rows on table {table:?}: {e}")))?;
            on_row(RawRow {
                rowkey,
                cells: cells
                    .into_iter()
                    .map(|c| RawCell {
                        family: c.family_name,
                        qualifier: c.qualifier,
                        timestamp_micros: c.timestamp_micros,
                        value: c.value,
                    })
                    .collect(),
            })?;
        }
        Ok(())
    }
}

/// One cell of a batched write. Crate-local, parallel to `RawCell`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellWrite {
    pub family: String,
    pub qualifier: Vec<u8>,
    pub timestamp_micros: i64,
    pub value: Vec<u8>,
}

/// One row of a batched write, parallel to `RawRow`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowWrite {
    pub rowkey: Vec<u8>,
    pub change: RowChange,
}

/// What a write does to its row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowChange {
    /// Sets these cells; the row's other cells stay.
    Set(Vec<CellWrite>),
    /// Removes the row: every cell, in every family, at every timestamp.
    Delete,
}

impl RowChange {
    /// The Bigtable mutations this change costs against the per-request
    /// limit.
    pub fn mutations(&self) -> usize {
        match self {
            RowChange::Set(cells) => cells.len(),
            RowChange::Delete => 1,
        }
    }

    fn into_mutations(self) -> Vec<Mutation> {
        match self {
            RowChange::Set(cells) => cells
                .into_iter()
                .map(|c| Mutation {
                    mutation: Some(MutationOneOf::SetCell(SetCell {
                        family_name: c.family,
                        column_qualifier: c.qualifier,
                        timestamp_micros: c.timestamp_micros,
                        value: c.value,
                    })),
                })
                .collect(),
            RowChange::Delete => vec![Mutation {
                mutation: Some(MutationOneOf::DeleteFromRow(DeleteFromRow {})),
            }],
        }
    }
}

/// Per-entry result of a `write_rows` call, index-parallel to the request.
///
/// `Failed { code: -1, .. }` is a LOCAL sentinel, not a gRPC code: it means
/// the server returned no status for the entry (stream ended early, or the
/// entry came back without one). Callers must treat it as retryable-unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryStatus {
    Ok,
    Failed { code: i32, message: String },
}

/// Bigtable's hard MutateRows limit.
const MAX_MUTATIONS_PER_REQUEST: usize = 100_000;

impl BtClient {
    /// One MutateRows RPC. Drains the whole response stream; per-entry
    /// status is the only success signal — an Ok RPC can carry failed
    /// entries, and a dropped stream measures gRPC buffering, not writes.
    /// Retry policy is the CALLER's: this method reports, it never retries.
    ///
    /// On a mid-stream `Err` the batch's outcome is UNKNOWN: some entries may
    /// have applied. The caller must treat the whole batch as unwritten and
    /// may retry everything — SetCell at explicit timestamps and DeleteFromRow
    /// are both idempotent.
    ///
    /// Timestamps must be whole milliseconds (delivery-date micros are).
    pub async fn write_rows(&self, table: &str, rows: Vec<RowWrite>) -> Result<Vec<EntryStatus>> {
        // Real Bigtable rejects a zero-entry MutateRows with INVALID_ARGUMENT;
        // an empty batch is trivially all-written.
        if rows.is_empty() {
            return Ok(vec![]);
        }
        let total: usize = rows.iter().map(|r| r.change.mutations()).sum();
        if total > MAX_MUTATIONS_PER_REQUEST {
            return Err(Error::Bigtable(format!(
                "write_rows: {total} mutations exceeds Bigtable's {MAX_MUTATIONS_PER_REQUEST} per-request limit — flush smaller batches"
            )));
        }
        let n_entries = rows.len();
        let entries: Vec<mutate_rows_request::Entry> = rows
            .into_iter()
            .map(|r| mutate_rows_request::Entry {
                row_key: r.rowkey,
                mutations: r.change.into_mutations(),
                // Guards double-apply only on aggregate families; these
                // tables have none. Required struct field in proto 0.39.
                idempotency: None,
            })
            .collect();

        let ctx = format!("mutate_rows on table {table:?}");
        let client = self.connection();
        let app_profile = self.app_profile.clone();
        let rpc = {
            let ctx = ctx.clone();
            async move {
                let mut client = client.client();
                let table_name = client.get_full_table_name(table);
                let stream = client
                    .mutate_rows(MutateRowsRequest {
                        table_name,
                        app_profile_id: app_profile,
                        entries,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| rpc_error(&ctx, e))?;
                futures::pin_mut!(stream);

                // Missing entries default to Failed, never Ok: a response
                // stream that ends early must not read as success.
                let mut statuses = vec![
                    EntryStatus::Failed {
                        code: -1,
                        message: "no status returned for entry".to_owned()
                    };
                    n_entries
                ];
                while let Some(msg) = stream.next().await {
                    // The stream item is already a `tonic::Status` (this is
                    // the one place raw `Streaming<T>` — not a bigtable_rs
                    // wrapper — crosses the boundary); re-wrapping it as
                    // `Error::RpcError` keeps `tonic` itself out of this
                    // file's production code and lets it share `rpc_error`'s
                    // code-preserving mapping.
                    let resp = msg.map_err(|e| {
                        rpc_error(
                            &format!("{ctx} (stream)"),
                            bigtable_rs::bigtable::Error::RpcError(e),
                        )
                    })?;
                    for entry in resp.entries {
                        let idx = entry.index as usize;
                        if idx >= n_entries {
                            return Err(Error::Bigtable(format!(
                                "mutate_rows returned entry index {idx} for a {n_entries}-entry request"
                            )));
                        }
                        statuses[idx] = match entry.status {
                            Some(st) if st.code == 0 => EntryStatus::Ok,
                            Some(st) => EntryStatus::Failed {
                                code: st.code,
                                message: st.message,
                            },
                            None => EntryStatus::Failed {
                                code: -1,
                                message: "entry missing status".to_owned(),
                            },
                        };
                    }
                }
                Ok(statuses)
            }
        };
        under_write_deadline(&ctx, rpc).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn rpc_error_keeps_the_grpc_code() {
        let st = tonic::Status::unavailable("frontend going away");
        match rpc_error("mutate_rows", bigtable_rs::bigtable::Error::RpcError(st)) {
            Error::BigtableRpc { code, message } => {
                assert_eq!(code, GRPC_UNAVAILABLE);
                assert!(message.contains("mutate_rows"), "{message}");
            }
            other => panic!("expected BigtableRpc, got {other:?}"),
        }
    }

    #[test]
    fn rpc_error_maps_timeout_and_auth_to_codes() {
        match rpc_error("x", bigtable_rs::bigtable::Error::TimeoutError(60)) {
            Error::BigtableRpc { code, .. } => assert_eq!(code, GRPC_DEADLINE_EXCEEDED),
            other => panic!("{other:?}"),
        }
        // `TransportError` shares this arm too, but it wraps
        // `tonic::transport::Error`, which has no public constructor — it
        // can only be produced by a real failed connection attempt, so it
        // isn't exercised directly here.
        match rpc_error(
            "x",
            bigtable_rs::bigtable::Error::AccessTokenError("boom".into()),
        ) {
            Error::BigtableRpc { code, .. } => assert_eq!(code, GRPC_UNAVAILABLE),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rpc_error_leaves_non_rpc_failures_as_plain_bigtable_errors() {
        match rpc_error("x", bigtable_rs::bigtable::Error::RowNotFound) {
            Error::Bigtable(m) => assert!(m.contains("Row not found"), "{m}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn write_deadline_elapses_into_a_retryable_deadline_exceeded() {
        match under_write_deadline(
            "mutate_rows on table \"t\"",
            std::future::pending::<Result<()>>(),
        )
        .await
        {
            Err(Error::BigtableRpc { code, .. }) => assert_eq!(code, GRPC_DEADLINE_EXCEEDED),
            other => panic!("expected a deadline-exceeded BigtableRpc, got {other:?}"),
        }
    }
}
