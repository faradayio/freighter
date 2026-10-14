//! BigQuery Storage Read API — session + Arrow stream decode.
//!
//! This module is the BQ swap boundary: the only module in the workspace
//! that names BQ Storage Read / tonic client types. It hands out
//! `RecordBatch`es; everything downstream is BQ-agnostic.
//!
//! Reads the `(key, json)` source table, which is one BQ row per Bigtable
//! CELL, so rows/s read == cells/s writable. Arrow rather than Avro:
//! Arrow's columnar batches decode far cheaper per row.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use futures::StreamExt;
use googleapis_tonic_google_cloud_bigquery_storage_v1::google::cloud::bigquery::storage::v1 as bq;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

use crate::error::{Error, Result};
use crate::manifest::ShardLabel;
use crate::retry::{backoff, retry_rpc, retryable};

const ENDPOINT: &str = "https://bigquerystorage.googleapis.com";
const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// One tonic `Channel` is one HTTP/2 connection. All of a shard's read
/// streams multiplexed over a single TCP connection contend on
/// connection-level flow control and per-connection throughput, so streams
/// are spread round-robin over a small pool (~8 streams per connection at
/// read_streams=64).
const READ_CHANNELS: usize = 8;

/// A stream that delivers nothing for this long is stalled and is re-opened
/// at the row offset already consumed (`ReadRowsRequest.offset`, the API's
/// resume path — what Google's own retrying readers do). Measured
/// 2026-08-21: ReadRows streams silently stall ~13 min (800±60 s) on ~5% of
/// slices — connection healthy, keepalives answered, no error, then data
/// resumes — costing 17–55% of lane-time. A healthy stream delivers every
/// few seconds.
pub const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// CONSECUTIVE re-opens of one stream — re-opens with no rows delivered in
/// between — before the slice fails: a permanently dead stream costs
/// ≤ 4 × 90 s before the fence re-runs the slice. Delivered rows reset the
/// count, so a long stream survives any number of interruptions it recovers
/// from. `ReadStats` counts every re-open taken, separately.
pub const READ_REOPEN_LIMIT: u32 = 4;

/// Reader-side counters for one shard, shared by its reader tasks (one
/// `Arc`, a clone per task — the write side's `Counters` shape). Separate
/// from `Counters` so `drain_shard`'s ownership tripwire and the emulator
/// seam are untouched.
#[derive(Debug)]
pub struct ReadStats {
    pub label: ShardLabel,
    started: Instant,
    reopens_idle: AtomicU64,
    reopens_status: AtomicU64,
    rpc_retries: AtomicU64,
    /// Micros since `started` at the latest decoder exit (`fetch_max`).
    decode_done_micros: AtomicU64,
}

impl ReadStats {
    /// Starts the read clock: call at session open.
    pub fn start(label: ShardLabel) -> Self {
        Self {
            label,
            started: Instant::now(),
            reopens_idle: AtomicU64::new(0),
            reopens_status: AtomicU64::new(0),
            rpc_retries: AtomicU64::new(0),
            decode_done_micros: AtomicU64::new(0),
        }
    }
    /// A decoder finished: the read phase ends at the LAST of these.
    pub fn mark_decode_done(&self) {
        let now = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.decode_done_micros.fetch_max(now, Ordering::Relaxed);
    }
    pub fn add_rpc_retries(&self, n: u64) {
        self.rpc_retries.fetch_add(n, Ordering::Relaxed);
    }
    /// A stream was re-opened after an idle timeout — this is the counter
    /// [`Self::stalled_seconds`] bills.
    pub fn note_reopen_idle(&self) {
        self.reopens_idle.fetch_add(1, Ordering::Relaxed);
    }
    /// A stream was re-opened after a transport-class status (a failed open
    /// or a stream that died mid-flight).
    pub fn note_reopen_status(&self) {
        self.reopens_status.fetch_add(1, Ordering::Relaxed);
    }
    pub fn read_reopens(&self) -> u64 {
        self.reopens_idle.load(Ordering::Relaxed) + self.reopens_status.load(Ordering::Relaxed)
    }
    /// `reopens_idle × READ_IDLE_TIMEOUT`: the stall cost this shard paid.
    pub fn stalled_seconds(&self) -> f64 {
        self.reopens_idle.load(Ordering::Relaxed) as f64 * READ_IDLE_TIMEOUT.as_secs_f64()
    }
    pub fn rpc_retries(&self) -> u64 {
        self.rpc_retries.load(Ordering::Relaxed)
    }
    /// Session open → last decoder finished (0 until a decoder finishes).
    pub fn read_seconds(&self) -> f64 {
        self.decode_done_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
}

/// Why a `read_rows` response stream stopped yielding messages.
enum Stop {
    End,
    Idle,
    Status(tonic::Status),
}

pub struct Session {
    pub streams: Vec<String>,
    pub arrow_schema: Vec<u8>,
    pub estimated_rows: i64,
}

/// The BigQuery Storage Read client: a pool of `READ_CHANNELS` h2
/// connections, round-robined per request, behind a `RwLock` so
/// [`BqReader::rotate`] can swap a fresh pool in (Google frontends close
/// connections at ~60 min; rotating every 30 min keeps requests off that
/// wall). In-flight streams hold their own `Channel` clone and outlive the
/// swap. The lock is never held across an await.
#[derive(Clone)]
pub struct BqReader {
    channels: Arc<RwLock<Vec<Channel>>>,
    /// Round-robin cursor for [`Self::channel`]; shared across clones so a
    /// shard's readers spread over the pool no matter who spawned them.
    next: Arc<AtomicUsize>,
    provider: Arc<dyn gcp_auth::TokenProvider>,
}

async fn connect_channel() -> Result<Channel> {
    let tls = ClientTlsConfig::new()
        .with_native_roots()
        .domain_name("bigquerystorage.googleapis.com");
    Channel::from_static(ENDPOINT)
        .tls_config(tls)
        .map_err(|e| Error::BqRead(format!("tls config: {e}")))?
        .connect_timeout(Duration::from_secs(10))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_while_idle(true)
        // Grow HTTP/2 flow-control windows with observed bandwidth-delay
        // instead of pinning h2's small defaults — server-push streaming
        // reads are exactly the shape those defaults throttle.
        .http2_adaptive_window(true)
        .connect()
        .await
        .map_err(|e| Error::BqRead(format!("connect {ENDPOINT}: {e}")))
}

impl BqReader {
    pub async fn connect() -> Result<Self> {
        let channels = Self::dial_pool().await?;
        let provider = gcp_auth::provider()
            .await
            .map_err(|e| Error::BqRead(format!("gcp auth provider: {e}")))?;
        Ok(Self {
            channels: Arc::new(RwLock::new(channels)),
            next: Arc::new(AtomicUsize::new(0)),
            provider,
        })
    }

    async fn dial_pool() -> Result<Vec<Channel>> {
        futures::future::try_join_all((0..READ_CHANNELS).map(|_| connect_channel())).await
    }

    /// Dials a fresh pool and swaps it in. On failure the old pool stays
    /// and the error is returned for the caller to log — rotation never
    /// takes the reader down.
    pub async fn rotate(&self) -> Result<()> {
        let fresh = Self::dial_pool().await?;
        *self
            .channels
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = fresh;
        Ok(())
    }

    fn channel(&self) -> Channel {
        let pool = self
            .channels
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let i = self.next.fetch_add(1, Ordering::Relaxed) % pool.len();
        pool[i].clone()
    }

    /// Mints a bearer token per request (the provider caches and refreshes
    /// internally, so multi-hour runs keep working across token expiry),
    /// retrying a failed mint — a metadata-server blip must not fail a
    /// slice.
    async fn request<T>(&self, msg: T, stats: &ReadStats) -> Result<Request<T>> {
        let (token, retries) = retry_rpc(
            "gcp token",
            stats.label,
            |_| true,
            |_| {
                let provider = Arc::clone(&self.provider);
                async move { provider.token(&[SCOPE]).await }
            },
        )
        .await
        .map_err(|e| Error::BqRead(format!("gcp token: {e}")))?;
        stats.add_rpc_retries(retries);
        let mut req = Request::new(msg);
        let val = format!("Bearer {}", token.as_str())
            .parse()
            .map_err(|_| Error::BqRead("bad auth header".to_string()))?;
        req.metadata_mut().insert("authorization", val);
        Ok(req)
    }

    /// Opens a read session over `table` for the caller's column list and
    /// row restriction (built from the manifest and shard range). A range
    /// restriction prunes at the storage layer — the source is
    /// integer-range PARTITIONED on its shard column — where a `MOD()`-style
    /// predicate would read everything and only then discard.
    // Six of these are one flat description of the session and `stats` is
    // the counter sink; a struct would only move the same list one call up.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_session(
        &self,
        billing_project: &str,
        table: &str,
        selected_fields: Vec<String>,
        row_restriction: String,
        max_streams: i32,
        stats: &ReadStats,
    ) -> Result<Session> {
        // Arrow-native buffer compression (decompressed transparently by
        // arrow-ipc's StreamDecoder via its `lz4` feature) —
        // NOT the application-level response_compression_codec, which the
        // API forbids combining with it.
        let serialization =
            bq::read_session::table_read_options::OutputFormatSerializationOptions::ArrowSerializationOptions(
                bq::ArrowSerializationOptions {
                    buffer_compression:
                        bq::arrow_serialization_options::CompressionCodec::Lz4Frame as i32,
                    ..Default::default()
                },
            );
        let session = bq::ReadSession {
            table: table.to_owned(),
            data_format: bq::DataFormat::Arrow as i32,
            read_options: Some(bq::read_session::TableReadOptions {
                selected_fields,
                row_restriction,
                output_format_serialization_options: Some(serialization),
                ..Default::default()
            }),
            ..Default::default()
        };
        let msg = bq::CreateReadSessionRequest {
            parent: format!("projects/{billing_project}"),
            read_session: Some(session),
            max_stream_count: max_streams,
            // 0 lets the server pick; we only care about the upper bound.
            preferred_min_stream_count: 0,
        };
        let (resp, retries) = retry_rpc(
            "create_read_session",
            stats.label,
            |st: &tonic::Status| retryable(st.code()),
            |_attempt| {
                // A fresh client per attempt: the round-robin channel may be
                // the one that just went away.
                let mut client = bq::big_query_read_client::BigQueryReadClient::new(self.channel())
                    .max_decoding_message_size(64 * 1024 * 1024);
                let msg = msg.clone();
                async move {
                    let req = match self.request(msg, stats).await {
                        Ok(r) => r,
                        // FailedPrecondition, which retry.rs classes as an
                        // answer rather than a transient: the token mint
                        // inside request() has already exhausted its own
                        // retries, and a header parse error is deterministic.
                        // Retrying either here would only burn the budget.
                        Err(e) => return Err(tonic::Status::failed_precondition(e.to_string())),
                    };
                    client
                        .create_read_session(req)
                        .await
                        .map(|r| r.into_inner())
                }
            },
        )
        .await
        .map_err(|e| Error::BqRead(format!("create_read_session: {e}")))?;
        stats.add_rpc_retries(retries);

        let arrow_schema = match resp.schema {
            Some(bq::read_session::Schema::ArrowSchema(s)) => s.serialized_schema,
            _ => {
                return Err(Error::BqRead(
                    "session returned no arrow schema".to_string(),
                ))
            }
        };
        Ok(Session {
            streams: resp.streams.into_iter().map(|s| s.name).collect(),
            arrow_schema,
            estimated_rows: resp.estimated_row_count,
        })
    }

    /// Reads one stream, pushing each decoded batch into `tx`.
    ///
    /// Sending into a BOUNDED channel rather than invoking a callback is
    /// what makes the pipeline a pipeline: the downstream encode+write runs
    /// concurrently with the next read, and a slow writer applies
    /// backpressure instead of letting decoded batches pile up in memory.
    ///
    /// A stream that stalls past `READ_IDLE_TIMEOUT`, or dies with a
    /// transport-class status, is re-opened at `offset` = rows consumed so
    /// far (`ReadRowsResponse.row_count` is the unit). The offset advances
    /// only after a message's batches have all been handed downstream, so a
    /// re-open between messages neither drops nor repeats a row, and each
    /// (re)open primes a FRESH decoder with the session schema so a resumed
    /// stream can never inherit half a message. `READ_REOPEN_LIMIT` bounds
    /// the re-opens taken in a row without a row delivered; past it the
    /// slice fails and the fence re-runs it. A status re-open backs off by
    /// that same consecutive count.
    ///
    /// BigQuery answers ReadRows at offset == the stream's row count with an
    /// empty stream, so a stream that fails after its last message re-opens
    /// and ends cleanly; at any greater offset it answers FAILED_PRECONDITION
    /// (fatal here: not retryable).
    ///
    /// The reader stops the instant its downstream closes: a closed `tx`
    /// means the writer failed and its error is the one that surfaces, so
    /// the wait for each next message races `tx.closed()` instead of
    /// sitting out the idle timeout first.
    pub async fn read_stream(
        &self,
        stream: &str,
        arrow_schema: &[u8],
        tx: tokio::sync::mpsc::Sender<RecordBatch>,
        stats: &ReadStats,
    ) -> Result<()> {
        Self::read_resuming(
            |offset| async move {
                let mut client = bq::big_query_read_client::BigQueryReadClient::new(self.channel())
                    .max_decoding_message_size(64 * 1024 * 1024);
                let req = self
                    .request(
                        bq::ReadRowsRequest {
                            read_stream: stream.to_owned(),
                            offset,
                        },
                        stats,
                    )
                    .await?;
                Ok(client.read_rows(req).await.map(tonic::Response::into_inner))
            },
            arrow_schema,
            tx,
            stats,
        )
        .await
    }

    /// [`Self::read_stream`]'s resume loop over `open`, which opens the
    /// stream's rows at a row offset: the outer error is fatal, the inner
    /// status is a failed open.
    async fn read_resuming<S, F, Fut>(
        mut open: F,
        arrow_schema: &[u8],
        tx: tokio::sync::mpsc::Sender<RecordBatch>,
        stats: &ReadStats,
    ) -> Result<()>
    where
        S: futures::Stream<Item = std::result::Result<bq::ReadRowsResponse, tonic::Status>> + Unpin,
        F: FnMut(i64) -> Fut,
        Fut: std::future::Future<Output = Result<std::result::Result<S, tonic::Status>>>,
    {
        let label = stats.label;
        let mut offset: i64 = 0;
        let mut reopens = 0u32;
        let mut last_throttle = 0i32;
        loop {
            // A closed receiver means the writer failed; its error is the
            // one that surfaces. Do not re-open into a dead slice.
            if tx.is_closed() {
                return Ok(());
            }
            // A FRESH decoder per (re)open, primed with the session schema.
            // That is one schema parse per open — not per batch, which is
            // the shortcut that would show up directly in CPU per row — and
            // it buys enforcement rather than assumption: a re-open resumes
            // at a message boundary, so carrying the old decoder across
            // would be correct only while every `serialized_record_batch`
            // is a whole IPC message. One that ever arrived split now fails
            // as a decode error instead of silently mis-framing the rows
            // after the resume.
            let mut decoder = StreamDecoder::new();
            let mut schema_buf = Buffer::from_vec(arrow_schema.to_vec());
            decoder
                .decode(&mut schema_buf)
                .map_err(|e| Error::BqRead(format!("decode arrow schema: {e}")))?;

            // A failed open and a stream that dies mid-flight are the same
            // event to the machinery below: one `Stop::Status`, one bound,
            // one message.
            let stop = match open(offset).await? {
                Err(e) => Stop::Status(e),
                Ok(mut rows) => {
                    loop {
                        // Racing `tx.closed()` — biased, so it is polled
                        // first — is what stops this reader the instant its
                        // writer dies rather than one idle timeout later.
                        // Cancel-safe: if `closed` wins we abandon the
                        // stream outright, and the abandoned `rows.next()`
                        // had nothing we still need.
                        let next = tokio::select! {
                            biased;
                            () = tx.closed() => return Ok(()),
                            r = tokio::time::timeout(READ_IDLE_TIMEOUT, rows.next()) => r,
                        };
                        let msg = match next {
                            Err(_elapsed) => break Stop::Idle,
                            Ok(None) => break Stop::End,
                            Ok(Some(Err(status))) => break Stop::Status(status),
                            Ok(Some(Ok(msg))) => msg,
                        };
                        if let Some(t) = msg.throttle_state.as_ref() {
                            // On change only: throttle_state rides EVERY
                            // message, so logging the level itself would put
                            // a line in the log per message per stream.
                            if t.throttle_percent != last_throttle {
                                log::info!(
                                    "{label} read_rows throttle_percent {last_throttle} -> {}",
                                    t.throttle_percent
                                );
                                last_throttle = t.throttle_percent;
                            }
                        }
                        let row_count = msg.row_count;
                        // Rows we are told about but never handed would be
                        // skipped by the offset advance below, i.e. silently
                        // dropped. Nothing observed does this; say so loudly
                        // if it ever starts.
                        if msg.rows.is_none() && row_count > 0 {
                            log::warn!(
                                "{label} read_rows message carried row_count={row_count} but no rows payload"
                            );
                        }
                        if let Some(bq::read_rows_response::Rows::ArrowRecordBatch(rb)) = msg.rows {
                            let mut buf = Buffer::from_vec(rb.serialized_record_batch);
                            while !buf.is_empty() {
                                match decoder.decode(&mut buf).map_err(|e| {
                                    Error::BqRead(format!("decode arrow batch: {e}"))
                                })? {
                                    Some(batch) => {
                                        if tx.send(batch).await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                    None => break,
                                }
                            }
                        }
                        // Only now: every batch of this message is downstream.
                        offset += row_count;
                        if row_count > 0 {
                            reopens = 0;
                        }
                    }
                }
            };
            match stop {
                Stop::End => return Ok(()),
                Stop::Idle if reopens < READ_REOPEN_LIMIT => {
                    reopens += 1;
                    stats.note_reopen_idle();
                    log::warn!(
                        "{label} read_rows stream idle {}s at offset {offset}; re-open {reopens}/{READ_REOPEN_LIMIT}",
                        READ_IDLE_TIMEOUT.as_secs()
                    );
                }
                Stop::Idle => {
                    return Err(Error::BqRead(format!(
                    "read_rows stream stalled past {READ_REOPEN_LIMIT} re-opens at offset {offset}"
                )))
                }
                Stop::Status(e) if reopens < READ_REOPEN_LIMIT && retryable(e.code()) => {
                    reopens += 1;
                    stats.note_reopen_status();
                    log::warn!(
                        "{label} read_rows stream failed at offset {offset}; re-open {reopens}/{READ_REOPEN_LIMIT}: {e}"
                    );
                    tokio::time::sleep(backoff(reopens)).await;
                }
                Stop::Status(e) => {
                    return Err(Error::BqRead(format!("read_rows stream: {e}")));
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests; the workspace denies unwrap_used
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use arrow_array::{Array, StringArray};
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
    use arrow_ipc::CompressionType;
    use arrow_schema::{DataType, Field, Schema};

    type Item = std::result::Result<bq::ReadRowsResponse, tonic::Status>;
    type Rows = futures::stream::BoxStream<'static, Item>;

    /// A one-column `key` table as BigQuery serializes it: the schema
    /// message, and one LZ4_FRAME-compressed record-batch message per `msg`
    /// call.
    struct Wire {
        writer: StreamWriter<Vec<u8>>,
        schema: Arc<Schema>,
        schema_bytes: Vec<u8>,
    }

    impl Wire {
        fn new() -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .unwrap();
            let mut writer =
                StreamWriter::try_new_with_options(Vec::new(), &schema, options).unwrap();
            writer.flush().unwrap();
            let schema_bytes = writer.get_ref().clone();
            Self {
                writer,
                schema,
                schema_bytes,
            }
        }

        fn msg(&mut self, keys: &[&str]) -> Item {
            let before = self.writer.get_ref().len();
            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(StringArray::from(keys.to_vec()))],
            )
            .unwrap();
            self.writer.write(&batch).unwrap();
            self.writer.flush().unwrap();
            Ok(bq::ReadRowsResponse {
                row_count: keys.len() as i64,
                rows: Some(bq::read_rows_response::Rows::ArrowRecordBatch(
                    bq::ArrowRecordBatch {
                        serialized_record_batch: self.writer.get_ref()[before..].to_vec(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })
        }
    }

    fn ended(items: Vec<Item>) -> std::result::Result<Rows, tonic::Status> {
        Ok(futures::stream::iter(items).boxed())
    }

    fn stalled(items: Vec<Item>) -> std::result::Result<Rows, tonic::Status> {
        Ok(futures::stream::iter(items)
            .chain(futures::stream::pending())
            .boxed())
    }

    struct Run {
        result: Result<()>,
        offsets: Vec<i64>,
        keys: Vec<String>,
        stats: ReadStats,
    }

    /// Drives the resume loop over `opens`, one scripted answer per open in
    /// order, recording the offset each open asked for.
    async fn run(wire: &Wire, opens: Vec<std::result::Result<Rows, tonic::Status>>) -> Run {
        let opens = Mutex::new(VecDeque::from(opens));
        let offsets = Mutex::new(Vec::new());
        let stats = ReadStats::start(ShardLabel { shard: 0, lane: 0 });
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let result = BqReader::read_resuming(
            |offset| {
                offsets.lock().unwrap().push(offset);
                let answer = opens
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("the loop opened more streams than the script has");
                async move { Ok(answer) }
            },
            &wire.schema_bytes,
            tx,
            &stats,
        )
        .await;
        let mut keys = Vec::new();
        while let Some(batch) = rx.recv().await {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            keys.extend((0..col.len()).map(|i| col.value(i).to_owned()));
        }
        assert!(opens.lock().unwrap().is_empty(), "unused scripted opens");
        Run {
            result,
            offsets: offsets.into_inner().unwrap(),
            keys,
            stats,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_transient_failure_reopens_at_the_rows_already_delivered() {
        let mut w = Wire::new();
        let opens = vec![
            ended(vec![
                w.msg(&["a", "b"]),
                w.msg(&["c"]),
                Err(tonic::Status::unavailable("connection dropped")),
            ]),
            ended(vec![w.msg(&["d"])]),
        ];
        let run = run(&w, opens).await;
        run.result.unwrap();
        assert_eq!(run.offsets, vec![0, 3]);
        assert_eq!(run.keys, vec!["a", "b", "c", "d"]);
        assert_eq!(run.stats.read_reopens(), 1);
        assert_eq!(run.stats.stalled_seconds(), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_at_end_of_stream_reopens_at_the_total_and_ends_cleanly() {
        let mut w = Wire::new();
        let opens = vec![
            ended(vec![
                w.msg(&["a", "b"]),
                Err(tonic::Status::unavailable("connection dropped")),
            ]),
            ended(vec![]),
        ];
        let run = run(&w, opens).await;
        run.result.unwrap();
        assert_eq!(run.offsets, vec![0, 2]);
        assert_eq!(run.keys, vec!["a", "b"]);
        assert_eq!(run.stats.read_reopens(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_non_retryable_status_is_fatal_without_a_reopen() {
        let mut w = Wire::new();
        let opens = vec![ended(vec![
            w.msg(&["a"]),
            Err(tonic::Status::invalid_argument("bad stream name")),
        ])];
        let run = run(&w, opens).await;
        let err = run.result.unwrap_err();
        assert!(matches!(err, Error::BqRead(_)), "{err}");
        assert!(err.to_string().contains("bad stream name"), "{err}");
        assert_eq!(run.offsets, vec![0]);
        assert_eq!(run.keys, vec!["a"]);
        assert_eq!(run.stats.read_reopens(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn interruptions_that_each_follow_delivered_rows_never_exhaust_the_limit() {
        let mut w = Wire::new();
        let interruptions = READ_REOPEN_LIMIT + 3;
        let keys: Vec<String> = (0..=interruptions).map(|i| format!("k{i}")).collect();
        let mut opens = Vec::new();
        for key in &keys[..keys.len() - 1] {
            opens.push(ended(vec![
                w.msg(&[key.as_str()]),
                Err(tonic::Status::unavailable("connection dropped")),
            ]));
        }
        opens.push(ended(vec![w.msg(&[keys[keys.len() - 1].as_str()])]));
        let run = run(&w, opens).await;
        run.result.unwrap();
        assert_eq!(
            run.offsets,
            (0..=i64::from(interruptions)).collect::<Vec<_>>()
        );
        assert_eq!(run.keys, keys);
        assert_eq!(run.stats.read_reopens(), u64::from(interruptions));
    }

    #[tokio::test(start_paused = true)]
    async fn consecutive_failed_opens_past_the_limit_are_fatal() {
        let mut w = Wire::new();
        let mut opens = vec![ended(vec![
            w.msg(&["a"]),
            Err(tonic::Status::unavailable("connection dropped")),
        ])];
        for _ in 0..READ_REOPEN_LIMIT {
            opens.push(Err(tonic::Status::unavailable("open refused")));
        }
        let run = run(&w, opens).await;
        let err = run.result.unwrap_err();
        assert!(matches!(err, Error::BqRead(_)), "{err}");
        assert!(err.to_string().contains("open refused"), "{err}");
        assert_eq!(run.offsets.len() as u32, READ_REOPEN_LIMIT + 1);
        assert!(run.offsets[1..].iter().all(|&o| o == 1));
        assert_eq!(run.keys, vec!["a"]);
        assert_eq!(run.stats.read_reopens(), u64::from(READ_REOPEN_LIMIT));
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_idle_past_the_limit_fails_stalled_and_bills_the_reopens_taken() {
        let mut w = Wire::new();
        let mut opens = vec![stalled(vec![w.msg(&["a"])])];
        for _ in 0..READ_REOPEN_LIMIT {
            opens.push(stalled(vec![]));
        }
        let started = tokio::time::Instant::now();
        let run = run(&w, opens).await;
        let err = run.result.unwrap_err();
        assert!(err.to_string().contains("stalled"), "{err}");
        assert_eq!(
            started.elapsed(),
            READ_IDLE_TIMEOUT * (READ_REOPEN_LIMIT + 1)
        );
        assert_eq!(run.offsets.len() as u32, READ_REOPEN_LIMIT + 1);
        assert_eq!(run.keys, vec!["a"]);
        assert_eq!(run.stats.read_reopens(), u64::from(READ_REOPEN_LIMIT));
        assert_eq!(
            run.stats.stalled_seconds(),
            READ_IDLE_TIMEOUT.as_secs_f64() * f64::from(READ_REOPEN_LIMIT)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_stream_reopens_at_its_offset_and_bills_the_stall() {
        let mut w = Wire::new();
        let opens = vec![stalled(vec![w.msg(&["a"])]), ended(vec![w.msg(&["b"])])];
        let started = tokio::time::Instant::now();
        let run = run(&w, opens).await;
        run.result.unwrap();
        assert_eq!(started.elapsed(), READ_IDLE_TIMEOUT);
        assert_eq!(run.offsets, vec![0, 1]);
        assert_eq!(run.keys, vec!["a", "b"]);
        assert_eq!(run.stats.read_reopens(), 1);
        assert_eq!(run.stats.stalled_seconds(), READ_IDLE_TIMEOUT.as_secs_f64());
    }
}
