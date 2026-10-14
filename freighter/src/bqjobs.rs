//! BigQuery jobs API over REST: run a statement, poll and page its result,
//! read a table's metadata. This module is the jobs-API swap boundary — the
//! only module naming `reqwest` or the REST endpoints. `prepare` and
//! `verify` use it; the mover never does (it only speaks Storage Read).
//!
//! Three endpoints of `bigquery/v2`: `POST projects/{p}/jobs/query`,
//! `GET projects/{p}/queries/{jobId}` (poll + page), and
//! `GET projects/{p}/datasets/{d}/tables/{t}`.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::retry::{retry_rpc, retryable_http};
use crate::source::TableRef;

const BASE: &str = "https://bigquery.googleapis.com/bigquery/v2";
const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
/// How long one `jobs.query` / `getQueryResults` call waits server-side
/// before answering "not done yet". The poll loop has no sleep of its own:
/// the server holds each request for this long.
const WAIT_MS: u64 = 10_000;
/// Longer than WAIT_MS, so a long-poll never trips the client timeout.
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub field_type: String,
}

/// One result cell. BigQuery's JSON API renders every scalar as a string
/// (an INT64 arrives as "123"); arrays nest `{"v": …}` wrappers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    Null,
    Str(String),
    Array(Vec<Cell>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row(pub Vec<Cell>);

impl Row {
    fn cell(&self, i: usize) -> Result<&Cell> {
        self.0
            .get(i)
            .ok_or_else(|| Error::BqJob(format!("result row has no column {i}")))
    }
    pub fn opt_str(&self, i: usize) -> Result<Option<&str>> {
        match self.cell(i)? {
            Cell::Null => Ok(None),
            Cell::Str(s) => Ok(Some(s)),
            Cell::Array(_) => Err(Error::BqJob(format!(
                "column {i} is an array, expected a scalar"
            ))),
        }
    }
    pub fn str(&self, i: usize) -> Result<&str> {
        self.opt_str(i)?
            .ok_or_else(|| Error::BqJob(format!("column {i} is NULL, expected a value")))
    }
    pub fn i64(&self, i: usize) -> Result<i64> {
        let s = self.str(i)?;
        s.parse()
            .map_err(|_| Error::BqJob(format!("column {i} value {s:?} is not an integer")))
    }
    pub fn strs(&self, i: usize) -> Result<Vec<&str>> {
        match self.cell(i)? {
            Cell::Array(items) => items
                .iter()
                .map(|c| match c {
                    Cell::Str(s) => Ok(s.as_str()),
                    _ => Err(Error::BqJob(format!("column {i} array holds a non-string"))),
                })
                .collect(),
            Cell::Null => Ok(Vec::new()),
            Cell::Str(_) => Err(Error::BqJob(format!(
                "column {i} is a scalar, expected an array"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub schema: Vec<Field>,
    pub rows: Vec<Row>,
    pub total_bytes_processed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePartitioning {
    pub field: String,
    pub start: i64,
    pub end: i64,
    pub interval: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub num_rows: u64,
    pub num_bytes: u64,
    pub schema: Vec<Field>,
    pub range_partitioning: Option<RangePartitioning>,
    pub clustering: Vec<String>,
    pub expiration_time_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRef {
    pub project_id: String,
    pub job_id: String,
    pub location: Option<String>,
}

/// One decoded response page of `jobs.query` / `getQueryResults`.
#[derive(Debug)]
pub struct Page {
    pub job_complete: bool,
    pub job: Option<JobRef>,
    pub schema: Vec<Field>,
    pub rows: Vec<Row>,
    pub page_token: Option<String>,
    pub total_bytes_processed: Option<u64>,
}

// ---- wire types: private serde mirrors of the REST JSON ---------------------

#[derive(Deserialize)]
struct WireField {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
}
#[derive(Deserialize)]
struct WireSchema {
    #[serde(default)]
    fields: Vec<WireField>,
}
#[derive(Deserialize)]
struct WireValue {
    v: serde_json::Value,
}
#[derive(Deserialize)]
struct WireRow {
    f: Vec<WireValue>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireJobReference {
    project_id: String,
    job_id: String,
    location: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireQueryResponse {
    #[serde(default)]
    job_complete: bool,
    job_reference: Option<WireJobReference>,
    schema: Option<WireSchema>,
    #[serde(default)]
    rows: Vec<WireRow>,
    page_token: Option<String>,
    total_bytes_processed: Option<String>,
}
#[derive(Deserialize)]
struct WireRange {
    start: String,
    end: String,
    interval: String,
}
#[derive(Deserialize)]
struct WireRangePartitioning {
    field: String,
    range: WireRange,
}
#[derive(Deserialize)]
struct WireClustering {
    #[serde(default)]
    fields: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireTable {
    num_rows: Option<String>,
    num_bytes: Option<String>,
    schema: Option<WireSchema>,
    range_partitioning: Option<WireRangePartitioning>,
    clustering: Option<WireClustering>,
    expiration_time: Option<String>,
}
#[derive(Deserialize)]
struct WireErrorItem {
    reason: Option<String>,
}
#[derive(Deserialize)]
struct WireError {
    message: Option<String>,
    #[serde(default)]
    errors: Vec<WireErrorItem>,
}
#[derive(Deserialize)]
struct WireErrorBody {
    error: WireError,
}

fn decode_cell(v: serde_json::Value) -> Result<Cell> {
    match v {
        serde_json::Value::Null => Ok(Cell::Null),
        serde_json::Value::String(s) => Ok(Cell::Str(s)),
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|item| {
                let wrapped: WireValue = serde_json::from_value(item)
                    .map_err(|e| Error::BqJob(format!("array element: {e}")))?;
                decode_cell(wrapped.v)
            })
            .collect::<Result<Vec<_>>>()
            .map(Cell::Array),
        serde_json::Value::Object(_) => Err(Error::BqJob(
            "a STRUCT value in a result row; freighter's queries return scalars and arrays only"
                .to_owned(),
        )),
        other => Ok(Cell::Str(other.to_string())),
    }
}

fn parse_u64(name: &str, s: Option<String>) -> Result<Option<u64>> {
    s.map(|s| {
        s.parse()
            .map_err(|_| Error::BqJob(format!("{name} {s:?} is not an integer")))
    })
    .transpose()
}

fn parse_i64(name: &str, s: &str) -> Result<i64> {
    s.parse()
        .map_err(|_| Error::BqJob(format!("{name} {s:?} is not an integer")))
}

fn fields(schema: Option<WireSchema>) -> Vec<Field> {
    schema
        .map(|s| {
            s.fields
                .into_iter()
                .map(|f| Field {
                    name: f.name,
                    field_type: f.field_type,
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn decode_query_page(body: &[u8]) -> Result<Page> {
    let w: WireQueryResponse =
        serde_json::from_slice(body).map_err(|e| Error::BqJob(format!("query response: {e}")))?;
    let rows = w
        .rows
        .into_iter()
        .map(|r| {
            r.f.into_iter()
                .map(|v| decode_cell(v.v))
                .collect::<Result<Vec<_>>>()
                .map(Row)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Page {
        job_complete: w.job_complete,
        job: w.job_reference.map(|j| JobRef {
            project_id: j.project_id,
            job_id: j.job_id,
            location: j.location,
        }),
        schema: fields(w.schema),
        rows,
        page_token: w.page_token.filter(|t| !t.is_empty()),
        total_bytes_processed: parse_u64("totalBytesProcessed", w.total_bytes_processed)?,
    })
}

pub fn decode_table(body: &[u8]) -> Result<TableInfo> {
    let w: WireTable =
        serde_json::from_slice(body).map_err(|e| Error::BqJob(format!("table: {e}")))?;
    let range_partitioning = w
        .range_partitioning
        .map(|rp| {
            Ok::<_, Error>(RangePartitioning {
                field: rp.field,
                start: parse_i64("range.start", &rp.range.start)?,
                end: parse_i64("range.end", &rp.range.end)?,
                interval: parse_i64("range.interval", &rp.range.interval)?,
            })
        })
        .transpose()?;
    Ok(TableInfo {
        num_rows: parse_u64("numRows", w.num_rows)?.unwrap_or(0),
        num_bytes: parse_u64("numBytes", w.num_bytes)?.unwrap_or(0),
        schema: fields(w.schema),
        range_partitioning,
        clustering: w.clustering.map(|c| c.fields).unwrap_or_default(),
        expiration_time_ms: w
            .expiration_time
            .as_deref()
            .map(|s| parse_i64("expirationTime", s))
            .transpose()?,
    })
}

/// A failed call, before retry policy is applied.
#[derive(Debug)]
pub enum CallError {
    Transport(reqwest::Error),
    Status {
        code: u16,
        reason: Option<String>,
        message: String,
    },
    /// A token mint or a decode failure: not the server's fault, never retried.
    Fatal(Error),
}

impl CallError {
    pub fn is_retryable(&self) -> bool {
        match self {
            CallError::Transport(e) => e.is_connect() || e.is_timeout(),
            CallError::Status { code, reason, .. } => retryable_http(*code, reason.as_deref()),
            CallError::Fatal(_) => false,
        }
    }
    pub fn status(&self) -> Option<u16> {
        match self {
            CallError::Status { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Transport(e) => write!(f, "transport: {e}"),
            CallError::Status {
                code,
                reason: Some(r),
                message,
            } => {
                write!(f, "{code} {r}: {message}")
            }
            CallError::Status {
                code,
                reason: None,
                message,
            } => write!(f, "{code}: {message}"),
            CallError::Fatal(e) => write!(f, "{e}"),
        }
    }
}

/// `error.{message, errors[0].reason}` from an error body; a non-JSON body
/// (a proxy's HTML) keeps the status and the first 200 bytes.
pub fn error_from_body(status: u16, body: &[u8]) -> CallError {
    match serde_json::from_slice::<WireErrorBody>(body) {
        Ok(b) => CallError::Status {
            code: status,
            reason: b.error.errors.into_iter().next().and_then(|e| e.reason),
            message: b.error.message.unwrap_or_default(),
        },
        Err(_) => CallError::Status {
            code: status,
            reason: None,
            message: String::from_utf8_lossy(&body[..body.len().min(200)]).into_owned(),
        },
    }
}

impl From<CallError> for Error {
    fn from(e: CallError) -> Self {
        match e {
            CallError::Fatal(inner) => inner,
            other => Error::BqJob(other.to_string()),
        }
    }
}

enum Call<'a> {
    /// The query string is already in the `Url`.
    Get {
        url: reqwest::Url,
    },
    Post {
        url: String,
        body: &'a [u8],
    },
}

fn url_with(url: &str, query: &[(&str, &str)]) -> Result<reqwest::Url> {
    reqwest::Url::parse_with_params(url, query).map_err(|e| Error::BqJob(format!("url {url}: {e}")))
}

/// The jobs client: one HTTP client and the ADC token provider (the same
/// chain the mover's Storage Read and Bigtable clients use).
#[derive(Clone)]
pub struct BqJobs {
    http: reqwest::Client,
    provider: Arc<dyn gcp_auth::TokenProvider>,
}

impl BqJobs {
    pub async fn connect() -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|e| Error::BqJob(format!("http client: {e}")))?;
        let provider = gcp_auth::provider()
            .await
            .map_err(|e| Error::BqJob(format!("gcp auth provider: {e}")))?;
        Ok(Self { http, provider })
    }

    /// One HTTP call with a fresh bearer token, retried per `retry.rs` on
    /// transport errors and retryable statuses. Returns the response body.
    async fn call(
        &self,
        what: &str,
        label: &str,
        call: &Call<'_>,
    ) -> std::result::Result<bytes::Bytes, CallError> {
        let (body, _retries) = retry_rpc(what, label, CallError::is_retryable, |_| async {
            let token = self
                .provider
                .token(&[SCOPE])
                .await
                .map_err(|e| CallError::Fatal(Error::BqJob(format!("gcp token: {e}"))))?;
            let req = match call {
                Call::Get { url } => self.http.get(url.clone()),
                Call::Post { url, body } => self
                    .http
                    .post(url.as_str())
                    .header("content-type", "application/json")
                    .body(body.to_vec()),
            };
            let resp = req
                .bearer_auth(token.as_str())
                .send()
                .await
                .map_err(CallError::Transport)?;
            let status = resp.status().as_u16();
            let bytes = resp.bytes().await.map_err(CallError::Transport)?;
            if (200..300).contains(&status) {
                Ok(bytes)
            } else {
                Err(error_from_body(status, &bytes))
            }
        })
        .await?;
        Ok(body)
    }

    /// Runs one statement to completion and returns its whole result,
    /// following pages. The `requestId` is minted once, outside the retry
    /// loop, so a re-sent POST dedupes against the job the server already
    /// started (BigQuery's 15-minute window) instead of starting a second one.
    pub async fn query(&self, billing_project: &str, sql: &str) -> Result<QueryResult> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let label = format!("bq request_id={request_id}");
        let body = serde_json::to_vec(&serde_json::json!({
            "query": sql,
            "useLegacySql": false,
            "timeoutMs": WAIT_MS,
            "requestId": request_id,
        }))
        .map_err(|e| Error::BqJob(format!("request body: {e}")))?;
        let url = format!("{BASE}/projects/{billing_project}/jobs/query");
        let first = self
            .call("jobs.query", &label, &Call::Post { url, body: &body })
            .await?;
        let mut page = decode_query_page(&first)?;
        let mut schema = std::mem::take(&mut page.schema);
        let mut rows = std::mem::take(&mut page.rows);
        let mut total_bytes_processed = page.total_bytes_processed;
        let mut token = page.page_token.take();
        while !page.job_complete || token.is_some() {
            let job = page
                .job
                .clone()
                .ok_or_else(|| Error::BqJob("query response carries no jobReference".to_owned()))?;
            let base = format!("{BASE}/projects/{}/queries/{}", job.project_id, job.job_id);
            let wait = WAIT_MS.to_string();
            let mut query: Vec<(&str, &str)> = vec![("timeoutMs", &wait)];
            if let Some(loc) = job.location.as_deref() {
                query.push(("location", loc));
            }
            if let Some(t) = token.as_deref() {
                query.push(("pageToken", t));
            }
            let url = url_with(&base, &query)?;
            let bytes = self
                .call("jobs.getQueryResults", &label, &Call::Get { url })
                .await?;
            let mut next = decode_query_page(&bytes)?;
            if next.job.is_none() {
                next.job = Some(job);
            }
            if schema.is_empty() {
                schema = std::mem::take(&mut next.schema);
            }
            rows.append(&mut next.rows);
            total_bytes_processed = next.total_bytes_processed.or(total_bytes_processed);
            token = next.page_token.take();
            page = next;
        }
        Ok(QueryResult {
            schema,
            rows,
            total_bytes_processed: total_bytes_processed.unwrap_or(0),
        })
    }

    /// `tables.get`; a 404 is `None`.
    pub async fn table(&self, t: &TableRef) -> Result<Option<TableInfo>> {
        let url = url_with(
            &format!(
                "{BASE}/projects/{}/datasets/{}/tables/{}",
                t.project, t.dataset, t.table
            ),
            &[],
        )?;
        let label = format!("bq table={t}");
        match self.call("tables.get", &label, &Call::Get { url }).await {
            Ok(bytes) => Ok(Some(decode_table(&bytes)?)),
            Err(e) if e.status() == Some(404) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests; the workspace denies unwrap_used
mod tests {
    use super::*;

    const PAGE: &str = r#"{
      "kind": "bigquery#queryResponse",
      "jobReference": {"projectId": "example-project", "jobId": "job_1", "location": "US"},
      "jobComplete": true,
      "schema": {"fields": [
        {"name": "cuts", "type": "STRING", "mode": "REPEATED"},
        {"name": "lowest", "type": "STRING", "mode": "NULLABLE"},
        {"name": "row_count", "type": "INTEGER", "mode": "NULLABLE"}]},
      "rows": [{"f": [{"v": [{"v": "a"}, {"v": "b"}]}, {"v": "a"}, {"v": "1500000"}]},
               {"f": [{"v": []}, {"v": null}, {"v": "0"}]}],
      "totalBytesProcessed": "12345",
      "pageToken": "next"
    }"#;

    #[test]
    fn decodes_a_page_with_arrays_nulls_and_integers() {
        let page = decode_query_page(PAGE.as_bytes()).unwrap();
        assert!(page.job_complete);
        let job = page.job.unwrap();
        assert_eq!(
            (job.project_id.as_str(), job.job_id.as_str()),
            ("example-project", "job_1")
        );
        assert_eq!(job.location.as_deref(), Some("US"));
        assert_eq!(page.page_token.as_deref(), Some("next"));
        assert_eq!(page.total_bytes_processed, Some(12345));
        assert_eq!(
            page.schema[2],
            Field {
                name: "row_count".into(),
                field_type: "INTEGER".into()
            }
        );
        assert_eq!(page.rows[0].strs(0).unwrap(), vec!["a", "b"]);
        assert_eq!(page.rows[0].str(1).unwrap(), "a");
        assert_eq!(page.rows[0].i64(2).unwrap(), 1_500_000);
        assert_eq!(page.rows[1].strs(0).unwrap(), Vec::<&str>::new());
        assert_eq!(page.rows[1].opt_str(1).unwrap(), None);
        assert!(
            page.rows[1].str(1).is_err(),
            "NULL where a string is required"
        );
        assert!(page.rows[0].i64(1).is_err(), "'a' is not an integer");
    }

    #[test]
    fn an_incomplete_job_has_no_rows_yet() {
        let doc = r#"{"jobReference": {"projectId": "p", "jobId": "j"}, "jobComplete": false}"#;
        let page = decode_query_page(doc.as_bytes()).unwrap();
        assert!(!page.job_complete);
        assert!(page.rows.is_empty());
        assert!(page.schema.is_empty());
    }

    #[test]
    fn a_struct_value_is_refused() {
        let doc = r#"{"jobComplete": true, "rows": [{"f": [{"v": {"f": [{"v": "x"}]}}]}]}"#;
        let err = decode_query_page(doc.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("STRUCT"), "{err}");
    }

    #[test]
    fn decodes_table_info() {
        let doc = r#"{
          "numRows": "1500000", "numBytes": "618900000",
          "schema": {"fields": [{"name": "key", "type": "STRING"}, {"name": "json", "type": "STRING"}, {"name": "bucket", "type": "INTEGER"}]},
          "rangePartitioning": {"field": "bucket", "range": {"start": "0", "end": "16", "interval": "1"}},
          "clustering": {"fields": ["key"]},
          "expirationTime": "1800000000000"
        }"#;
        let t = decode_table(doc.as_bytes()).unwrap();
        assert_eq!((t.num_rows, t.num_bytes), (1_500_000, 618_900_000));
        assert_eq!(t.schema[0].field_type, "STRING");
        let rp = t.range_partitioning.unwrap();
        assert_eq!(
            (rp.field.as_str(), rp.start, rp.end, rp.interval),
            ("bucket", 0, 16, 1)
        );
        assert_eq!(t.clustering, vec!["key"]);
        assert_eq!(t.expiration_time_ms, Some(1_800_000_000_000));
        let bare = decode_table(br#"{"numRows": "0", "numBytes": "0"}"#).unwrap();
        assert!(bare.range_partitioning.is_none());
        assert!(bare.clustering.is_empty());
        assert!(bare.expiration_time_ms.is_none());
    }

    #[test]
    fn error_bodies_classify_by_status_and_reason() {
        let body = br#"{"error": {"code": 403, "message": "Exceeded rate limits", "errors": [{"reason": "rateLimitExceeded", "message": "Exceeded rate limits"}], "status": "PERMISSION_DENIED"}}"#;
        let e = error_from_body(403, body);
        assert!(e.is_retryable());
        assert_eq!(e.to_string(), "403 rateLimitExceeded: Exceeded rate limits");
        let e = error_from_body(404, br#"{"error": {"code": 404, "message": "Not found: Table p:d.t", "errors": [{"reason": "notFound"}]}}"#);
        assert!(!e.is_retryable());
        assert_eq!(e.status(), Some(404));
        assert!(e.to_string().contains("notFound"));
        let e = error_from_body(503, b"<html>bad gateway</html>");
        assert!(e.is_retryable());
        assert!(e.to_string().starts_with("503"));
    }

    #[test]
    fn a_200_with_a_warning_errors_array_is_not_a_failure() {
        let doc = r#"{"jobComplete": true, "errors": [{"reason": "warning", "message": "x"}], "rows": []}"#;
        assert!(decode_query_page(doc.as_bytes()).is_ok());
    }
}
