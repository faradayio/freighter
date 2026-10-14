//! The mover: a sharded, verified BigQuery -> Bigtable loader. Each source
//! row is a `(key, json)` pair and becomes one gzipped cell (lookup.rs).
//! The mover is deliberately dumb — it moves rows and reports counts;
//! policy lives in whatever launches it.

pub mod bqjobs;
pub mod bqread;
pub mod cells;
pub mod client;
pub mod error;
pub mod lanes;
pub mod lookup;
pub mod manifest;
pub mod pipeline;
pub mod plan;
pub mod prepare;
pub mod retry;
pub mod source;
pub mod write;
