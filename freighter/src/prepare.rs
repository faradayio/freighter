//! `freighter prepare`: build the sharded source table from a `(key, json)`
//! query and measure what `plan` needs. Orchestration lives here (Task 9);
//! the cut is `boundaries`, the SQL is `queries`.

pub mod boundaries;
pub mod queries;
