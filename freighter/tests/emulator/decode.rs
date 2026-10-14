//! The per-stream decode task: RecordBatches in, decoded rows out.
//! Pure CPU — these tests need no emulator connection.

#![allow(clippy::unwrap_used)]

use freighter::lookup::decode_lookup;
use freighter::pipeline::decode_stream;
use tokio::sync::mpsc;

use crate::support;

#[tokio::test]
async fn decode_stream_decodes_each_batch() {
    let shape = support::lookup_config();
    let b1 = support::lookup_keys(&["k1", "k2"]);
    let b2 = support::lookup_keys(&["k3"]);
    let expected1 = decode_lookup(&b1, &shape).unwrap();
    let expected2 = decode_lookup(&b2, &shape).unwrap();

    let (btx, brx) = mpsc::channel(4);
    let (dtx, mut drx) = mpsc::channel(4);
    btx.send(b1).await.unwrap();
    btx.send(b2).await.unwrap();
    drop(btx);
    decode_stream(brx, &shape, dtx).await.unwrap();

    let d1 = drx.recv().await.unwrap();
    assert_eq!(d1.len(), 2);
    assert_eq!(d1, expected1);
    let d2 = drx.recv().await.unwrap();
    assert_eq!(d2.len(), 1);
    assert_eq!(d2, expected2);
    assert!(
        drx.recv().await.is_none(),
        "sender dropped after input drained"
    );
}

#[tokio::test]
async fn decode_stream_stops_quietly_when_receiver_closes() {
    // A closed downstream means the writer failed; the writer's error is
    // the one that surfaces — the decoder just stops, mirroring read_stream.
    let shape = support::lookup_config();
    let (btx, brx) = mpsc::channel(4);
    let (dtx, drx) = mpsc::channel(1);
    btx.send(support::lookup_keys(&["k1"])).await.unwrap();
    drop(btx);
    drop(drx);
    decode_stream(brx, &shape, dtx)
        .await
        .expect("closed receiver is not an error");
}
