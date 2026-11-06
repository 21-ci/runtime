//! Integration tests of `rt-backup`: one test binary. `cargo test -p rt-backup --test it`.

mod common;
mod mock_s3;

mod app;
mod forged;
mod roundtrip;
mod s3;
