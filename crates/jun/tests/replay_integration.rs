//! Integration tests for `jun replay` against the live mainnet R2 archive.
//!
//! These tests touch the network, so they're all marked `#[ignore]`. Run with:
//!
//!     cargo test -p jun --release --test replay_integration -- --ignored
//!
//! Defaults in `jun replay` already point at the public archive
//! (https://archive.checkpoints.mainnet.sui.unconfirmed.cloud), so no env
//! plumbing is required.
//!
//! Epoch 0 is sparse (early mainnet) — checkpoints there are tiny, mostly
//! single-tx + 1 balance-change-for-gas, so a 0..=99 range is fast and gives
//! a deterministic record fanout.
//!
//! Each test enforces its own wall-clock timeout via a watcher thread that
//! kills the child if it overruns. We don't rely on Command's lack of native
//! timeout — we want a hard ceiling.

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Path to the freshly-built `jun` binary, resolved by Cargo.
fn jun_bin() -> &'static str {
    env!("CARGO_BIN_EXE_jun")
}

/// Result of running `jun` once.
struct RunOutput {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
    elapsed: Duration,
}

/// Spawn `jun` with the given args, enforce a wall-clock timeout, and capture
/// stdout/stderr. Panics if the child exceeds the timeout — that's a test
/// failure, not a "skip". Logs at info level so error context stays readable.
fn run_jun(args: &[&str], timeout: Duration) -> RunOutput {
    let start = Instant::now();
    let mut child = Command::new(jun_bin())
        .args(args)
        // Force a small env filter so we don't drown in trace logs but still
        // see "100 cps" and friends from eprintln! lines.
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn jun binary");

    // Drain stdout/stderr in background so the child can't block on a full
    // pipe. We collect into shared buffers we read after wait().
    let mut stdout_pipe = child.stdout.take().expect("stdout piped");
    let mut stderr_pipe = child.stderr.take().expect("stderr piped");
    let stdout_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let stderr_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let so = stdout_buf.clone();
    let se = stderr_buf.clone();
    let so_thread = thread::spawn(move || {
        let mut tmp = Vec::with_capacity(64 * 1024);
        let _ = stdout_pipe.read_to_end(&mut tmp);
        so.lock().unwrap().extend_from_slice(&tmp);
    });
    let se_thread = thread::spawn(move || {
        let mut tmp = Vec::with_capacity(16 * 1024);
        let _ = stderr_pipe.read_to_end(&mut tmp);
        se.lock().unwrap().extend_from_slice(&tmp);
    });

    // Poll for completion with a hard timeout. try_wait avoids blocking so we
    // can kill the child cleanly if it overruns.
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = so_thread.join();
                    let _ = se_thread.join();
                    let stderr = String::from_utf8_lossy(&stderr_buf.lock().unwrap()).to_string();
                    panic!(
                        "jun {:?} exceeded timeout {:?}\nstderr so far:\n{}",
                        args, timeout, stderr
                    );
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("error waiting on jun child: {e}"),
        }
    };

    let _ = so_thread.join();
    let _ = se_thread.join();
    let stdout = String::from_utf8_lossy(&stdout_buf.lock().unwrap()).to_string();
    let stderr = String::from_utf8_lossy(&stderr_buf.lock().unwrap()).to_string();
    RunOutput {
        status,
        stdout,
        stderr,
        elapsed: start.elapsed(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Fetch-only over the first 100 checkpoints — exercises archive fetch +
/// frame counting, no decode, no write. Should be the cheapest end-to-end
/// path against the real archive.
#[test]
#[ignore]
fn replay_fetch_only_first_100_checkpoints() {
    let out = run_jun(
        &["replay", "--from", "0", "--to", "99", "--mode", "fetch-only"],
        Duration::from_secs(60),
    );
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstderr:\n{}",
        out.status,
        out.stderr
    );
    assert!(
        out.stderr.contains("100 cps"),
        "expected stderr to contain '100 cps', got:\n{}",
        out.stderr
    );
    assert!(
        out.elapsed < Duration::from_secs(60),
        "took too long: {:?}",
        out.elapsed
    );
}

/// no-write mode: full fetch + decode pipeline, drop records on the floor.
/// Validates that decode doesn't blow up on real epoch-0 data.
#[test]
#[ignore]
fn replay_no_write_first_100_checkpoints() {
    let out = run_jun(
        &["replay", "--from", "0", "--to", "99", "--mode", "no-write"],
        Duration::from_secs(90),
    );
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstderr:\n{}",
        out.status,
        out.stderr
    );
    assert!(
        out.stderr.contains("100 cps"),
        "expected stderr to contain '100 cps', got:\n{}",
        out.stderr
    );
}

/// Single-table pipe of `transactions` over the first 100 checkpoints.
/// Asserts each line is JSON, has a non-empty `digest`, and (because we
/// selected exactly one table) is *untagged* — no `_table` field.
#[test]
#[ignore]
fn replay_pipe_single_table_transactions() {
    let out = run_jun(
        &[
            "replay",
            "--from",
            "0",
            "--to",
            "99",
            "--pipe",
            "--table=transactions",
        ],
        Duration::from_secs(90),
    );
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstderr:\n{}",
        out.status,
        out.stderr
    );

    let lines: Vec<&str> = out.stdout.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        (50..=200).contains(&lines.len()),
        "expected 50..=200 transaction lines, got {} (epoch 0 should be ~100 cps with mostly 1 tx each)",
        lines.len(),
    );

    for (i, line) in lines.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line {i} not parseable as JSON: {e}\nline: {line}"));
        let digest = v
            .get("digest")
            .unwrap_or_else(|| panic!("line {i} missing 'digest' field: {line}"));
        let s = digest
            .as_str()
            .unwrap_or_else(|| panic!("line {i} 'digest' is not a string: {digest}"));
        assert!(!s.is_empty(), "line {i} has empty digest: {line}");
        assert!(
            v.get("_table").is_none(),
            "line {i} has unexpected '_table' in single-table mode: {line}",
        );
    }
}

/// Multi-table pipe (no --table) over 10 checkpoints — every record carries a
/// `_table` discriminator. Validates the tagged JSONL stream and confirms
/// both `checkpoints` and `transactions` rows are emitted.
#[test]
#[ignore]
fn replay_pipe_multi_table_tagged() {
    let out = run_jun(
        &["replay", "--from", "0", "--to", "9", "--pipe"],
        Duration::from_secs(60),
    );
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstderr:\n{}",
        out.status,
        out.stderr
    );

    let allowed: std::collections::HashSet<&str> = [
        "checkpoints",
        "transactions",
        "move_calls",
        "balance_changes",
        "object_changes",
        "dependencies",
    ]
    .into_iter()
    .collect();

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let lines: Vec<&str> = out.stdout.lines().filter(|l| !l.is_empty()).collect();
    assert!(!lines.is_empty(), "expected at least some output");

    for (i, line) in lines.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line {i} not parseable as JSON: {e}\nline: {line}"));
        let table = v
            .get("_table")
            .unwrap_or_else(|| panic!("line {i} missing '_table' in multi-table mode: {line}"))
            .as_str()
            .unwrap_or_else(|| panic!("line {i} '_table' is not a string: {line}"));
        assert!(
            allowed.contains(table),
            "line {i} has unexpected _table={table:?}",
        );
        seen.insert(table.to_string());
    }

    assert!(
        seen.contains("checkpoints"),
        "expected at least one 'checkpoints' record, saw {seen:?}",
    );
    assert!(
        seen.contains("transactions"),
        "expected at least one 'transactions' record, saw {seen:?}",
    );
}

/// from > to — the binary should either non-zero-exit *or* run zero work.
/// What it must NOT do is fetch a wrap-around 2^64-sized range. The current
/// implementation falls into the "zero work success" bucket; either is fine
/// per the task spec, but if it ever silently succeeds *with* checkpoints
/// processed, that's a real bug.
#[test]
#[ignore]
fn replay_invalid_range_fails_cleanly() {
    let out = run_jun(
        &["replay", "--from", "100", "--to", "50", "--mode", "fetch-only"],
        Duration::from_secs(30),
    );

    if out.status.success() {
        // Acceptable only if it did zero work. We look for the "done:"
        // summary line and require it to report 0 cps — anything else means
        // the binary actually fetched data on a backwards range, which is
        // wrong.
        let did_zero_work = out
            .stderr
            .lines()
            .any(|l| l.starts_with("done:") && l.contains("0 cps"));
        assert!(
            did_zero_work,
            "from > to succeeded but did non-zero work\nstderr:\n{}",
            out.stderr,
        );
    }
    // Non-zero exit is also fine — no further assertions.
}
