# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-07-05

### Changed

- Bump `rusqlite` dependency from `0.32` to `0.37` for the
  `libsqlite3-sys 0.35` ecosystem (latest release, e.g.
  `outline-mcp-rmcp 0.10`). Same stable API subset — no public API
  changes. Consumers on older `rusqlite` clusters should pin
  `rusqlite-isle = "0.2"` (0.31) or `"0.3"` (0.32).

## [0.3.0] - 2026-07-05

### Changed

- Bump `rusqlite` dependency from `0.31` to `0.32` for the
  `libsqlite3-sys 0.30` ecosystem (e.g. `journal-mcp-core`). Same
  stable API subset (`Connection`, `Error`, `ErrorCode`,
  `InterruptHandle`, `OpenFlags`, `Transaction`) — no public API
  changes. Consumers on the `rusqlite 0.31` cluster should stay on
  `rusqlite-isle = "0.2"`.

## [0.2.0] - 2026-07-05

### Changed

- Downgrade `rusqlite` dependency from `0.37` to `0.31` to restore
  co-existence with downstream crates still on the `rusqlite 0.31` /
  `libsqlite3-sys 0.28` ecosystem (e.g. `agent-block-core 0.30`). Only
  a stable subset of `rusqlite` API (`Connection`, `Error`, `ErrorCode`,
  `InterruptHandle`, `OpenFlags`, `Transaction`) is used, so no public
  API changes in `rusqlite-isle` itself. See
  [#1](https://github.com/ynishi/rusqlite-isle/issues/1) for the
  `libsqlite3-sys` `links = "sqlite3"` conflict rationale.

## [0.1.0] - 2026-07-04

### Added

- `Isle` — synchronous handle to a `rusqlite::Connection` confined to a
  dedicated thread: `spawn` / `open_in_memory` / `call` / `call_timeout` /
  `try_call` / `spawn_call` / `shutdown`, plus `IsleBuilder` (channel
  capacity, thread name, `busy_timeout`, open flags).
- `Task<T>` — cancellable pending result for the sync API
  (`wait` / `cancel` / `try_recv` / `cancel_token`).
- `CancelToken` — two-stage cancellation: queued jobs are dropped before
  execution; running statements are interrupted via
  `rusqlite::InterruptHandle` (`sqlite3_interrupt`), with
  `SQLITE_INTERRUPT` normalized back to `Cancelled` / `Timeout`.
- `IsleError` — `Sqlite` / `Cancelled` / `Timeout` / `Closed` /
  `QueueFull` / `Panicked`; job panics are caught with `catch_unwind`
  and followed by a `SELECT 1` health check (failing check closes the
  isle).
- `tokio` feature: `AsyncIsle` (cloneable handle) + `AsyncIsleBuilder` +
  `AsyncIsleDriver` (`shutdown().await` = drain + join,
  `shutdown_now()` = abort) over a bounded tokio mpsc channel, and
  `AsyncTask<T>` (implements `Future`, **drop = cancel**, opt out via
  `detach()`).
- `pool` feature: `IslePool` — lazy checkout/return pool of isles
  (`checkout` / `try_checkout` / `checkout_timeout` / `active` / `idle`
  / `shutdown`) with the `PooledIsle` RAII guard, intended for
  WAL-style 1-writer / N-reader arrangements.
