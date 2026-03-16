# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is TaskChampion

TaskChampion is the Rust library that implements task storage and synchronization behind [Taskwarrior](https://taskwarrior.org/). It provides both Rust and C APIs. This repo contains the core `taskchampion` crate (root) and a private `xtask` crate for MSRV management.

## Build & Test Commands

```bash
cargo build                    # build with default features
cargo test                     # run all tests
cargo test -p taskchampion     # run only taskchampion crate tests
cargo test <test_name>         # run a single test by name

# Feature-specific testing (CI tests these combinations):
cargo test -p taskchampion --no-default-features --features "server-sync,tls-webpki-roots"
cargo test -p taskchampion --no-default-features --features "storage,bundled"

# Linting
cargo fmt --all -- --check     # format check
cargo clippy --all-features --no-deps -- -D warnings   # clippy (native)
cargo clippy --target wasm32-unknown-unknown --no-default-features --features storage-indexeddb --no-deps -- -D warnings  # clippy (WASM)

# Docs
cargo doc --release --open -p taskchampion
```

MSRV: **1.91.1** (update with `cargo xtask msrv <version>`)

## Architecture

### Core Abstractions

- **`Replica`** (`src/replica.rs`) — Main entry point. Represents one instance of a user's task data. Wraps a `TaskDb` and provides high-level task CRUD, sync, undo, and working set management.
- **`TaskDb`** (`src/taskdb/`) — Applies operations to storage, manages sync protocol, snapshots, and undo. Not public API; used internally by `Replica`.
- **`Operation`/`Operations`** (`src/operation.rs`) — All mutations are expressed as operations that get committed atomically via `Replica::commit_operations(ops)`. Operations are stored for later sync.
- **`Task`/`TaskData`** (`src/task/`) — `TaskData` is the low-level key-value map wrapper; `Task` is the high-level type with typed property accessors, dependency management, and tag support.
- **`WorkingSet`** (`src/workingset.rs`) — Maps small 1-based integer IDs to task UUIDs for user-facing short IDs.
- **`DependencyMap`** (`src/depmap.rs`) — Tracks task dependency relationships.

### Storage Layer (`src/storage/`)

Trait-based: `Storage` creates `StorageTxn` (transaction) objects. All storage access is async and transactional.

Backends (feature-gated):
- `storage-sqlite` — SQLite via rusqlite (default)
- `storage-powersync` — PowerSync-compatible SQLite
- `storage-indexeddb` — Browser IndexedDB (WASM only)
- `inmemory` — In-memory (always available, used in tests)

### Server/Sync Layer (`src/server/`)

Trait-based: `Server` trait with multiple sync backends (all feature-gated):
- `server-sync` — Remote sync server over HTTP with encryption
- `server-gcp` — Google Cloud Storage
- `server-aws` — AWS S3
- `server-local` — Local SQLite-to-SQLite sync

Encryption (`src/server/encryption.rs`) is used by all remote sync methods. The sync protocol uses operations and snapshots (`src/taskdb/sync.rs`, `src/taskdb/snapshot.rs`).

### Integration Tests (`tests/`)

Separate test binaries for cross-sync scenarios, TLS, and proptest-based sync testing. These are not in a separate crate — they're standalone `.rs` files in the `tests/` directory.

## Key Patterns

- **Async throughout**: All storage and server traits use `async_trait`. Tests use `#[tokio::test]`.
- **Feature flags are pervasive**: Most modules are conditionally compiled. The `default` feature set includes `sync`, `bundled`, `storage`, and `tls-webpki-roots`.
- **WASM support**: The crate compiles to `wasm32-unknown-unknown` with `storage-indexeddb`. WASM has different dependency sets and some platform-specific code paths.
- **Crate type**: Both `cdylib` (for C FFI consumers like Taskwarrior) and `rlib`.
- **Strict lints**: `clippy::all`, `unreachable_pub`, `unnameable_types`, `clippy::dbg_macro` are all denied in `lib.rs`.
