#![deny(clippy::all)]
#![deny(unreachable_pub)]
#![deny(unnameable_types)]
#![deny(clippy::dbg_macro)]
#![doc = include_str!("crate-doc.md")]

mod depmap;
mod errors;
mod operation;
pub mod plan;
pub mod position;
mod replica;
pub mod server;
pub mod storage;
mod task;
mod taskdb;
mod treemap;
mod utils;
mod workingset;

pub use depmap::DependencyMap;
pub use errors::Error;
pub use operation::{Operation, Operations};
pub use position::{append_position, between_position, prepend_position, sequential_positions};
pub use replica::Replica;
pub use treemap::TreeMap;
pub use server::{Server, ServerConfig};
#[cfg(all(target_arch = "wasm32", feature = "storage-indexeddb"))]
pub use storage::indexeddb::IndexedDbStorage;
#[cfg(feature = "storage-powersync")]
pub use storage::powersync::PowerSyncStorage;
#[cfg(feature = "storage-sqlite")]
pub use storage::sqlite::SqliteStorage;
pub use task::{utc_timestamp, Annotation, Status, Tag, Task, TaskData};
pub use workingset::WorkingSet;

/// Re-exported type from the `uuid` crate, for ease of compatibility for consumers of this crate.
pub use uuid::Uuid;

/// Re-exported chrono module.
pub use chrono;
