//! Exported FFI functions for Replica-level operations.
//!
//! Every function is stateless — the caller passes a raw `sqlite3*` handle as
//! an `i64`, and Rust wraps it in an ephemeral [`DirectPowerSyncStorage`] and
//! [`Replica`], performs the work, then drops everything before returning.

use std::sync::LazyLock;
use taskchampion::{DirectPowerSyncStorage, Operation, Operations, Replica, Status};
use uuid::Uuid;

use chrono::Utc;

use crate::convert::tree_map_to_ffi;
use crate::types::{FfiDependencyEdge, FfiError, FfiTask, FfiTreeNode};

// ---------------------------------------------------------------------------
// Global single-threaded Tokio runtime
// ---------------------------------------------------------------------------

/// Shared `current_thread` runtime — initialized once per process.
///
/// `block_on` is used to drive each FFI call synchronously.  No futures are
/// moved between threads, satisfying the safety contract of
/// [`DirectPowerSyncStorage`].
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
});

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build an ephemeral [`Replica`] from the raw handle and run `f` on it.
pub(crate) fn with_replica<F, Fut, T>(db_handle: i64, user_id: &str, f: F) -> Result<T, FfiError>
where
    F: FnOnce(Replica<DirectPowerSyncStorage>) -> Fut,
    Fut: std::future::Future<Output = Result<T, FfiError>>,
{
    RUNTIME.block_on(async {
        let user_uuid = Uuid::parse_str(user_id).map_err(|e| FfiError::Usage {
            message: format!("Invalid user_id: {e}"),
        })?;
        let ptr = db_handle as *mut rusqlite::ffi::sqlite3;
        // SAFETY: FFI caller guarantees a valid, open handle; RUNTIME is current_thread
        // so the !Send connection is never accessed from multiple threads.
        let storage = unsafe { DirectPowerSyncStorage::from_handle(ptr, user_uuid) }
            .map_err(FfiError::from)?;
        let replica = Replica::new(storage);
        f(replica).await
    })
}

pub(crate) fn parse_uuid(s: &str) -> Result<Uuid, FfiError> {
    Uuid::parse_str(s).map_err(|e| FfiError::Usage {
        message: format!("Invalid UUID: {e}"),
    })
}

// ---------------------------------------------------------------------------
// Exported FFI functions
// ---------------------------------------------------------------------------

/// Fetch a single task by UUID. Returns `None` if not found.
#[uniffi::export]
pub fn get_task(
    db_handle: i64,
    user_id: String,
    uuid: String,
) -> Result<Option<FfiTask>, FfiError> {
    with_replica(db_handle, &user_id, |mut replica| async move {
        let uuid = parse_uuid(&uuid)?;
        let task = replica.get_task(uuid).await.map_err(FfiError::from)?;
        Ok(task.as_ref().map(FfiTask::from))
    })
}

/// Return all tasks (pending, completed, deleted).
#[uniffi::export]
pub fn all_tasks(db_handle: i64, user_id: String) -> Result<Vec<FfiTask>, FfiError> {
    with_replica(db_handle, &user_id, |mut replica| async move {
        let tasks = replica.all_tasks().await.map_err(FfiError::from)?;
        Ok(tasks.values().map(FfiTask::from).collect())
    })
}

/// Return pending tasks only.
#[uniffi::export]
pub fn pending_tasks(db_handle: i64, user_id: String) -> Result<Vec<FfiTask>, FfiError> {
    with_replica(db_handle, &user_id, |mut replica| async move {
        let tasks = replica.pending_tasks().await.map_err(FfiError::from)?;
        Ok(tasks.iter().map(FfiTask::from).collect())
    })
}

/// Return the task tree as a flat list of [`FfiTreeNode`]s.
#[uniffi::export]
pub fn tree_map(db_handle: i64, user_id: String) -> Result<Vec<FfiTreeNode>, FfiError> {
    with_replica(db_handle, &user_id, |mut replica| async move {
        let tm = replica.tree_map().await.map_err(FfiError::from)?;
        Ok(tree_map_to_ffi(&tm))
    })
}

/// Return all dependency edges as `(from_uuid depends_on to_uuid)` pairs.
#[uniffi::export]
pub fn dependency_map(db_handle: i64, user_id: String) -> Result<Vec<FfiDependencyEdge>, FfiError> {
    with_replica(db_handle, &user_id, |mut replica| async move {
        let uuids = replica.all_task_uuids().await.map_err(FfiError::from)?;
        let dm = replica
            .dependency_map(false)
            .await
            .map_err(FfiError::from)?;
        let mut edges = Vec::new();
        for uuid in &uuids {
            for dep in dm.dependencies(*uuid) {
                edges.push(FfiDependencyEdge {
                    from_uuid: uuid.to_string(),
                    to_uuid: dep.to_string(),
                });
            }
        }
        Ok(edges)
    })
}

/// Create a new task with the given UUID and description.
///
/// The task is immediately committed with `status: Pending` and `entry: now`.
#[uniffi::export]
pub fn create_task(
    db_handle: i64,
    user_id: String,
    uuid: String,
    description: String,
) -> Result<FfiTask, FfiError> {
    with_replica(db_handle, &user_id, |mut replica| async move {
        let task_uuid = parse_uuid(&uuid)?;
        let mut ops = Operations::new();
        ops.push(Operation::UndoPoint);
        let mut task = replica
            .create_task(task_uuid, &mut ops)
            .await
            .map_err(FfiError::from)?;
        task.set_description(description, &mut ops)
            .map_err(FfiError::from)?;
        task.set_status(Status::Pending, &mut ops)
            .map_err(FfiError::from)?;
        task.set_entry(Some(Utc::now()), &mut ops)
            .map_err(FfiError::from)?;
        replica
            .commit_operations(ops)
            .await
            .map_err(FfiError::from)?;
        // Re-fetch to get the dependency-map-aware Task
        let created = replica
            .get_task(task_uuid)
            .await
            .map_err(FfiError::from)?
            .ok_or_else(|| FfiError::Internal {
                message: "Task missing after create".into(),
            })?;
        Ok(FfiTask::from(&created))
    })
}

/// Atomically undo the last operation group.
///
/// Returns `true` if an undo was performed, `false` if there is nothing to undo.
#[uniffi::export]
pub fn undo(db_handle: i64, user_id: String) -> Result<bool, FfiError> {
    with_replica(db_handle, &user_id, |mut replica| async move {
        let ops = replica
            .get_undo_operations()
            .await
            .map_err(FfiError::from)?;
        if ops.is_empty() {
            return Ok(false);
        }
        replica
            .commit_reversed_operations(ops)
            .await
            .map_err(FfiError::from)
    })
}
