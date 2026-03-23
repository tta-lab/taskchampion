//! Conversions between taskchampion types and FFI-friendly types.

use taskchampion::{DependencyMap, Status, Task, TreeMap};
use uuid::Uuid;

use crate::types::{FfiAnnotation, FfiDependencyEdge, FfiError, FfiStatus, FfiTask, FfiTreeNode};

// ---------------------------------------------------------------------------
// Task → FfiTask
// ---------------------------------------------------------------------------

impl From<&Task> for FfiTask {
    fn from(task: &Task) -> Self {
        FfiTask {
            uuid: task.get_uuid().to_string(),
            status: FfiStatus::from(task.get_status()),
            description: task.get_description().to_string(),
            priority: task.get_priority().to_string(),
            // Timestamp is pub(crate) in taskchampion, but DateTime<Utc> methods are accessible.
            entry: task.get_entry().map(|ts| ts.timestamp()),
            modified: task.get_modified().map(|ts| ts.timestamp()),
            due: task.get_due().map(|ts| ts.timestamp()),
            wait: task.get_wait().map(|ts| ts.timestamp()),
            parent: task.get_parent().map(|u| u.to_string()),
            position: task.get_position().map(|s| s.to_string()),
            tags: task
                .get_tags()
                .filter(|t| !t.is_synthetic())
                .map(|t| t.to_string())
                .collect(),
            annotations: task
                .get_annotations()
                .map(|a| FfiAnnotation {
                    entry: a.entry.timestamp(),
                    description: a.description.clone(),
                })
                .collect(),
            dependencies: task.get_dependencies().map(|u| u.to_string()).collect(),
            is_waiting: task.is_waiting(),
            is_active: task.is_active(),
            is_blocked: task.is_blocked(),
            is_blocking: task.is_blocking(),
        }
    }
}

// ---------------------------------------------------------------------------
// Status ↔ FfiStatus
// ---------------------------------------------------------------------------

impl From<Status> for FfiStatus {
    fn from(s: Status) -> Self {
        match s {
            Status::Pending => FfiStatus::Pending,
            Status::Completed => FfiStatus::Completed,
            Status::Deleted => FfiStatus::Deleted,
            Status::Recurring => FfiStatus::Recurring,
            Status::Unknown(v) => FfiStatus::Unknown { value: v },
        }
    }
}

impl From<FfiStatus> for Status {
    fn from(s: FfiStatus) -> Self {
        match s {
            FfiStatus::Pending => Status::Pending,
            FfiStatus::Completed => Status::Completed,
            FfiStatus::Deleted => Status::Deleted,
            FfiStatus::Recurring => Status::Recurring,
            FfiStatus::Unknown { value } => Status::Unknown(value),
        }
    }
}

// ---------------------------------------------------------------------------
// Error conversion
// ---------------------------------------------------------------------------

impl From<taskchampion::Error> for FfiError {
    fn from(e: taskchampion::Error) -> Self {
        match e {
            taskchampion::Error::Database(msg) => FfiError::Database { message: msg },
            taskchampion::Error::Usage(msg) => FfiError::Usage { message: msg },
            taskchampion::Error::Other(e) => FfiError::Internal {
                message: e.to_string(),
            },
            // Error is #[non_exhaustive] — catch future variants
            _ => FfiError::Internal {
                message: e.to_string(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// TreeMap → Vec<FfiTreeNode>
// ---------------------------------------------------------------------------

/// Convert a [`TreeMap`] to a flat list of [`FfiTreeNode`]s.
///
/// `position` is not available from `TreeMap` (it lives on `Task`), so it is
/// always `None`. Callers that need per-node position should overlay it from
/// the task list.
pub fn tree_map_to_ffi(tm: &TreeMap) -> Vec<FfiTreeNode> {
    let mut nodes = Vec::new();
    collect_tree(tm, None, &tm.roots(), &mut nodes);
    nodes
}

fn collect_tree(tm: &TreeMap, parent: Option<Uuid>, uuids: &[Uuid], nodes: &mut Vec<FfiTreeNode>) {
    for &uuid in uuids {
        let children = tm.children(uuid);
        let has_pending_children = !tm.pending_child_ids(uuid).is_empty();
        nodes.push(FfiTreeNode {
            uuid: uuid.to_string(),
            children: children.iter().map(|u| u.to_string()).collect(),
            parent: parent.map(|u| u.to_string()),
            position: None,
            is_pending: has_pending_children,
        });
        collect_tree(tm, Some(uuid), &children, nodes);
    }
}

// ---------------------------------------------------------------------------
// DependencyMap → Vec<FfiDependencyEdge>
//
// NOTE: DependencyMap does not expose all task UUIDs, so this function is a
// placeholder. The actual edge enumeration is performed at the call site in
// `replica_ops.rs` where the full UUID list is available.
// ---------------------------------------------------------------------------

pub fn depmap_to_ffi(_dm: &DependencyMap) -> Vec<FfiDependencyEdge> {
    Vec::new()
}
