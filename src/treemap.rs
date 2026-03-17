//! Tree-structured task relationships via parent/child links.
//!
//! [`TreeMap`] builds a parent/child graph from a task set using the `parent` property,
//! providing traversal helpers for tree-structured task hierarchies.
//!
//! See also [`crate::position`] for sibling ordering and [`crate::plan`] for creating
//! subtask trees from markdown.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::task::{Status, Task};

/// TreeMap stores parent/child relationships between tasks.
///
/// Built from a full task set (not just the working set), this structure enables
/// tree traversal, ancestor checks, and sibling ordering queries.
///
/// This information requires a scan of all tasks to generate, so it is
/// typically calculated once and re-used via [`crate::Replica::tree_map`].
pub struct TreeMap {
    children: HashMap<Uuid, Vec<Uuid>>,
    parent: HashMap<Uuid, Uuid>,
    all_uuids: Vec<Uuid>,
}

impl TreeMap {
    /// Build a TreeMap from a full task set.
    ///
    /// Tasks with a `parent` property pointing to a valid UUID are linked as children.
    /// Children are sorted by `(position, entry, uuid)` — positioned tasks first in
    /// lexicographic order, then unpositioned by creation time, UUID as tiebreaker.
    pub fn from_tasks(tasks: &HashMap<Uuid, Task>) -> Self {
        let mut children: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        let mut parent_map: HashMap<Uuid, Uuid> = HashMap::new();

        // Initialize children list for all tasks so every known UUID has an entry.
        for uuid in tasks.keys() {
            children.entry(*uuid).or_default();
        }

        for (uuid, task) in tasks {
            if let Some(parent_str) = task.get_value("parent") {
                match Uuid::parse_str(parent_str) {
                    Ok(parent_uuid) => {
                        parent_map.insert(*uuid, parent_uuid);
                        children.entry(parent_uuid).or_default().push(*uuid);
                    }
                    Err(_) => {
                        log::warn!(
                            "task {} has invalid parent UUID {:?} — treating as root",
                            uuid,
                            parent_str
                        );
                    }
                }
            }
        }

        // Sort children by (position, entry) — positioned first in order,
        // then unpositioned by creation time, then by UUID as final tiebreaker.
        for list in children.values_mut() {
            list.sort_by(|a, b| {
                let task_a = tasks.get(a);
                let task_b = tasks.get(b);
                let pos_a = task_a.and_then(|t| t.get_value("position"));
                let pos_b = task_b.and_then(|t| t.get_value("position"));

                match (pos_a, pos_b) {
                    // Both have positions — lexicographic compare
                    (Some(pa), Some(pb)) => pa.cmp(pb),
                    // Only a has position — a comes first
                    (Some(_), None) => std::cmp::Ordering::Less,
                    // Only b has position — b comes first
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    // Neither has position — sort by entry timestamp, then UUID
                    (None, None) => {
                        let entry_a = task_a.and_then(|t| t.get_value("entry"));
                        let entry_b = task_b.and_then(|t| t.get_value("entry"));
                        entry_a.cmp(&entry_b).then_with(|| a.cmp(b))
                    }
                }
            });
        }

        Self {
            children,
            parent: parent_map,
            all_uuids: tasks.keys().copied().collect(),
        }
    }

    /// Return the direct children of `uuid`, in position order.
    pub fn children(&self, uuid: Uuid) -> Vec<Uuid> {
        self.children.get(&uuid).cloned().unwrap_or_default()
    }

    /// Return all descendants of `uuid` in depth-first order (not including `uuid` itself).
    ///
    /// Includes a visited-set guard to break on cyclic parent data.
    pub fn descendants(&self, uuid: Uuid) -> Vec<Uuid> {
        let mut result = Vec::new();
        let mut stack = vec![uuid];
        let mut visited = HashSet::new();
        visited.insert(uuid);

        while let Some(current) = stack.pop() {
            for child in self.children(current) {
                if visited.insert(child) {
                    result.push(child);
                    stack.push(child);
                } else {
                    log::warn!(
                        "cycle detected in task tree at {} — breaking traversal",
                        child
                    );
                }
            }
        }
        result
    }

    /// Return the tasks with no parent (root tasks).
    pub fn roots(&self) -> Vec<Uuid> {
        self.all_uuids
            .iter()
            .filter(|uuid| !self.parent.contains_key(*uuid))
            .copied()
            .collect()
    }

    /// Check if `ancestor` is an ancestor of `uuid` (walk up parent chain).
    ///
    /// Includes a visited-set guard to break on cyclic parent data.
    /// Returns `false` if `uuid == ancestor`.
    pub fn is_ancestor(&self, uuid: Uuid, ancestor: Uuid) -> bool {
        let mut current = uuid;
        let mut visited = HashSet::new();
        visited.insert(current);

        loop {
            match self.parent.get(&current).copied() {
                None => return false,
                Some(p) if p == ancestor => return true,
                Some(p) => {
                    if !visited.insert(p) {
                        log::warn!("cycle detected in task parent chain at {} — breaking", p);
                        return false;
                    }
                    current = p;
                }
            }
        }
    }

    /// Get the position values of siblings under a parent (in order).
    ///
    /// `parent` is `Some(uuid)` for child tasks, `None` for root tasks.
    /// `exclude` optionally removes a UUID from results (for move operations —
    /// the task being moved should not appear in its own sibling list).
    ///
    /// Returns `(uuid, position_string)` for siblings that have positions.
    pub fn sibling_positions(
        &self,
        parent: Option<Uuid>,
        tasks: &HashMap<Uuid, Task>,
        exclude: Option<Uuid>,
    ) -> Vec<(Uuid, String)> {
        let siblings = match parent {
            Some(p) => self.children(p),
            None => self.roots(),
        };
        siblings
            .into_iter()
            .filter(|uuid| exclude != Some(*uuid))
            .filter_map(|uuid| {
                let task = tasks.get(&uuid)?;
                let pos = task.get_value("position")?;
                Some((uuid, pos.to_string()))
            })
            .collect()
    }

    /// Return the UUIDs of pending direct children.
    ///
    /// This is a shared guard for done/delete commands that need to warn about
    /// pending children before cascading.
    pub fn pending_child_ids(&self, uuid: Uuid, all_tasks: &HashMap<Uuid, Task>) -> Vec<Uuid> {
        self.children(uuid)
            .into_iter()
            .filter(|child_uuid| {
                all_tasks
                    .get(child_uuid)
                    .map(|t| matches!(t.get_status(), Status::Pending))
                    .unwrap_or(false)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::inmemory::InMemoryStorage, Operations, Replica, Status};

    async fn make_task(replica: &mut Replica<InMemoryStorage>, description: &str) -> (Uuid, Task) {
        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut task = replica.create_task(uuid, &mut ops).await.unwrap();
        task.set_description(description.to_string(), &mut ops)
            .unwrap();
        task.set_status(Status::Pending, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        let task = replica.get_task(uuid).await.unwrap().unwrap();
        (uuid, task)
    }

    async fn set_parent(replica: &mut Replica<InMemoryStorage>, child: Uuid, parent: Uuid) {
        let mut ops = Operations::new();
        let mut task = replica.get_task(child).await.unwrap().unwrap();
        task.set_value("parent", Some(parent.to_string()), &mut ops)
            .unwrap();
        replica.commit_operations(ops).await.unwrap();
    }

    async fn set_position(replica: &mut Replica<InMemoryStorage>, uuid: Uuid, pos: &str) {
        let mut ops = Operations::new();
        let mut task = replica.get_task(uuid).await.unwrap().unwrap();
        task.set_value("position", Some(pos.to_string()), &mut ops)
            .unwrap();
        replica.commit_operations(ops).await.unwrap();
    }

    #[tokio::test]
    async fn empty_task_set() {
        let tasks: HashMap<Uuid, Task> = HashMap::new();
        let tree = TreeMap::from_tasks(&tasks);
        assert!(tree.roots().is_empty());
    }

    #[tokio::test]
    async fn flat_tasks_all_roots() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (a, _) = make_task(&mut rep, "a").await;
        let (b, _) = make_task(&mut rep, "b").await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        let roots = tree.roots();
        assert!(roots.contains(&a));
        assert!(roots.contains(&b));
        assert!(tree.children(a).is_empty());
        assert!(tree.children(b).is_empty());
    }

    #[tokio::test]
    async fn simple_parent_child() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (parent, _) = make_task(&mut rep, "parent").await;
        let (child, _) = make_task(&mut rep, "child").await;
        set_parent(&mut rep, child, parent).await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        let roots = tree.roots();
        assert!(roots.contains(&parent));
        assert!(!roots.contains(&child));
        let children = tree.children(parent);
        assert_eq!(children, vec![child]);
    }

    #[tokio::test]
    async fn multilevel_descendants() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (p, _) = make_task(&mut rep, "p").await;
        let (c, _) = make_task(&mut rep, "c").await;
        let (g, _) = make_task(&mut rep, "g").await;
        set_parent(&mut rep, c, p).await;
        set_parent(&mut rep, g, c).await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        let desc = tree.descendants(p);
        assert!(desc.contains(&c));
        assert!(desc.contains(&g));
    }

    #[tokio::test]
    async fn invalid_parent_uuid_treated_as_root() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (uuid, _) = make_task(&mut rep, "task").await;
        {
            let mut ops = Operations::new();
            let mut task = rep.get_task(uuid).await.unwrap().unwrap();
            task.set_value("parent", Some("not-a-uuid".to_string()), &mut ops)
                .unwrap();
            rep.commit_operations(ops).await.unwrap();
        }
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        // Should be treated as root (no panic, just logged)
        assert!(tree.roots().contains(&uuid));
    }

    #[tokio::test]
    async fn nonexistent_parent_still_tracked() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let phantom_parent = Uuid::new_v4();
        let (child, _) = make_task(&mut rep, "child").await;
        set_parent(&mut rep, child, phantom_parent).await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        // child has a parent, so it should NOT be in roots
        assert!(!tree.roots().contains(&child));
        // phantom parent is not in roots since it's not in task set at all
        // children of phantom_parent still tracked
        assert!(tree.children(phantom_parent).contains(&child));
    }

    #[tokio::test]
    async fn leaf_children_returns_empty() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (uuid, _) = make_task(&mut rep, "leaf").await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        assert!(tree.children(uuid).is_empty());
    }

    #[tokio::test]
    async fn children_sorted_by_position_then_entry() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (parent, _) = make_task(&mut rep, "parent").await;
        // c1 with position "80" (default fractional index)
        let (c1, _) = make_task(&mut rep, "c1").await;
        // c2 with position "V0" (after 80)
        let (c2, _) = make_task(&mut rep, "c2").await;
        // c3 without position (created last)
        let (c3, _) = make_task(&mut rep, "c3").await;
        set_parent(&mut rep, c1, parent).await;
        set_parent(&mut rep, c2, parent).await;
        set_parent(&mut rep, c3, parent).await;
        set_position(&mut rep, c1, "80").await;
        set_position(&mut rep, c2, "V0").await;
        // c3 has no position
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        let children = tree.children(parent);
        // Positioned children first in lex order, then unpositioned
        assert_eq!(children[0], c1); // "80" < "V0"
        assert_eq!(children[1], c2); // "V0"
        assert_eq!(children[2], c3); // no position, last
    }

    #[tokio::test]
    async fn descendants_cycle_safe() {
        // Manually construct a cyclic tree (A→B→A)
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        // Build TreeMap directly with a cycle
        let mut children = HashMap::new();
        children.insert(a, vec![b]);
        children.insert(b, vec![a]);
        let mut parent = HashMap::new();
        parent.insert(b, a);
        parent.insert(a, b);
        let tree = TreeMap {
            children,
            parent,
            all_uuids: vec![a, b],
        };
        // Should not infinite loop
        let desc = tree.descendants(a);
        // Should contain b (first child), then stop when it tries to revisit a
        assert!(desc.contains(&b));
        assert!(!desc.contains(&a)); // root itself excluded
    }

    #[tokio::test]
    async fn is_ancestor_basic() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (p, _) = make_task(&mut rep, "p").await;
        let (c, _) = make_task(&mut rep, "c").await;
        let (g, _) = make_task(&mut rep, "g").await;
        set_parent(&mut rep, c, p).await;
        set_parent(&mut rep, g, c).await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        assert!(tree.is_ancestor(g, p));
        assert!(tree.is_ancestor(g, c));
        assert!(tree.is_ancestor(c, p));
        assert!(!tree.is_ancestor(p, c));
        assert!(!tree.is_ancestor(g, g)); // not its own ancestor
    }

    #[tokio::test]
    async fn is_ancestor_self() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (uuid, _) = make_task(&mut rep, "task").await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        assert!(!tree.is_ancestor(uuid, uuid));
    }

    #[tokio::test]
    async fn is_ancestor_cycle_safe() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let mut children = HashMap::new();
        children.insert(a, vec![b]);
        children.insert(b, vec![a]);
        let mut parent = HashMap::new();
        parent.insert(b, a);
        parent.insert(a, b);
        let tree = TreeMap {
            children,
            parent,
            all_uuids: vec![a, b],
        };
        // Should not infinite loop
        let _ = tree.is_ancestor(a, b);
        let _ = tree.is_ancestor(b, a);
    }

    #[tokio::test]
    async fn roots_empty_when_all_have_parents() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let mut children = HashMap::new();
        children.insert(a, vec![b]);
        children.insert(b, vec![a]);
        let mut parent = HashMap::new();
        parent.insert(a, b); // a's parent is b
        parent.insert(b, a); // b's parent is a (cyclic but both have parents)
        let tree = TreeMap {
            children,
            parent,
            all_uuids: vec![a, b],
        };
        assert!(tree.roots().is_empty());
    }

    #[tokio::test]
    async fn sibling_positions_basic() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (parent, _) = make_task(&mut rep, "parent").await;
        let (c1, _) = make_task(&mut rep, "c1").await;
        let (c2, _) = make_task(&mut rep, "c2").await;
        set_parent(&mut rep, c1, parent).await;
        set_parent(&mut rep, c2, parent).await;
        set_position(&mut rep, c1, "80").await;
        set_position(&mut rep, c2, "V0").await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        let positions = tree.sibling_positions(Some(parent), &tasks, None);
        assert_eq!(positions.len(), 2);
        let position_map: HashMap<Uuid, String> = positions.into_iter().collect();
        assert_eq!(position_map[&c1], "80");
        assert_eq!(position_map[&c2], "V0");
    }

    #[tokio::test]
    async fn sibling_positions_exclude() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (parent, _) = make_task(&mut rep, "parent").await;
        let (c1, _) = make_task(&mut rep, "c1").await;
        let (c2, _) = make_task(&mut rep, "c2").await;
        set_parent(&mut rep, c1, parent).await;
        set_parent(&mut rep, c2, parent).await;
        set_position(&mut rep, c1, "80").await;
        set_position(&mut rep, c2, "V0").await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        let positions = tree.sibling_positions(Some(parent), &tasks, Some(c1));
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].0, c2);
    }

    #[tokio::test]
    async fn sibling_positions_root_level() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (r1, _) = make_task(&mut rep, "r1").await;
        set_position(&mut rep, r1, "80").await;
        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        let positions = tree.sibling_positions(None, &tasks, None);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].0, r1);
    }

    #[tokio::test]
    async fn pending_child_ids_returns_uuids() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let (parent, _) = make_task(&mut rep, "parent").await;
        let (c1, _) = make_task(&mut rep, "c1").await;
        let (c2, _) = make_task(&mut rep, "c2").await;
        set_parent(&mut rep, c1, parent).await;
        set_parent(&mut rep, c2, parent).await;

        // Mark c2 as completed
        {
            let mut ops = Operations::new();
            let mut task = rep.get_task(c2).await.unwrap().unwrap();
            task.set_status(Status::Completed, &mut ops).unwrap();
            rep.commit_operations(ops).await.unwrap();
        }

        let tasks = rep.all_tasks().await.unwrap();
        let tree = TreeMap::from_tasks(&tasks);
        let result: Vec<Uuid> = tree.pending_child_ids(parent, &tasks);
        // Only c1 should be returned (c2 is completed)
        assert_eq!(result, vec![c1]);
        // Assert these are full UUIDs, not 8-char truncations
        assert_eq!(result[0], c1);
    }
}
