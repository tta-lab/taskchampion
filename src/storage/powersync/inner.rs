use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::send_wrapper::{WrappedStorage, WrappedStorageTxn};
use crate::storage::{TaskMap, VersionId, DEFAULT_BASE_VERSION};
use anyhow::Context;
use async_trait::async_trait;
use chrono::DateTime;
use powersync_core::powersync_init_static;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::path::Path;
use uuid::Uuid;

/// Convert Unix epoch string (e.g. "1724612771") to ISO 8601 string.
fn epoch_to_iso(epoch_str: &str) -> Option<String> {
    let secs: i64 = epoch_str.parse().ok()?;
    let dt = DateTime::from_timestamp(secs, 0)?;
    Some(dt.to_rfc3339())
}

/// Convert ISO 8601 string back to Unix epoch string.
fn iso_to_epoch(iso_str: &str) -> Option<String> {
    let dt = DateTime::parse_from_rfc3339(iso_str).ok()?;
    Some(dt.timestamp().to_string())
}

/// Remove a timestamp field from `task_data`, converting epoch → ISO 8601.
/// Returns `Err` if the value is present but not a valid epoch integer.
fn extract_timestamp(task_data: &mut TaskMap, key: &str) -> Result<Option<String>> {
    match task_data.remove(key) {
        None => Ok(None),
        Some(epoch_str) => match epoch_to_iso(&epoch_str) {
            Some(iso) => Ok(Some(iso)),
            None => Err(Error::Database(format!(
                "Invalid epoch timestamp for field {key:?}: {epoch_str:?}"
            ))),
        },
    }
}

/// Raw row fetched from tc_tasks JOIN projects.
struct RawTaskRow {
    id: String,
    data: String,
    status: Option<String>,
    description: Option<String>,
    priority: Option<String>,
    entry_at: Option<String>,
    modified_at: Option<String>,
    due_at: Option<String>,
    scheduled_at: Option<String>,
    start_at: Option<String>,
    end_at: Option<String>,
    wait_at: Option<String>,
    parent_id: Option<String>,
    project_name: Option<String>,
}

fn read_raw_task_row(r: &rusqlite::Row) -> rusqlite::Result<RawTaskRow> {
    Ok(RawTaskRow {
        id: r.get("id")?,
        data: r.get("data")?,
        status: r.get("status")?,
        description: r.get("description")?,
        priority: r.get("priority")?,
        entry_at: r.get("entry_at")?,
        modified_at: r.get("modified_at")?,
        due_at: r.get("due_at")?,
        scheduled_at: r.get("scheduled_at")?,
        start_at: r.get("start_at")?,
        end_at: r.get("end_at")?,
        wait_at: r.get("wait_at")?,
        parent_id: r.get("parent_id")?,
        project_name: r.get("project_name")?,
    })
}

fn raw_to_task(raw: RawTaskRow) -> Result<(Uuid, TaskMap)> {
    let uuid = Uuid::parse_str(&raw.id)
        .map_err(|e| Error::Database(format!("Invalid UUID in tc_tasks: {e}")))?;
    let mut task_map: TaskMap = serde_json::from_str(&raw.data)
        .map_err(|e| Error::Database(format!("Failed to parse task data: {e}")))?;

    // Inject string columns back into the task map.
    if let Some(v) = raw.status {
        task_map.insert("status".into(), v);
    }
    if let Some(v) = raw.description {
        task_map.insert("description".into(), v);
    }
    if let Some(v) = raw.priority {
        task_map.insert("priority".into(), v);
    }
    if let Some(v) = raw.parent_id {
        task_map.insert("parent".into(), v);
    }
    if let Some(v) = raw.project_name {
        task_map.insert("project".into(), v);
    }

    // Inject timestamp columns (ISO 8601 → epoch string).
    // An ISO value present in the DB but failing to parse is a data integrity error.
    for (col_val, taskmap_key) in [
        (raw.entry_at, "entry"),
        (raw.modified_at, "modified"),
        (raw.due_at, "due"),
        (raw.scheduled_at, "scheduled"),
        (raw.start_at, "start"),
        (raw.end_at, "end"),
        (raw.wait_at, "wait"),
    ] {
        if let Some(iso) = col_val {
            let epoch = iso_to_epoch(&iso).ok_or_else(|| {
                Error::Database(format!(
                    "Malformed ISO timestamp in column {taskmap_key}_at: {iso:?}"
                ))
            })?;
            task_map.insert(taskmap_key.into(), epoch);
        }
    }

    Ok((uuid, task_map))
}

/// Shared column projection for all tc_tasks queries (requires `t` and `p` aliases).
const TASK_SELECT_COLS: &str = "t.id, t.data, t.status, t.description, t.priority, \
    t.entry_at, t.modified_at, t.due_at, t.scheduled_at, \
    t.start_at, t.end_at, t.wait_at, t.parent_id, p.name as project_name";

pub(super) struct PowerSyncStorageInner {
    conn: Connection,
    user_id: Uuid,
}

impl PowerSyncStorageInner {
    /// Open an existing PowerSync-managed database file and create local-only tables.
    pub(super) fn new(db_path: &Path, user_id: Uuid) -> Result<Self> {
        // Register the PowerSync extension as a SQLite auto-extension.
        // Safe to call multiple times — subsequent calls are no-ops.
        let rc = powersync_init_static();
        if rc != 0 {
            return Err(Error::Database(format!(
                "Failed to load PowerSync extension (rc={rc})"
            )));
        }

        // Open the connection. The auto-extension fires on open, registering all
        // PowerSync functions (powersync_strip_subtype, etc.).
        let conn = Connection::open(db_path)?;

        // Verify the DB has been initialized by flicknote-sync (tc_tasks view must exist).
        let has_tc_tasks: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='view' AND name='tc_tasks'",
                [],
                |r| r.get(0),
            )
            .context("Checking for tc_tasks view")?;
        if !has_tc_tasks {
            return Err(Error::Database(
                "tc_tasks view not found — the database must be initialized by flicknote-sync \
                 before flicktask can use it. Run flicknote-sync first to set up PowerSync views."
                    .into(),
            ));
        }

        // Belt-and-suspenders: ensure WAL mode and busy_timeout for multi-process safety.
        // flicknote-sync already sets WAL (persists in DB header), but set it explicitly
        // in case it was somehow reset. busy_timeout is per-connection — must always be set.
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("Setting WAL mode")?;
        conn.pragma_update(None, "busy_timeout", 30_000)
            .context("Setting busy timeout")?;

        // Initialize PowerSync internal tables (ps_migration, ps_oplog, etc.).
        // This does NOT create user-facing views — those already exist from flicknote-sync.
        // We intentionally do NOT call powersync_replace_schema here because it performs
        // a FULL REPLACE — it would drop views for notes, projects, note_extractions
        // that flicknote-sync registered. We only need the extension functions loaded
        // (which happened at Connection::open via auto-extension).
        conn.prepare("SELECT powersync_init()")?
            .query_row([], |_| Ok(()))
            .context("PowerSync init")?;

        // Create local-only tables that PowerSync doesn't manage.
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS tc_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                data TEXT NOT NULL,
                synced BOOLEAN NOT NULL DEFAULT false,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS tc_working_set (
                id INTEGER PRIMARY KEY,
                uuid TEXT
            );
            CREATE TABLE IF NOT EXISTS tc_sync_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        ",
        )
        .context("Creating local-only tables")?;

        Ok(Self { conn, user_id })
    }

    /// Create an in-memory database with all required tables for testing.
    #[cfg(test)]
    pub(super) fn new_for_test() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS tc_tasks (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                data TEXT NOT NULL DEFAULT '{}',
                entry_at TEXT,
                status TEXT,
                description TEXT,
                priority TEXT,
                modified_at TEXT,
                due_at TEXT,
                scheduled_at TEXT,
                start_at TEXT,
                end_at TEXT,
                wait_at TEXT,
                parent_id TEXT,
                project_id TEXT
            );
            CREATE TABLE IF NOT EXISTS tc_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                data TEXT NOT NULL,
                synced BOOLEAN NOT NULL DEFAULT false,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS tc_working_set (
                id INTEGER PRIMARY KEY,
                uuid TEXT
            );
            CREATE TABLE IF NOT EXISTS tc_sync_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT,
                user_id TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        ",
        )
        .context("Creating PowerSync test tables")?;
        Ok(Self {
            conn,
            user_id: Uuid::nil(),
        })
    }
}

#[async_trait(?Send)]
impl WrappedStorage for PowerSyncStorageInner {
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn WrappedStorageTxn + 'a>> {
        let txn = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Ok(Box::new(PowerSyncTxn {
            txn: Some(txn),
            user_id: self.user_id,
        }))
    }
}

pub(super) struct PowerSyncTxn<'t> {
    txn: Option<rusqlite::Transaction<'t>>,
    user_id: Uuid,
}

impl<'t> PowerSyncTxn<'t> {
    fn get_txn(&self) -> Result<&rusqlite::Transaction<'t>> {
        self.txn
            .as_ref()
            .ok_or_else(|| Error::Database("Transaction already committed".into()))
    }

    fn get_next_working_set_number(&self) -> Result<usize> {
        let t = self.get_txn()?;
        let next_id: Option<usize> = t
            .query_row(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM tc_working_set",
                [],
                |r| r.get(0),
            )
            .optional()
            .context("Getting highest working set ID")?;
        Ok(next_id.unwrap_or(0))
    }
}

#[async_trait(?Send)]
impl WrappedStorageTxn for PowerSyncTxn<'_> {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        let raw_opt: Option<RawTaskRow> = {
            let t = self.get_txn()?;
            let sql = format!(
                "SELECT {TASK_SELECT_COLS}
                 FROM tc_tasks t
                 LEFT JOIN projects p ON t.project_id = p.id
                 WHERE t.id = ? LIMIT 1"
            );
            t.query_row(&sql, [&uuid.to_string()], read_raw_task_row)
                .optional()?
        };
        match raw_opt {
            None => Ok(None),
            Some(raw) => {
                let (_, task_map) = raw_to_task(raw)?;
                Ok(Some(task_map))
            }
        }
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let rows: Vec<RawTaskRow> = {
            let t = self.get_txn()?;
            let sql = format!(
                "SELECT {TASK_SELECT_COLS}
                 FROM tc_working_set ws
                 INNER JOIN tc_tasks t ON ws.uuid = t.id
                 LEFT JOIN projects p ON t.project_id = p.id
                 WHERE ws.uuid IS NOT NULL"
            );
            let mut q = t.prepare(&sql)?;
            let rows = q.query_map([], read_raw_task_row)?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        rows.into_iter().map(raw_to_task).collect()
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        let t = self.get_txn()?;
        let count: usize = t.query_row(
            "SELECT count(id) FROM tc_tasks WHERE id = ?",
            [&uuid.to_string()],
            |x| x.get(0),
        )?;
        if count > 0 {
            return Ok(false);
        }
        t.execute(
            "INSERT INTO tc_tasks (id, user_id, data) VALUES (?, ?, '{}')",
            params![&uuid.to_string(), &self.user_id.to_string()],
        )
        .context("Create task query")?;
        Ok(true)
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        let mut task_data = task;
        let user_id_str = self.user_id.to_string();

        // Extract string columns.
        let status = task_data.remove("status");
        let description = task_data.remove("description");
        let priority = task_data.remove("priority");
        let parent_id = task_data.remove("parent");

        // Extract and convert timestamp columns. An Err propagates immediately,
        // aborting set_task before any DB write, so no partial state is committed.
        let entry_at = extract_timestamp(&mut task_data, "entry")?;
        let modified_at = extract_timestamp(&mut task_data, "modified")?;
        let due_at = extract_timestamp(&mut task_data, "due")?;
        let scheduled_at = extract_timestamp(&mut task_data, "scheduled")?;
        let start_at = extract_timestamp(&mut task_data, "start")?;
        let end_at = extract_timestamp(&mut task_data, "end")?;
        let wait_at = extract_timestamp(&mut task_data, "wait")?;

        // Resolve project name → project_id (look up or create in projects table).
        let project_name = task_data.remove("project");
        let project_id: Option<String> = if let Some(ref name) = project_name {
            let existing: Option<String> = {
                let t = self.get_txn()?;
                t.query_row(
                    "SELECT id FROM projects WHERE name = ? ORDER BY created_at LIMIT 1",
                    [name],
                    |r| r.get(0),
                )
                .optional()?
            };
            if existing.is_some() {
                existing
            } else {
                let new_id = Uuid::new_v4().to_string();
                let t = self.get_txn()?;
                t.execute(
                    "INSERT OR IGNORE INTO projects (id, name, user_id) VALUES (?, ?, ?)",
                    params![&new_id, name, &user_id_str],
                )?;
                // If INSERT was ignored (PK collision, astronomically unlikely with UUIDs),
                // re-query to get the authoritative ID rather than returning a dangling ref.
                if t.changes() == 0 {
                    t.query_row(
                        "SELECT id FROM projects WHERE name = ? ORDER BY created_at LIMIT 1",
                        [name],
                        |r| r.get(0),
                    )
                    .optional()?
                } else {
                    Some(new_id)
                }
            }
        } else {
            None
        };

        let data_str = serde_json::to_string(&task_data)
            .map_err(|e| Error::Database(format!("Failed to serialize task data: {e}")))?;

        // Use upsert (INSERT ... ON CONFLICT DO UPDATE) rather than INSERT OR REPLACE.
        // INSERT OR REPLACE performs a DELETE + INSERT, which resets any columns not in
        // the INSERT list and may interfere with PowerSync's change-tracking triggers.
        let t = self.get_txn()?;
        t.execute(
            "INSERT INTO tc_tasks
             (id, user_id, data, status, description, priority,
              entry_at, modified_at, due_at, scheduled_at, start_at, end_at, wait_at,
              parent_id, project_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
               user_id = excluded.user_id,
               data = excluded.data,
               status = excluded.status,
               description = excluded.description,
               priority = excluded.priority,
               entry_at = excluded.entry_at,
               modified_at = excluded.modified_at,
               due_at = excluded.due_at,
               scheduled_at = excluded.scheduled_at,
               start_at = excluded.start_at,
               end_at = excluded.end_at,
               wait_at = excluded.wait_at,
               parent_id = excluded.parent_id,
               project_id = excluded.project_id",
            params![
                &uuid.to_string(),
                &user_id_str,
                &data_str,
                &status,
                &description,
                &priority,
                &entry_at,
                &modified_at,
                &due_at,
                &scheduled_at,
                &start_at,
                &end_at,
                &wait_at,
                &parent_id,
                &project_id,
            ],
        )
        .context("Set task query")?;
        Ok(())
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        let t = self.get_txn()?;
        let changed = t
            .execute(
                "DELETE FROM tc_tasks WHERE id = ?",
                [&uuid.to_string()],
            )
            .context("Delete task query")?;
        Ok(changed > 0)
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let rows: Vec<RawTaskRow> = {
            let t = self.get_txn()?;
            let sql = format!(
                "SELECT {TASK_SELECT_COLS}
                 FROM tc_tasks t
                 LEFT JOIN projects p ON t.project_id = p.id"
            );
            let mut q = t.prepare(&sql)?;
            let rows = q.query_map([], read_raw_task_row)?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        rows.into_iter().map(raw_to_task).collect()
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        let t = self.get_txn()?;
        let mut q = t.prepare("SELECT id FROM tc_tasks")?;
        let rows = q.query_map([], |r| r.get::<_, String>(0))?;
        let raw: Vec<std::result::Result<String, _>> = rows.collect();
        let mut ret = vec![];
        for r in raw {
            let s = r?;
            let uuid = Uuid::parse_str(&s)
                .map_err(|e| Error::Database(format!("Invalid UUID: {e}")))?;
            ret.push(uuid);
        }
        Ok(ret)
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        let t = self.get_txn()?;
        let version: Option<String> = t
            .query_row(
                "SELECT value FROM tc_sync_meta WHERE key = 'base_version'",
                [],
                |r| r.get("value"),
            )
            .optional()?;
        match version {
            None => Ok(DEFAULT_BASE_VERSION),
            Some(s) => Uuid::parse_str(&s)
                .map_err(|e| Error::Database(format!("Invalid base_version UUID: {e}"))),
        }
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        let t = self.get_txn()?;
        t.execute(
            "INSERT OR REPLACE INTO tc_sync_meta (key, value) VALUES ('base_version', ?)",
            [&version.to_string()],
        )
        .context("Set base version")?;
        Ok(())
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        // tc_operations has no UUID column (schema is PowerSync-managed).
        // Filter in memory after deserializing; acceptable for the expected operation count.
        let raw: Vec<String> = {
            let t = self.get_txn()?;
            let mut q = t.prepare("SELECT data FROM tc_operations ORDER BY id ASC")?;
            let rows = q.query_map([], |r| r.get::<_, String>("data"))?;
            rows.collect::<std::result::Result<_, _>>()?
        };

        let mut ret = vec![];
        for data_str in raw {
            let op: Operation = serde_json::from_str(&data_str)
                .map_err(|e| Error::Database(format!("Failed to parse operation: {e}")))?;
            if op.get_uuid() == Some(uuid) {
                ret.push(op);
            }
        }
        Ok(ret)
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        let raw: Vec<String> = {
            let t = self.get_txn()?;
            let mut q =
                t.prepare("SELECT data FROM tc_operations WHERE synced = false ORDER BY id ASC")?;
            let rows = q.query_map([], |r| r.get::<_, String>("data"))?;
            rows.collect::<std::result::Result<_, _>>()?
        };

        let mut ret = vec![];
        for data_str in raw {
            let op: Operation = serde_json::from_str(&data_str)
                .map_err(|e| Error::Database(format!("Failed to parse operation: {e}")))?;
            ret.push(op);
        }
        Ok(ret)
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        let t = self.get_txn()?;
        let count: usize = t.query_row(
            "SELECT COUNT(*) FROM tc_operations WHERE synced = false",
            [],
            |x| x.get(0),
        )?;
        Ok(count)
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        let data_str = serde_json::to_string(&op)
            .map_err(|e| Error::Database(format!("Failed to serialize operation: {e}")))?;
        let t = self.get_txn()?;
        t.execute(
            "INSERT INTO tc_operations (user_id, data) VALUES (?, ?)",
            params![&self.user_id.to_string(), &data_str],
        )
        .context("Add operation query")?;
        Ok(())
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        let last: Option<(i64, String)> = {
            let t = self.get_txn()?;
            t.query_row(
                "SELECT id, data FROM tc_operations WHERE synced = false ORDER BY id DESC LIMIT 1",
                [],
                |x| Ok((x.get(0)?, x.get(1)?)),
            )
            .optional()?
        };

        let Some((last_id, last_data)) = last else {
            return Err(Error::Database("No unsynced operations to remove".into()));
        };

        let last_op: Operation = serde_json::from_str(&last_data)
            .map_err(|e| Error::Database(format!("Failed to parse operation: {e}")))?;

        if last_op != op {
            return Err(Error::Database(format!(
                "Last operation does not match -- cannot remove \
                 (expected {op:?}, got {last_op:?})"
            )));
        }

        let t = self.get_txn()?;
        t.execute("DELETE FROM tc_operations WHERE id = ?", [last_id])
            .context("Remove operation")?;

        Ok(())
    }

    /// Mark all unsynced operations as synced and delete operations for tasks that no longer exist.
    /// Changes are not persisted until `commit()` is called.
    async fn sync_complete(&mut self) -> Result<()> {
        let t = self.get_txn()?;

        // Mark all locally-tracked operations as synced.
        t.execute(
            "UPDATE tc_operations SET synced = true WHERE synced = false",
            [],
        )
        .context("Marking operations as synced")?;

        // Delete operations for non-existent (deleted) tasks.
        // UndoPoint operations (where all json_extract paths return NULL) are excluded.
        t.execute(
            r#"DELETE FROM tc_operations WHERE id IN (
                SELECT o.id FROM tc_operations o
                LEFT JOIN tc_tasks t ON t.id = coalesce(
                    json_extract(o.data, '$.Create.uuid'),
                    json_extract(o.data, '$.Update.uuid'),
                    json_extract(o.data, '$.Delete.uuid')
                )
                WHERE t.id IS NULL
                AND coalesce(
                    json_extract(o.data, '$.Create.uuid'),
                    json_extract(o.data, '$.Update.uuid'),
                    json_extract(o.data, '$.Delete.uuid')
                ) IS NOT NULL
            )"#,
            [],
        )
        .context("Deleting orphaned operations")?;

        Ok(())
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        let t = self.get_txn()?;
        let mut q = t.prepare("SELECT id, uuid FROM tc_working_set ORDER BY id ASC")?;
        let rows = q
            .query_map([], |r| Ok((r.get::<_, usize>("id")?, r.get::<_, String>("uuid")?)))
            .context("Get working set query")?;
        let rows: Vec<std::result::Result<(usize, String), _>> = rows.collect();

        let mut res = Vec::new();
        for _ in 0..self
            .get_next_working_set_number()
            .context("Getting working set number")?
        {
            res.push(None);
        }

        for r in rows {
            let (id, uuid_str) = r?;
            let uuid = Uuid::parse_str(&uuid_str)
                .map_err(|e| Error::Database(format!("Invalid UUID in working set: {e}")))?;
            if id >= res.len() {
                return Err(Error::Database(format!(
                    "Working set id {id} is out of range ({} slots allocated)",
                    res.len()
                )));
            }
            res[id] = Some(uuid);
        }

        Ok(res)
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let next_id = self.get_next_working_set_number()?;
        let t = self.get_txn()?;
        t.execute(
            "INSERT INTO tc_working_set (id, uuid) VALUES (?, ?)",
            params![next_id, &uuid.to_string()],
        )
        .context("Add to working set query")?;
        Ok(next_id)
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        let t = self.get_txn()?;
        match uuid {
            Some(uuid) => t.execute(
                "INSERT OR REPLACE INTO tc_working_set (id, uuid) VALUES (?, ?)",
                params![index, &uuid.to_string()],
            ),
            None => t.execute(
                "DELETE FROM tc_working_set WHERE id = ?",
                [index],
            ),
        }
        .context("Set working set item query")?;
        Ok(())
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        let t = self.get_txn()?;
        t.execute("DELETE FROM tc_working_set", [])
            .context("Clear working set query")?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        let t = self
            .txn
            .take()
            .ok_or_else(|| Error::Database("Transaction already committed".into()))?;
        t.commit().context("Committing transaction")?;
        Ok(())
    }
}
