use crate::errors::Result;
use crate::storage::send_wrapper::Wrapper;
use crate::storage::{Storage, StorageTxn};
use async_trait::async_trait;
use std::path::Path;
use uuid::Uuid;

mod inner;
use inner::PowerSyncStorageInner;

/// PowerSyncStorage stores task data in a PowerSync-managed SQLite database.
///
/// PowerSync handles sync to a Postgres/Supabase backend transparently. This
/// storage layer reads and writes to the local SQLite file that PowerSync
/// maintains, using the same `send_wrapper` actor pattern as `SqliteStorage`.
pub struct PowerSyncStorage(Wrapper);

impl PowerSyncStorage {
    /// Open a PowerSync-managed database at `db_path` for the given `user_id`.
    ///
    /// The `tc_tasks` and `tc_operations` tables must already exist (created by
    /// PowerSync from its schema definition). Local-only tables (`tc_working_set`,
    /// `tc_sync_meta`, `tc_operations_sync`) are created automatically.
    pub async fn new(db_path: &Path, user_id: Uuid) -> Result<Self> {
        let path = db_path.to_path_buf();
        Ok(Self(
            Wrapper::new(async move || PowerSyncStorageInner::new(&path, user_id)).await?,
        ))
    }
}

#[async_trait]
impl Storage for PowerSyncStorage {
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>> {
        Ok(self.0.txn().await?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::Result;

    async fn storage() -> Result<PowerSyncStorage> {
        Ok(PowerSyncStorage(
            Wrapper::new(async || PowerSyncStorageInner::new_for_test()).await?,
        ))
    }

    crate::storage::test::storage_tests!(storage().await?);
}
