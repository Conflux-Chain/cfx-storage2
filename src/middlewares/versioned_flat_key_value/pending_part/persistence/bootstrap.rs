use thiserror::Error;

/// Errors that can occur during the schema bootstrapping process.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapError {
    #[error("Schema initialization failed because the database is not empty.")]
    DatabaseNotEmpty,
}

pub mod primitives {
    //! Contains low-level, generic primitives for the bootstrapping process.
    //!
    //! **Warning**: These functions are low-level building blocks. You should generally
    //! prefer the high-level, encapsulated bootstrap functions provided by a concrete
    //! application (e.g., `lvmt`), unless you are building a new composite application
    //! yourself.

    use std::{borrow::Cow, sync::Arc};

    use super::{
        super::{
            DatabaseTrait, ModificationId, PendingKeyValueSchema, PendingTableName,
            PersistenceTracker, Result, SnapshotId, SnapshotKey, SnapshotValue, SnapshotsTable,
            TableRead, Tree, TreeWithTracker, WalTable, WriteSchemaTrait,
        },
        BootstrapError,
    };

    /// Clears all existing data (snapshots and WALs) for a specific schema.
    ///
    /// This is a low-level primitive. See the [module-level documentation](self) for usage guidelines.
    ///
    /// # TODO
    /// A more efficient implementation could drop and recreate the Column Family.
    pub fn clear_pending_schema<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
        db: &Arc<P>,
        write_schema: &impl WriteSchemaTrait<PendingTableName>,
    ) -> Result<()> {
        let snapshots_view = Arc::new(db.view::<SnapshotsTable<S>>()?);
        for item in snapshots_view.iter_from_start()? {
            write_schema.write::<SnapshotsTable<S>>((item?.0, None));
        }

        let wal_view = Arc::new(db.view::<WalTable<S>>()?);
        for item in wal_view.iter_from_start()? {
            write_schema.write::<WalTable<S>>((item?.0, None));
        }
        Ok(())
    }

    /// Verifies that all persistent tables for a specific schema `S` are empty.
    ///
    /// Returns an error if any data is found in the schema's tables.
    ///
    /// This is a low-level primitive used as a precondition for initialization.
    /// See the [module-level documentation](self) for usage guidelines.
    pub fn verify_schema_is_empty<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
        db: &Arc<P>,
    ) -> Result<()> {
        let snapshots_view = Arc::new(db.view::<SnapshotsTable<S>>()?);
        if snapshots_view.iter_from_start()?.next().is_some() {
            return Err(BootstrapError::DatabaseNotEmpty)?;
        }

        let wal_view = Arc::new(db.view::<WalTable<S>>()?);
        if wal_view.iter_from_start()?.next().is_some() {
            return Err(BootstrapError::DatabaseNotEmpty)?;
        }

        Ok(())
    }

    /// Creates the initial state for a specific schema, assuming the database is empty.
    ///
    /// This function does not perform any checks. It is the caller's responsibility to ensure
    /// that the underlying tables for this schema are empty before calling.
    ///
    /// This is a low-level primitive. See the [module-level documentation](self) for usage guidelines.
    pub fn initialize_empty_schema<S: PendingKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
        write_schema: &impl WriteSchemaTrait<PendingTableName>,
        parent_of_root: Option<S::CommitId>,
        height_of_root: u64,
    ) -> Result<TreeWithTracker<S>> {
        // Create a new empty tree.
        let tree = Tree::new(parent_of_root, height_of_root);

        // Create the very first snapshot (id=0) for this new tree.
        let initial_snapshot_id = SnapshotId(0);
        let key = SnapshotKey(height_of_root);
        let value = SnapshotValue {
            parent_of_root,
            snapshot_id: initial_snapshot_id,
        };
        let snapshot_op = (Cow::Owned(key), Some(Cow::Owned(value)));
        write_schema.write::<SnapshotsTable<S>>(snapshot_op);

        let tracker = PersistenceTracker {
            snapshot_id: initial_snapshot_id,
            next_modification_id: ModificationId(0), // No modifications yet.
        };

        Ok(TreeWithTracker { tree, tracker })
    }
}
