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

    use std::sync::Arc;

    use log::warn;

    use crate::middlewares::versioned_flat_key_value::pending_part::persistence::write_tree_snapshot;

    use super::{
        super::{
            DatabaseTrait, ModificationId, PendingKeyValueSchema, PendingTableName,
            PersistenceTracker, Result, SnapshotId, SnapshotsTable, TableRead, Tree,
            TreeWithTracker, WalTable, WriteSchemaTrait,
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
        write_schema: &P::WriteSchema,
    ) -> Result<()> {
        warn!(
            "Clearing all pending data for schema {:?} as a remediation step. \
            This is an intentional action, likely triggered by a prior recovery failure, \
            to discard potentially inconsistent data.",
            S::KV_NAME
        );

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
            Err(BootstrapError::DatabaseNotEmpty)?
        }

        let wal_view = Arc::new(db.view::<WalTable<S>>()?);
        if wal_view.iter_from_start()?.next().is_some() {
            Err(BootstrapError::DatabaseNotEmpty)?
        }

        Ok(())
    }

    /// Creates the initial state for a specific schema, assuming the database is empty.
    ///
    /// This function does not perform any checks. It is the caller's responsibility to ensure
    /// that the underlying tables for this schema are empty before calling.
    ///
    /// This is a low-level primitive. See the [module-level documentation](self) for usage guidelines.
    pub fn initialize_empty_schema<S: PendingKeyValueSchema>(
        write_schema: &impl WriteSchemaTrait<PendingTableName>,
        parent_of_root: Option<S::CommitId>,
        height_of_root: u64,
    ) -> TreeWithTracker<S> {
        // Create a new empty tree.
        let tree = Tree::new(parent_of_root, height_of_root);

        // Create the very first snapshot (id=0) for this new tree.
        let initial_snapshot_id = SnapshotId(0);
        write_tree_snapshot(&tree, initial_snapshot_id, write_schema);

        let tracker = PersistenceTracker {
            snapshot_id: initial_snapshot_id,
            next_modification_id: ModificationId(0), // No modifications yet.
        };

        TreeWithTracker { tree, tracker }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        format::{
            ModificationId, SnapshotId, SnapshotKey, SnapshotRecordType, SnapshotValue,
            SnapshotsTable, WalKey, WalKeySpecificPart, WalTable, WalValue,
        },
        test_util::TestSchema,
        DatabaseTrait, Decode, PendingTableName, StorageError, TableSchema, WrappedInMemoryDb,
        WriteSchemaTrait,
    };
    use super::primitives::*;
    use super::*;
    use core::panic;
    use ethereum_types::H256;
    use std::{borrow::Cow, sync::Arc};

    #[test]
    fn test_verify_schema_is_empty_when_truly_empty() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());

        // Act & Assert
        let result = verify_schema_is_empty::<TestSchema, _>(&db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_schema_is_empty_when_not_empty() {
        // Arrange
        let db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();

        // Add some data to make it non-empty. No need to make the data valid.
        let wal_key = WalKey::<TestSchema> {
            snapshot_id: SnapshotId(0),
            modification_id: ModificationId(0),
            operation_specific_parts: WalKeySpecificPart::AddNodeMeta,
        };
        let wal_value = WalValue::<TestSchema>::MetaValue {
            commit_id: H256::zero(),
            maybe_parent_cid: None,
            map_value_count: 0,
        };
        let op = (Cow::Owned(wal_key), Some(Cow::Owned(wal_value)));
        write_schema.write::<WalTable<TestSchema>>(op);
        db.commit(write_schema).unwrap();

        // Act & Assert
        let result = verify_schema_is_empty::<TestSchema, _>(&db);
        assert!(matches!(
            result,
            Err(StorageError::BootstrapError(
                BootstrapError::DatabaseNotEmpty
            ))
        ));
    }

    #[test]
    fn test_initialize_empty_schema() {
        // Arrange
        let db = WrappedInMemoryDb::<PendingTableName>::empty();
        let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();
        let parent_of_root = Some(H256::from_low_u64_be(99));
        let height_of_root = 100;

        // Act
        let result =
            initialize_empty_schema::<TestSchema>(&write_schema, parent_of_root, height_of_root);

        // Assert
        // 1. Check returned tracker
        assert_eq!(result.tracker.snapshot_id.0, 0);
        assert_eq!(result.tracker.next_modification_id.0, 0);

        // 2. Check written data
        let ops = write_schema.drain();
        assert_eq!(ops.len(), 1);
        let op = &ops[0];
        assert_eq!(op.0, SnapshotsTable::<TestSchema>::NAME);

        let key = SnapshotKey::<TestSchema>::decode_owned(op.1.clone()).unwrap();
        let value = SnapshotValue::<TestSchema>::decode_owned(op.2.clone().unwrap()).unwrap();

        assert_eq!(key.snapshot_root_height, height_of_root);
        assert_eq!(key.record_type, SnapshotRecordType::Meta);
        let SnapshotValue::MetaValue {
            parent_of_root: written_parent_of_root,
            snapshot_id,
            nodes_count,
        } = value
        else {
            panic!()
        };
        assert_eq!(written_parent_of_root, parent_of_root);
        assert_eq!(snapshot_id.0, 0);
        assert_eq!(nodes_count, 0);
    }
}
