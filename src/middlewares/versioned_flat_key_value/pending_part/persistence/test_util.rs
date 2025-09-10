use std::{borrow::Cow, sync::Arc};

use ethereum_types::H256;

use super::{
    DatabaseTrait, ModificationId, PendingKeyValueConfig, PendingTableName, SnapshotId,
    SnapshotKey, SnapshotKeyTreePart, SnapshotMapValue, SnapshotNodeDataType, SnapshotRecordType,
    SnapshotValue, SnapshotsTable, VersionedKVName, VersionedKeyValueSchema, WalKey,
    WalKeySpecificPart, WalTable, WalValue, WrappedInMemoryDb, WriteSchemaTrait,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionedKVTestSchema;

impl VersionedKeyValueSchema for VersionedKVTestSchema {
    const NAME: VersionedKVName = VersionedKVName::FlatKV;
    type Key = Box<[u8]>;
    type Value = Box<[u8]>;
}

pub type TestSchema = PendingKeyValueConfig<VersionedKVTestSchema, H256>;

// Helper: construct keys quickly
pub fn k(bytes: &[u8]) -> Box<[u8]> {
    bytes.to_vec().into_boxed_slice()
}

// ... (The rest of your existing test_util code like VersionedKeyValueSchema, TestSchema, k, v) ...

/// Helper: Pre-populates a mock database with a series of snapshots and their WAL entries.
///
/// The data generated is "structurally valid" but not necessarily "logically valid".
/// This means the keys and values conform to the schema, which is sufficient for testing
/// operations like cleanup that don't rely on recovering the full tree state.
///
/// # Arguments
///
/// * `db` - The in-memory database to populate.
/// * `snapshots_meta` - A list of tuples describing each snapshot to create:
///   - `u64`: The block height of the snapshot (`snapshot_root_height`).
///   - `u64`: The unique ID for the snapshot (`snapshot_id`).
///   - `u64`: The number of WAL records to generate for this `snapshot_id`.
///   - `u64`: The number of node-related records (e.g., `NodeMeta`) to generate for this snapshot.
pub fn setup_db_with_snapshots_and_wals(
    db: &Arc<WrappedInMemoryDb<PendingTableName>>,
    snapshots_meta: &[(u64, u64, u64, u64)],
) {
    let write_schema = WrappedInMemoryDb::<PendingTableName>::write_schema();

    for &(height, sid, wal_count, nodes_count) in snapshots_meta {
        let snapshot_id = SnapshotId(sid);

        // 1. Write the main Snapshot Meta Record for this height.
        // This record links the height to a snapshot_id. Its structure is correct.
        let meta_key = SnapshotKey::<TestSchema> {
            snapshot_root_height: height,
            record_type: SnapshotRecordType::Meta,
        };
        let meta_value = SnapshotValue::MetaValue {
            parent_of_root: None, // This value is not relevant for the cleanup test.
            snapshot_id,
            nodes_count,
        };
        write_schema.write::<SnapshotsTable<TestSchema>>((
            Cow::Owned(meta_key),
            Some(Cow::Owned(meta_value)),
        ));

        // 2. Write associated Snapshot Map Records (representing nodes).
        // The cleanup function needs to delete all records for a given height, so we must create them
        // with the correct key structure.
        for i in 0..nodes_count {
            // Create a unique CID for each node to ensure distinct database keys.
            let commit_id = H256::from_low_u64_be(height * 1000 + sid * 100 + i);

            // Create a key for the node's metadata record. This now uses the correct complex enum structure.
            let node_key = SnapshotKey::<TestSchema> {
                snapshot_root_height: height,
                record_type: SnapshotRecordType::Map(SnapshotKeyTreePart {
                    // For simplicity in this mock setup, we can set node_height equal to the snapshot height.
                    // The exact value doesn't matter for the cleanup test.
                    node_height: height,
                    node_commit_id: commit_id,
                    node_data_type: SnapshotNodeDataType::NodeMeta,
                }),
            };

            // The corresponding value must now be a `SnapshotValue::MapValue(SnapshotMapValue::NodeMeta { ... })`.
            let node_value = SnapshotValue::MapValue(SnapshotMapValue::NodeMeta {
                parent_commit_id: None, // Not relevant for cleanup logic.
                modifications_count: 0, // Not relevant for cleanup logic.
            });

            write_schema.write::<SnapshotsTable<TestSchema>>((
                Cow::Owned(node_key),
                Some(Cow::Owned(node_value)),
            ));
        }

        // 3. Write the associated WAL Records for the snapshot_id.
        // This part was already structurally correct and needs no changes.
        for i in 0..wal_count {
            let wal_key = WalKey::<TestSchema> {
                snapshot_id,
                modification_id: ModificationId(i),
                // Use a simple, representative operation type.
                operation_specific_parts: WalKeySpecificPart::AddNodeMeta,
            };
            // Dummy value, as the content is not checked by the cleanup logic.
            let wal_value = WalValue::MetaValue {
                commit_id: H256::zero(),
                maybe_parent_cid: None,
                map_value_count: 0,
            };
            write_schema
                .write::<WalTable<TestSchema>>((Cow::Owned(wal_key), Some(Cow::Owned(wal_value))));
        }
    }

    // Commit all the generated records to the mock database.
    db.commit(write_schema).unwrap();
}
