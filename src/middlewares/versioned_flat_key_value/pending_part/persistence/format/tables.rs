use super::{
    super::{Decode, Encode, PendingKeyValueSchema, PendingTableName, TableSchema},
    snapshot_key::SnapshotKey,
    snapshot_value::SnapshotValue,
    wal_key::WalKey,
    wal_value::WalValue,
};

#[derive(Clone, Copy)]
pub struct SnapshotsTable<S: PendingKeyValueSchema>(S);

impl<S: PendingKeyValueSchema> TableSchema for SnapshotsTable<S>
where
    SnapshotValue<S>: Encode + Decode + ToOwned<Owned = SnapshotValue<S>>,
{
    type TableName = PendingTableName;
    const NAME: PendingTableName = PendingTableName::Snapshots(S::KV_NAME);
    type Key = SnapshotKey<S>;
    type Value = SnapshotValue<S>;
}

#[derive(Clone, Copy)]
pub struct WalTable<S: PendingKeyValueSchema>(S);

impl<S: PendingKeyValueSchema> TableSchema for WalTable<S> {
    type TableName = PendingTableName;
    const NAME: PendingTableName = PendingTableName::Wal(S::KV_NAME);
    type Key = WalKey<S>;
    type Value = WalValue<S>;
}
