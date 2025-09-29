use crate::{
    backends::{DatabaseTrait, PendingTableName},
    errors::Result,
    middlewares::{table_schema::VersionedKeyValueSchema, CommitID, SnapshotView},
    traits::{KeyValueStoreManager, KeyValueStoreRead},
};

use super::{storage::LvmtStore, table_schema::FlatKeyValue};

pub struct LvmtSnapshot<'a, 'db, P: DatabaseTrait<PendingTableName>> {
    key_value_view: SnapshotView<'a, 'db, FlatKeyValue, P>,
}

impl<'cache, 'db, P: DatabaseTrait<PendingTableName>> LvmtStore<'cache, 'db, P> {
    pub fn get_state(&self, commit: CommitID) -> Result<LvmtSnapshot<P>> {
        let key_value_view = self.get_key_value_store().get_versioned_store(&commit)?;

        Ok(LvmtSnapshot { key_value_view })
    }
}

impl<'a, 'db, P: DatabaseTrait<PendingTableName>> LvmtSnapshot<'a, 'db, P> {
    pub fn get(
        &self,
        key: &<FlatKeyValue as VersionedKeyValueSchema>::Key,
    ) -> Result<Option<<FlatKeyValue as VersionedKeyValueSchema>::Value>> {
        self.key_value_view.get(key)
    }

    pub fn iter_range(
        &self,
        lower_bound_incl: Box<[u8]>,
        upper_bound_excl: Option<Box<[u8]>>,
    ) -> Result<
        impl '_
            + Iterator<
                Item = (
                    <FlatKeyValue as VersionedKeyValueSchema>::Key,
                    <FlatKeyValue as VersionedKeyValueSchema>::Value,
                ),
            >,
    > {
        self.key_value_view
            .iter_range(lower_bound_incl, upper_bound_excl)
    }
}
