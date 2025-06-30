use crate::{
    errors::Result,
    middlewares::{table_schema::VersionedKeyValueSchema, CommitID, SnapshotView},
    traits::{KeyValueStoreManager, KeyValueStoreRead},
};

use super::{storage::LvmtStore, table_schema::FlatKeyValue};

pub struct LvmtSnapshot<'db> {
    key_value_view: SnapshotView<'db, FlatKeyValue>,
}

impl<'db> LvmtStore<'db> {
    pub fn get_state(&self, commit: CommitID) -> Result<LvmtSnapshot> {
        let key_value_view = self.get_key_value_store().get_versioned_store(&commit)?;

        Ok(LvmtSnapshot { key_value_view })
    }
}

impl<'db> LvmtSnapshot<'db> {
    pub fn get(
        &self,
        key: &<FlatKeyValue as VersionedKeyValueSchema>::Key,
    ) -> Result<Option<<FlatKeyValue as VersionedKeyValueSchema>::Value>> {
        self.key_value_view.get(key)
    }

    pub fn iter_prefix(
        &self,
        key_prefix: Box<[u8]>,
    ) -> Result<
        impl '_
            + Iterator<
                Item = (
                    <FlatKeyValue as VersionedKeyValueSchema>::Key,
                    <FlatKeyValue as VersionedKeyValueSchema>::Value,
                ),
            >,
    > {
        self.key_value_view.iter_prefix(key_prefix)
    }
}
