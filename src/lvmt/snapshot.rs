use crate::{
    errors::Result,
    middlewares::{
        table_schema::{KeyValueSnapshotRead, VersionedKeyValueSchema},
        CommitID,
    },
    traits::KeyValueStoreManager,
};

use super::{storage::LvmtStore, table_schema::FlatKeyValue};

pub struct LvmtSnapshot<'db> {
    key_value_view: Box<KeyValueSnapshotRead<'db, FlatKeyValue>>,
}

impl<'cache, 'db> LvmtStore<'cache, 'db> {
    pub fn get_state(&self, commit: CommitID) -> Result<LvmtSnapshot> {
        let key_value_view = self.get_key_value_store().get_versioned_store(&commit)?;

        Ok(LvmtSnapshot {
            key_value_view: Box::new(key_value_view),
        })
    }
}

impl<'db> LvmtSnapshot<'db> {
    pub fn get(
        &self,
        key: &<FlatKeyValue as VersionedKeyValueSchema>::Key,
    ) -> Result<Option<<FlatKeyValue as VersionedKeyValueSchema>::Value>> {
        self.key_value_view.get(key)
    }
}
