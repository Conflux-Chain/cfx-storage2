use crate::{
    errors::Result,
    middlewares::{table_schema::KeyValueSnapshotRead, CommitID},
    traits::KeyValueStoreManager,
};

use super::{storage::LvmtStore, table_schema::FlatKeyValue};

pub struct LvmtSnapshot<'db> {
    key_value_view: Box<KeyValueSnapshotRead<'db, FlatKeyValue>>,
}

impl<'cache, 'db> LvmtStore<'cache, 'db> {
    fn get_state(&self, commit: CommitID) -> Result<LvmtSnapshot> {
        let key_value_view = self.get_key_value_store().get_versioned_store(&commit)?;

        Ok(LvmtSnapshot {
            key_value_view: Box::new(key_value_view),
        })
    }
}
