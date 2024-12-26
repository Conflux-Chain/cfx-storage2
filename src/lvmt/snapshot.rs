use ethereum_types::H256;

use crate::{
    errors::Result,
    middlewares::{KeyValueStoreBulks, SnapshotView},
    traits::KeyValueStoreManager,
};

use super::{
    auth_changes::AuthChangeTable,
    storage::LvmtStore,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
};

pub struct LvmtSnapshot<'db> {
    key_value_view: SnapshotView<'db, FlatKeyValue>,
    amt_node_view: SnapshotView<'db, AmtNodes>,
    slot_alloc_view: SnapshotView<'db, SlotAllocations>,
    auth_changes: &'db KeyValueStoreBulks<'db, AuthChangeTable>,
}

impl<'cache, 'db> LvmtStore<'cache, 'db> {
    fn get_versioned_store(&self, commit: H256) -> Result<LvmtSnapshot> {
        let amt_node_view = self.get_amt_node_store().get_versioned_store(&commit)?;
        let slot_alloc_view = self.get_slot_alloc_store().get_versioned_store(&commit)?;
        let key_value_view = self.get_key_value_store().get_versioned_store(&commit)?;
        let auth_changes = self.get_auth_changes();

        Ok(LvmtSnapshot {
            key_value_view,
            amt_node_view,
            slot_alloc_view,
            auth_changes,
        })
    }
}
