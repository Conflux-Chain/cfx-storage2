use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use amt::AmtParams;

use super::{
    amt_change_manager::AmtChangeManager,
    auth_changes::{amt_change_hash, key_value_hash, process_dump_items, AuthChangeTable},
    crypto::PE,
    state_root::{StateRoot, StateRootTable},
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
    types::{AllocatePosition, AmtNodeId},
};
use crate::{
    backends::{DatabaseTrait, PendingTableName, TableRead, WriteSchemaTrait},
    errors::Result,
    lvmt::types::{compute_amt_node_id, AllocationKeyInfo, KEY_SLOT_SIZE},
    middlewares::{table_schema::KeyValueSnapshotRead, CommitID},
    traits::KeyValueStoreBulksTrait,
};
use crate::{
    lvmt::types::LvmtValue,
    middlewares::{KeyValueStoreBulks, VersionedStore},
    traits::{KeyValueStoreManager, KeyValueStoreRead},
    utils::hash::blake2s,
};

pub struct LvmtStore<'db, P: DatabaseTrait<PendingTableName>> {
    pending_persistence_backend: Arc<P>,
    key_value_store: VersionedStore<'db, FlatKeyValue, P>,
    amt_node_store: VersionedStore<'db, AmtNodes, P>,
    slot_alloc_store: VersionedStore<'db, SlotAllocations, P>,
    auth_changes: KeyValueStoreBulks<'db, AuthChangeTable>,
}

const ALLOC_START_VERSION: u64 = 1;
type KeyValueVec = Vec<(Box<[u8]>, LvmtValue)>;

// Read-only
impl<'db, P: DatabaseTrait<PendingTableName>> LvmtStore<'db, P> {
    /// Get the state root of the given commit.
    /// If not found in the pending persistence db, return `None`.
    pub fn get_state_root(&self, commit: CommitID) -> Result<Option<StateRoot>> {
        let state_root_view = Arc::new(self.pending_persistence_backend.view::<StateRootTable>()?);
        Ok(state_root_view.get(&commit)?.map(|x| x.into_owned()))
    }

    pub fn get(&self, commit: CommitID, key: Box<[u8]>) -> Result<Option<LvmtValue>> {
        self.get_state(commit)?.get(&key)
    }

    pub fn iter_range(
        &self,
        commit: CommitID,
        lower_bound_incl: Box<[u8]>,
        upper_bound_excl: Option<Box<[u8]>>,
    ) -> Result<KeyValueVec> {
        Ok(self
            .get_state(commit)?
            .iter_range(lower_bound_incl, upper_bound_excl)?
            .collect::<Vec<_>>())
    }

    pub fn new(
        pending_persistence_backend: Arc<P>,
        key_value_store: VersionedStore<'db, FlatKeyValue, P>,
        amt_node_store: VersionedStore<'db, AmtNodes, P>,
        slot_alloc_store: VersionedStore<'db, SlotAllocations, P>,
        auth_changes: KeyValueStoreBulks<'db, AuthChangeTable>,
    ) -> Self {
        Self {
            pending_persistence_backend,
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        }
    }

    pub fn is_newer_than_pending_root(&self, height: u64) -> bool {
        let height_of_root = self.key_value_store.get_height_of_root();
        height > height_of_root
    }

    pub fn query_commit_existence(&self, commit: &CommitID) -> Result<bool> {
        self.key_value_store.query_commit_existence(commit)
    }

    // check whether `commit_id` is already in historical part
    // TODO: this function should be invoked in many interfaces
    fn is_in_historical_part(&self, commit: &CommitID) -> Result<bool> {
        self.key_value_store.is_in_historical_part(commit)
    }
}

// Write
impl<'db, P: DatabaseTrait<PendingTableName>> LvmtStore<'db, P> {
    pub fn checkout_current(&mut self, commit: CommitID) -> Result<()> {
        self.key_value_store.checkout_current(commit)
    }

    fn commit_to_pending_db(&self, pending_write_schema: P::WriteSchema) -> Result<()> {
        self.pending_persistence_backend
            .commit(pending_write_schema)
    }

    /// This function discards the siblings of the nodes from the root (excluded) to `commit_id` (included).
    /// If there is at least one node discarded, return `Ok(true)`; otherwise, return `Ok(false)`.
    /// The input `commit_id` may be already in historical part, so the first thing is to check this.
    pub fn make_pivot(&mut self, commit_id: CommitID) -> Result<bool> {
        let pending_write_schema = P::write_schema();

        if self.is_in_historical_part(&commit_id)? {
            return Ok(false);
        }

        let key_value_has_discarded_nodes = self
            .key_value_store
            .make_pivot(commit_id, &pending_write_schema)?;
        let amt_node_has_discarded_nodes = self
            .amt_node_store
            .make_pivot(commit_id, &pending_write_schema)?;
        let slot_alloc_has_discarded_nodes = self
            .slot_alloc_store
            .make_pivot(commit_id, &pending_write_schema)?;

        if (key_value_has_discarded_nodes != amt_node_has_discarded_nodes)
            || (key_value_has_discarded_nodes != slot_alloc_has_discarded_nodes)
        {
            return Err(crate::StorageError::ConsistencyCheckFailure);
        }

        self.commit_to_pending_db(pending_write_schema)?;

        Ok(key_value_has_discarded_nodes)
    }

    /// Commits all transient data for a new commit ID to the pending database in a single
    /// atomic operation. The update to the in-memory instance is also
    /// performed here, making this function the designated place for these changes.
    ///
    /// The `AuthChange` and `StateRoot` tables are now managed within the `pending_db`.
    /// This is because they are generated for every new commit, which aligns with the
    /// high-frequency write pattern of the pending database, but not with the less
    /// frequent confirmation cycle of the historical database. This architectural change
    /// allows all related data for a commit to be written within a single `pending_write_schema`.
    ///
    /// To ensure that all five data components (`key-values`, `AMT nodes`, `slot allocations`,
    /// `auth changes`, and the `state root`) are written atomically, the `state_root` is
    /// passed in as a parameter. This enables the function to create and manage the
    /// `pending_write_schema` internally, rather than delegating that responsibility to
    /// the caller.
    ///
    /// TODO: If the historical database ever needs this information, a process can be added
    /// to copy the `AuthChange` and `StateRoot` data from the pending DB to the historical DB
    /// when a state is confirmed. Currently, there is no such requirement.
    pub fn commit(
        &self,
        old_commit: Option<CommitID>,
        new_commit: CommitID,
        state_root: StateRoot,
        changes: impl Iterator<Item = (Box<[u8]>, Option<Box<[u8]>>)>,
        pp: &AmtParams<PE>,
    ) -> Result<()> {
        let (amt_node_view, slot_alloc_view, key_value_view) = if let Some(old_commit) = old_commit
        {
            (
                Some(self.amt_node_store.get_versioned_store(&old_commit)?),
                Some(self.slot_alloc_store.get_versioned_store(&old_commit)?),
                Some(self.key_value_store.get_versioned_store(&old_commit)?),
            )
        } else {
            (None, None, None)
        };

        let mut key_value_changes = vec![];
        let mut allocations = AllocationCacheDb::new(&slot_alloc_view);
        let mut amt_change_manager = AmtChangeManager::default();

        let mut set_of_keys = HashSet::new();

        // Update version number
        for (key, value) in changes {
            // skip the duplicated keys
            if !set_of_keys.insert(key.clone()) {
                continue;
            }

            let (allocation, version) = if let Some(old_value) = key_value_view.get(&key)? {
                (old_value.allocation, old_value.version + 1)
            } else {
                let allocation = allocate_version_slot(&key, &mut allocations)?;
                (allocation, ALLOC_START_VERSION)
            };

            amt_change_manager.record_with_allocation(allocation, &key);

            key_value_changes.push((
                key,
                LvmtValue {
                    allocation,
                    version,
                    value,
                },
            ));
        }

        let amt_changes = amt_change_manager.compute_amt_changes(&amt_node_view, pp)?;

        // Update auth changes
        let auth_changes = {
            let auth_change_iter = amt_changes
                .iter()
                .filter(|&(amt_id, curve_point)| (amt_id.len() > 0))
                .map(|(amt_id, curve_point)| amt_change_hash(amt_id, curve_point));
            let key_value_iter = key_value_changes
                .iter()
                .map(|(key, value)| key_value_hash(key, value));

            let hashes = key_value_iter.chain(auth_change_iter).collect();
            process_dump_items(hashes)
        };

        // Write to the pending part of db.
        // TODO: Write to the history part is beyond the range of LvmtStore.
        let pending_write_schema = P::write_schema();

        // write amt_node schema
        let amt_node_updates: HashMap<_, _> =
            amt_changes.into_iter().map(|(k, v)| (k, Some(v))).collect();
        self.amt_node_store.add_to_pending_part(
            old_commit,
            new_commit,
            amt_node_updates,
            &pending_write_schema,
        )?;

        // write key_value schema
        let key_value_updates: HashMap<_, _> = key_value_changes
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.key_value_store.add_to_pending_part(
            old_commit,
            new_commit,
            key_value_updates,
            &pending_write_schema,
        )?;

        // write slot_alloc schema
        let slot_alloc_updates: HashMap<_, _> = allocations
            .into_changes()
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.slot_alloc_store.add_to_pending_part(
            old_commit,
            new_commit,
            slot_alloc_updates,
            &pending_write_schema,
        )?;

        // write auth_change
        let auth_change_bulk = auth_changes.into_iter().map(|(k, v)| (k, Some(v)));
        // TODO: Will there be a situation where the same commit but different content occurs?
        self.auth_changes
            .commit(new_commit, auth_change_bulk, &pending_write_schema)?;

        // write state_root
        pending_write_schema
            .write::<StateRootTable>((Cow::Owned(new_commit), Some(Cow::Owned(state_root))));

        self.commit_to_pending_db(pending_write_schema)?;

        Ok(())
    }
}

struct AllocationCacheDb<'db> {
    db: &'db KeyValueSnapshotRead<'db, SlotAllocations>,
    cache: HashMap<AmtNodeId, AllocationKeyInfo>,
}

impl<'db> AllocationCacheDb<'db> {
    fn new(db: &'db KeyValueSnapshotRead<SlotAllocations>) -> Self {
        Self {
            db,
            cache: Default::default(),
        }
    }

    fn get(&self, amt_node_id: &AmtNodeId) -> Result<Option<AllocationKeyInfo>> {
        match self.cache.get(amt_node_id) {
            Some(cached_value) => Ok(Some(cached_value.clone())),
            None => Ok(self.db.get(amt_node_id)?),
        }
    }

    fn set(&mut self, amt_node_id: AmtNodeId, alloc_info: AllocationKeyInfo) {
        self.cache.insert(amt_node_id, alloc_info);
    }

    fn into_changes(self) -> HashMap<AmtNodeId, AllocationKeyInfo> {
        self.cache
    }
}

fn allocate_version_slot(
    key: &[u8],
    allocation_cache_db: &mut AllocationCacheDb,
) -> Result<AllocatePosition> {
    let key_digest = blake2s(key);

    let mut depth = 1;
    loop {
        let amt_node_id = compute_amt_node_id(key_digest, depth);
        let slot_alloc = allocation_cache_db.get(&amt_node_id)?;
        let next_index = match slot_alloc {
            None => 0,
            Some(x) if (x.index as usize) < KEY_SLOT_SIZE - 1 => x.index + 1,
            _ => {
                depth += 1;
                continue;
            }
        };

        allocation_cache_db.set(amt_node_id, AllocationKeyInfo::new(next_index, key.into()));

        return Ok(AllocatePosition {
            depth: depth as u8,
            slot_index: next_index as u8,
        });
    }
}

impl<'db, P: DatabaseTrait<PendingTableName>> LvmtStore<'db, P> {
    pub fn get_key_value_store(&self) -> &VersionedStore<'db, FlatKeyValue, P> {
        &self.key_value_store
    }

    #[cfg(test)]
    pub fn get_amt_node_store(&self) -> &VersionedStore<'db, AmtNodes, P> {
        &self.amt_node_store
    }

    #[cfg(test)]
    pub fn get_slot_alloc_store(&self) -> &VersionedStore<'db, SlotAllocations, P> {
        &self.slot_alloc_store
    }
}
