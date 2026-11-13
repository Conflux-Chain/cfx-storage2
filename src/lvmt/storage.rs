use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use amt::AmtParams;

use super::{
    amt_change_manager::AmtChangeManager,
    auth_changes::{amt_change_hash, key_value_hash, process_dump_items, AuthChangeTable},
    checkpoint::{ProofConfig, RootLeafCheckpointTable, ROOT_LEAF_COUNT},
    crypto::PE,
    mmr::{bag_peaks, creating_leaf, MmrEntry, MmrProof, MmrTable, EMPTY_HROOT},
    state_root::{compute_state_root, StateRoot, StateRootTable, EMPTY_AUTH_CHANGE_ROOT},
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
    types::{
        AllocatePosition, AmtId, AmtNodeId, AuthChangeKey, AuthChangeNode, CurvePointWithVersion,
    },
};
use crate::{
    backends::{DatabaseTrait, PendingTableName, TableRead, TableReader, WriteSchemaTrait},
    errors::Result,
    lvmt::types::{compute_amt_node_id, AllocationKeyInfo, KEY_SLOT_SIZE},
    middlewares::{table_schema::KeyValueSnapshotRead, CommitID},
    traits::KeyValueStoreBulksTrait,
    StorageError,
};
use crate::{
    lvmt::types::LvmtValue,
    middlewares::{KeyValueStoreBulks, VersionedStore, VersionedStoreReader},
    traits::{KeyValueStoreManager, KeyValueStoreRead},
};
use ethereum_types::H256;

pub struct LvmtStore<'cache, 'db, P: DatabaseTrait<PendingTableName>> {
    pending_persistence_backend: Arc<P>,
    key_value_store: VersionedStore<'cache, 'db, FlatKeyValue, P>,
    amt_node_store: VersionedStore<'cache, 'db, AmtNodes, P>,
    slot_alloc_store: VersionedStore<'cache, 'db, SlotAllocations, P>,
    auth_changes: KeyValueStoreBulks<'static, AuthChangeTable>,
    checkpoints: TableReader<'static, RootLeafCheckpointTable>,
    proof_config: ProofConfig,
}

const ALLOC_START_VERSION: u64 = 1;
type KeyValueVec = Vec<(Box<[u8]>, LvmtValue)>;

// Read-only
impl<'cache, 'db, P: DatabaseTrait<PendingTableName>> LvmtStore<'cache, 'db, P> {
    /// A read-only view of this store, see [`LvmtStoreReader`].
    pub fn reader(&self) -> LvmtStoreReader<'_, 'db, P> {
        LvmtStoreReader {
            pending_persistence_backend: self.pending_persistence_backend.clone(),
            key_value_reader: self.key_value_store.as_reader(),
            amt_node_reader: self.amt_node_store.as_reader(),
            slot_alloc_reader: self.slot_alloc_store.as_reader(),
            auth_changes: self.auth_changes.clone(),
            checkpoints: self.checkpoints.clone(),
            proof_config: self.proof_config,
        }
    }

    /// See [`LvmtStoreReader::get_state_root`].
    pub fn get_state_root(&self, commit: CommitID) -> Result<Option<StateRoot>> {
        self.reader().get_state_root(commit)
    }

    /// See [`LvmtStoreReader::get_root_commitment`].
    pub fn get_root_commitment(&self, commit: CommitID) -> Result<CurvePointWithVersion> {
        self.reader().get_root_commitment(commit)
    }

    /// See [`LvmtStoreReader::get_auth_change_node`].
    pub fn get_auth_change_node(
        &self,
        commit: CommitID,
        key: &AuthChangeKey,
    ) -> Result<Option<AuthChangeNode>> {
        self.reader().get_auth_change_node(commit, key)
    }

    /// See [`LvmtStoreReader::get_auth_change_root`].
    pub fn get_auth_change_root(&self, commit: CommitID) -> Result<Option<H256>> {
        self.reader().get_auth_change_root(commit)
    }

    /// See [`LvmtStoreReader::get_mmr_entry`].
    pub fn get_mmr_entry(&self, commit: CommitID) -> Result<Option<MmrEntry>> {
        self.reader().get_mmr_entry(commit)
    }

    /// See [`LvmtStoreReader::get_hroot`].
    pub fn get_hroot(&self, commit: CommitID) -> Result<H256> {
        self.reader().get_hroot(commit)
    }

    /// See [`LvmtStoreReader::prove_hroot_leaf`].
    pub fn prove_hroot_leaf(&self, commit: CommitID, ancestor: CommitID) -> Result<MmrProof> {
        self.reader().prove_hroot_leaf(commit, ancestor)
    }

    /// See [`LvmtStoreReader::get_key_last_modified`].
    pub fn get_key_last_modified(
        &self,
        commit: CommitID,
        key: &[u8],
    ) -> Result<Option<(CommitID, LvmtValue)>> {
        self.reader().get_key_last_modified(commit, key)
    }

    /// See [`LvmtStoreReader::get_leaf_keys`].
    pub fn get_leaf_keys(
        &self,
        commit: CommitID,
        amt_id: &AmtId,
        node_index: u16,
    ) -> Result<Option<AllocationKeyInfo>> {
        self.reader().get_leaf_keys(commit, amt_id, node_index)
    }

    /// See [`LvmtStoreReader::get_root_leaf_versions`].
    pub fn get_root_leaf_versions(&self, commit: CommitID) -> Result<Vec<u64>> {
        self.reader().get_root_leaf_versions(commit)
    }

    /// See [`LvmtStoreReader::get_amt_slot_keys`].
    pub fn get_amt_slot_keys(
        &self,
        commit: CommitID,
        amt_id: &AmtId,
    ) -> Result<Vec<(u16, AllocationKeyInfo)>> {
        self.reader().get_amt_slot_keys(commit, amt_id)
    }

    /// See [`LvmtStoreReader::get_historical_amt_changes`].
    pub(crate) fn get_historical_amt_changes(
        &self,
        commit: CommitID,
    ) -> Result<Vec<(AmtId, CurvePointWithVersion)>> {
        self.reader().get_historical_amt_changes(commit)
    }

    /// See [`LvmtStoreReader::get_amt_last_modified`].
    pub fn get_amt_last_modified(
        &self,
        commit: CommitID,
        amt_id: &AmtId,
    ) -> Result<Option<(CommitID, CurvePointWithVersion)>> {
        self.reader().get_amt_last_modified(commit, amt_id)
    }

    pub fn get(&self, commit: CommitID, key: Box<[u8]>) -> Result<Option<LvmtValue>> {
        // dbg!(&commit, &key);
        self.get_state(commit, false)?.get(&key)
    }

    pub fn iter_range(
        &self,
        commit: CommitID,
        lower_bound_incl: Box<[u8]>,
        upper_bound_excl: Option<Box<[u8]>>,
    ) -> Result<KeyValueVec> {
        Ok(self
            .get_state(commit, true)?
            .iter_range(lower_bound_incl, upper_bound_excl)?
            .collect::<Vec<_>>())
    }

    pub fn new(
        pending_persistence_backend: Arc<P>,
        key_value_store: VersionedStore<'cache, 'db, FlatKeyValue, P>,
        amt_node_store: VersionedStore<'cache, 'db, AmtNodes, P>,
        slot_alloc_store: VersionedStore<'cache, 'db, SlotAllocations, P>,
        auth_changes: KeyValueStoreBulks<'static, AuthChangeTable>,
        checkpoints: TableReader<'static, RootLeafCheckpointTable>,
        proof_config: ProofConfig,
    ) -> Self {
        Self {
            pending_persistence_backend,
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
            checkpoints,
            proof_config,
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
impl<P: DatabaseTrait<PendingTableName>> LvmtStore<'_, '_, P> {
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

        let pending_before = self.key_value_store.pending_commit_ids();
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

        if key_value_has_discarded_nodes {
            let remaining: HashSet<_> = self
                .key_value_store
                .pending_commit_ids()
                .into_iter()
                .collect();
            delete_discarded_commit_rows(
                &self.pending_persistence_backend,
                pending_before
                    .into_iter()
                    .filter(|c| !remaining.contains(c)),
                &pending_write_schema,
            )?;
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
    /// All six data components (`key-values`, `AMT nodes`, `slot allocations`,
    /// `auth changes`, the `state root` and the MMR entry) are written atomically in
    /// one `pending_write_schema` created and managed here.
    ///
    /// The state root is computed here by [`compute_state_root`] from the new root AMT
    /// commitment, the root of this commit's auth-change tree and the `hroot` over the
    /// ancestors' state roots, written to the `StateRoot` table, and returned to the
    /// caller. The commit's state root is then appended to its parent's MMR.
    ///
    /// The state root depends on the set of `changes`, not on their order: they are
    /// sorted by key before new keys take their slots. Of a duplicated key, the
    /// first occurrence counts.
    ///
    /// TODO: If the historical database ever needs this information, a process can be added
    /// to copy the `AuthChange` and `StateRoot` data from the pending DB to the historical DB
    /// when a state is confirmed. Currently, there is no such requirement.
    pub fn commit(
        &mut self,
        old_commit: Option<CommitID>,
        new_commit: CommitID,
        changes: impl Iterator<Item = (Box<[u8]>, Option<Box<[u8]>>)>,
        pp: &AmtParams<PE>,
    ) -> Result<StateRoot> {
        // TODO
        
        let (amt_node_view, slot_alloc_view, key_value_view) = if let Some(old_commit) = old_commit
        {
            // dbg!("1");
            (
                Some(self.amt_node_store.get_versioned_store(&old_commit, true)?),
                Some(
                    self.slot_alloc_store
                        .get_versioned_store(&old_commit, true)?,
                ),
                Some(
                    self.key_value_store
                        .get_versioned_store(&old_commit, true)?,
                ),
            )
        } else {
            (None, None, None)
        };
        let (height, parent_peaks) = self.reader().mmr_parent(old_commit)?;
        let prepared = compute_commit(
            old_commit,
            &amt_node_view,
            &slot_alloc_view,
            &key_value_view,
            height,
            &parent_peaks,
            changes,
            pp,
        )?;
        drop((amt_node_view, slot_alloc_view, key_value_view));
        self.write_prepared(new_commit, prepared)
    }

    /// Commits a [`PreparedCommit`] from [`LvmtStoreReader::prepare_commit`] under
    /// `new_commit`, on the base it was prepared on, without computing it again.
    /// Writes and returns the same as [`commit`](Self::commit) with that base and
    /// those changes, and fails the same way: if the base is no longer there, or
    /// cannot take a child.
    ///
    /// `prepared` must come from this storage. It stays valid while its base
    /// exists: a commit's state never changes once written.
    pub fn commit_prepared(
        &mut self,
        new_commit: CommitID,
        prepared: PreparedCommit,
    ) -> Result<StateRoot> {
        // Fails with `CommitIDNotFound` if the base is gone, before anything is written.
        let (height, _) = self.reader().mmr_parent(prepared.base)?;
        if height != prepared.height {
            return Err(StorageError::ConsistencyCheckFailure);
        }
        self.write_prepared(new_commit, prepared)
    }

    fn write_prepared(
        &mut self,
        new_commit: CommitID,
        prepared: PreparedCommit,
    ) -> Result<StateRoot> {
        let PreparedCommit {
            base: old_commit,
            height,
            key_value_changes,
            allocations,
            amt_changes,
            auth_changes,
            state_root,
            mmr_entry,
        } = prepared;

        // Write to the pending part of db.
        // TODO: Write to the history part is beyond the range of LvmtStore.
        let pending_write_schema = P::write_schema();

        // dbg!("3");

        // write amt_node schema. AMT nodes are only ever written, never deleted (see
        // `AmtChangeManager::compute_amt_changes` for why historical proofs need it).
        let amt_node_updates = amt_changes.into_iter().map(|(k, v)| (k, Some(v)));
        self.amt_node_store.add_to_pending_part(
            old_commit,
            new_commit,
            amt_node_updates,
            &pending_write_schema,
        )?;

        // dbg!("4");

        // write key_value schema
        let key_value_updates = key_value_changes.into_iter().map(|(k, v)| (k, Some(v)));
        // dbg!(&old_commit, &new_commit, &key_value_updates);
        self.key_value_store.add_to_pending_part(
            old_commit,
            new_commit,
            key_value_updates,
            &pending_write_schema,
        )?;

        // write slot_alloc schema
        let slot_alloc_updates = allocations.into_iter().map(|(k, v)| (k, Some(v)));
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

        // write state_root and the MMR entry
        pending_write_schema
            .write::<StateRootTable>((Cow::Owned(new_commit), Some(Cow::Owned(state_root))));
        pending_write_schema
            .write::<MmrTable>((Cow::Owned(new_commit), Some(Cow::Owned(mmr_entry))));
        debug_assert_eq!(
            self.key_value_store.get_commit_height(&new_commit)?,
            Some(height)
        );

        self.commit_to_pending_db(pending_write_schema)?;

        Ok(state_root)
    }
}

/// Deletes the per-commit rows of commits discarded from the pending part (on a
/// fork no longer reachable from the pending root): their state root, auth-change
/// tree and MMR entry. Nothing refers to them any more: they are not ancestors of
/// any remaining commit.
pub(super) fn delete_discarded_commit_rows<P: DatabaseTrait<PendingTableName>>(
    pending_db: &Arc<P>,
    commits: impl IntoIterator<Item = CommitID>,
    write_schema: &P::WriteSchema,
) -> Result<()> {
    let auth_changes = KeyValueStoreBulks::new(pending_db.view::<AuthChangeTable>()?);
    for commit in commits {
        write_schema.write::<StateRootTable>((Cow::Owned(commit), None));
        write_schema.write::<MmrTable>((Cow::Owned(commit), None));
        // The root has the smallest key encoding.
        let rows = auth_changes.iter_commit(commit, AuthChangeKey::root())?;
        auth_changes.commit(
            commit,
            rows.into_iter().map(|(key, _)| (key, None)),
            write_schema,
        )?;
    }
    Ok(())
}

/// What a commit of some changes on a base writes, computed without writing:
/// see [`LvmtStoreReader::prepare_commit`] and [`LvmtStore::commit_prepared`].
pub struct PreparedCommit {
    base: Option<CommitID>,
    height: u64,
    key_value_changes: Vec<(Box<[u8]>, LvmtValue)>,
    allocations: HashMap<AmtNodeId, AllocationKeyInfo>,
    amt_changes: Vec<(AmtId, CurvePointWithVersion)>,
    auth_changes: std::collections::BTreeMap<AuthChangeKey, AuthChangeNode>,
    state_root: StateRoot,
    mmr_entry: MmrEntry,
}

impl PreparedCommit {
    /// The state root the commit gets, under any commit id.
    pub fn state_root(&self) -> StateRoot {
        self.state_root
    }

    /// The base it was prepared on (`None` for the first commit).
    pub fn base(&self) -> Option<CommitID> {
        self.base
    }
}

/// The computation of [`LvmtStore::commit`]: versions and slot allocations of the
/// changed keys, the new AMT commitments, the auth-change tree, the state root and
/// the MMR entry. The views are the parent's state (`None` for the first commit);
/// `height` is the new commit's and `parent_peaks` the MMR peaks after the parent.
/// Pure: the result depends on the parent and the set of `changes` only, not on
/// their order or the new commit's id.
#[allow(clippy::too_many_arguments)]
fn compute_commit(
    base: Option<CommitID>,
    amt_node_view: &KeyValueSnapshotRead<AmtNodes>,
    slot_alloc_view: &KeyValueSnapshotRead<SlotAllocations>,
    key_value_view: &KeyValueSnapshotRead<FlatKeyValue>,
    height: u64,
    parent_peaks: &[H256],
    changes: impl Iterator<Item = (Box<[u8]>, Option<Box<[u8]>>)>,
    pp: &AmtParams<PE>,
) -> Result<PreparedCommit> {
    let mut key_value_changes = vec![];
    let mut allocations = AllocationCacheDb::new(slot_alloc_view);
    let mut amt_change_manager = AmtChangeManager::default();

    // Keep the first occurrence of a duplicated key, then sort by key: new keys
    // take their slots in this order, so the state root depends on the set of
    // changes and not on the order the caller iterates them in.
    let mut set_of_keys = HashSet::new();
    let mut changes: Vec<_> = changes
        .filter(|(key, _)| set_of_keys.insert(key.clone()))
        .collect();
    changes.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    // Update version number
    for (key, value) in changes {
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

    let amt_changes = amt_change_manager.compute_amt_changes(amt_node_view, pp)?;

    // Update auth changes
    let auth_changes = {
        let auth_change_iter = amt_changes
            .iter()
            .filter(|&(amt_id, _)| (!amt_id.is_empty()))
            .map(|(amt_id, curve_point)| amt_change_hash(amt_id, curve_point));
        let key_value_iter = key_value_changes
            .iter()
            .map(|(key, value)| key_value_hash(key, value));

        let hashes = key_value_iter.chain(auth_change_iter).collect();
        process_dump_items(hashes)
    };

    // Compute the state root. Any change propagates to the root AMT, so the root
    // commitment is in `amt_changes` unless this commit changes nothing; then it
    // is inherited from the parent.
    let hroot = bag_peaks(height, parent_peaks);
    let state_root = {
        let root_commitment = match amt_changes.iter().find(|(id, _)| id.is_empty()) {
            Some((_, point)) => point.clone(),
            None => amt_node_view.get(&AmtId::root())?.unwrap_or_default(),
        };
        let auth_change_root = auth_changes
            .get(&AuthChangeKey::root())
            .map_or(EMPTY_AUTH_CHANGE_ROOT, |node| node.hash());
        compute_state_root(&root_commitment, &auth_change_root, &hroot)
    };
    let mmr_entry = MmrEntry::append(parent_peaks, height, &state_root);

    Ok(PreparedCommit {
        base,
        height,
        key_value_changes,
        allocations: allocations.into_changes(),
        amt_changes,
        auth_changes,
        state_root,
        mmr_entry,
    })
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
    let key_digest = crate::lvmt::types::key_digest(key);

    let mut depth = 1;
    loop {
        let amt_node_id = compute_amt_node_id(key_digest, depth);
        let slot_alloc = allocation_cache_db.get(&amt_node_id)?;
        let alloc_info = match slot_alloc {
            None => AllocationKeyInfo::first(key.into()),
            Some(x) if (x.index as usize) < KEY_SLOT_SIZE - 1 => x.with_next(key.into()),
            _ => {
                depth += 1;
                continue;
            }
        };
        let next_index = alloc_info.index;

        allocation_cache_db.set(amt_node_id, alloc_info);

        return Ok(AllocatePosition {
            depth: depth as u8,
            slot_index: next_index as u8,
        });
    }
}

impl<'cache, 'db, P: DatabaseTrait<PendingTableName>> LvmtStore<'cache, 'db, P> {
    pub fn get_key_value_store(&self) -> &VersionedStore<'cache, 'db, FlatKeyValue, P> {
        &self.key_value_store
    }

    #[cfg(any(test, fuzzing))]
    pub fn get_amt_node_store(&self) -> &VersionedStore<'cache, 'db, AmtNodes, P> {
        &self.amt_node_store
    }

    #[cfg(any(test, fuzzing))]
    pub fn get_slot_alloc_store(&self) -> &VersionedStore<'cache, 'db, SlotAllocations, P> {
        &self.slot_alloc_store
    }
}

/// Read-only variant of [`LvmtStore`] that holds shared references to the caches.
///
/// This enables concurrent read access when `LvmtStorage` is protected by an `RwLock`:
/// nothing here checks out a commit or writes, so reads and proofs can run beside a
/// writer holding the lock for `commit`.
pub struct LvmtStoreReader<'cache, 'db, P: DatabaseTrait<PendingTableName>> {
    pending_persistence_backend: Arc<P>,
    key_value_reader: VersionedStoreReader<'cache, 'db, FlatKeyValue, P>,
    amt_node_reader: VersionedStoreReader<'cache, 'db, AmtNodes, P>,
    slot_alloc_reader: VersionedStoreReader<'cache, 'db, SlotAllocations, P>,
    auth_changes: KeyValueStoreBulks<'static, AuthChangeTable>,
    checkpoints: TableReader<'static, RootLeafCheckpointTable>,
    proof_config: ProofConfig,
}

impl<'cache, 'db, P: DatabaseTrait<PendingTableName>> LvmtStoreReader<'cache, 'db, P> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pending_persistence_backend: Arc<P>,
        key_value_reader: VersionedStoreReader<'cache, 'db, FlatKeyValue, P>,
        amt_node_reader: VersionedStoreReader<'cache, 'db, AmtNodes, P>,
        slot_alloc_reader: VersionedStoreReader<'cache, 'db, SlotAllocations, P>,
        auth_changes: KeyValueStoreBulks<'static, AuthChangeTable>,
        checkpoints: TableReader<'static, RootLeafCheckpointTable>,
        proof_config: ProofConfig,
    ) -> Self {
        Self {
            pending_persistence_backend,
            key_value_reader,
            amt_node_reader,
            slot_alloc_reader,
            auth_changes,
            checkpoints,
            proof_config,
        }
    }

    /// Get the state root of the given commit.
    /// If not found in the pending persistence db, return `None`.
    pub fn get_state_root(&self, commit: CommitID) -> Result<Option<StateRoot>> {
        let state_root_view = Arc::new(self.pending_persistence_backend.view::<StateRootTable>()?);
        Ok(state_root_view.get(&commit)?.map(|x| x.into_owned()))
    }

    /// Get the commitment of the root AMT at the given commit.
    ///
    /// Returns the default (identity point, version 0) if the root AMT has never
    /// been changed up to this commit.
    pub fn get_root_commitment(&self, commit: CommitID) -> Result<CurvePointWithVersion> {
        let amt_node_view = self.amt_node_reader.get_versioned_store(&commit, false)?;
        Ok(amt_node_view.get(&AmtId::root())?.unwrap_or_default())
    }

    /// Get a node of the auth-change tree written by the given commit.
    pub fn get_auth_change_node(
        &self,
        commit: CommitID,
        key: &AuthChangeKey,
    ) -> Result<Option<AuthChangeNode>> {
        self.auth_changes.get_versioned_key(&commit, key)
    }

    /// Get the root hash of the auth-change tree written by the given commit.
    ///
    /// Returns [`EMPTY_AUTH_CHANGE_ROOT`] for a commit without any change, and
    /// `None` if the commit is unknown to the pending persistence db (i.e. its
    /// state root is not recorded).
    pub fn get_auth_change_root(&self, commit: CommitID) -> Result<Option<H256>> {
        if self.get_state_root(commit)?.is_none() {
            return Ok(None);
        }
        Ok(Some(
            self.get_auth_change_node(commit, &AuthChangeKey::root())?
                .map_or(EMPTY_AUTH_CHANGE_ROOT, |node| node.hash()),
        ))
    }

    /// The MMR nodes written by `commit` (see [`super::mmr`]), or `None` if unknown
    /// to the pending persistence db.
    pub fn get_mmr_entry(&self, commit: CommitID) -> Result<Option<MmrEntry>> {
        let view = self.pending_persistence_backend.view::<MmrTable>()?;
        Ok(view.get(&commit)?.map(|x| x.into_owned()))
    }

    pub(super) fn mmr_entry(&self, commit: CommitID) -> Result<MmrEntry> {
        self.get_mmr_entry(commit)?
            .ok_or(StorageError::MmrEntryNotFound(commit))
    }

    pub(super) fn commit_height(&self, commit: CommitID) -> Result<u64> {
        self.key_value_reader
            .get_commit_height(&commit)?
            .ok_or(StorageError::CommitIDNotFound)
    }

    /// The history accumulator of `commit`: the MMR root over the state roots of
    /// its ancestors, as hashed into its state root.
    pub fn get_hroot(&self, commit: CommitID) -> Result<H256> {
        let height = self.commit_height(commit)?;
        if height == 0 {
            return Ok(EMPTY_HROOT);
        }
        let parent = self
            .key_value_reader
            .get_ancestor_at_height(&commit, height - 1)?;
        Ok(bag_peaks(height, &self.mmr_entry(parent)?.peaks))
    }

    /// Proof that the state root of `ancestor`, a strict ancestor of `commit`, is
    /// accumulated in the `hroot` of `commit`.
    pub fn prove_hroot_leaf(&self, commit: CommitID, ancestor: CommitID) -> Result<MmrProof> {
        let leaf_count = self.commit_height(commit)?;
        let leaf_index = self.commit_height(ancestor)?;
        if leaf_index >= leaf_count
            || self
                .key_value_reader
                .get_ancestor_at_height(&commit, leaf_index)?
                != ancestor
        {
            return Err(StorageError::ProofUnavailable(format!(
                "{ancestor:?} is not a strict ancestor of {commit:?}"
            )));
        }
        let siblings = MmrProof::sibling_positions(leaf_count, leaf_index)
            .expect("leaf_index < leaf_count")
            .into_iter()
            .map(|(level, index)| {
                let creator = self
                    .key_value_reader
                    .get_ancestor_at_height(&commit, creating_leaf(level, index))?;
                Ok(self.mmr_entry(creator)?.created[level as usize])
            })
            .collect::<Result<_>>()?;
        let parent = self
            .key_value_reader
            .get_ancestor_at_height(&commit, leaf_count - 1)?;
        Ok(MmrProof {
            leaf_count,
            leaf_index,
            siblings,
            peaks: self.mmr_entry(parent)?.peaks,
        })
    }

    /// Get the commit at which `key` was last written as of `commit` (the closest
    /// ancestor-or-self that wrote it), together with the value written there.
    ///
    /// Its auth-change tree is the one containing `key_value_hash(key, value)`.
    /// Returns `None` if `key` was never written up to `commit`. LVMT never deletes
    /// at the versioned-store level (a deleted key keeps an `LvmtValue` with
    /// `value: None`), so a found entry always carries a value.
    pub fn get_key_last_modified(
        &self,
        commit: CommitID,
        key: &[u8],
    ) -> Result<Option<(CommitID, LvmtValue)>> {
        let key: Box<[u8]> = key.into();
        Ok(self
            .key_value_reader
            .get_with_last_modified(&commit, &key)?
            .and_then(|(modified_at, value)| value.map(|value| (modified_at, value))))
    }

    /// Get the keys allocated to the key slots of leaf `node_index` of AMT `amt_id`
    /// at `commit` (`keys()[i]` is in slot `i`), or `None` if no slot is allocated.
    pub fn get_leaf_keys(
        &self,
        commit: CommitID,
        amt_id: &AmtId,
        node_index: u16,
    ) -> Result<Option<AllocationKeyInfo>> {
        let slot_alloc_view = self.slot_alloc_reader.get_versioned_store(&commit, false)?;
        slot_alloc_view.get(&amt_id.child(node_index))
    }

    /// The leaf values of the root AMT at `commit`: entry `i` is the version of the
    /// depth-1 AMT `[i]`, i.e. slot 5 of root leaf `i` (0 if that AMT was never
    /// written). The root AMT has no keys, so these are its whole leaf values.
    ///
    /// - Pending commit: the depth-1 AMTs changed on the pending path to `commit`
    ///   (read once per pending node), then point lookups at the latest historical
    ///   commit for the other leaves only; the shared checked-out state is not
    ///   touched.
    /// - Latest historical commit: one point lookup per leaf.
    /// - Older historical commit: from the nearest base among the stored
    ///   checkpoints and the latest historical commit, corrected by the number of
    ///   depth-1 AMT changes in between (see [`super::checkpoint`]); direct point
    ///   lookups if no base is close enough.
    pub fn get_root_leaf_versions(&self, commit: CommitID) -> Result<Vec<u64>> {
        let Some(changes) = self
            .amt_node_reader
            .get_pending_path_changes(&commit, |amt_id| amt_id.len() == 1)?
        else {
            return self.get_historical_root_leaf_versions(commit);
        };

        let mut versions = vec![0u64; ROOT_LEAF_COUNT];
        let mut known = vec![false; ROOT_LEAF_COUNT];
        for (amt_id, point) in changes {
            let i = amt_id[0] as usize;
            debug_assert!(point.is_some(), "AMT nodes are never deleted");
            versions[i] = point.map_or(0, |p| p.version);
            known[i] = true;
        }
        if let Some(base) = self.amt_node_reader.get_latest_historical_commit() {
            self.read_root_leaf_versions(base, &mut versions, Some(&known))?;
        }
        Ok(versions)
    }

    /// Point lookups of the root leaf versions at `commit` into `versions`,
    /// skipping the leaves marked in `skip`.
    fn read_root_leaf_versions(
        &self,
        commit: CommitID,
        versions: &mut [u64],
        skip: Option<&[bool]>,
    ) -> Result<()> {
        let view = self.amt_node_reader.get_versioned_store(&commit, false)?;
        for (i, version) in versions.iter_mut().enumerate() {
            if skip.is_some_and(|skip| skip[i]) {
                continue;
            }
            *version = view
                .get(&AmtId::root().child(i as u16))?
                .map_or(0, |p| p.version);
        }
        Ok(())
    }

    fn get_historical_root_leaf_versions(&self, commit: CommitID) -> Result<Vec<u64>> {
        let mut versions = vec![0u64; ROOT_LEAF_COUNT];
        let tip = self.amt_node_reader.get_latest_historical_commit();
        let q = self
            .amt_node_reader
            .get_history_number_by_commit_id(commit)?;
        let tip = tip.expect("a historical commit exists");
        if tip == commit {
            self.read_root_leaf_versions(commit, &mut versions, None)?;
            return Ok(versions);
        }
        let tip_n = self.amt_node_reader.get_history_number_by_commit_id(tip)?;

        // Candidate bases with their costs, in commits to scan. Reading the tip
        // costs about as much as scanning `TIP_READ_COST` commits, reading every
        // leaf at `commit` directly about `DIRECT_READ_COST`.
        const TIP_READ_COST: u64 = 50;
        const DIRECT_READ_COST: u64 = 500;
        let s = self.proof_config.checkpoint_interval;
        let floor = q - q % s;
        let mut candidates = vec![
            (q - floor, Some(floor)),
            ((tip_n - q) + TIP_READ_COST, None),
        ];
        if floor + s <= tip_n {
            candidates.push((floor + s - q, Some(floor + s)));
        }
        candidates.sort();

        let mut base = None;
        for (cost, checkpoint) in candidates {
            if cost >= DIRECT_READ_COST {
                break;
            }
            match checkpoint {
                Some(n) => {
                    if let Some(stored) = self.checkpoints.get(&n)? {
                        versions = stored.into_owned().0;
                        base = Some(n);
                        break;
                    }
                }
                None => {
                    self.read_root_leaf_versions(tip, &mut versions, None)?;
                    base = Some(tip_n);
                    break;
                }
            }
        }
        let Some(base) = base else {
            self.read_root_leaf_versions(commit, &mut versions, None)?;
            return Ok(versions);
        };

        // A depth-1 AMT version goes up by one in every commit that changes it.
        let (from, to) = if base > q { (q, base) } else { (base, q) };
        for n in from + 1..=to {
            for (amt_id, _) in self
                .amt_node_reader
                .get_historical_changes_by_number(n, AmtId::root())?
            {
                if amt_id.len() == 1 {
                    let v = &mut versions[amt_id[0] as usize];
                    *v = if base > q { *v - 1 } else { *v + 1 };
                }
            }
        }
        Ok(versions)
    }

    /// Get the allocated key slots of every leaf of AMT `amt_id` at `commit`, as
    /// `(node_index, keys)` in increasing `node_index`. Leaves without an allocated
    /// slot are omitted.
    pub fn get_amt_slot_keys(
        &self,
        commit: CommitID,
        amt_id: &AmtId,
    ) -> Result<Vec<(u16, AllocationKeyInfo)>> {
        let slot_alloc_view = self.slot_alloc_reader.get_versioned_store(&commit, false)?;
        // Leaf ids of `amt_id` are `amt_id ‖ index`; the range also covers deeper ids
        // (leaves of its descendant AMTs), which are filtered out.
        let leaf_len = amt_id.len() + 1;
        Ok(slot_alloc_view
            .iter_range(amt_id.child(0), amt_id.prefix_successor())?
            .filter(|(node_id, _)| node_id.len() == leaf_len)
            .map(|(node_id, info)| (node_id[leaf_len - 1], info))
            .collect())
    }

    /// All AMT commitments updated by `commit`, which must be in the historical
    /// part, in increasing `AmtId` order (the root AMT first, if updated).
    pub(crate) fn get_historical_amt_changes(
        &self,
        commit: CommitID,
    ) -> Result<Vec<(AmtId, CurvePointWithVersion)>> {
        self.amt_node_reader
            .get_historical_commit_changes(&commit, AmtId::root())
    }

    /// Get the commit at which the commitment of AMT `amt_id` was last updated as of
    /// `commit`, together with the commitment written there.
    ///
    /// For a non-root AMT, that commit's auth-change tree contains
    /// `amt_change_hash(amt_id, commitment)`. Returns `None` if the AMT was never
    /// updated up to `commit`.
    pub fn get_amt_last_modified(
        &self,
        commit: CommitID,
        amt_id: &AmtId,
    ) -> Result<Option<(CommitID, CurvePointWithVersion)>> {
        Ok(self
            .amt_node_reader
            .get_with_last_modified(&commit, amt_id)?
            .and_then(|(modified_at, value)| value.map(|value| (modified_at, value))))
    }

    pub fn get(&self, commit: CommitID, key: Box<[u8]>) -> Result<Option<LvmtValue>> {
        self.get_state(commit, false)?.get(&key)
    }

    pub fn iter_range(
        &self,
        commit: CommitID,
        lower_bound_incl: Box<[u8]>,
        upper_bound_excl: Option<Box<[u8]>>,
    ) -> Result<KeyValueVec> {
        Ok(self
            .get_state(commit, true)?
            .iter_range(lower_bound_incl, upper_bound_excl)?
            .collect::<Vec<_>>())
    }

    pub fn is_newer_than_pending_root(&self, height: u64) -> bool {
        let height_of_root = self.key_value_reader.get_height_of_root();
        height > height_of_root
    }

    pub fn query_commit_existence(&self, commit: &CommitID) -> Result<bool> {
        self.key_value_reader.query_commit_existence(commit)
    }

    pub fn proof_config(&self) -> ProofConfig {
        self.proof_config
    }

    /// The new commit's height and the MMR peaks after its parent `parent`.
    pub(super) fn mmr_parent(&self, parent: Option<CommitID>) -> Result<(u64, Vec<H256>)> {
        // The parent's entry holds the peaks over the state roots at heights
        // `0..=height(parent)`.
        Ok(match parent {
            Some(old) => (self.commit_height(old)? + 1, self.mmr_entry(old)?.peaks),
            None => (0, vec![]),
        })
    }

    /// The state root [`LvmtStore::commit`] would return for `changes` on top of
    /// `base`, without writing anything or checking out `base`. The state root does
    /// not depend on the new commit's id, so none is taken: committing the same
    /// `changes` on `base` later under any id returns this value.
    pub fn commit_dry_run(
        &self,
        base: Option<CommitID>,
        changes: impl Iterator<Item = (Box<[u8]>, Option<Box<[u8]>>)>,
        pp: &AmtParams<PE>,
    ) -> Result<StateRoot> {
        Ok(self.prepare_commit(base, changes, pp)?.state_root)
    }

    /// Like [`commit_dry_run`](Self::commit_dry_run), but keeps everything the
    /// commit would write, so that [`LvmtStore::commit_prepared`] can write it later
    /// under the new commit's id without computing it again.
    pub fn prepare_commit(
        &self,
        base: Option<CommitID>,
        changes: impl Iterator<Item = (Box<[u8]>, Option<Box<[u8]>>)>,
        pp: &AmtParams<PE>,
    ) -> Result<PreparedCommit> {
        let (height, parent_peaks) = self.mmr_parent(base)?;
        let (amt_node_view, slot_alloc_view, key_value_view) = match base {
            Some(base) => (
                Some(self.amt_node_reader.get_versioned_store(&base, false)?),
                Some(self.slot_alloc_reader.get_versioned_store(&base, false)?),
                Some(self.key_value_reader.get_versioned_store(&base, false)?),
            ),
            None => (None, None, None),
        };
        compute_commit(
            base,
            &amt_node_view,
            &slot_alloc_view,
            &key_value_view,
            height,
            &parent_peaks,
            changes,
            pp,
        )
    }

    pub(super) fn key_value_reader(&self) -> &VersionedStoreReader<'cache, 'db, FlatKeyValue, P> {
        &self.key_value_reader
    }

    pub(super) fn amt_node_reader(&self) -> &VersionedStoreReader<'cache, 'db, AmtNodes, P> {
        &self.amt_node_reader
    }

    pub fn checkout_current(&self, commit: CommitID) -> Result<()> {
        self.key_value_reader.checkout_current(commit)
    }

    pub fn get_key_value_reader(&self) -> &VersionedStoreReader<'cache, 'db, FlatKeyValue, P> {
        &self.key_value_reader
    }
}
