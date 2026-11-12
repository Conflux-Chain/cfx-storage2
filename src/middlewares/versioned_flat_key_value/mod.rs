#[cfg(fuzzing)]
pub mod history_indices;

#[cfg(not(fuzzing))]
mod history_indices;

mod history_indices_cache;
mod manager_impl;

#[cfg(fuzzing)]
pub mod pending_part;

#[cfg(not(fuzzing))]
mod pending_part;

mod serde;
pub mod table_schema;
#[cfg(any(test, fuzzing))]
pub mod tests;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub use history_indices::{BitmapValidationError, PushError};
pub use manager_impl::SnapshotView;
use nonempty::NonEmpty;
pub use pending_part::TreeWithTracker;
pub use pending_part::{
    pending_schema::PendingKeyValueConfig, primitives_clear_pending_schema,
    primitives_gc_until_height, primitives_initialize_empty_schema, primitives_recover_schema,
    primitives_verify_no_newer_records, primitives_verify_schema_is_empty, BootstrapError,
    PendingError, RecoveryError, SnapshotReadError,
};

#[cfg(any(test, fuzzing))]
pub use tests::{
    clear_dir, clear_dir_then_create, gen_random_commit_id, gen_updates, get_rng_for_test,
};

use self::history_indices::LATEST;
use self::history_indices_cache::HistoryIndexCache;
use self::table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema};
pub use pending_part::VersionedMap;

use super::commit_id_schema::HistoryNumberSchema;
use super::ChangeKey;
use super::CommitIDSchema;
use crate::backends::{
    DatabaseTrait, HistoricalTableName, PendingTableName, TableRead, TableReader, WriteSchemaTrait,
};
use crate::errors::Result;
use crate::middlewares::commit_id_schema::{height_to_history_number, history_number_to_height};
use crate::middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks};
use crate::traits::KeyValueStoreBulksTrait;
use crate::StorageError;

pub type VersionedStoreCache<Schema, P> = VersionedMap<PendingKeyValueConfig<Schema, CommitID>, P>;

/// Key for accessing version history records in storage.
///
/// Consists of two components:
/// 1. The database `key` being tracked
/// 2. A version specifier which is either:
///    - `LATEST` for the latest (mutable) record
///    - An `end_version_number` for a previous (immutable) record
///
/// Used in conjunction with [`history_indices::HistoryIndices`] to maintain version history through chained records.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct HistoryIndexKey<K: Clone>(K, HistoryNumber);

pub type HistoryChangeKey<K> = ChangeKey<HistoryNumber, K>;

impl<K: Clone> HistoryIndexKey<K> {
    pub fn is_latest(&self) -> bool {
        self.1 == LATEST
    }
}

/// # Atomicity Guarantee
///
/// When performing a single logical operation that spans multiple `VersionedKeyValueSchema`
/// types (i.e., different `T`s), the caller holds the responsibility for ensuring
/// the atomicity of the database write.
///
/// To achieve this, all writes related to that operation **must** be submitted via the
/// **same** `pending_write_schema` instance provided to the functions of this store.
pub struct VersionedStore<
    'cache,
    'db,
    T: VersionedKeyValueSchema,
    P: DatabaseTrait<PendingTableName>,
> {
    pending_part: &'cache mut VersionedMap<PendingKeyValueConfig<T, CommitID>, P>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    history_number_table: TableReader<'db, HistoryNumberSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

/// Read-only variant of [`VersionedStore`] that holds a shared reference to the pending part.
///
/// This enables concurrent read access when the outer storage is protected by an `RwLock`.
pub struct VersionedStoreReader<
    'cache,
    'db,
    T: VersionedKeyValueSchema,
    P: DatabaseTrait<PendingTableName>,
> {
    pending_part: &'cache VersionedMap<PendingKeyValueConfig<T, CommitID>, P>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    history_number_table: TableReader<'db, HistoryNumberSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'cache, 'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
    VersionedStore<'cache, 'db, T, P>
{
    pub fn is_in_historical_part(&self, commit: &CommitID) -> Result<bool> {
        if self.commit_id_table.get(commit)?.is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn query_commit_existence(&self, commit: &CommitID) -> Result<bool> {
        if self.pending_part.contains_commit_id(commit) {
            return Ok(true);
        }

        self.is_in_historical_part(commit)
    }

    pub fn get_height_of_root(&self) -> u64 {
        self.pending_part.get_height_of_root()
    }

    /// The ids of all pending commits, in no particular order.
    pub fn pending_commit_ids(&self) -> Vec<CommitID> {
        self.pending_part.pending_commit_ids()
    }

    /// A read-only view sharing this store's pending part and tables.
    pub fn as_reader(&self) -> VersionedStoreReader<'_, 'db, T, P> {
        VersionedStoreReader {
            pending_part: &*self.pending_part,
            history_index_table: self.history_index_table.clone(),
            commit_id_table: self.commit_id_table.clone(),
            history_number_table: self.history_number_table.clone(),
            change_history_table: self.change_history_table.clone(),
        }
    }

    /// See [`VersionedStoreReader::get_latest_historical_commit`].
    pub fn get_latest_historical_commit(&self) -> Option<CommitID> {
        self.as_reader().get_latest_historical_commit()
    }

    /// See [`VersionedStoreReader::get_pending_path_changes`].
    pub fn get_pending_path_changes(
        &self,
        commit: &CommitID,
        filter: impl Fn(&T::Key) -> bool,
    ) -> Result<Option<HashMap<T::Key, Option<T::Value>>>> {
        self.as_reader().get_pending_path_changes(commit, filter)
    }

    /// See [`VersionedStoreReader::get_commit_height`].
    pub fn get_commit_height(&self, commit: &CommitID) -> Result<Option<u64>> {
        self.as_reader().get_commit_height(commit)
    }

    /// See [`VersionedStoreReader::get_ancestor_at_height`].
    pub fn get_ancestor_at_height(&self, commit: &CommitID, height: u64) -> Result<CommitID> {
        self.as_reader().get_ancestor_at_height(commit, height)
    }

    /// See [`VersionedStoreReader::get_historical_changes_by_number`].
    pub(crate) fn get_historical_changes_by_number(
        &self,
        history_number: HistoryNumber,
        from: T::Key,
    ) -> Result<Vec<(T::Key, T::Value)>> {
        self.as_reader()
            .get_historical_changes_by_number(history_number, from)
    }

    /// See [`VersionedStoreReader::get_historical_commit_changes`].
    pub(crate) fn get_historical_commit_changes(
        &self,
        commit: &CommitID,
        from: T::Key,
    ) -> Result<Vec<(T::Key, T::Value)>> {
        self.as_reader().get_historical_commit_changes(commit, from)
    }

    pub fn checkout_current(&self, commit: CommitID) -> Result<()> {
        Ok(self.pending_part.checkout_current(commit)?)
    }

    /// # Atomicity
    /// To ensure atomicity when writing to multiple schemas, see the main documentation
    /// on [`VersionedStore`].
    pub fn make_pivot(
        &mut self,
        commit_id: CommitID,
        pending_write_schema: &P::WriteSchema,
    ) -> Result<bool> {
        Ok(self
            .pending_part
            .make_pivot(commit_id, pending_write_schema)?)
    }

    pub fn new<D: DatabaseTrait<HistoricalTableName>>(
        db: Arc<D>,
        pending_part: &'cache mut VersionedMap<PendingKeyValueConfig<T, CommitID>, P>,
    ) -> Result<Self> {
        let history_index_table = db.view::<HistoryIndicesTable<T>>()?;
        let commit_id_table = db.view::<CommitIDSchema>()?;
        let history_number_table = db.view::<HistoryNumberSchema>()?;
        let change_history_table = KeyValueStoreBulks::new(db.view::<HistoryChangeTable<T>>()?);

        let versioned_store = VersionedStore {
            pending_part,
            history_index_table,
            commit_id_table,
            history_number_table,
            change_history_table,
        };

        Ok(versioned_store)
    }

    /// # Atomicity
    /// To ensure atomicity when writing to multiple schemas, see the main documentation
    /// on [`VersionedStore`].
    pub fn add_to_pending_part(
        &mut self,
        parent_commit: Option<CommitID>,
        commit: CommitID,
        updates: impl IntoIterator<Item = (T::Key, Option<T::Value>)>,
        pending_write_schema: &P::WriteSchema,
    ) -> Result<()> {
        if self.commit_id_table.get(&commit)?.is_some() {
            return Err(StorageError::CommitIdAlreadyExistsInHistory);
        }

        Ok(self
            .pending_part
            .add_node(updates, commit, parent_commit, pending_write_schema)?)
    }

    pub(crate) fn get_history_number_by_commit_id(
        &self,
        commit: CommitID,
    ) -> Result<HistoryNumber> {
        if let Some(value) = self.commit_id_table.get(&commit)? {
            Ok(value.into_owned())
        } else {
            Err(StorageError::CommitIDNotFound)
        }
    }

    /// See [`get_last_modified`].
    pub fn get_with_last_modified(
        &self,
        commit: &CommitID,
        key: &T::Key,
    ) -> Result<Option<(CommitID, Option<T::Value>)>> {
        get_last_modified(
            &*self.pending_part,
            &self.commit_id_table,
            &self.history_number_table,
            &self.history_index_table,
            &self.change_history_table,
            commit,
            key,
        )
    }

    fn get_historical_part(
        &self,
        query_version_number: HistoryNumber,
        key: &T::Key,
        is_latest: bool,
    ) -> Result<Option<T::Value>> {
        if is_latest {
            get_versioned_key_latest(query_version_number, key, &self.history_index_table)
        } else {
            get_versioned_key_previous(
                query_version_number,
                key,
                &self.history_index_table,
                &self.change_history_table,
            )
        }
    }
}

impl<'cache, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>
    VersionedStoreReader<'cache, '_, T, P>
{
    /// The latest historical commit (the parent of the pending root), if any.
    pub fn get_latest_historical_commit(&self) -> Option<CommitID> {
        self.pending_part.get_parent_of_root()
    }

    /// If `commit` is in the pending part, returns the value as of `commit` (`None`
    /// if deleted) of every key accepted by `filter` that is modified on the pending
    /// path from the pending root to `commit`. Keys not in the result have their
    /// value at [`Self::get_latest_historical_commit`].
    ///
    /// Unlike a pending [`SnapshotView`], this does not check out `commit`, so it
    /// leaves the shared current state alone. Returns `Ok(None)` if `commit` is not
    /// in the pending part.
    pub fn get_pending_path_changes(
        &self,
        commit: &CommitID,
        filter: impl Fn(&T::Key) -> bool,
    ) -> Result<Option<HashMap<T::Key, Option<T::Value>>>> {
        if !self.pending_part.contains_commit_id(commit) {
            return Ok(None);
        }
        let changes = self.pending_part.get_path_changes(commit, filter)?;
        Ok(Some(
            changes
                .into_iter()
                .map(|(key, value)| (key, value.into_option()))
                .collect(),
        ))
    }

    /// The historical commit with the given history number, if any.
    pub(crate) fn get_commit_id_by_history_number(
        &self,
        history_number: HistoryNumber,
    ) -> Result<Option<CommitID>> {
        Ok(self
            .history_number_table
            .get(&history_number)?
            .map(|c| c.into_owned()))
    }

    /// The height of `commit` (pending or historical), or `None` if unknown.
    pub fn get_commit_height(&self, commit: &CommitID) -> Result<Option<u64>> {
        if let Some(height) = self.pending_part.get_height_of_commit(commit) {
            return Ok(Some(height));
        }
        Ok(self
            .commit_id_table
            .get(commit)?
            .map(|n| history_number_to_height(n.into_owned())))
    }

    /// The ancestor-or-self of `commit` at `height`. `commit` must be known and
    /// `height` no greater than its height.
    pub fn get_ancestor_at_height(&self, commit: &CommitID, height: u64) -> Result<CommitID> {
        if self.pending_part.contains_commit_id(commit) && height >= self.get_height_of_root() {
            return Ok(self
                .pending_part
                .get_ancestor_commit_at_height(height, *commit)?);
        }
        let own = self
            .get_commit_height(commit)?
            .ok_or(StorageError::CommitIDNotFound)?;
        if height > own {
            return Err(StorageError::CommitIDNotFound);
        }
        // Below the pending root, every chain goes through the historical part.
        self.get_commit_id_by_history_number(height_to_history_number(height))?
            .ok_or(StorageError::CommitIDNotFound)
    }

    /// Like [`Self::get_historical_commit_changes`], by history number.
    pub(crate) fn get_historical_changes_by_number(
        &self,
        history_number: HistoryNumber,
        from: T::Key,
    ) -> Result<Vec<(T::Key, T::Value)>> {
        self.change_history_table.iter_commit(history_number, from)
    }

    /// All `(key, value)` changes written by `commit`, which must be in the
    /// historical part. Deletions are not included (the change table keeps no row
    /// for them). `from` is a key encoding no greater than any key of the schema.
    pub(crate) fn get_historical_commit_changes(
        &self,
        commit: &CommitID,
        from: T::Key,
    ) -> Result<Vec<(T::Key, T::Value)>> {
        let history_number = self.get_history_number_by_commit_id(*commit)?;
        self.change_history_table.iter_commit(history_number, from)
    }

    pub fn is_in_historical_part(&self, commit: &CommitID) -> Result<bool> {
        if self.commit_id_table.get(commit)?.is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn query_commit_existence(&self, commit: &CommitID) -> Result<bool> {
        if self.pending_part.contains_commit_id(commit) {
            return Ok(true);
        }

        self.is_in_historical_part(commit)
    }

    pub fn get_height_of_root(&self) -> u64 {
        self.pending_part.get_height_of_root()
    }

    pub fn checkout_current(&self, commit: CommitID) -> Result<()> {
        Ok(self.pending_part.checkout_current(commit)?)
    }

    pub fn new<D: DatabaseTrait<HistoricalTableName>>(
        db: Arc<D>,
        pending_part: &'cache VersionedMap<PendingKeyValueConfig<T, CommitID>, P>,
    ) -> Result<Self> {
        let history_index_table = db.view::<HistoryIndicesTable<T>>()?;
        let commit_id_table = db.view::<CommitIDSchema>()?;
        let history_number_table = db.view::<HistoryNumberSchema>()?;
        let change_history_table = KeyValueStoreBulks::new(db.view::<HistoryChangeTable<T>>()?);

        Ok(VersionedStoreReader {
            pending_part,
            history_index_table,
            commit_id_table,
            history_number_table,
            change_history_table,
        })
    }

    /// See [`get_last_modified`].
    pub fn get_with_last_modified(
        &self,
        commit: &CommitID,
        key: &T::Key,
    ) -> Result<Option<(CommitID, Option<T::Value>)>> {
        get_last_modified(
            &*self.pending_part,
            &self.commit_id_table,
            &self.history_number_table,
            &self.history_index_table,
            &self.change_history_table,
            commit,
            key,
        )
    }

    pub(crate) fn get_history_number_by_commit_id(
        &self,
        commit: CommitID,
    ) -> Result<HistoryNumber> {
        if let Some(value) = self.commit_id_table.get(&commit)? {
            Ok(value.into_owned())
        } else {
            Err(StorageError::CommitIDNotFound)
        }
    }

    pub(crate) fn get_historical_part(
        &self,
        query_version_number: HistoryNumber,
        key: &T::Key,
        is_latest: bool,
    ) -> Result<Option<T::Value>> {
        if is_latest {
            get_versioned_key_latest(query_version_number, key, &self.history_index_table)
        } else {
            get_versioned_key_previous(
                query_version_number,
                key,
                &self.history_index_table,
                &self.change_history_table,
            )
        }
    }
}

/// Returns the value for the key at the maximum version_number such that
/// version_number <= the latest_version_number in the historical part.
/// If there is no such version_number, returns `None`.
// # Algorithm Explanation
// - If the lastest record exists, since the latest record has at least one
//   version_number, and all recorded version_numbers are <= latest_version_number,
//   we can always obtain the value from the lastest record.
// - If the latest record does not exists, then there is no record for this key.
// Therefore, there is no need to view previous records.
fn get_versioned_key_latest<T: VersionedKeyValueSchema>(
    latest_version_number: HistoryNumber,
    key: &T::Key,
    history_index_table: &TableReader<'_, HistoryIndicesTable<T>>,
) -> Result<Option<T::Value>> {
    let range_query_key = HistoryIndexKey(key.clone(), LATEST);
    match history_index_table.get(&range_query_key)? {
        Some(history_indices) => history_indices
            .into_owned()
            .get_latest_value(latest_version_number),
        None => Ok(None),
    }
}

/// Returns the value for the key at the maximum version_number such that
/// version_number <= the query_version_number in the historical part.
/// If there is no such version_number, returns `None`.
// # Algorithm Explanation
// Find the record with the minimum version specifier such that the version specifier >= query_version_number.
// Since LATEST >= query_version_number, if no such record, there is no record for this key.
// If there is such record,
// - If there is a previous_record before this record, then the previous_version_specifier < query_version_number,
//   and previous_version_specifier is the start_version_number of this record.
//   I.e., the start_version_number of this record < query_version_number <= the version specifier of this record.
//   Thus, we can always obtain the value from this record.
// - If there is no previous_record before this record, version_number can only be within this record or non-existing.
// Therefore, there is no need to view previous records before this record.
fn get_versioned_key_previous<'db, T: VersionedKeyValueSchema>(
    query_version_number: HistoryNumber,
    key: &T::Key,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: &KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
) -> Result<Option<T::Value>> {
    // dbg!(query_version_number, key);
    let range_query_key = HistoryIndexKey(key.clone(), query_version_number);

    // dbg!(&range_query_key);
    // for miao in history_index_table.iter_from_start()? {
    //     dbg!(miao);
    // }
    // dbg!(history_index_table.iter(&range_query_key)?.next());
    match history_index_table.iter(&range_query_key)?.next() {
        None => Ok(None),
        Some(Err(e)) => Err(e.into()),
        Some(Ok((k, _))) if &k.as_ref().0 != key => Ok(None),
        Some(Ok((k, indices))) => {
            let HistoryIndexKey(_, end_history_number) = k.as_ref();
            if let Some(found_version_number) = indices
                .as_ref()
                .last_le(query_version_number, *end_history_number)?
            {
                change_history_table.get_versioned_key(&found_version_number, key)
            } else {
                Ok(None)
            }
        }
    }
}

/// Like [`get_versioned_key_latest`], but also returns the history number at which
/// the returned value was written.
fn get_versioned_key_latest_with_version<T: VersionedKeyValueSchema>(
    latest_version_number: HistoryNumber,
    key: &T::Key,
    history_index_table: &TableReader<'_, HistoryIndicesTable<T>>,
) -> Result<Option<(HistoryNumber, Option<T::Value>)>> {
    let range_query_key = HistoryIndexKey(key.clone(), LATEST);
    match history_index_table.get(&range_query_key)? {
        Some(history_indices) => Ok(Some(
            history_indices
                .into_owned()
                .get_latest_value_with_version(latest_version_number)?,
        )),
        None => Ok(None),
    }
}

/// Like [`get_versioned_key_previous`], but also returns the history number at which
/// the returned value was written (the value is `None` if that change deleted the key).
fn get_versioned_key_previous_with_version<'db, T: VersionedKeyValueSchema>(
    query_version_number: HistoryNumber,
    key: &T::Key,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: &KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
) -> Result<Option<(HistoryNumber, Option<T::Value>)>> {
    let range_query_key = HistoryIndexKey(key.clone(), query_version_number);
    match history_index_table.iter(&range_query_key)?.next() {
        None => Ok(None),
        Some(Err(e)) => Err(e.into()),
        Some(Ok((k, _))) if &k.as_ref().0 != key => Ok(None),
        Some(Ok((k, indices))) => {
            let HistoryIndexKey(_, end_history_number) = k.as_ref();
            match indices
                .as_ref()
                .last_le(query_version_number, *end_history_number)?
            {
                Some(found_version_number) => Ok(Some((
                    found_version_number,
                    change_history_table.get_versioned_key(&found_version_number, key)?,
                ))),
                None => Ok(None),
            }
        }
    }
}

/// Returns the commit at which `key` was last modified as of `commit`, i.e. the
/// closest ancestor-or-self of `commit` that wrote `key`, together with the value
/// written there (`None` if that write deleted the key).
///
/// Returns `Ok(None)` if `key` was never written up to `commit`, and
/// `Err(CommitIDNotFound)` if `commit` is unknown.
#[allow(clippy::too_many_arguments)]
fn get_last_modified<'db, T: VersionedKeyValueSchema, P: DatabaseTrait<PendingTableName>>(
    pending_part: &VersionedMap<PendingKeyValueConfig<T, CommitID>, P>,
    commit_id_table: &TableReader<'db, CommitIDSchema>,
    history_number_table: &TableReader<'db, HistoryNumberSchema>,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: &KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
    commit: &CommitID,
    key: &T::Key,
) -> Result<Option<(CommitID, Option<T::Value>)>> {
    let history_number_of = |commit: &CommitID| -> Result<HistoryNumber> {
        match commit_id_table.get(commit)? {
            Some(value) => Ok(value.into_owned()),
            None => Err(StorageError::CommitIDNotFound),
        }
    };

    let (history_number, is_latest) = if pending_part.contains_commit_id(commit) {
        if let Some((modified_at, value)) =
            pending_part.get_versioned_key_with_commit(commit, key)?
        {
            return Ok(Some((modified_at, value.into_option())));
        }
        // Not modified in the pending part: fall back to the latest historical version.
        match pending_part.get_parent_of_root() {
            Some(latest_history_commit) => (history_number_of(&latest_history_commit)?, true),
            None => return Ok(None),
        }
    } else {
        let history_number = history_number_of(commit)?;
        let is_latest = pending_part.get_parent_of_root() == Some(*commit);
        (history_number, is_latest)
    };

    let found = if is_latest {
        get_versioned_key_latest_with_version(history_number, key, history_index_table)?
    } else {
        get_versioned_key_previous_with_version(
            history_number,
            key,
            history_index_table,
            change_history_table,
        )?
    };

    match found {
        None => Ok(None),
        Some((modified_at, value)) => match history_number_table.get(&modified_at)? {
            Some(commit_id) => Ok(Some((commit_id.into_owned(), value))),
            None => Err(StorageError::CorruptedHistoryIndices(format!(
                "history number {} has no commit id",
                modified_at
            ))),
        },
    }
}

/// Collects all exsiting keys in the range of [lower_bound_incl, upper_bound_excl) and their values
/// at the view of `query_version_number`.
fn iter_history_range<'db, T>(
    query_version_number: HistoryNumber,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    maybe_change_history_table: Option<&KeyValueStoreBulks<'db, HistoryChangeTable<T>>>,
    lower_bound_incl: T::Key,
    upper_bound_excl: Option<T::Key>,
) -> Result<BTreeMap<T::Key, T::Value>>
where
    T: VersionedKeyValueSchema,
{
    let range_query_key = HistoryIndexKey(lower_bound_incl.clone(), 0);
    let (history_index_key, _) = match history_index_table.iter(&range_query_key)?.next() {
        Some(item) => item.unwrap(),
        None => return Ok(BTreeMap::new()),
    };
    let HistoryIndexKey(mut key, _) = history_index_key.as_ref().clone();

    let mut history_map = BTreeMap::new();

    loop {
        // check whether key is in range
        if let Some(ref upper_bound_excl_inner) = upper_bound_excl {
            if &key >= upper_bound_excl_inner {
                break;
            }
        }

        let value = if let Some(change_history_table) = maybe_change_history_table {
            get_versioned_key_previous(
                query_version_number,
                &key,
                history_index_table,
                change_history_table,
            )?
        } else {
            get_versioned_key_latest(query_version_number, &key, history_index_table)?
        };

        if let Some(existing_value) = value {
            history_map.insert(key.clone(), existing_value);
        }

        let range_query_key = HistoryIndexKey(key.clone(), LATEST);
        let mut find_next_key_iter = history_index_table.iter(&range_query_key)?;

        let this_key = match find_next_key_iter.next().transpose()? {
            Some((this_historical_index_key, _)) => this_historical_index_key.as_ref().0.clone(),
            None => break,
        };
        assert_eq!(this_key, key, "The latest record of a key should exist when there is at least one record of that key.");

        let next_key = match find_next_key_iter.next().transpose()? {
            Some((next_historical_index_key, _)) => next_historical_index_key.0.clone(),
            None => break,
        };
        assert_ne!(next_key, key, "Iterator should have moved to a different key after processing the lastest record for the current key.");

        key = next_key;
    }

    Ok(history_map)
}

fn iter_history<'db, T: VersionedKeyValueSchema>(
    query_version_number: HistoryNumber,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    maybe_change_history_table: Option<&KeyValueStoreBulks<'db, HistoryChangeTable<T>>>,
) -> Result<BTreeMap<T::Key, T::Value>> {
    let (history_index_key, _) = match history_index_table.iter_from_start()?.next() {
        Some(item) => item.unwrap(),
        None => return Ok(BTreeMap::new()),
    };

    let HistoryIndexKey(mut key, _) = history_index_key.as_ref().clone();

    let mut history_map = BTreeMap::new();

    loop {
        let value = if let Some(change_history_table) = maybe_change_history_table {
            get_versioned_key_previous(
                query_version_number,
                &key,
                history_index_table,
                change_history_table,
            )?
        } else {
            get_versioned_key_latest(query_version_number, &key, history_index_table)?
        };

        if let Some(existing_value) = value {
            history_map.insert(key.clone(), existing_value);
        }

        let range_query_key = HistoryIndexKey(key.clone(), LATEST);
        let mut find_next_key_iter = history_index_table.iter(&range_query_key)?;

        let this_key = match find_next_key_iter.next().transpose()? {
            Some((this_historical_index_key, _)) => this_historical_index_key.as_ref().0.clone(),
            None => break,
        };
        assert_eq!(this_key, key, "The latest record of a key should exist when there is at least one record of that key.");

        let next_key = match find_next_key_iter.next().transpose()? {
            Some((next_historical_index_key, _)) => next_historical_index_key.0.clone(),
            None => break,
        };
        assert_ne!(next_key, key, "Iterator should have moved to a different key after processing the lastest record for the current key.");

        key = next_key;
    }

    Ok(history_map)
}

/// # Atomicity
/// To ensure atomicity when writing to multiple schemas, see the main documentation
/// on [`VersionedStore`].
pub fn confirmed_pending_to_history<
    D: DatabaseTrait<HistoricalTableName>,
    P: DatabaseTrait<PendingTableName>,
    T: VersionedKeyValueSchema,
>(
    historical_db: Arc<D>,
    pending_part: &mut VersionedMap<PendingKeyValueConfig<T, CommitID>, P>,
    new_root_commit_id: CommitID,
    historical_write_schema: &D::WriteSchema,
    pending_write_schema: &P::WriteSchema,
) -> Result<()> {
    let maybe_confirmed_path =
        pending_part.change_root(new_root_commit_id, pending_write_schema)?;

    if let Some(confirmed_path) = maybe_confirmed_path {
        confirm_ids_to_history::<D>(
            historical_db.clone(),
            confirmed_path.start_height,
            &confirmed_path.commit_ids,
            historical_write_schema,
        )?;

        confirm_maps_to_history::<D, T>(
            historical_db.clone(),
            confirmed_path.start_height,
            confirmed_path.key_value_maps,
            historical_write_schema,
        )?;
    }

    Ok(())
}

pub fn confirm_maps_to_history<
    D: DatabaseTrait<HistoricalTableName>,
    T: VersionedKeyValueSchema,
>(
    db: Arc<D>,
    to_confirm_start_height: u64,
    to_confirm_maps: NonEmpty<HashMap<T::Key, impl Into<Option<T::Value>>>>,
    historical_write_schema: &D::WriteSchema,
) -> Result<()> {
    let history_index_table = db.view::<HistoryIndicesTable<T>>()?;
    let change_history_table = KeyValueStoreBulks::new(db.view::<HistoryChangeTable<T>>()?);

    let mut history_index_cache = HistoryIndexCache::new();
    for (delta_height, updates) in to_confirm_maps.into_iter().enumerate() {
        let height = to_confirm_start_height + delta_height as u64;
        let history_number = height_to_history_number(height);

        let commit_data = updates
            .into_iter()
            .map(|(k, v)| {
                let value = v.into();
                history_index_cache.insert(
                    k.clone(),
                    value.clone(),
                    history_number,
                    &*history_index_table,
                )?;
                Ok((k, value))
            })
            .collect::<Result<Vec<_>>>()?;

        change_history_table.commit(
            history_number,
            commit_data.into_iter(),
            &historical_write_schema,
        )?;
    }

    let history_indices_table_op = history_index_cache.into_write_batch();
    historical_write_schema
        .write_batch::<HistoryIndicesTable<T>>(history_indices_table_op.into_iter());

    Ok(())
}

pub fn confirm_ids_to_history<D: DatabaseTrait<HistoricalTableName>>(
    db: Arc<D>,
    to_confirm_start_height: u64,
    to_confirm_ids: &NonEmpty<CommitID>,
    historical_write_schema: &D::WriteSchema,
) -> Result<()> {
    let commit_id_table = db.view::<CommitIDSchema>()?;
    let history_number_table = db.view::<HistoryNumberSchema>()?;

    for (delta_height, confirmed_commit_id) in to_confirm_ids.iter().enumerate() {
        let height = to_confirm_start_height + delta_height as u64;
        let history_number = height_to_history_number(height);

        if commit_id_table.get(confirmed_commit_id)?.is_some()
            || history_number_table.get(&history_number)?.is_some()
        {
            return Err(StorageError::ConsistencyCheckFailure);
        }

        let commit_id_table_op = (
            Cow::Owned(*confirmed_commit_id),
            Some(Cow::Owned(history_number)),
        );
        historical_write_schema.write::<CommitIDSchema>(commit_id_table_op);

        let history_number_table_op = (
            Cow::Owned(history_number),
            Some(Cow::Owned(*confirmed_commit_id)),
        );
        historical_write_schema.write::<HistoryNumberSchema>(history_number_table_op);
    }

    Ok(())
}
