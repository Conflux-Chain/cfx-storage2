mod manager_impl;
mod pending_part;
mod serde;
mod table_schema;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

pub use pending_part::PendingError;

use self::pending_part::pending_schema::PendingKeyValueConfig;
use self::table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema};
use pending_part::VersionedMap;

use super::commit_id_schema::HistoryNumberSchema;
use super::ChangeKey;
use super::CommitIDSchema;
use crate::backends::{DatabaseTrait, TableRead, TableReader, WriteSchemaTrait};
use crate::errors::Result;
use crate::middlewares::commit_id_schema::height_to_history_number;
use crate::middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks};
use crate::traits::KeyValueStoreBulksTrait;
use crate::StorageError;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct HistoryIndexKey<K: Clone>(K, HistoryNumber);

pub type HistoryChangeKey<K> = ChangeKey<HistoryNumber, K>;

#[derive(Clone, Debug)]
pub struct HistoryIndices;
impl HistoryIndices {
    fn last(&self, offset: HistoryNumber) -> HistoryNumber {
        offset
    }
}

/// The underlying database allows for forking and is multi-versioned.
/// It is tree-structured where
/// - each node has a unique commit_id representing a snapshot of a key-value store,
/// - and each edge represents the changes from a parent node to a child node.
///
/// Implementation concept:
/// The underlying database is divided into two parts: the historical part and the pending part.
/// - historical part: persisted on disk, does not allow forking, indexed by continuous history_numbers.
/// - pending part: in memory, forking is permitted. It is maintained by a tree-structured `VersionedMap`.
/// The root of the pending tree is unique, and its parent is the most recent node from the historical part.
/// (If the historical part is empty, then the parent of the root is None.)
/// As a consequence, neither the historical part nor the root of the pending part are allowed to have siblings.
/// It is possible that the root of the pending part has not yet been generated, but this does not alter this property.
///
/// struct `VersionedStore`:
/// Holds a reference to the underlying database, including:
/// - an immutable reference to the historical part
/// - and a mutable reference to the pending part.
/// Supports query and modification operations on the underlying database,
/// - Supported query interfaces include:
///   - querying the snapshot of the key-value store at a specified commit_id,
///   - querying the value of a specified key at a specified commit_id snapshot,
///   - and querying the modification history of a specified key.
/// - Supported modification interfaces include:
///   - adding nodes (limited to the pending part)
///   - and pruning nodes (limited to the pending part).
///
/// Upon destruction of the `VersionedStore`,
/// the mutable references to the historical part and the pending part
/// are passed as parameters to the confirmed_pending_to_history function,
/// which moves parts of the pending part that no longer have forks into the historical part.
pub struct VersionedStore<'cache, 'db, T: VersionedKeyValueSchema> {
    /// Mutable reference to the underlying pending part
    pending_part: &'cache mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,

    /// Immutable reference to the underlying historical part, including:
    /// - A mapping table from `commit_id` to `history_number`
    commit_id_table: TableReader<'db, CommitIDSchema>,
    /// - A mapping table from `history_number` to `commit_id`
    history_number_table: TableReader<'db, HistoryNumberSchema>,
    /// - A mapping table from `(key, start_history_number)` to `history_number_indices`.
    ///   Currently, `history_number_indices` are an empty struct (i.e., `()`), and each `history_number` is its own `start_history_number`.
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    /// - A mapping table from `(history_number, key)` to the corresponding value.
    /// In mapping tables, the value is wrapped in an `Option` type, where `None` indicates a deletion.
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'cache, 'db, T: VersionedKeyValueSchema> VersionedStore<'cache, 'db, T> {
    /// Creates a new instance of `VersionedStore`.
    ///
    /// This constructor initializes the `VersionedStore` by establishing connections to
    /// both the historical and pending parts of the database.
    /// It retrieves immutable references to the mapping tables from the historical part of the database
    /// and holds a mutable reference to the pending part, allowing modifications like node additions or pruning.
    ///
    /// # Parameters:
    /// - `db`: A reference to the database providing views to the historical data tables.
    /// - `pending_part`: A mutable reference to the pending part of the database for modifications.
    ///
    /// # Returns:
    /// A `Result` containing the new `VersionedStore` if successful,
    /// otherwise returns an error if any of the database views cannot be initialized.
    pub fn new<D: DatabaseTrait>(
        db: &'db D,
        pending_part: &'cache mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
    ) -> Result<Self> {
        // Obtain immutable references to the mapping tables in the historical part of the database
        let history_index_table = Arc::new(db.view::<HistoryIndicesTable<T>>()?);
        let commit_id_table = Arc::new(db.view::<CommitIDSchema>()?);
        let history_number_table = Arc::new(db.view::<HistoryNumberSchema>()?);
        let change_history_table =
            KeyValueStoreBulks::new(Arc::new(db.view::<HistoryChangeTable<T>>()?));

        Ok(VersionedStore {
            pending_part,
            history_index_table,
            commit_id_table,
            history_number_table,
            change_history_table,
        })
    }

    /// Adds a node to the pending part.
    ///
    /// # Parameters:
    /// - `parent_commit`: Specifies the `CommitID` of the parent node for the node being added.
    ///   If the node being added is the first node in the underlying database, then `parent_commit` should be set to `None`.
    /// - `commit`: The `CommitID` of the node being added.
    /// - `updates`: A `BTreeMap<T::Key, Option<T::Value>>` representing the changes from the parent node to the new node.
    ///   Here, a pair `(key, None)` indicates the deletion of the key.
    ///
    /// # Returns:
    /// A `Result` that is empty if successful, or returns an error if the operation fails.
    /// Failure can occur under several circumstances:
    /// - `commit` is already in the underlying database.
    /// - `parent_commit` equals to the parent of the pending root, which indicates that this invocation is to add a pending root,
    ///   but the pending root already exists.
    /// - `parent_commit` is different from the parent of the pending root,
    ///   which indicates that this invocation is to add a pending non-root node
    ///   and `parent_commit` must already exist in the pending part,
    ///   but `parent_commit` does not exist in the pending part yet.
    pub fn add_to_pending_part(
        &mut self,
        parent_commit: Option<CommitID>,
        commit: CommitID,
        updates: BTreeMap<T::Key, Option<T::Value>>,
    ) -> Result<()> {
        if self.commit_id_table.get(&commit)?.is_some() {
            return Err(StorageError::CommitIdAlreadyExistsInHistory);
        }

        Ok(self.pending_part.add_node(updates, commit, parent_commit)?)
    }

    /// Queries the `HistoryNumber` of a given `CommitID` in the historical part.
    ///
    /// # Parameters:
    /// - `commit`: The `CommitID` to query.
    ///
    /// # Returns:
    /// A `Result` containing the queried `HistoryNumber` if successful,
    /// otherwise returns an error if the operation fails. Failures include:
    /// - The `commit` does not exist in the historical part.
    /// - An error occurs while reading data from the historical part of the underlying database.
    fn get_history_number_by_commit_id(&self, commit: CommitID) -> Result<HistoryNumber> {
        if let Some(value) = self.commit_id_table.get(&commit)? {
            Ok(value.into_owned())
        } else {
            Err(StorageError::CommitIDNotFound)
        }
    }

    /// Queries the value of a given `Key` in the snapshot of the key-value store at a given `HistoryNumber` in the historical part.
    ///
    /// # Parameters:
    /// - `query_version_number`: The `HistoryNumber` of the snapshot to query.
    /// - `key`: The `Key` to query.
    ///
    /// # Returns:
    /// A `Result` containing the queried value (of type `Option<Value>`,
    /// where `None` indicates that the key is not present in the key-value store at this snapshot) if successful,
    /// otherwise returns an error if the operation fails. Failures include:
    /// - An error occurs while reading data from the historical part of the underlying database.
    fn get_historical_part(
        &self,
        query_version_number: HistoryNumber,
        key: &T::Key,
    ) -> Result<Option<T::Value>> {
        get_versioned_key(
            query_version_number,
            key,
            &self.history_index_table,
            &self.change_history_table,
        )
    }
}

/// Queries the value of a specified `Key` in the snapshot of the key-value store at a given `HistoryNumber` in the historical part.
///
/// # Parameters:
/// - `query_version_number`: The `HistoryNumber` of the snapshot to query.
/// - `key`: The `Key` to query.
/// - Immutable reference to the underlying historical part, including:
///   - `history_index_table`: A mapping table from `(key, start_history_number)` to `history_number_indices`.
///     Currently, `history_number_indices` are an empty struct (i.e., `()`), and each `history_number` is its own `start_history_number`.
///   - `change_history_table`: A mapping table from `(history_number, key)` to the corresponding value.
///   In mapping tables, the value is wrapped in an `Option` type, where `None` indicates a deletion.
///
/// # Algorithm:
/// The function leverages a detail in how `HistoryIndexKey(key, history_number)` is encoded in the underlying database:
/// - The `history_number` is negated during encoding.
///   This ensures that for identical keys, more recent history numbers will result in smaller encoded HistoryIndexKey.
///
/// The algorithm proceeds as follows:
/// 1. Use an iterator over `history_index_table`, starting from `HistoryIndexKey(key, query_version_number)`, to find the smallest
///    `HistoryIndexKey(k, history_number)` whose encoded HistoryIndexKey is larger or equal to
///    that of `HistoryIndexKey(key, query_version_number)`.
///    This allows finding the most recent modification of the `key` at or before the `query_version_number` node,
///    referred to as `found_version_number` (if it exists).
///    If not, it means that the `key` was not modified at or before the `query_version_number` node
///    (i.e., the key does not exist in the snapshot corresponding to `query_version_number`, and `None` is returned directly).
/// 2. If a valid `found_version_number` is found, query `change_history_table` at `ChangeKey(found_version_number, key)` to
///    retrieve the actual value (`Option<Value>`), where `None` indicates that the key was deleted.
///
/// # Returns:
/// A `Result` containing the queried value (of type `Option<Value>`,
/// where `None` indicates that the key is not present in the key-value store at this snapshot) if successful,
/// otherwise returns an error if the operation fails. Failures include:
/// - An error occurs while reading data from the historical part of the underlying database.
fn get_versioned_key<'db, T: VersionedKeyValueSchema>(
    query_version_number: HistoryNumber,
    key: &T::Key,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: &KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
) -> Result<Option<T::Value>> {
    let range_query_key = HistoryIndexKey(key.clone(), query_version_number);

    let found_version_number = match history_index_table.iter(&range_query_key)?.next() {
        None => {
            return Ok(None);
        }
        Some(Err(e)) => {
            return Err(e.into());
        }
        Some(Ok((k, _))) if &k.as_ref().0 != key => {
            return Ok(None);
        }
        Some(Ok((k, indices))) => {
            let HistoryIndexKey(_, history_number) = k.as_ref();
            // let offset = target_history_number - history_number;
            indices.as_ref().last(*history_number)
        }
    };

    change_history_table.get_versioned_key(&found_version_number, key)
}

#[allow(clippy::type_complexity)]
fn confirm_series_to_history<D: DatabaseTrait, T: VersionedKeyValueSchema>(
    db: &mut D,
    to_confirm_start_height: usize,
    to_confirm_ids_maps: Vec<(CommitID, BTreeMap<T::Key, impl Into<Option<T::Value>>>)>,
) -> Result<()> {
    let history_index_table = Arc::new(db.view::<HistoryIndicesTable<T>>()?);
    let commit_id_table = Arc::new(db.view::<CommitIDSchema>()?);
    let history_number_table = Arc::new(db.view::<HistoryNumberSchema>()?);
    let change_history_table =
        KeyValueStoreBulks::new(Arc::new(db.view::<HistoryChangeTable<T>>()?));

    let write_schema = D::write_schema();

    for (delta_height, (confirmed_commit_id, updates)) in
        to_confirm_ids_maps.into_iter().enumerate()
    {
        let height = to_confirm_start_height + delta_height;
        let history_number = height_to_history_number(height);

        if commit_id_table.get(&confirmed_commit_id)?.is_some()
            || history_number_table.get(&history_number)?.is_some()
        {
            return Err(StorageError::ConsistencyCheckFailure);
        }

        let commit_id_table_op = (
            Cow::Owned(confirmed_commit_id),
            Some(Cow::Owned(history_number)),
        );
        write_schema.write::<CommitIDSchema>(commit_id_table_op);

        let history_number_table_op = (
            Cow::Owned(history_number),
            Some(Cow::Owned(confirmed_commit_id)),
        );
        write_schema.write::<HistoryNumberSchema>(history_number_table_op);

        let history_indices_table_op = updates.keys().map(|key| {
            (
                Cow::Owned(HistoryIndexKey(key.clone(), history_number)),
                Some(Cow::Owned(HistoryIndices)),
            )
        });
        write_schema.write_batch::<HistoryIndicesTable<T>>(history_indices_table_op);

        change_history_table.commit(
            history_number,
            updates.into_iter().map(|(key, value)| (key, value.into())),
            &write_schema,
        )?;
    }

    std::mem::drop(history_index_table);
    std::mem::drop(commit_id_table);
    std::mem::drop(history_number_table);
    std::mem::drop(change_history_table);

    db.commit(write_schema)?;

    Ok(())
}

pub fn confirmed_pending_to_history<D: DatabaseTrait, T: VersionedKeyValueSchema>(
    db: &mut D,
    pending_part: &mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
    new_root_commit_id: CommitID,
) -> Result<()> {
    // old root..=new root's parent
    let (to_confirm_start_height, to_confirm_ids_maps) =
        pending_part.change_root(new_root_commit_id)?;

    confirm_series_to_history::<D, T>(db, to_confirm_start_height, to_confirm_ids_maps)?;

    Ok(())
}
