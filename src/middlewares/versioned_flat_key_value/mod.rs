mod history_indices;
mod manager_impl;
mod pending_part;
mod serde;
pub mod table_schema;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

pub use pending_part::PendingError;

#[cfg(test)]
pub use tests::{empty_rocksdb, gen_random_commit_id, gen_updates, get_rng_for_test};

use self::history_indices::HistoryIndices;
use self::pending_part::pending_schema::PendingKeyValueConfig;
use self::table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema};
use pending_part::VersionedMap;

use super::commit_id_schema::HistoryNumberSchema;
use super::ChangeKey;
use super::CommitIDSchema;
use crate::backends::{DatabaseTrait, TableReader, WriteSchemaTrait};
use crate::errors::Result;
use crate::middlewares::commit_id_schema::height_to_history_number;
use crate::middlewares::{CommitID, HistoryNumber, KeyValueStoreBulks};
use crate::traits::KeyValueStoreBulksTrait;
use crate::StorageError;

pub type VersionedStoreCache<Schema> = VersionedMap<PendingKeyValueConfig<Schema, CommitID>>;

#[cfg(test)]
use crate::types::ValueEntry;
#[cfg(test)]
use std::collections::BTreeMap;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct HistoryIndexKey<K: Clone>(K, HistoryNumber);

pub type HistoryChangeKey<K> = ChangeKey<HistoryNumber, K>;

impl<K: Clone> HistoryIndexKey<K> {
    /// Naive scheme has no "latest" sentinel record — every record is a plain
    /// `(key, version)`. Kept so callers depending on the old API (e.g. the
    /// AmtNode cache-path warmup) compile without needing to be rewritten.
    pub fn is_latest(&self) -> bool {
        false
    }
}

pub struct VersionedStore<'cache, 'db, T: VersionedKeyValueSchema> {
    pending_part: &'cache mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    history_number_table: TableReader<'db, HistoryNumberSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
}

impl<'cache, 'db, T: VersionedKeyValueSchema> VersionedStore<'cache, 'db, T> {
    pub fn new<D: DatabaseTrait>(
        db: &'db D,
        pending_part: &'cache mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
    ) -> Result<Self> {
        let history_index_table = Arc::from(db.view::<HistoryIndicesTable<T>>()?);
        let commit_id_table = Arc::from(db.view::<CommitIDSchema>()?);
        let history_number_table = Arc::from(db.view::<HistoryNumberSchema>()?);
        let change_history_table =
            KeyValueStoreBulks::new(Arc::from(db.view::<HistoryChangeTable<T>>()?));

        let versioned_store = VersionedStore {
            pending_part,
            history_index_table,
            commit_id_table,
            history_number_table,
            change_history_table,
        };

        Ok(versioned_store)
    }

    pub fn add_to_pending_part(
        &mut self,
        parent_commit: Option<CommitID>,
        commit: CommitID,
        updates: HashMap<T::Key, Option<T::Value>>,
    ) -> Result<()> {
        if self.commit_id_table.get(&commit)?.is_some() {
            return Err(StorageError::CommitIdAlreadyExistsInHistory);
        }

        Ok(self.pending_part.add_node(updates, commit, parent_commit)?)
    }

    fn get_history_number_by_commit_id(&self, commit: CommitID) -> Result<HistoryNumber> {
        if let Some(value) = self.commit_id_table.get(&commit)? {
            Ok(value.into_owned())
        } else {
            Err(StorageError::CommitIDNotFound)
        }
    }

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
        Some(Ok((k, _))) => {
            let HistoryIndexKey(_, history_number) = k.as_ref();
            *history_number
        }
    };

    change_history_table.get_versioned_key(&found_version_number, key)
}

#[cfg(test)]
fn iter_history<'db, T: VersionedKeyValueSchema>(
    query_version_number: HistoryNumber,
    history_index_table: &TableReader<'db, HistoryIndicesTable<T>>,
    change_history_table: &KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
) -> Result<BTreeMap<T::Key, ValueEntry<T::Value>>> {
    // Walk every distinct key in `history_index_table` and, for each, compute
    // the value visible at `query_version_number` via `get_versioned_key`.
    let mut history_map: BTreeMap<T::Key, ValueEntry<T::Value>> = BTreeMap::new();

    let iter = history_index_table.iter_from_start()?;
    for item in iter {
        let (k, _) = item?;
        let HistoryIndexKey(key, _) = k.as_ref();
        if history_map.contains_key(key) {
            continue;
        }
        let value = get_versioned_key(
            query_version_number,
            key,
            history_index_table,
            change_history_table,
        )?;
        history_map.insert(key.clone(), ValueEntry::from_option(value));
    }

    Ok(history_map)
}

pub fn confirmed_pending_to_history<D: DatabaseTrait, T: VersionedKeyValueSchema>(
    db: &D,
    pending_part: &mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
    new_root_commit_id: CommitID,
    write_schema: &D::WriteSchema,
) -> Result<()> {
    let confirmed_path = pending_part.change_root(new_root_commit_id)?;

    confirm_ids_to_history::<D>(
        db,
        confirmed_path.start_height,
        &confirmed_path.commit_ids,
        write_schema,
    )?;

    confirm_maps_to_history::<D, T>(
        db,
        confirmed_path.start_height,
        confirmed_path.key_value_maps,
        write_schema,
    )?;

    Ok(())
}

pub fn confirm_maps_to_history<D: DatabaseTrait, T: VersionedKeyValueSchema>(
    _db: &D,
    to_confirm_start_height: usize,
    to_confirm_maps: Vec<HashMap<T::Key, impl Into<Option<T::Value>>>>,
    write_schema: &D::WriteSchema,
) -> Result<()> {
    let change_history_table =
        KeyValueStoreBulks::new(Arc::from(_db.view::<HistoryChangeTable<T>>()?));

    for (delta_height, updates) in to_confirm_maps.into_iter().enumerate() {
        let height = to_confirm_start_height + delta_height;
        let history_number = height_to_history_number(height);

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
            write_schema,
        )?;
    }

    Ok(())
}

pub fn confirm_ids_to_history<D: DatabaseTrait>(
    db: &D,
    to_confirm_start_height: usize,
    to_confirm_ids: &[CommitID],
    write_schema: &D::WriteSchema,
) -> Result<()> {
    let commit_id_table = db.view::<CommitIDSchema>()?;
    let history_number_table = db.view::<HistoryNumberSchema>()?;

    for (delta_height, confirmed_commit_id) in to_confirm_ids.iter().enumerate() {
        let height = to_confirm_start_height + delta_height;
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
        write_schema.write::<CommitIDSchema>(commit_id_table_op);

        let history_number_table_op = (
            Cow::Owned(history_number),
            Some(Cow::Owned(*confirmed_commit_id)),
        );
        write_schema.write::<HistoryNumberSchema>(history_number_table_op);
    }

    Ok(())
}
