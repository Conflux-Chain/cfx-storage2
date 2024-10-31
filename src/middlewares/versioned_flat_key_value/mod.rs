mod key_value_store_manager_impl;
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

#[cfg(test)]
use crate::middlewares::commit_id_schema::history_number_to_height;

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

pub struct VersionedStore<'cache, 'db, T: VersionedKeyValueSchema> {
    pending_part: &'cache mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    history_number_table: TableReader<'db, HistoryNumberSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
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
    to_confirm_ids_maps: Vec<(CommitID, BTreeMap<T::Key, Option<T::Value>>)>,
) -> Result<()> {
    let write_schema = D::write_schema();

    {
        let history_index_table = Arc::new(db.view::<HistoryIndicesTable<T>>()?);
        let commit_id_table = Arc::new(db.view::<CommitIDSchema>()?);
        let history_number_table = Arc::new(db.view::<HistoryNumberSchema>()?);
        let change_history_table =
            KeyValueStoreBulks::new(Arc::new(db.view::<HistoryChangeTable<T>>()?));

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

            change_history_table.commit(history_number, updates.into_iter(), &write_schema)?;
        }
    }

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

// private helper methods
impl<'cache, 'db, T: VersionedKeyValueSchema> VersionedStore<'cache, 'db, T> {
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

    // fn find_larger_historical_key(&self, key: &T::Key) -> Result<Option<T::Key>> {
    //     // todo: here correct?
    //     let range_query_key = HistoryIndexKey(key.clone(), MIN_HISTORY_NUMBER_MINUS_ONE);

    //     match self.history_index_table.iter(&range_query_key)?.next() {
    //         None => Ok(None),
    //         Some(Err(e)) => Err(e.into()),
    //         Some(Ok((k, _))) if &k.as_ref().0 > key => Ok(Some(k.as_ref().0.to_owned())),
    //         _ => unreachable!(
    //             "iter((key, MIN_HISTORY_NUMBER_MINUS_ONE)) should not result in k <= key"
    //         ),
    //     }
    // }
}

// callable methods
impl<'cache, 'db, T: VersionedKeyValueSchema> VersionedStore<'cache, 'db, T> {
    #[cfg(test)]
    pub fn check_consistency(&self) -> Result<()> {
        if self.check_consistency_inner().is_err() {
            Err(StorageError::ConsistencyCheckFailure)
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn check_consistency_inner(&self) -> Result<()> {
        if let Some(parent) = self.pending_part.get_parent_of_root() {
            let parent_history_number =
                if let Some(parent_history_number) = self.commit_id_table.get(&parent)? {
                    parent_history_number.into_owned()
                } else {
                    return Err(StorageError::ConsistencyCheckFailure);
                };

            let mut history_number = parent_history_number;
            let min_history_number = height_to_history_number(0);
            while history_number >= min_history_number {
                let commit_id =
                    if let Some(commit_id) = self.history_number_table.get(&history_number)? {
                        commit_id.into_owned()
                    } else {
                        return Err(StorageError::ConsistencyCheckFailure);
                    };
                let check_history_number =
                    if let Some(check_history_number) = self.commit_id_table.get(&commit_id)? {
                        check_history_number.into_owned()
                    } else {
                        return Err(StorageError::ConsistencyCheckFailure);
                    };
                if history_number != check_history_number {
                    return Err(StorageError::ConsistencyCheckFailure);
                };
                history_number -= 1;
            }

            let parent_history_number_plus_one = parent_history_number + 1;
            if self
                .history_number_table
                .iter(&parent_history_number_plus_one)?
                .next()
                .is_some()
            {
                return Err(StorageError::ConsistencyCheckFailure);
            }

            if self.commit_id_table.iter_from_start()?.count()
                != self.history_number_table.iter_from_start()?.count()
            {
                return Err(StorageError::ConsistencyCheckFailure);
            }

            if !self
                .pending_part
                .check_consistency(history_number_to_height(parent_history_number + 1))
            {
                return Err(StorageError::ConsistencyCheckFailure);
            }
            // todo: history_index_table, change_table, min_key
        } else if self.commit_id_table.iter_from_start()?.next().is_some()
            || self
                .history_number_table
                .iter_from_start()?
                .next()
                .is_some()
            || self.history_index_table.iter_from_start()?.next().is_some()
            || self
                .change_history_table
                .iter_from_start()?
                .next()
                .is_some()
        {
            return Err(StorageError::ConsistencyCheckFailure);
        }

        Ok(())
    }

    pub fn new<D: DatabaseTrait>(
        db: &'db D,
        pending_part: &'cache mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
    ) -> Result<Self> {
        let history_index_table = Arc::new(db.view::<HistoryIndicesTable<T>>()?);
        let commit_id_table = Arc::new(db.view::<CommitIDSchema>()?);
        let history_number_table = Arc::new(db.view::<HistoryNumberSchema>()?);
        let change_history_table =
            KeyValueStoreBulks::new(Arc::new(db.view::<HistoryChangeTable<T>>()?));

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
        updates: BTreeMap<T::Key, Option<T::Value>>,
    ) -> Result<()> {
        if self.commit_id_table.get(&commit)?.is_some() {
            return Err(StorageError::CommitIdAlreadyExistsInHistory);
        }

        Ok(self.pending_part.add_node(updates, commit, parent_commit)?)
    }
}
