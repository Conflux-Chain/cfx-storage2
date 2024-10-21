mod key_value_store_manager_impl;
mod pending_part;
mod serde;
mod table_schema;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::BTreeMap;

pub use pending_part::PendingError;

use self::pending_part::pending_schema::PendingKeyValueConfig;
use self::table_schema::{HistoryChangeTable, HistoryIndicesTable, VersionedKeyValueSchema};
use pending_part::VersionedMap;

use super::commit_id_schema::{HistoryNumberSchema, MIN_HISTORY_NUMBER_MINUS_ONE};
use super::ChangeKey;
use super::CommitIDSchema;
use crate::backends::{TableReader, WriteSchemaTrait};
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

pub struct VersionedStore<'db, T: VersionedKeyValueSchema> {
    pending_part: &'db mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
    history_index_table: TableReader<'db, HistoryIndicesTable<T>>,
    commit_id_table: TableReader<'db, CommitIDSchema>,
    history_number_table: TableReader<'db, HistoryNumberSchema>,
    change_history_table: KeyValueStoreBulks<'db, HistoryChangeTable<T>>,
    history_min_key: Option<T::Key>,
}

// private helper methods
impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
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
        let range_query_key = HistoryIndexKey(key.clone(), query_version_number);

        let found_version_number = match self.history_index_table.iter(&range_query_key)?.next() {
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

        self.change_history_table
            .get_versioned_key(&found_version_number, key)
    }

    fn find_larger_historical_key(&self, key: &T::Key) -> Result<Option<T::Key>> {
        // todo: here correct?
        let range_query_key = HistoryIndexKey(key.clone(), MIN_HISTORY_NUMBER_MINUS_ONE);

        match self.history_index_table.iter(&range_query_key)?.next() {
            None => Ok(None),
            Some(Err(e)) => Err(e.into()),
            Some(Ok((k, _))) if &k.as_ref().0 > key => Ok(Some(k.as_ref().0.to_owned())),
            _ => unreachable!(
                "iter((key, MIN_HISTORY_NUMBER_MINUS_ONE)) should not result in k <= key"
            ),
        }
    }
}

fn need_update_min_key<K: Ord>(original_min: Option<&K>, challenge_min: &K) -> bool {
    if let Some(original_min) = original_min {
        original_min > challenge_min
    } else {
        true
    }
}

// callable methods
impl<'db, T: VersionedKeyValueSchema> VersionedStore<'db, T> {
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

    pub fn confirmed_pending_to_history(
        &mut self,
        new_root_commit_id: CommitID,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()> {
        // old root..=new root's parent
        let (start_height, confirmed_ids_maps) =
            self.pending_part.change_root(new_root_commit_id)?;

        for (delta_height, (confirmed_commit_id, updates)) in
            confirmed_ids_maps.into_iter().enumerate()
        {
            let height = start_height + delta_height;
            let history_number = height_to_history_number(height);

            assert!(self.commit_id_table.get(&confirmed_commit_id)?.is_none());
            assert!(self.history_number_table.get(&history_number)?.is_none());

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

            if let Some(this_min_k) = updates.keys().min().cloned() {
                if need_update_min_key(self.history_min_key.as_ref(), &this_min_k) {
                    self.history_min_key = Some(this_min_k);
                }
            }

            self.change_history_table
                .commit(history_number, updates.into_iter(), write_schema)?;
        }

        Ok(())
    }
}
