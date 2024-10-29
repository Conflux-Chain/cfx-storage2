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

use super::commit_id_schema::{HistoryNumberSchema, MIN_HISTORY_NUMBER_MINUS_ONE};
use super::ChangeKey;
use super::CommitIDSchema;
use crate::backends::{DatabaseTrait, TableRead, TableReader, WriteSchemaTrait};
use crate::errors::Result;
use crate::middlewares::commit_id_schema::{height_to_history_number, history_number_to_height};
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

    fn confirm_one_to_history(
        &mut self,
        height: usize,
        confirmed_commit_id: CommitID,
        updates: BTreeMap<T::Key, Option<T::Value>>,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()> {
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

        Ok(())
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
    pub fn check_consistency(&self) -> Result<()> {
        if self.check_consistency_inner().is_err() {
            Err(StorageError::ConsistencyCheckFailure)
        } else {
            Ok(())
        }
    }
    fn check_consistency_inner(&self) -> Result<()> {
        if let Some(parent) = self.pending_part.get_parent_of_root() {
            let parent_history_number =
                if let Some(parent_history_number) = self.commit_id_table.get(&parent)? {
                    parent_history_number.into_owned()
                } else {
                    return Err(StorageError::ConsistencyCheckFailure);
                };

            let mut last_history_number = parent_history_number;
            for history_number_cid in self.history_number_table.iter(&parent_history_number)? {
                let (history_number, commit_id) = history_number_cid?;
                let history_number = history_number.into_owned();
                let commit_id = commit_id.into_owned();

                if history_number + 1 != last_history_number {
                    return Err(StorageError::ConsistencyCheckFailure);
                }

                let check_history_number =
                    if let Some(check_history_number) = self.commit_id_table.get(&commit_id)? {
                        check_history_number.into_owned()
                    } else {
                        return Err(StorageError::ConsistencyCheckFailure);
                    };
                if history_number != check_history_number {
                    return Err(StorageError::ConsistencyCheckFailure);
                };

                last_history_number = history_number;
            }

            if last_history_number != 1 {
                return Err(StorageError::ConsistencyCheckFailure);
            };

            if self.commit_id_table.len() != self.history_number_table.len() {
                return Err(StorageError::ConsistencyCheckFailure);
            }

            if !self
                .pending_part
                .check_consistency(history_number_to_height(parent_history_number + 1))
            {
                return Err(StorageError::ConsistencyCheckFailure);
            }

            // todo: history_index_table, change_table, min_key
        } else if !self.commit_id_table.is_empty()
            || !self.history_number_table.is_empty()
            || !self.history_index_table.is_empty()
            || !self.change_history_table.is_empty()
            || self.history_min_key.is_some()
        {
            return Err(StorageError::ConsistencyCheckFailure);
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn help_new<D: DatabaseTrait>(
        db: &'db D,
        write_schema: &impl WriteSchemaTrait,
        pending_part: &'db mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
        to_confirm_start_height: usize,
        to_confirm_cids: Vec<CommitID>,
        to_confirm_updates: Vec<BTreeMap<T::Key, Option<T::Value>>>,
    ) -> Result<()> {
        let mut versioned_store = VersionedStore::new(db, pending_part, false)?;

        assert_eq!(to_confirm_cids.len(), to_confirm_updates.len());

        for (delta_height, (confirmed_commit_id, updates)) in to_confirm_cids
            .into_iter()
            .zip(to_confirm_updates.into_iter())
            .enumerate()
        {
            let height = to_confirm_start_height + delta_height;
            versioned_store.confirm_one_to_history(
                height,
                confirmed_commit_id,
                updates,
                &write_schema,
            )?;
        }

        Ok(())
    }

    pub fn new<D: DatabaseTrait>(
        db: &'db D,
        pending_part: &'db mut VersionedMap<PendingKeyValueConfig<T, CommitID>>,
        check_consistency: bool,
    ) -> Result<Self> {
        let history_index_table = Arc::new(db.view::<HistoryIndicesTable<T>>()?);
        let commit_id_table = Arc::new(db.view::<CommitIDSchema>()?);
        let history_number_table = Arc::new(db.view::<HistoryNumberSchema>()?);
        let change_history_table =
            KeyValueStoreBulks::new(Arc::new(db.view::<HistoryChangeTable<T>>()?));

        dbg!("1");
        // todo: here correct?
        let history_min_key = history_index_table
            .min_key()?
            .map(|min_k| min_k.into_owned().0);

        dbg!("2");
        let versioned_store = VersionedStore {
            pending_part,
            history_index_table,
            commit_id_table,
            history_number_table,
            change_history_table,
            history_min_key,
        };

        if check_consistency {
            versioned_store.check_consistency()?;
        }
        dbg!("3");

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
            self.confirm_one_to_history(height, confirmed_commit_id, updates, write_schema)?;
        }

        Ok(())
    }
}
