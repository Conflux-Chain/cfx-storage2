use std::{collections::HashMap, fmt::Debug, hash::Hash, marker::PhantomData};

use crate::middlewares::versioned_flat_key_value::table_schema::VersionedKeyValueSchema;
use crate::types::ValueEntry;

use super::PendingError;

pub trait PendingKeyValueSchema {
    type Key: Eq + Hash + Clone + Ord;
    type CommitId: Debug + Eq + Hash + Copy;
    type Value: Clone;
}

type Key<S> = <S as PendingKeyValueSchema>::Key;
type Value<S> = <S as PendingKeyValueSchema>::Value;
type CommitId<S> = <S as PendingKeyValueSchema>::CommitId;

pub struct RecoverRecord<S: PendingKeyValueSchema> {
    pub value: ValueEntry<S::Value>,
    pub last_commit_id: Option<S::CommitId>,
}

pub struct ApplyRecord<S: PendingKeyValueSchema> {
    pub value: ValueEntry<S::Value>,
    pub commit_id: S::CommitId,
}

/// `commit_ids` and `key_value_maps` should be ordered from the smallest height to the largest height.
pub struct ConfirmedPathInfo<S: PendingKeyValueSchema> {
    pub start_height: usize,
    pub commit_ids: Vec<S::CommitId>,
    pub key_value_maps: Vec<KeyValueMap<S>>,
}

impl<S: PendingKeyValueSchema> ConfirmedPathInfo<S> {
    pub fn is_same_path<T: PendingKeyValueSchema>(&self, other: &ConfirmedPathInfo<T>) -> bool
    where
        S::CommitId: PartialEq<T::CommitId>,
    {
        self.start_height == other.start_height && self.commit_ids == other.commit_ids
    }
}

pub type KeyValueMap<S> = HashMap<Key<S>, ValueEntry<Value<S>>>;
pub type RecoverMap<S> = HashMap<Key<S>, RecoverRecord<S>>;
pub type ApplyMap<S> = HashMap<Key<S>, ApplyRecord<S>>;
pub type LastCommitIdMap<S> = HashMap<Key<S>, Option<CommitId<S>>>;

pub type CommitIdVec<S> = Vec<CommitId<S>>;
pub type Result<T, S> = std::result::Result<T, PendingError<CommitId<S>>>;

pub struct PendingKeyValueConfig<T, CId> {
    _marker: PhantomData<(T, CId)>,
}

impl<T, CId> PendingKeyValueSchema for PendingKeyValueConfig<T, CId>
where
    T: VersionedKeyValueSchema,
    CId: Debug + Eq + Hash + Copy,
{
    type Key = T::Key;
    type CommitId = CId;
    type Value = T::Value;
}
