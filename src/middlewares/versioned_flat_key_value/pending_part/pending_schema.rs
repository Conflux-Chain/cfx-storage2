use std::{collections::HashMap, fmt::Debug, hash::Hash, marker::PhantomData};

use nonempty::NonEmpty;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::backends::serde::{Decode, Encode, FixedLengthEncoded};
use crate::backends::{TableKey, VersionedKVName};
use crate::middlewares::versioned_flat_key_value::table_schema::VersionedKeyValueSchema;
use crate::types::ValueEntry;

use super::PendingError;

pub trait PendingKeyValueSchema: 'static + Copy + Send + Sync + Debug {
    const KV_NAME: VersionedKVName;

    type Key: TableKey + ToOwned<Owned = Self::Key> + Clone + Hash;
    type CommitId: ToOwned<Owned = Self::CommitId>
        + Debug
        + Eq
        + Ord
        + Hash
        + Copy
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static
        + Encode
        + Decode
        + FixedLengthEncoded;
    type Value: ToOwned<Owned = Self::Value>
        + Clone
        + Eq
        + Debug
        + Send
        + Sync
        + 'static
        + Encode
        + Decode;
}

type Key<S> = <S as PendingKeyValueSchema>::Key;
type Value<S> = <S as PendingKeyValueSchema>::Value;
type CommitId<S> = <S as PendingKeyValueSchema>::CommitId;

#[derive(Clone, PartialEq, Eq, Debug)]
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
    pub start_height: u64,
    pub commit_ids: NonEmpty<S::CommitId>,
    pub key_value_maps: NonEmpty<KeyValueMap<S>>,
}

impl<S: PendingKeyValueSchema> ConfirmedPathInfo<S> {
    pub fn get_new_height_of_root(&self) -> u64 {
        self.start_height + self.key_value_maps.len() as u64
    }

    pub fn get_new_parent_of_root(&self) -> S::CommitId {
        *self.commit_ids.last()
    }

    pub fn is_same_path<T: PendingKeyValueSchema>(&self, other: &ConfirmedPathInfo<T>) -> bool
    where
        S::CommitId: PartialEq<T::CommitId>,
    {
        self.start_height == other.start_height
            && self.commit_ids.iter().eq(other.commit_ids.iter())
    }
}

pub type KeyValueMap<S> = HashMap<Key<S>, ValueEntry<Value<S>>>;
pub type KeyValueIter<'a, S> = Box<dyn Iterator<Item = (Key<S>, ValueEntry<Value<S>>)> + 'a>;
pub type RecoverMap<S> = HashMap<Key<S>, RecoverRecord<S>>;
pub type ApplyMap<S> = HashMap<Key<S>, ApplyRecord<S>>;
pub type LastCommitIdMap<S> = HashMap<Key<S>, Option<CommitId<S>>>;

pub type CommitIdVec<S> = Vec<CommitId<S>>;
pub type Result<T> = std::result::Result<T, PendingError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingKeyValueConfig<T, CId> {
    _marker: PhantomData<(T, CId)>,
}

impl<T, CId> PendingKeyValueSchema for PendingKeyValueConfig<T, CId>
where
    T: VersionedKeyValueSchema,
    CId: ToOwned<Owned = CId>
        + Debug
        + Eq
        + Ord
        + Hash
        + Copy
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static
        + Encode
        + Decode
        + FixedLengthEncoded,
{
    const KV_NAME: VersionedKVName = T::NAME;

    type Key = T::Key;
    type CommitId = CId;
    type Value = T::Value;
}
