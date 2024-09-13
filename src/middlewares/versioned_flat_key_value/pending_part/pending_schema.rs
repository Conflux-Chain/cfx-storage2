use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
};

use crate::middlewares::versioned_flat_key_value::table_schema::VersionedKeyValueSchema;

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
    pub value: Option<S::Value>,
    pub last_commit_id: Option<S::CommitId>,
}

pub struct ApplyRecord<S: PendingKeyValueSchema> {
    pub value: Option<S::Value>,
    pub commit_id: S::CommitId,
}

pub type RecoverMap<S> = BTreeMap<Key<S>, RecoverRecord<S>>;
pub type KeyValueMap<S> = BTreeMap<Key<S>, Option<Value<S>>>;
pub type ApplyMap<S> = BTreeMap<Key<S>, ApplyRecord<S>>;

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
