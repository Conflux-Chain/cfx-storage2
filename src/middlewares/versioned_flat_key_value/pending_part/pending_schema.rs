use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
};

use crate::middlewares::versioned_flat_key_value::table_schema::VersionedKeyValueSchema;

pub trait PendingKeyValueSchema {
    type Key: Eq + Hash + Clone + Ord;
    type CommitId: Debug + Eq + Hash + Copy;
    type Value: Clone;
}

pub type Key<S> = <S as PendingKeyValueSchema>::Key;
pub type Value<S> = <S as PendingKeyValueSchema>::Value;
pub type CommitId<S> = <S as PendingKeyValueSchema>::CommitId;

pub type Modifications<S> = Vec<(Key<S>, Option<Value<S>>, Option<CommitId<S>>)>;
pub type Commits<S> = HashMap<Key<S>, (CommitId<S>, Option<Value<S>>)>;
pub type Rollbacks<S> = HashMap<Key<S>, Option<CommitId<S>>>;
pub type History<S> = HashMap<CommitId<S>, HashMap<Key<S>, Option<Value<S>>>>;
pub type Current<S> = BTreeMap<Key<S>, (CommitId<S>, Option<Value<S>>)>;
pub type ToCommit<S> = Vec<(CommitId<S>, Option<HashMap<Key<S>, Option<Value<S>>>>)>;
pub type CIdVecPair<S> = (Vec<CommitId<S>>, Vec<CommitId<S>>);
pub type RollComm<S> = (Rollbacks<S>, Commits<S>);

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
