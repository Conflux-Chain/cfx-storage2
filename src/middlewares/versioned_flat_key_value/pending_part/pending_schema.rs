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

pub type Modifications<S> = Vec<(
    <S as PendingKeyValueSchema>::Key,
    Option<<S as PendingKeyValueSchema>::Value>,
    Option<<S as PendingKeyValueSchema>::CommitId>,
)>;
pub type Commits<S> = HashMap<
    <S as PendingKeyValueSchema>::Key,
    (
        <S as PendingKeyValueSchema>::CommitId,
        Option<<S as PendingKeyValueSchema>::Value>,
    ),
>;
pub type Rollbacks<S> =
    HashMap<<S as PendingKeyValueSchema>::Key, Option<<S as PendingKeyValueSchema>::CommitId>>;
pub type History<S> = HashMap<
    <S as PendingKeyValueSchema>::CommitId,
    HashMap<<S as PendingKeyValueSchema>::Key, Option<<S as PendingKeyValueSchema>::Value>>,
>;
pub type Current<S> = BTreeMap<
    <S as PendingKeyValueSchema>::Key,
    (
        <S as PendingKeyValueSchema>::CommitId,
        Option<<S as PendingKeyValueSchema>::Value>,
    ),
>;
pub type ToCommit<S> = Vec<(
    <S as PendingKeyValueSchema>::CommitId,
    Option<HashMap<<S as PendingKeyValueSchema>::Key, Option<<S as PendingKeyValueSchema>::Value>>>,
)>;
pub type CIdVecPair<S> = (
    Vec<<S as PendingKeyValueSchema>::CommitId>,
    Vec<<S as PendingKeyValueSchema>::CommitId>,
);
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
