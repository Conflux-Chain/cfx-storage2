use std::{collections::HashMap, fmt::Debug, hash::Hash, marker::PhantomData};

use crate::middlewares::versioned_flat_key_value::table_schema::VersionedKeyValueSchema;

use super::PendingError;

pub trait PendingKeyValueSchema {
    type Key: Eq + Hash + Clone + Ord;
    type CommitId: Debug + Eq + Hash + Copy;
    type Value: Clone;
}

pub type Key<S> = <S as PendingKeyValueSchema>::Key;
pub type Value<S> = <S as PendingKeyValueSchema>::Value;
pub type CommitId<S> = <S as PendingKeyValueSchema>::CommitId;

pub type OptValue<S> = Option<Value<S>>;
pub type OptCId<S> = Option<CommitId<S>>;
pub type CIdOptValue<S> = (CommitId<S>, OptValue<S>);

pub type CIdVec<S> = Vec<CommitId<S>>;
pub type ToCommit<S> = Vec<(CommitId<S>, Option<HashMap<Key<S>, OptValue<S>>>)>;
pub type RollComm<S> = (HashMap<Key<S>, OptCId<S>>, HashMap<Key<S>, CIdOptValue<S>>);

pub type PendResult<T, S> = Result<T, PendingError<CommitId<S>>>;

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
