use std::{fmt::Debug, hash::Hash, marker::PhantomData};

use crate::middlewares::versioned_flat_key_value::table_schema::VersionedKeyValueSchema;

pub trait PendingKeyValueSchema {
    type Key: Eq + Hash + Clone + Ord;
    type CommitId: Debug + Eq + Hash + Copy;
    type Value: Clone;
}

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