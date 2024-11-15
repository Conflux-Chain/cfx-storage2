use std::{collections::BTreeMap, fmt::Debug, hash::Hash, marker::PhantomData};

use crate::middlewares::versioned_flat_key_value::table_schema::VersionedKeyValueSchema;
use crate::types::ValueEntry;

use super::PendingError;

/// A trait that constrains the types used in the pending part of the versioned key-value store.
pub trait PendingKeyValueSchema {
    type Key: Eq + Hash + Clone + Ord;
    type CommitId: Debug + Eq + Hash + Copy;
    type Value: Clone;
}

type Key<S> = <S as PendingKeyValueSchema>::Key;
type Value<S> = <S as PendingKeyValueSchema>::Value;
type CommitId<S> = <S as PendingKeyValueSchema>::CommitId;

/// Defines a record that tracks both this modification (`value`) made to a key in a node relative to its parent node
/// and information about where the last modification (`last_commit_id`) occurred before this one.
///
/// # Notes:
/// - Even if `last_commit_id` is `Some`, it may no longer exist in the tree if it has been removed from the pending part.
pub struct ChangeWithRecoverRecord<S: PendingKeyValueSchema> {
    pub value: ValueEntry<S::Value>,
    /// - `None`: no previous modification exists in the tree.
    /// - `Some`: the `last_commit_id` exists or once existed in the pending part,
    ///   but additional checks are needed to confirm its current presence.
    pub last_commit_id: Option<S::CommitId>,
}

/// Defines a modification (`value`) made to a key in a node (`commit_id`) relative to its parent node.
pub struct ApplyRecord<S: PendingKeyValueSchema> {
    pub value: ValueEntry<S::Value>,
    pub commit_id: S::CommitId,
}

pub type KeyValueMap<S> = BTreeMap<Key<S>, ValueEntry<Value<S>>>;
pub type ChangeWithRecoverMap<S> = BTreeMap<Key<S>, ChangeWithRecoverRecord<S>>;
pub type ApplyMap<S> = BTreeMap<Key<S>, ApplyRecord<S>>;
pub type LastCommitIdMap<S> = BTreeMap<Key<S>, Option<CommitId<S>>>;

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
