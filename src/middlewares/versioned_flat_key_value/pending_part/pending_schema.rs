//! This module defines the core structures and types used in the pending part of a versioned key-value store.
//! The pending part is responsible for managing in-memory changes that have not yet been committed to the historical part.
//! All structures in this module are constrained by the `PendingKeyValueSchema` trait, which defines the types of keys,
//! values, and commit IDs used in the system.

use std::{collections::BTreeMap, fmt::Debug, hash::Hash, marker::PhantomData};

use crate::middlewares::versioned_flat_key_value::table_schema::VersionedKeyValueSchema;
use crate::types::ValueEntry;

use super::PendingError;

/// Defines a trait that constrains the types used in the pending part of the versioned key-value store.
/// All structures related to the pending part (e.g., `VersionedMap`, `Tree`, `CurrentMap`, `TreeNode`)
/// are generic over a type `S` that must implement this trait.
/// This ensures compatibility with the system by enforcing consistent types for keys, values, and commit IDs.
///
/// # Associated Types:
/// - `Key`: The type used for keys in the key-value store.
/// - `CommitId`: The type used for commit IDs.
/// - `Value`: The type used for values stored in the key-value store.
pub trait PendingKeyValueSchema {
    type Key: Eq + Hash + Clone + Ord;
    type CommitId: Debug + Eq + Hash + Copy;
    type Value: Clone;
}

/// Type aliases for commonly used types based on the `PendingKeyValueSchema` trait.
/// These aliases simplify code by referring to types using a generic schema (`S`).
type Key<S> = <S as PendingKeyValueSchema>::Key;
type Value<S> = <S as PendingKeyValueSchema>::Value;
type CommitId<S> = <S as PendingKeyValueSchema>::CommitId;

/// Defines a record that tracks both this modification made to a key in a node relative to its parent node
/// and information about where the last modification occurred before this one.
///
/// # Fields:
/// - `value`: An enum (`ValueEntry`) representing this modification:
///   - `Deleted`: Indicates that the key was deleted.
///   - `Value(value)`: Indicates that the key was modified to a specific value.
/// - `last_commit_id`: An optional commit ID indicating where the last modification occurred before this one:
///   - If `None`, no previous modification exists in the tree.
///   - If `Some(last_cid)`, the last modification occurred at commit ID `last_cid`.
///     Note that this commit may no longer exist in the tree if it has been removed from the pending part.
pub struct ChangeWithRecoverRecord<S: PendingKeyValueSchema> {
    pub value: ValueEntry<S::Value>,
    pub last_commit_id: Option<S::CommitId>,
}

/// Defines a record that tracks changes made to a key in a node relative to its parent node.
///
/// # Fields:
/// - `value`: An enum (`ValueEntry`) representing this modification:
///   - `Deleted`: Indicates that the key was deleted.
///   - `Value(value)`: Indicates that the key was modified to a specific value.
/// - `commit_id`: The commit ID where this modification occurred. It is ensured that this commit exists in the tree.
pub struct ApplyRecord<S: PendingKeyValueSchema> {
    pub value: ValueEntry<S::Value>,
    pub commit_id: S::CommitId,
}

/// Type aliases for commonly used maps based on the schema (`S`).
/// These maps are used throughout the pending part of the system to track changes and manage rollbacks.
pub type KeyValueMap<S> = BTreeMap<Key<S>, ValueEntry<Value<S>>>;
pub type ChangeWithRecoverMap<S> = BTreeMap<Key<S>, ChangeWithRecoverRecord<S>>;
pub type ApplyMap<S> = BTreeMap<Key<S>, ApplyRecord<S>>;
pub type LastCommitIdMap<S> = BTreeMap<Key<S>, Option<CommitId<S>>>;

/// Additional type aliases for convenience.
pub type CommitIdVec<S> = Vec<CommitId<S>>;
pub type Result<T, S> = std::result::Result<T, PendingError<CommitId<S>>>;

/// A configuration struct that implements the `PendingKeyValueSchema` trait.
/// This struct is parameterized by two types:
/// - `T`: A schema that implements versioning for keys and values (`VersionedKeyValueSchema`).
/// - `CId`: The type used for commit IDs.
///
/// This struct serves as a placeholder configuration for defining how keys, values, and commit IDs
/// are handled within the pending part of a versioned key-value store.
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
