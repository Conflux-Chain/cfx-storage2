//! Implementation of [`TreeNode`]

use std::collections::BTreeSet;

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::{
    ApplyMap, ApplyRecord, ChangeWithRecoverMap, ChangeWithRecoverRecord, KeyValueMap,
    LastCommitIdMap, PendingKeyValueSchema,
};
use crate::types::ValueEntry;

use super::SlabIndex;

/// Represents a node in [`super::Tree`].
pub(super) struct TreeNode<S: PendingKeyValueSchema> {
    /// The [`SlabIndex`] of this node's parent node.
    /// `None` means this node is the root of the tree.
    ///
    /// # Notes:
    /// The `parent` field in [`TreeNode`] is used for indexing within the tree itself.
    /// For a root, the `parent` is `None`, meaning that this node cannot be indexed to a parent within the tree.
    /// However, this has no relation to whether the parent of the tree root exists in the entire underlying database.
    /// The concept of the parent of the tree root is maintained by the [`super::Tree`] structure and refers to a broader context
    /// beyond just this tree.
    parent: Option<SlabIndex>,
    /// The [`SlabIndex`] of this node's children nodes.
    children: BTreeSet<SlabIndex>,

    /// The height of this node in the entire underlying database.
    /// The height will not be changed even if the pending tree root is changed.
    height: usize,

    /// The `CommitId` of this node.
    commit_id: S::CommitId,

    /// The changes from the parent node to this node.
    /// Each change is recorded as a (`Key`, [`ChangeWithRecoverRecord`]) pair.
    modifications: ChangeWithRecoverMap<S>,
}

impl<S: PendingKeyValueSchema> TreeNode<S> {
    /// Creates a new [`TreeNode`] instance representing the root node in the tree.
    pub fn new_root(
        commit_id: S::CommitId,
        modifications: ChangeWithRecoverMap<S>,
        height: usize,
    ) -> Self {
        Self {
            height,
            commit_id,
            parent: None,
            children: BTreeSet::new(),
            modifications,
        }
    }

    /// Creates a new [`TreeNode`] instance representing a non-root node in the tree.
    pub fn new_non_root_node(
        commit_id: S::CommitId,
        parent: SlabIndex,
        height: usize,
        modifications: ChangeWithRecoverMap<S>,
    ) -> Self {
        Self {
            height,
            commit_id,
            parent: Some(parent),
            children: BTreeSet::new(),
            modifications,
        }
    }

    /// Retrieves the [`SlabIndex`] of this node's parent node.
    /// `None` means this node is the root of the tree.
    pub fn get_parent(&self) -> Option<SlabIndex> {
        self.parent
    }

    /// Sets this node as the root by removing its parent reference.
    pub fn set_as_root(&mut self) {
        self.parent = None;
    }

    /// Retrieves a reference to this node's children.
    pub fn get_children(&self) -> &BTreeSet<SlabIndex> {
        &self.children
    }

    /// Inserts a new child into this node's set of children.
    pub fn insert_child(&mut self, new_child: SlabIndex) {
        self.children.insert(new_child);
    }

    /// Clears children nodes leaving only one specified child.
    pub fn remove_child_except(&mut self, child_to_retain: &SlabIndex) {
        self.children = BTreeSet::from([*child_to_retain]);
    }

    /// Retrieves the height of this node in the underlying database.
    pub fn get_height(&self) -> usize {
        self.height
    }

    /// Retrieves the `CommitId` of this node.
    pub fn get_commit_id(&self) -> S::CommitId {
        self.commit_id
    }

    /// Retrieves the modification for a given `key` in this node relative to its parent.
    ///
    /// # Returns:
    /// - `Some(ValueEntry::Value(value))` indicates that the `key` was modified to the `value`.
    /// - `Some(ValueEntry::Deleted)` indicates that the `key` was deleted in this node.
    /// - `None` indicates that no modification for this `key` exists in this node.
    pub fn get_modified_value(&self, key: &S::Key) -> Option<ValueEntry<S::Value>> {
        self.modifications.get(key).map(|v| v.value.clone())
    }

    /// Retrieves the modification with the recovery record for a given `key` in this node relative to its parent.
    ///
    /// Returns `None` if no modification for the `key` exists in this node.
    pub fn get_recover_record(&self, key: &S::Key) -> Option<&ChangeWithRecoverRecord<S>> {
        self.modifications.get(key)
    }

    /// Retrieves all updates (modifications) made in this node relative to its parent as a `Key`-[`ValueEntry`] map.
    pub fn get_updates(&self) -> KeyValueMap<S> {
        self.modifications
            .iter()
            .map(|(k, ChangeWithRecoverRecord { value, .. })| (k.clone(), value.clone()))
            .collect()
    }

    /// Exports rollback data (the `CommitId` of the last modification relative to each modification made by this node)
    /// into `rollbacks` based on the `OVERRIDE` flag.
    ///
    /// # Notes:
    /// - The `last_commit_id`s populated to `rollbacks` may not exist in the tree, which should be handled by the caller.
    /// - Only the case where `OVERRIDE` is `true` has been used and tested.
    pub fn export_rollback_data<const OVERRIDE: bool>(&self, rollbacks: &mut LastCommitIdMap<S>) {
        for (key, ChangeWithRecoverRecord { last_commit_id, .. }) in self.modifications.iter() {
            if OVERRIDE {
                rollbacks.insert(key.clone(), *last_commit_id);
            } else {
                rollbacks.entry(key.clone()).or_insert(*last_commit_id);
            }
        }
    }

    /// Exports commit data (the modifications made by this node) into `commits` based on the `OVERRIDE` flag.
    ///
    /// # Notes:
    /// Only the case where `OVERRIDE` is `false` has been used and tested.
    pub fn export_commit_data<const OVERRIDE: bool>(&self, commits: &mut ApplyMap<S>) {
        let commit_id = self.commit_id;
        for (key, ChangeWithRecoverRecord { value, .. }) in self.modifications.iter() {
            let new_record = || ApplyRecord {
                commit_id,
                value: value.clone(),
            };
            if OVERRIDE {
                commits.insert(key.clone(), new_record());
            } else {
                commits.entry(key.clone()).or_insert_with(new_record);
            }
        }
    }
}
