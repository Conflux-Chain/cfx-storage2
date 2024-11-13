use std::collections::BTreeSet;

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::{
    ApplyMap, ApplyRecord, ChangeWithRecoverMap, ChangeWithRecoverRecord, KeyValueMap,
    LastCommitIdMap, PendingKeyValueSchema,
};
use crate::types::ValueEntry;

use super::SlabIndex;

/// The structure representing a node in `Tree`.
pub(super) struct TreeNode<S: PendingKeyValueSchema> {
    /// The `SlabIndex` of this node's parent node.
    /// `None` means this node is the root of the tree.
    ///
    /// # Notes:
    /// The `parent` field in `TreeNode` is used for indexing within the tree itself.
    /// For a root `TreeNode`, `parent` is `None`, meaning that this node cannot be indexed to a parent within the tree.
    /// However, this has no relation to whether the parent of the tree root exists in the entire underlying database.
    /// The concept of the parent of the tree root is maintained by the `Tree` structure and refers to a broader context
    /// beyond just this tree.
    parent: Option<SlabIndex>,
    /// The `SlabIndex` of this node's children nodes.
    children: BTreeSet<SlabIndex>,

    /// The height of this node in the entire underlying database.
    /// The height will not be changed even if the pending tree root is changed.
    height: usize,

    /// The `CommitId` of this node.
    commit_id: S::CommitId,

    /// A `BTreeMap<Key, ChangeWithRecoverRecord>` representing the changes from the parent node to this node.
    ///   Each change is recorded as a (`Key`, `ChangeWithRecoverRecord`) pair.
    ///   `ChangeWithRecoverRecord` includes two fields:
    ///   - `value`: an enum type `ValueEntry` representing this modification:
    ///     - `ValueEntry::Deleted` represents deletion.
    ///     - `ValueEntry::Value(value)` represents a specific value.
    ///   - `last_commit_id`: an `Option<CommitId>` recording where the last modification before this modification occurred:
    ///     - `None`: no modification occurred before this modification in the tree.
    ///     - `Some(last_cid)`: the last modification occurred at `last_cid` in the `tree`.
    ///       `last_cid` may not be in the `Tree`, since the node at `last_cid` may have already been removed from the pending part.
    modifications: ChangeWithRecoverMap<S>,
}

impl<S: PendingKeyValueSchema> TreeNode<S> {
    /// Creates a new `TreeNode` instance representing the root node in the tree.
    ///
    /// This function initializes a root `TreeNode` with no parent, an empty set of children,
    /// and the given `commit_id`, modifications, and height.
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitId` of this root node.
    /// - `modifications`: A `ChangeWithRecoverMap` representing the changes made in this node relative to its parent.
    /// - `height`: The height of this node in the entire underlying database.
    ///
    /// # Returns:
    /// A new `TreeNode` that acts as the root of the tree.
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

    /// Creates a new `TreeNode` instance representing a non-root node in the tree.
    ///
    /// This function initializes a non-root `TreeNode` with a given parent, an empty set of children,
    /// and the given `commit_id`, modifications, and height.
    ///
    /// # Parameters:
    /// - `commit_id`: The `CommitId` of this node.
    /// - `parent`: The `SlabIndex` of this node's parent in the tree.
    /// - `height`: The height of this node in the entire underlying database.
    /// - `modifications`: A `ChangeWithRecoverMap` representing the changes made in this node relative to its parent.
    ///
    /// # Returns:
    /// A new non-root `TreeNode`.
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

    /// Retrieves the parent node of this node.
    ///
    /// # Returns:
    /// An optional `SlabIndex` representing the parent of this node.
    /// If this is a root node, it returns `None`.
    pub fn get_parent(&self) -> Option<SlabIndex> {
        self.parent
    }

    /// Sets this node as the root by removing its parent reference.
    ///
    /// This function updates the node to become a root by setting its parent to `None`.
    pub fn set_as_root(&mut self) {
        self.parent = None;
    }

    /// Retrieves a reference to this node's children.
    ///
    /// # Returns:
    /// A reference to a `BTreeSet` containing the `SlabIndex` of all child nodes of this node.
    pub fn get_children(&self) -> &BTreeSet<SlabIndex> {
        &self.children
    }

    /// Inserts a new child into this node's set of children.
    ///
    /// # Parameters:
    /// - `new_child`: The `SlabIndex` of the child to be added to this node's children set.
    pub fn insert_child(&mut self, new_child: SlabIndex) {
        self.children.insert(new_child);
    }

    /// Clears children nodes leaving only one specified child.
    ///
    /// Parameters:
    /// `child_to_retain`: `SlabIndex` referencing which child will remain.
    pub fn remove_child_except(&mut self, child_to_retain: &SlabIndex) {
        self.children = BTreeSet::from([*child_to_retain]);
    }

    /// Retrieves the height of this node in the underlying database.
    ///
    /// # Returns:
    /// A `usize` representing the height of this node.
    pub fn get_height(&self) -> usize {
        self.height
    }

    /// Retrieves the `CommitId` of this node.
    ///
    /// # Returns:
    /// A `S::CommitId` representing the `CommitId` of this node.
    pub fn get_commit_id(&self) -> S::CommitId {
        self.commit_id
    }

    /// Retrieves the modification for a given key in this node, specifically the change made in this node
    /// relative to its parent, not any prior modifications.
    ///
    /// This function looks up the specified `key` in the node's `modifications` map and returns
    /// the corresponding `ValueEntry` if it exists.
    ///
    /// # Parameters:
    /// - `key`: The key whose modification is being queried.
    ///
    /// # Returns:
    /// - An `Option<ValueEntry<S::Value>>`, where:
    ///   - `Some(ValueEntry::Value(value))` indicates that the key was modified to the given value.
    ///   - `Some(ValueEntry::Deleted)` indicates that the key was deleted in this node.
    ///   - `None` indicates that no modification for this key exists in this node.
    pub fn get_modified_value(&self, key: &S::Key) -> Option<ValueEntry<S::Value>> {
        self.modifications.get(key).map(|v| v.value.clone())
    }

    /// Retrieves the modification with the recovery record for a given key in this node,
    /// specifically the change made in this node relative to its parent, not any prior modifications.
    ///
    /// This function looks up the specified `key` in the node's `modifications` map and returns
    /// the corresponding `ChangeWithRecoverRecord` if it exists.
    ///
    /// # Parameters:
    /// - `key`: The key whose recovery record is being queried.
    ///
    /// # Returns:
    /// - An `Option<&ChangeWithRecoverRecord<S>>`, where:
    ///   - `Some(&ChangeWithRecoverRecord)` provides both this modified value and the last commit ID where this key was previously modified (if it exists).
    ///   - `None` indicates that no modification for this key exists in this node.
    pub fn get_recover_record(&self, key: &S::Key) -> Option<&ChangeWithRecoverRecord<S>> {
        self.modifications.get(key)
    }

    /// Retrieves all updates (modifications) made in this node as a `Key`-`ValueEntry` map.
    /// Only consider the changes made in this node relative to its parent, not any prior modifications.
    ///
    /// This function iterates over all modifications made in this node and returns them as a
    /// `KeyValueMap`, where each entry consists of a key and its corresponding modified value (`ValueEntry`).
    /// The `ValueEntry` is an enum type:
    /// - `ValueEntry::Deleted` represents deletion.
    /// - `ValueEntry::Value(value)` represents a specific value.
    ///
    /// # Returns:
    /// - A `KeyValueMap<S>`, which is a map of keys to their modified values (`ValueEntry`).
    pub fn get_updates(&self) -> KeyValueMap<S> {
        self.modifications
            .iter()
            .map(|(k, ChangeWithRecoverRecord { value, .. })| (k.clone(), value.clone()))
            .collect()
    }

    /// Exports rollback data by populating the provided `rollbacks` with the `last_commit_id` for each key of the changes in this node.
    ///
    /// This function iterates over all modifications in the current node and exports the `last_commit_id` (which records where
    /// the last modification occurred before this node) for each key into the provided `rollbacks` map (`LastCommitIdMap`).
    ///
    /// # Parameters:
    /// - `rollbacks`: A mutable reference to a `LastCommitIdMap`, which is a `BTreeMap` of `(Key, Option<CommitId>)`.
    ///   This map will be populated with the `last_commit_id` for each key in this node's modifications.
    ///
    /// - `OVERRIDE`: A boolean constant that determines how entries are handled:
    ///   - If `OVERRIDE` is `true`, any entry in the `rollbacks` map for a given key will be replaced with the new one from this node.
    ///   - If `OVERRIDE` is `false`, existing entries in the `rollbacks` map will not be overwritten, and only new keys will be inserted.
    ///
    /// # Behavior:
    /// For each key in this node's modifications,
    /// - if `OVERRIDE` is set to `true`, its corresponding `last_commit_id` will be inserted into or overwrite any existing entry
    ///   in the `rollbacks` map.
    /// - If `OVERRIDE` is set to `false`, only keys that do not already exist in the `rollbacks` map will be inserted.
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

    /// Exports commit data by populating the provided `commits` with the modifications made in this node.
    ///
    /// This function iterates over all modifications in the current node and exports each modification as
    /// a (`Key`, `ApplyRecord`) pair into the provided `commits` map (`ApplyMap`).
    /// `ApplyRecord` includes two fields:
    ///   - `value`: an enum type `ValueEntry` representing this modification:
    ///     - `ValueEntry::Deleted` represents deletion.
    ///     - `ValueEntry::Value(value)` represents a specific value.
    ///   - `commit_id`: a `CommitId` recording where this modification occurred;
    /// The `commit_id` populated into `commits` is exactly the commit_id of this node.
    ///
    /// # Parameters:
    /// - `commits`: A mutable reference to an `ApplyMap`, which is a `BTreeMap<Key, ApplyRecord>`.
    ///   This map will be populated with the modifications made in this node.
    ///
    /// - `OVERRIDE`: A boolean constant that determines how entries are handled:
    ///   - If `OVERRIDE` is `true`, any entry in the `commits` map for a given key will be replaced with the new one from this node.
    ///   - If `OVERRIDE` is `false`, existing entries in the `commits` map will not be overwritten, and only new keys will be inserted.
    ///
    /// # Behavior:
    /// For each key in this node's modifications,
    /// - if `OVERRIDE` is set to `true`, its corresponding modification will
    ///   be inserted into or overwrite any existing entry in the `commits` map.
    /// - If `OVERRIDE` is set to `false`, only keys that do not already exist in the `commits` map will be inserted.
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
