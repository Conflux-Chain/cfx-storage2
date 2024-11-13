use std::collections::BTreeMap;

use crate::middlewares::versioned_flat_key_value::pending_part::{
    current_map::CurrentMap,
    pending_schema::{ApplyMap, ApplyRecord, PendingKeyValueSchema, Result as PendResult},
};

use super::Tree;

// methods to support VersionedMap::checkout_current()
impl<S: PendingKeyValueSchema> Tree<S> {
    /// Updates the mutable reference `maybe_current` to the `CurrentMap` corresponding to `target_commit_id`.
    /// This target `CurrentMap` contains all changes made in `target_commit_id` relative to the parent of the tree root.
    ///
    /// # Parameters:
    /// - `target_commit_id`: The `CommitId` of the target node for which the `CurrentMap` is to be calculated.
    /// - `maybe_current`: A mutable reference to an `Option<CurrentMap<S>>`. If it is `None`, a new `CurrentMap`
    ///   will be created. If it is `Some(current)`, it will be updated to reflect the state at `target_commit_id`.
    ///
    /// # Algorithm:
    /// The algorithm has two cases:
    /// 1. If `maybe_current` is initially `None`, the function calls `make_current` to compute the `CurrentMap` from scratch.
    /// 2. If `maybe_current` is initially `Some(current)`, the function calls `switch_current_head` to compute
    ///    the `CurrentMap` for `target_commit_id`, updating the existing `current` to reflect this state.
    ///
    /// # Returns:
    /// A `Result` that is empty if successful, or returns an error if the operation fails.
    /// Failure can occur due to:
    /// - `target_commit_id` does not exist in the tree;
    /// - or the existing commit ID in `maybe_current` (if present) does not exist in the tree.
    pub fn checkout_current(
        &self,
        target_commit_id: S::CommitId,
        maybe_current: &mut Option<CurrentMap<S>>,
    ) -> PendResult<(), S> {
        if let Some(current) = maybe_current.as_mut() {
            self.switch_current_head(target_commit_id, current)?;
        } else {
            *maybe_current = Some(self.make_current(target_commit_id)?);
        }

        assert_eq!(
            maybe_current.as_ref().unwrap().get_commit_id(),
            target_commit_id
        );

        Ok(())
    }

    /// Switches the current `CurrentMap` to the one corresponding to `target_commit_id`.
    ///
    /// This function collects the necessary rollback and apply operations to transition from
    /// the current state (represented by `current`) to the state at `target_commit_id`.
    /// It then updates `current` to reflect the new state.
    ///
    /// # Parameters:
    /// - `target_commit_id`: The `CommitId` of the target node to switch to.
    /// - `current`: A mutable reference to the current `CurrentMap`, which will be updated.
    ///
    /// # Returns:
    /// A `Result` that is empty if successful, or returns an error if the operation fails.
    /// Failure can occur due to:
    /// - `target_commit_id` does not exist in the tree;
    /// - or the `CommitId` in the initial `current` does not exist in the tree.
    fn switch_current_head(
        &self,
        target_commit_id: S::CommitId,
        current: &mut CurrentMap<S>,
    ) -> PendResult<(), S> {
        let (rollbacks, applys) =
            self.collect_rollback_and_apply_ops(current.get_commit_id(), target_commit_id)?;
        current.switch_to_commit(target_commit_id, rollbacks, applys);
        Ok(())
    }

    /// Creates a new `CurrentMap` for the given `target_commit_id`.
    ///
    /// This function computes all changes made in `target_commit_id` relative to the parent of the tree root
    /// and creates a new `CurrentMap` representing this state.
    ///
    /// # Parameters:
    /// - `target_commit_id`: The `CommitId` of the node for which a new `CurrentMap` will be created.
    ///
    /// # Returns:
    /// A `Result` containing the newly created `CurrentMap`, or an error if the operation fails.
    /// Failure can occur due to:
    /// - `target_commit_id` does not exist in the tree.
    fn make_current(&self, target_commit_id: S::CommitId) -> PendResult<CurrentMap<S>, S> {
        let applys = self.get_apply_map_from_root_included(target_commit_id)?;
        let new_current = CurrentMap::<S>::new(target_commit_id, applys);
        Ok(new_current)
    }

    /// Retrieves all changes made in a node relative to the parent of the tree root.
    ///
    /// This function starts from the node corresponding to `target_commit_id` and traverses up
    /// to the root of the tree (included), collecting all modifications made along the way.
    /// It returns these modifications as an `ApplyMap`. Each modification is a (`Key`, `ApplyRecord`) pair.
    /// `ApplyRecord` includes two fields:
    ///   - `value`: an enum type `ValueEntry` representing this modification:
    ///     - `ValueEntry::Deleted` represents deletion.
    ///     - `ValueEntry::Value(value)` represents a specific value.
    ///   - `commit_id`: a `CommitId` recording where this modification occurred; it is ensured in the tree.
    /// For the same `Key`, the more recent (i.e., further from the root) `ApplyRecord` takes precedence.
    ///
    /// # Parameters:
    /// - `target_commit_id`: The `CommitId` of the node for which changes are being collected.
    ///
    /// # Returns:
    /// A `Result` containing an `ApplyMap` with all changes made in this node relative to the parent of the tree root,
    /// or an error if the operation fails.
    /// Failure can occur due to:
    /// - `target_commit_id` does not exist in the tree.
    fn get_apply_map_from_root_included(
        &self,
        target_commit_id: S::CommitId,
    ) -> PendResult<ApplyMap<S>, S> {
        let mut target_node = self.get_node_by_commit_id(target_commit_id)?;
        let mut commits_rev = BTreeMap::new();
        target_node.export_commit_data::<false>(&mut commits_rev);
        while let Some(parent_slab_index) = target_node.get_parent() {
            target_node = self.get_node_by_slab_index(parent_slab_index);
            target_node.export_commit_data::<false>(&mut commits_rev);
        }
        Ok(commits_rev)
    }

    /// Collects rollback and apply operations needed to transition between two commits.
    ///
    /// This function computes both:
    /// - rollback operations: to undo changes made in nodes between `current_commit_id` (included)
    ///   and their lowest common ancestor (LCA) (excluded),
    /// - apply operations: to apply changes from nodes between their LCA (excluded) and `target_commit_id` (included).
    /// The result is a tuple containing both rollback and apply maps.
    ///
    /// # Parameters:
    /// - `current_commit_id`: The starting commit ID (the current state).
    /// - `target_commit_id`: The target commit ID (the desired state).
    ///
    /// # Returns:
    /// - If successful, a tuple containing:
    ///   - A map of rollbacks (`BTreeMap<S::Key, Option<ApplyRecord<S>>>`) that need to be applied to revert changes.
    ///     For the same `Key`, the older (i.e., closer to the root) `Option<ApplyRecord>` takes precedence.
    ///     If `None`, the `Key` is not changed at the LCA relative to the parent of the tree root.
    ///     If `Some(ApplyRecord)`, the `Key` is changed at the LCA relative to the parent of the tree root.
    ///     `ApplyRecord` includes two fields:
    ///       - `value`: an enum type `ValueEntry` representing this modification:
    ///         - `ValueEntry::Deleted` represents deletion.
    ///         - `ValueEntry::Value(value)` represents a specific value.
    ///       - `commit_id`: a `CommitId` recording where this modification occurred; it is ensured in the tree.
    ///   - An apply map (`ApplyMap`) that contains changes that need to be applied to reach the target state.
    ///     Each change is a (`Key`, `ApplyRecord`) pair.
    ///     For the same `Key`, the more recent (i.e., further from the root) `ApplyRecord` takes precedence.
    /// - If fails, an error because:
    ///   - `target_commit_id` does not exist in the tree;
    ///   - or `current_commit_id` does not exist in the tree.
    ///
    /// # Notes:
    /// - Correctness is based on no more than one root.
    /// - The node containing the previous modification of the `key` may have already been removed from the tree.
    ///   However, the node where the current modification occurred still stores a `last_commit_id` with a value of `Some`,
    ///   which points to that previous modification. This is done to ensure efficient operation of the `change_root` function
    ///   (i.e., in `change_root`, nodes that remain do not have their `last_commit_id` set to `None` even if the previous node was removed).
    ///   Thus, when calculating the map of rollbacks, we additional check whether `old_commit_id` exists in the tree.
    #[allow(clippy::type_complexity)]
    fn collect_rollback_and_apply_ops(
        &self,
        current_commit_id: S::CommitId,
        target_commit_id: S::CommitId,
    ) -> PendResult<(BTreeMap<S::Key, Option<ApplyRecord<S>>>, ApplyMap<S>), S> {
        let mut current_node = self.get_node_by_commit_id(current_commit_id)?;
        let mut target_node = self.get_node_by_commit_id(target_commit_id)?;
        let mut rollbacks = BTreeMap::new();
        let mut commits_rev = BTreeMap::new();

        while current_node.get_height() > target_node.get_height() {
            current_node.export_rollback_data::<true>(&mut rollbacks);
            current_node = self.get_parent_node(current_node).unwrap();
        }

        while target_node.get_height() > current_node.get_height() {
            target_node.export_commit_data::<false>(&mut commits_rev);
            target_node = self.get_parent_node(target_node).unwrap();
        }

        while current_node.get_commit_id() != target_node.get_commit_id() {
            current_node.export_rollback_data::<true>(&mut rollbacks);
            current_node = self.get_parent_node(current_node).unwrap();

            target_node.export_commit_data::<false>(&mut commits_rev);
            target_node = self.get_parent_node(target_node).unwrap();
        }

        let mut rollbacks_with_value = BTreeMap::new();
        for (key, old_commit_id) in rollbacks.into_iter() {
            let actual_old_commit_id = if let Some(ref old_commit_id) = old_commit_id {
                // check rollbacks' old_commit_id because TreeNodes are deleted in a lazy way with respect to TreeNodes.modifications
                self.contains_commit_id(old_commit_id)
                    .then_some(old_commit_id)
            } else {
                None
            };
            if let Some(old_commit_id) = actual_old_commit_id {
                let old_value = self
                    .get_modification_by_commit_id(*old_commit_id, &key)
                    .expect("old commit must exist in tree")
                    .expect("key must be exist in given id");
                let apply_record = Some(ApplyRecord::<S> {
                    value: old_value,
                    commit_id: *old_commit_id,
                });
                rollbacks_with_value.insert(key, apply_record);
            } else {
                rollbacks_with_value.insert(key, None);
            };
        }

        // rollbacks or commits_rev may be empty,
        // they contain current and target (if they are not lca), respectively,
        // but they do not contain lca
        Ok((rollbacks_with_value, commits_rev))
    }

    #[cfg(test)]
    pub fn get_apply_map_from_root_included_for_test(
        &self,
        target_commit_id: S::CommitId,
    ) -> PendResult<ApplyMap<S>, S> {
        self.get_apply_map_from_root_included(target_commit_id)
    }
}
