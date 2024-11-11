use std::collections::BTreeMap;

use crate::middlewares::versioned_flat_key_value::pending_part::{
    current_map::CurrentMap,
    pending_schema::{ApplyMap, ApplyRecord, PendingKeyValueSchema, Result as PendResult},
};

use super::Tree;

// methods to support VersionedMap::checkout_current()
impl<S: PendingKeyValueSchema> Tree<S> {
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

    fn make_current(&self, target_commit_id: S::CommitId) -> PendResult<CurrentMap<S>, S> {
        let applys = self.get_apply_map_from_root_included(target_commit_id)?;
        let new_current = CurrentMap::<S>::new(target_commit_id, applys);
        Ok(new_current)
    }

    #[cfg(test)]
    pub fn get_apply_map_from_root_included_for_test(
        &self,
        target_commit_id: S::CommitId,
    ) -> PendResult<ApplyMap<S>, S> {
        self.get_apply_map_from_root_included(target_commit_id)
    }

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

    // correctness based on single root
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
}
