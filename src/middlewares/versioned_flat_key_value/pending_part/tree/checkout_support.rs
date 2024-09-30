use std::collections::BTreeMap;

use crate::middlewares::versioned_flat_key_value::pending_part::pending_schema::{
    ApplyMap, ApplyRecord, PendingKeyValueSchema, Result as PendResult,
};

use super::Tree;

// methods to support VersionedMap::checkout_current()
impl<S: PendingKeyValueSchema> Tree<S> {
    pub fn get_apply_map_from_root_included(
        &self,
        target_commit_id: S::CommitId,
    ) -> PendResult<ApplyMap<S>, S> {
        let mut target_node = self.get_node_by_commit_id(target_commit_id)?;
        let mut commits_rev = BTreeMap::new();
        target_node.export_commit_data(&mut commits_rev);
        while let Some(parent_slab_index) = target_node.get_parent() {
            target_node = self.get_node_by_slab_index(parent_slab_index);
            target_node.export_commit_data(&mut commits_rev);
        }
        Ok(commits_rev)
    }

    // correctness based on single root
    #[allow(clippy::type_complexity)]
    pub fn collect_rollback_and_apply_ops(
        &self,
        current_commit_id: S::CommitId,
        target_commit_id: S::CommitId,
    ) -> PendResult<(BTreeMap<S::Key, Option<ApplyRecord<S>>>, ApplyMap<S>), S> {
        let mut current_node = self.get_node_by_commit_id(current_commit_id).unwrap();
        let mut target_node = self.get_node_by_commit_id(target_commit_id)?;
        let mut rollbacks = BTreeMap::new();
        let mut commits_rev = BTreeMap::new();
        while current_node.get_height() > target_node.get_height() {
            current_node.export_rollback_data(&mut rollbacks);
            current_node = self.get_parent_node(current_node).unwrap();
        }
        while target_node.get_height() > current_node.get_height() {
            target_node.export_commit_data(&mut commits_rev);
            target_node = self.get_parent_node(target_node).unwrap();
        }
        while current_node.get_commit_id() != target_node.get_commit_id() {
            current_node.export_rollback_data(&mut rollbacks);
            current_node = self.get_parent_node(current_node).unwrap();
            target_node.export_commit_data(&mut commits_rev);
            target_node = self.get_parent_node(target_node).unwrap();
        }
        // check rollbacks' old_commit_id because TreeNodes are deleted
        // in a lazy way with respect to TreeNodes.modifications
        // todo: test this lazy method
        for (_, old_commit_id_option) in rollbacks.iter_mut() {
            if let Some(ref old_commit_id) = old_commit_id_option {
                if !self.contains_commit_id(old_commit_id) {
                    *old_commit_id_option = None;
                }
            }
        }

        let rollbacks_with_value: BTreeMap<_, _> = rollbacks
            .into_iter()
            .map(|(k, old_cid_opt)| match old_cid_opt {
                None => (k, None),
                Some(rollback_cid) => {
                    let rollback_value = self.get_by_commit_id(rollback_cid, &k).unwrap().unwrap();
                    (
                        k,
                        Some(ApplyRecord::<S> {
                            value: rollback_value,
                            commit_id: rollback_cid,
                        }),
                    )
                }
            })
            .collect();

        // rollbacks or commits_rev may be empty,
        // they contain current and target (if they are not lca), respectively,
        // but they do not contain lca
        Ok((rollbacks_with_value, commits_rev))
    }
}
