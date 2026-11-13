use crate::backends::{DatabaseTrait, PendingTableName};

use super::pending_schema::ConfirmedPathInfo;
use super::persistence::{
    log_add_non_root_node, log_add_root, log_change_root, log_discard, log_make_pivot,
};
use super::{
    pending_schema::{PendingKeyValueSchema, RecoverMap, Result as PendResult},
    persistence::PersistenceTracker,
    tree::Tree,
};

/// A composite struct that bundles the in-memory data `Tree` with its `PersistenceTracker`.
///
/// It encapsulates the complete in-memory representation of the pending state.
/// This bundling ensures that the data tree and the metadata for sequencing its
/// modifications are always managed together as a single, consistent unit.
pub struct TreeWithTracker<S: PendingKeyValueSchema> {
    pub tree: Tree<S>,
    pub tracker: PersistenceTracker,
}

impl<S: PendingKeyValueSchema> TreeWithTracker<S> {
    pub fn add_root<P: DatabaseTrait<PendingTableName>>(
        &mut self,
        commit_id: S::CommitId,
        modifications: RecoverMap<S>,
        write_schema: &P::WriteSchema,
    ) -> PendResult<()> {
        self.tree.add_root(commit_id, modifications.clone())?;

        log_add_root(write_schema, &mut self.tracker, commit_id, &modifications);

        Ok(())
    }

    pub fn add_non_root_node<P: DatabaseTrait<PendingTableName>>(
        &mut self,
        commit_id: S::CommitId,
        parent_commit_id: S::CommitId,
        modifications: RecoverMap<S>,
        write_schema: &P::WriteSchema,
    ) -> PendResult<()> {
        self.tree
            .add_non_root_node(commit_id, parent_commit_id, modifications.clone())?;

        log_add_non_root_node(
            write_schema,
            &mut self.tracker,
            commit_id,
            parent_commit_id,
            &modifications,
        );

        Ok(())
    }

    /// Do nothing if the given `commit_id` was already the current root. See [Tree::change_root].
    pub fn change_root<P: DatabaseTrait<PendingTableName>>(
        &mut self,
        commit_id: S::CommitId,
        write_schema: &P::WriteSchema,
    ) -> PendResult<Option<ConfirmedPathInfo<S>>> {
        let in_memory_res = self.tree.change_root(commit_id)?;

        if let Some(confirmed_path_info) = in_memory_res {
            log_change_root::<S>(write_schema, &mut self.tracker, commit_id, &self.tree);

            Ok(Some(confirmed_path_info))
        } else {
            Ok(None)
        }
    }

    #[cfg(test)]
    pub fn change_root_without_persistence(
        &mut self,
        commit_id: S::CommitId,
    ) -> PendResult<Option<ConfirmedPathInfo<S>>> {
        self.tree.change_root(commit_id)
    }

    pub fn make_pivot<P: DatabaseTrait<PendingTableName>>(
        &mut self,
        commit_id: S::CommitId,
        write_schema: &P::WriteSchema,
    ) -> PendResult<bool> {
        let in_memory_res = self.tree.make_pivot(commit_id)?;

        log_make_pivot::<S>(write_schema, &mut self.tracker, commit_id);

        Ok(in_memory_res)
    }

    pub fn discard<P: DatabaseTrait<PendingTableName>>(
        &mut self,
        commit_id: S::CommitId,
        write_schema: &P::WriteSchema,
    ) -> PendResult<bool> {
        let in_memory_res = self.tree.discard(commit_id)?;

        log_discard::<S>(write_schema, &mut self.tracker, commit_id);

        Ok(in_memory_res)
    }
}
