use std::{fmt::Debug, hash::Hash};

use thiserror::Error;

#[derive(Debug, PartialEq, Error, Eq)]
pub enum PendingError<CommitId: Debug + Eq + Hash> {
    #[error("commit id not found")]
    CommitIDNotFound(CommitId),
    #[error("multiple roots are not allowed")]
    MultipleRootsNotAllowed,
    #[error("commit id already exists")]
    CommitIdAlreadyExists(CommitId),
    #[error("non_root node should have parent")]
    NonRootNodeHasNoParentError,
    #[error("root should not be discarded")]
    RootShouldNotBeDiscarded,
}
