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
    NonRootNodeShouldHaveParent,
    #[error("ancestor height should be in the range of the root height and this node height")]
    InvalidAncestorHeight,
    #[error("persistence io error of kind {0:?}")]
    PersistenceIOError(std::io::ErrorKind),
    #[error("recovery inconsistent error")]
    RecoveryInconsistentError,
}

impl<CommitId: Debug + Eq + Hash> From<std::io::Error> for PendingError<CommitId> {
    fn from(err: std::io::Error) -> Self {
        PendingError::PersistenceIOError(err.kind())
    }
}
