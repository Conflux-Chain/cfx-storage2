use std::fmt::Debug;

use thiserror::Error;

#[derive(Debug, PartialEq, Error, Eq)]
pub enum PendingError {
    #[error("commit id not found: {0}")]
    CommitIDNotFound(String),
    #[error("multiple roots are not allowed")]
    MultipleRootsNotAllowed,
    #[error("commit id already exists: {0}")]
    CommitIdAlreadyExists(String),
    #[error("non_root node should have parent")]
    NonRootNodeShouldHaveParent,
    #[error("ancestor height should be in the range of the root height and this node height")]
    InvalidAncestorHeight,
}
