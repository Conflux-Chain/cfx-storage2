use std::{fmt::Debug, hash::Hash};

use thiserror::Error;

#[derive(Debug, PartialEq, Error)]
pub enum PendingError<CommitId: Debug + Eq + Hash> {
    #[error("commit id not found")]
    CommitIDNotFound(CommitId),
    #[error("multiple roots are not allowed")]
    MultipleRootsNotAllowed,
    #[error("commit id already exists")]
    CommitIdAlreadyExists(CommitId),
}
