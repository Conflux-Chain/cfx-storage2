use thiserror::Error;

use crate::middlewares::{CommitID, PendingError};

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("unknown version")]
    VersionNotFound,

    #[error("commit id not found in the historical part")]
    CommitIDNotFound,

    #[error("commit id already in the historical part but try to add to pending")]
    CommitIdAlreadyExistsInHistory,

    #[error("io error {0:?}")]
    IoError(#[from] std::io::Error),

    #[error("decode error {0:?}")]
    DecodeError(#[from] DecodeError),

    #[error("pending error {0:?}")]
    PendingError(#[from] PendingError<CommitID>),
}

pub type Result<T> = ::std::result::Result<T, StorageError>;

#[derive(Error, Debug, Clone, Copy, PartialEq)]
pub enum DecodeError {
    #[error("incorrect input length")]
    IncorrectLength,
}
pub type DecResult<T> = ::std::result::Result<T, DecodeError>;

#[cfg(test)]
impl PartialEq for StorageError {
    fn eq(&self, other: &Self) -> bool {
        use StorageError::*;
        match (self, other) {
            (VersionNotFound, VersionNotFound) => true,
            (CommitIDNotFound, CommitIDNotFound) => true,
            (CommitIdAlreadyExistsInHistory, CommitIdAlreadyExistsInHistory) => true,
            (IoError(_), IoError(_)) => true,
            (DecodeError(e1), DecodeError(e2)) => e1 == e2,
            (PendingError(e1), PendingError(e2)) => e1 == e2,
            _ => false,
        }
    }
}
