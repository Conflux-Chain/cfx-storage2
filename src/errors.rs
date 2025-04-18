use ark_serialize::SerializationError;
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

    #[error("backend db fails consistency check")]
    ConsistencyCheckFailure,

    #[error("database error {0:?}")]
    DatabaseError(#[from] DatabaseError),

    #[error("pending error {0:?}")]
    PendingError(#[from] PendingError<CommitID>),

    #[error("corrupted history indices")]
    CorruptedHistoryIndices,
}

impl From<DecodeError> for StorageError {
    fn from(value: DecodeError) -> Self {
        Self::DatabaseError(DatabaseError::DecodeError(value))
    }
}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::DatabaseError(DatabaseError::IoError(value))
    }
}

pub type Result<T> = ::std::result::Result<T, StorageError>;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("decode error {0:?}")]
    DecodeError(#[from] DecodeError),

    #[error("io error {0:?}")]
    IoError(std::io::Error),
}

pub type DbResult<T> = ::std::result::Result<T, DatabaseError>;

#[derive(Error, Debug, Clone, Copy, PartialEq)]
pub enum DecodeError {
    #[error("incorrect input length")]
    IncorrectLength,
    #[error("too short header")]
    TooShortHeader,
    #[error("Cannot parse crypto element")]
    CryptoError,
    #[error("Custom error: {0}")]
    Custom(&'static str),
}

impl From<SerializationError> for DecodeError {
    fn from(value: SerializationError) -> Self {
        Self::CryptoError
    }
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
            (ConsistencyCheckFailure, ConsistencyCheckFailure) => true,
            (DatabaseError(e1), DatabaseError(e2)) => e1 == e2,
            (PendingError(e1), PendingError(e2)) => e1 == e2,
            (CorruptedHistoryIndices, CorruptedHistoryIndices) => true,

            (VersionNotFound, _) => false,
            (CommitIDNotFound, _) => false,
            (CommitIdAlreadyExistsInHistory, _) => false,
            (ConsistencyCheckFailure, _) => false,
            (DatabaseError(_), _) => false,
            (PendingError(_), _) => false,
            (CorruptedHistoryIndices, _) => false,
        }
    }
}

#[cfg(test)]
impl PartialEq for DatabaseError {
    fn eq(&self, other: &Self) -> bool {
        use DatabaseError::*;
        match (self, other) {
            (DecodeError(e1), DecodeError(e2)) => e1 == e2,
            (IoError(_), IoError(_)) => true,
            _ => false,
        }
    }
}
