use in_memory_tree::TreeError;
use thiserror::Error;

use crate::middlewares::CommitID;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("unknown version")]
    VersionNotFound,

    #[error("commit id not found in the historical part")]
    CommitIDNotFound,

    #[error("io error {0:?}")]
    IoError(#[from] std::io::Error),

    #[error("decode error {0:?}")]
    DecodeError(#[from] DecodeError),
    
    #[error("tree error {0:?}")]
    TreeError(#[from] TreeError<CommitID>),
}

pub type Result<T> = ::std::result::Result<T, StorageError>;

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("incorrect input length")]
    IncorrectLength,
}
pub type DecResult<T> = ::std::result::Result<T, DecodeError>;
