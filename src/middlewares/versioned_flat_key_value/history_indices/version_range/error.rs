use thiserror::Error;

use super::bitmap::BitmapCreationError;

/// Error type representing possible failures when attempting to push an offset.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum PushError {
    #[error("invalid state")]
    /// The current range does not satisfy the constraints of [`super::OffsetBasedVersionRange`]
    InvalidState,

    /// The new offset is not greater than the current maximum offset.
    #[error("offset not larger")]
    OffsetNotLarger,

    /// An internal logic error occurred during the conversion to a Bitmap.
    /// This indicates a bug in the calling logic, as inputs should have been pre-validated.
    #[error("internal logic error during bitmap conversion")]
    BitmapConversionFailed {
        #[from]
        source: BitmapCreationError,
    },
}

#[derive(Debug, PartialEq, Eq, Error)]
pub enum VersionError {
    #[error("overflow")]
    /// Indicates an arithmetic overflow when calculating a version number.
    Overflow,
}
