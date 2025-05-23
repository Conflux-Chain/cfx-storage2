/// Error type representing possible failures when attempting to push an offset.
#[derive(Debug, PartialEq, Eq)]
pub enum PushError {
    /// The current range does not satisfy the constraints of [`super::OffsetBasedVersionRange`]
    InvalidState,
    /// The new offset is not greater than the current maximum offset.
    OffsetNotLarger,
}
