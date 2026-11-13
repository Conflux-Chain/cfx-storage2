use std::borrow::Cow;

use super::super::{subkey_not_support, DecResult, Decode, Encode, SeekKey};

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct SnapshotKey(pub u64);

impl SnapshotKey {
    /// Generates the `SeekKey` to start an iteration from the entry at a
    /// specific `height`.
    ///
    /// An iterator starting from this key will first yield the entry for `height`
    /// (if it exists) and then proceed to subsequent heights (`height + 1`, etc.).
    ///
    /// **Note**: This only provides a starting point. If the caller is only
    /// interested in the single entry at the specified `height`, they are
    /// responsible for stopping the iteration after the first item.
    pub fn seek_key_for_height(height: u64) -> SeekKey<Self> {
        let start_key = SnapshotKey(height);

        SeekKey { key: start_key }
    }
}

impl Encode for SnapshotKey {
    fn encode(&self) -> Cow<[u8]> {
        self.0.encode()
    }
}

subkey_not_support!(SnapshotKey);

impl Decode for SnapshotKey {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(SnapshotKey(u64::decode(input)?.into_owned())))
    }
}
