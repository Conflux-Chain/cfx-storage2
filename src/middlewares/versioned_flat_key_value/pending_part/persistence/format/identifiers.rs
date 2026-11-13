use std::borrow::Cow;

use super::super::{DecResult, Decode, Encode, FixedLengthEncoded};

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct SnapshotId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct ModificationId(pub u64);

impl Encode for SnapshotId {
    fn encode(&self) -> Cow<[u8]> {
        self.0.encode()
    }
}

impl FixedLengthEncoded for SnapshotId {
    const LENGTH: usize = u64::LENGTH;
}

impl Decode for SnapshotId {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(SnapshotId(u64::decode(input)?.into_owned())))
    }
}

impl Encode for ModificationId {
    fn encode(&self) -> Cow<[u8]> {
        self.0.encode()
    }
}

impl FixedLengthEncoded for ModificationId {
    const LENGTH: usize = u64::LENGTH;
}

impl Decode for ModificationId {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(ModificationId(u64::decode(input)?.into_owned())))
    }
}
