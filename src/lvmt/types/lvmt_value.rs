use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};
use std::borrow::Cow;

use super::allocation::AllocatePosition;

#[derive(Clone, Debug)]
pub struct LvmtValue {
    pub(in crate::lvmt) allocation: AllocatePosition,
    pub(in crate::lvmt) version: u64,
    pub(in crate::lvmt) value: Box<[u8]>,
}

impl Encode for LvmtValue {
    fn encode(&self) -> std::borrow::Cow<[u8]> {
        let mut encoded: Vec<u8> = self.allocation.encode().into_owned();
        encoded.extend(&self.version.to_le_bytes()[0..5]);
        encoded.extend(&*self.value);
        Cow::Owned(encoded)
    }
}

impl Decode for LvmtValue {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() < 8 {
            return Err(DecodeError::TooShortHeader);
        }
        let (header, body) = input.split_at(8);
        let (allocation_raw, version_raw) = header.split_at(3);
        let allocation = AllocatePosition::decode(allocation_raw)?.into_owned();

        let mut version_bytes = [0u8; 8];
        version_bytes[3..8].copy_from_slice(version_raw);

        let version = u64::from_be_bytes(version_bytes);
        let value = input[8..].to_vec().into_boxed_slice();

        Ok(Cow::Owned(Self {
            allocation,
            version,
            value,
        }))
    }
}
