use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};
use crate::utils::hash::blake2s;
use std::borrow::Cow;

use super::{amt_node_id, AmtId};

pub const SLOT_SIZE: usize = 6;
pub const KEY_SLOT_SIZE: usize = SLOT_SIZE - 1;

#[derive(Clone, Copy, Debug)]
pub struct AllocatePosition {
    pub(in crate::lvmt) depth: u8,
    pub(in crate::lvmt) node_index: u16,
    pub(in crate::lvmt) slot_index: u8,
}

impl AllocatePosition {
    pub fn amt_info(&self, key: &[u8]) -> (AmtId, u16, u8) {
        let digest = blake2s(key);
        let mut amt_node_id = amt_node_id(digest, self.depth as usize);
        let node_index = amt_node_id.pop().unwrap();
        let amt_id = amt_node_id;
        (amt_id, node_index, self.slot_index)
    }
}

#[derive(Clone, Debug)]
pub struct AllocationInfo {
    pub(in crate::lvmt) index: u8,
    key: Box<[u8]>,
}

impl AllocationInfo {
    pub fn new(index: u8, key: Box<[u8]>) -> Self {
        Self { index, key }
    }
}

impl Encode for AllocatePosition {
    fn encode(&self) -> std::borrow::Cow<[u8]> {
        let idx = (self.node_index as u16).to_le_bytes();
        let meta = (self.depth as u8 & 0x1f) | ((self.slot_index as u8) << 5);
        Cow::Owned(vec![idx[0], idx[1], meta])
    }
}

impl Decode for AllocatePosition {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() != 3 {
            return Err(DecodeError::IncorrectLength);
        }

        let node_index = u16::from_le_bytes(input[0..2].try_into().unwrap());
        let meta = input[2];
        let (depth, slot_index) = (meta & 0x1f, meta >> 5);

        if slot_index as usize >= KEY_SLOT_SIZE {
            return Err(DecodeError::Custom("slot_index overflow"));
        }

        if depth == 0 {
            return Err(DecodeError::Custom("depth cannot be zero"));
        }

        Ok(Cow::Owned(AllocatePosition {
            depth,
            node_index,
            slot_index,
        }))
    }
}

impl Encode for AllocationInfo {
    fn encode(&self) -> Cow<[u8]> {
        let mut raw = vec![self.index as u8];
        raw.extend(self.key.as_ref());
        Cow::Owned(raw)
    }
}

impl Decode for AllocationInfo {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() == 0 {
            return Err(DecodeError::IncorrectLength);
        }
        let index = input[0];
        let key = input[1..].to_vec().into_boxed_slice();
        Ok(Cow::Owned(Self { index, key }))
    }
}
