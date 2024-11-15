use super::{log2_floor, MAX_NODE_SIZE, MAX_NODE_SIZE_LOG};
use crate::backends::serde::{Decode, Encode, FixedLengthEncoded};
use crate::errors::{DecResult, DecodeError};
use std::borrow::Cow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthChangeKey {
    height: usize,
    index: usize,
}

impl AuthChangeKey {
    pub fn is_root(&self) -> bool {
        self.height == 0
    }

    pub fn child(&self, index: usize) -> Self {
        assert!(index < MAX_NODE_SIZE);
        Self {
            height: self.height + 1,
            index: self.index * MAX_NODE_SIZE + index,
        }
    }

    pub fn root() -> Self {
        Self {
            height: 0,
            index: 0,
        }
    }
}

impl Encode for AuthChangeKey {
    fn encode(&self) -> std::borrow::Cow<[u8]> {
        let value = 1u32 << (self.height * MAX_NODE_SIZE_LOG) + self.index;
        Cow::Owned(value.to_be_bytes().to_vec())
    }
}

impl FixedLengthEncoded for AuthChangeKey {
    const LENGTH: usize = 4;
}

impl Decode for AuthChangeKey {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        use DecodeError::*;
        if input.len() != 4 {
            return Err(IncorrectLength);
        }
        let value = u32::from_be_bytes(input.try_into().unwrap()) as usize;
        let log_value = log2_floor(value);
        if log_value % MAX_NODE_SIZE_LOG != 1 {
            return Err(Custom("Cannot parse"));
        }
        Ok(Cow::Owned(Self {
            height: (log_value - 1) / MAX_NODE_SIZE_LOG,
            index: value - (1 << log_value),
        }))
    }
}
