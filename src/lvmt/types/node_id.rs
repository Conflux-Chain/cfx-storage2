use ethereum_types::H256;
use tinyvec::ArrayVec;

use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};
use std::borrow::Cow;

pub type AmtId = ArrayVec<[u16; 16]>;
pub type AmtNodeId = AmtId;

pub fn amt_node_id(digest: H256, depth: usize) -> AmtNodeId {
    let length = depth + 1;
    let mut data = [0u16; 16];
    for i in 0..length {
        data[i] = u16::from_be_bytes(digest[i * 2..(i + 1) * 2].try_into().unwrap());
    }
    ArrayVec::from_array_len(data, length)
}

impl Encode for AmtId {
    fn encode(&self) -> Cow<[u8]> {
        let raw_slice = self.iter().flat_map(|x| x.to_be_bytes().into_iter());

        Cow::Owned(raw_slice.collect())
    }
}

impl Decode for AmtId {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() % 2 != 0 {
            return Err(DecodeError::IncorrectLength);
        }

        let input_iter = input
            .chunks_exact(2)
            .map(|x| u16::from_be_bytes([x[0], x[1]]));
        let array = ArrayVec::from_iter(input_iter);
        Ok(Cow::Owned(array))
    }
}

// #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
// pub struct MerkleNodeId {
//     height: usize,
//     index: usize,
// }

// impl Encode for MerkleNodeId {
//     fn encode(&self) -> Cow<[u8]> {
//         assert!(self.index < 1 << 12);
//         assert!(self.height < 4);
//         let mut raw = (self.index as u16).to_be_bytes();
//         raw[0] = (self.height << 4) as u8;
//         Cow::Owned(raw.to_vec())
//     }
// }

// impl Decode for MerkleNodeId {
//     fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
//         let raw: [u8; 2] = input.try_into().map_err(|_| DecodeError::IncorrectLength)?;
//         let index = (u16::from_be_bytes(raw) & 0x0fff) as usize;
//         let height = (raw[0] >> 4) as usize;
//         Ok(Cow::Owned(Self { height, index }))
//     }
// }
