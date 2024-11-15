use super::{tree_height, MAX_NODE_SIZE};
use crate::errors::{DecResult, DecodeError};
use crate::{
    backends::serde::{Decode, Encode},
    utils::hash::blake2s_tuple,
};
use ethereum_types::H256;
use std::borrow::Cow;
use tinyvec::ArrayVec;

type Prefix = ArrayVec<[u8; 32]>;
#[derive(Debug, Clone)]
pub struct AuthChangeNode {
    hashes: ArrayVec<[H256; MAX_NODE_SIZE]>,
    ticks: Option<ArrayVec<[Prefix; MAX_NODE_SIZE - 1]>>,
    avail_bitmap: u8,
}

impl AuthChangeNode {
    pub fn from_leaves(leaves: &[H256]) -> Self {
        let size = leaves.len();

        assert!(size > 0);
        assert!(size <= MAX_NODE_SIZE);

        let hashes = ArrayVec::try_from(leaves).unwrap();
        let avail_bitmap = (1u8 << size).overflowing_sub(1).0;
        Self {
            hashes,
            ticks: None,
            avail_bitmap,
        }
    }

    pub fn from_nodes(nodes: &[Self], ticks: Vec<H256>, shared_prefix_len: usize) -> Self {
        let size = nodes.len();

        assert!(size > 0);
        assert!(size <= MAX_NODE_SIZE);
        assert_eq!(size - 1, ticks.len());

        let hashes: ArrayVec<_> = nodes.iter().map(Self::hash).collect();
        let avail_bitmap = (1 << size) - 1;
        let ticks = ticks
            .into_iter()
            .map(|x| ArrayVec::from_array_len(x.0, shared_prefix_len + 1))
            .collect();

        Self {
            hashes,
            ticks: Some(ticks),
            avail_bitmap,
        }
    }

    fn hash(&self) -> H256 {
        if self.hashes.len() == 1 {
            return self.hashes[0];
        }
        let height = tree_height(self.hashes.len());
        let pairs = self.hashes.len() - (1 << (height - 2));

        let mut hashes: Vec<_> = self
            .hashes
            .chunks_exact(2)
            .take(pairs)
            .map(|x| blake2s_tuple(&x[0], &x[1]))
            .collect();
        hashes.extend(self.hashes[pairs * 2..].iter().cloned());

        while hashes.len() > 1 {
            hashes = hashes
                .chunks_exact(2)
                .map(|x| blake2s_tuple(&x[0], &x[1]))
                .collect();
        }

        hashes[0]
    }

    fn is_leaf(&self) -> bool {
        self.ticks.is_none()
    }
}

impl Encode for AuthChangeNode {
    fn encode(&self) -> Cow<[u8]> {
        let ticks_length = self.ticks.as_ref().and_then(|x| x.first()).map(|x| x.len());
        let output_len = 32 * self.hashes.len() + ticks_length.unwrap_or(0) * self.hashes.len() + 3;
        let mut res = Vec::with_capacity(output_len);

        let mut size = self.hashes.len() as u8;
        if self.is_leaf() {
            size |= 0x80;
        }
        res.push(size);
        res.push(self.avail_bitmap);
        res.push(ticks_length.map_or(0, |x| x as u8));
        for hash in self.hashes {
            res.extend_from_slice(&hash.0);
        }
        if let Some(ticks) = self.ticks {
            for tick in ticks {
                res.extend_from_slice(tick.as_slice());
            }
        }

        Cow::Owned(res)
    }
}

impl Decode for AuthChangeNode {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        use DecodeError::*;

        if input.len() < 3 {
            return Err(TooShortHeader);
        }

        let (header, body) = input.split_at(3);

        let meta = header[0];
        let size = (0x7f & meta) as usize;
        let is_leaf = (meta & 0x80) != 0;

        let avail_bitmap = header[1];
        let ticks_length = header[2] as usize;

        if size == 0 || size > 8 {
            return Err(Custom("Inconsistent size"));
        }

        if ticks_length >= 32 {
            return Err(Custom("Too large ticks length"));
        }

        if is_leaf && ticks_length != 0 {
            return Err(Custom("Inconsistent leaf information"));
        }

        let rest_length = size * 32 + (size - 1) * ticks_length;
        if body.len() != rest_length {
            return Err(IncorrectLength);
        }

        let (hash_part, tick_part) = body.split_at(size * 32);
        let hashes = hash_part
            .chunks_exact(32)
            .map(|x| H256(x.try_into().unwrap()))
            .collect();
        let ticks = if is_leaf {
            None
        } else {
            let ticks = tick_part
                .chunks_exact(ticks_length)
                .map(|x| x.try_into().unwrap())
                .collect();
            Some(ticks)
        };

        Ok(Cow::Owned(Self {
            hashes,
            ticks,
            avail_bitmap,
        }))
    }
}
