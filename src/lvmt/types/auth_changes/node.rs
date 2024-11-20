use super::{log2_ceil, MAX_NODE_SIZE};
use crate::errors::{DecResult, DecodeError};
use crate::lvmt::types::auth_changes::bit_ones;
use crate::{
    backends::serde::{Decode, Encode},
    utils::hash::blake2s_tuple,
};
use ethereum_types::H256;
use std::borrow::Cow;
use tinyvec::ArrayVec;

type Prefix = ArrayVec<[u8; 32]>;
#[derive(Debug, Clone, PartialEq, Eq)]
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
        let avail_bitmap = bit_ones(size);
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
        let avail_bitmap = bit_ones(size);
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

    pub fn hash(&self) -> H256 {
        if self.hashes.len() == 1 {
            return self.hashes[0];
        }
        let height = log2_ceil(self.hashes.len());
        let pairs = self.hashes.len() - (1 << (height - 1));

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

    pub fn is_leaf(&self) -> bool {
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

        if ticks_length > 32 {
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

#[cfg(test)]
mod tests {
    use crate::lvmt::types::test_utils::{self, bytes32_strategy};

    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::sample::SizeRange;

    fn leaves_strategy(size: impl Into<SizeRange>) -> impl Strategy<Value = Vec<H256>> {
        vec(bytes32_strategy(), size).prop_filter_map("duplicated item", |mut x| {
            x.sort();
            if x.len() > 0 {
                for i in 0..(x.len() - 1) {
                    if x[i + 1] == x[i] {
                        return None;
                    }
                }
            }
            Some(x)
        })
    }

    fn leave_node_strategy() -> impl Strategy<Value = AuthChangeNode> {
        leaves_strategy(1..8).prop_map(|x| AuthChangeNode::from_leaves(&x))
    }

    fn inner_node_strategy() -> impl Strategy<Value = AuthChangeNode> {
        vec(4usize..=8, 2..=8).prop_flat_map(|size_list| {
            let num_leaves: usize = size_list.iter().sum();
            let num_nodes = size_list.len();
            let size_list: ArrayVec<[usize; 8]> = size_list[..].try_into().unwrap();

            (
                leaves_strategy(num_leaves),
                leaves_strategy(num_nodes - 1),
                0usize..32,
            )
                .prop_map(move |(mut leaves, ticks, shared_prefix_len)| {
                    leaves.sort();

                    let mut rest = &leaves[..];
                    let mut leaf_nodes = vec![];
                    for size in size_list {
                        let head;
                        (head, rest) = rest.split_at(size);
                        leaf_nodes.push(AuthChangeNode::from_leaves(head));
                    }

                    AuthChangeNode::from_nodes(&leaf_nodes, ticks, shared_prefix_len)
                })
        })
    }

    impl Arbitrary for AuthChangeNode {
        type Parameters = bool;
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(all_valid_bitmap: bool) -> Self::Strategy {
            let new_node_strategy = prop_oneof![leave_node_strategy(), inner_node_strategy()];

            (new_node_strategy, any::<u8>())
                .prop_map(move |(mut node, alter_bitmap)| {
                    if !all_valid_bitmap {
                        node.avail_bitmap &= alter_bitmap;
                    }
                    node
                })
                .boxed()
        }
    }

    fn f(a: H256, b: H256) -> H256 {
        blake2s_tuple(&a, &b)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn test_serde(data in any::<AuthChangeNode>()) {
            test_utils::test_serde(data)
        }

        #[test]
        fn test_consistent_len(data in any::<AuthChangeNode>()) {
            if data.ticks.as_ref().map_or(true, |x|x.is_empty()) {
                return Ok(());
            }

            let ticks = data.ticks.unwrap();
            let prefix_len = ticks[0].len();
            prop_assert!(ticks[1..].iter().all(|x| x.len() == prefix_len));
        }

        #[test]
        fn test_leaves_length_1(l in leaves_strategy(1)) {
            let actual_hash = AuthChangeNode::from_leaves(&l[..]).hash();
            let expect_hash = l[0];
            prop_assert_eq!(actual_hash, expect_hash);
        }

        #[test]
        fn test_leaves_length_2(l in leaves_strategy(2)) {
            let actual_hash = AuthChangeNode::from_leaves(&l[..]).hash();
            let expect_hash = f(l[0], l[1]);
            prop_assert_eq!(actual_hash, expect_hash);
        }

        #[test]
        fn test_leaves_length_3(l in leaves_strategy(3)) {
            let actual_hash = AuthChangeNode::from_leaves(&l[..]).hash();
            let expect_hash = f(f(l[0], l[1]), l[2]);
            prop_assert_eq!(actual_hash, expect_hash);
        }

        #[test]
        fn test_leaves_length_4(l in leaves_strategy(4)) {
            let actual_hash = AuthChangeNode::from_leaves(&l[..]).hash();
            let expect_hash = f(f(l[0], l[1]), f(l[2], l[3]));
            prop_assert_eq!(actual_hash, expect_hash);
        }

        #[test]
        fn test_leaves_length_5(l in leaves_strategy(5)) {
            let actual_hash = AuthChangeNode::from_leaves(&l[..]).hash();
            let expect_hash = f(f(f(l[0], l[1]), l[2]), f(l[3], l[4]));
            prop_assert_eq!(actual_hash, expect_hash);
        }

        #[test]
        fn test_leaves_length_6(l in leaves_strategy(6)) {
            let actual_hash = AuthChangeNode::from_leaves(&l[..]).hash();
            let expect_hash = f(f(f(l[0], l[1]), f(l[2], l[3])), f(l[4], l[5]));
            prop_assert_eq!(actual_hash, expect_hash);
        }

        #[test]
        fn test_leaves_length_7(l in leaves_strategy(7)) {
            let actual_hash = AuthChangeNode::from_leaves(&l[..]).hash();
            let expect_hash = f(f(f(l[0], l[1]), f(l[2], l[3])), f(f(l[4], l[5]), l[6]));
            prop_assert_eq!(actual_hash, expect_hash);
        }

        #[test]
        fn test_leaves_length_8(l in leaves_strategy(8)) {
            let actual_hash = AuthChangeNode::from_leaves(&l[..]).hash();
            let expect_hash = f(f(f(l[0], l[1]), f(l[2], l[3])), f(f(l[4], l[5]), f(l[6], l[7])));
            prop_assert_eq!(actual_hash, expect_hash);
        }
    }
}
