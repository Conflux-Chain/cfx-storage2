use super::SLOT_SIZE;
use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};
use crate::lvmt::types::{compute_amt_node_id, AmtId};
use crate::utils::hash::blake2s;
use std::borrow::Cow;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AllocatePosition {
    pub(in crate::lvmt) depth: u8,
    pub(in crate::lvmt) slot_index: u8,
}

impl AllocatePosition {
    pub fn amt_info(&self, key: &[u8]) -> (AmtId, u16, u8) {
        let digest = blake2s(key);
        let mut amt_node_id = compute_amt_node_id(digest, self.depth as usize);
        let node_index = amt_node_id.pop().unwrap();
        let amt_id = amt_node_id;
        (amt_id, node_index, self.slot_index)
    }
}

impl Encode for AllocatePosition {
    fn encode(&self) -> std::borrow::Cow<[u8]> {
        let meta = (self.depth as u8 & 0x1f) | ((self.slot_index as u8) << 5);
        Cow::Owned(vec![meta])
    }
}

impl Decode for AllocatePosition {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() != 1 {
            return Err(DecodeError::IncorrectLength);
        }

        let meta = input[0];
        let (depth, slot_index) = (meta & 0x1f, meta >> 5);

        if slot_index as usize >= SLOT_SIZE {
            return Err(DecodeError::Custom("slot_index overflow"));
        }

        if depth == 0 {
            return Err(DecodeError::Custom("depth cannot be zero"));
        }

        Ok(Cow::Owned(AllocatePosition { depth, slot_index }))
    }
}

#[cfg(test)]
mod tests {
    use proptest::arbitrary::Arbitrary;
    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;

    use crate::lvmt::types::test_utils;

    use super::*;

    impl Arbitrary for AllocatePosition {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            (1u8..=14, 0..SLOT_SIZE)
                .prop_map(|(depth, slot_index)| Self {
                    depth,
                    slot_index: slot_index as u8,
                })
                .boxed()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn test_serde(data in any::<AllocatePosition>()) {
            test_utils::test_serde(data)
        }

        #[test]
        fn test_decode_raw(raw in 0u8..255) {
            if let Ok(decoded) = AllocatePosition::decode(&[raw]) {
                prop_assert!(decoded.depth > 0);
                prop_assert!(decoded.slot_index < SLOT_SIZE as u8);
            }
        }
    }
}
