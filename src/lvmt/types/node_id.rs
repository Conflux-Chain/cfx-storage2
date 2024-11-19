use ethereum_types::H256;
use tinyvec::ArrayVec;

use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};
use std::borrow::Cow;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Copy, Hash, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AmtId(ArrayVec<[u16; 16]>);

impl Deref for AmtId {
    type Target = ArrayVec<[u16; 16]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AmtId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub type AmtNodeId = AmtId;

pub fn compute_amt_node_id(digest: H256, depth: usize) -> AmtNodeId {
    let length = depth + 1;
    let mut data = [0u16; 16];
    for i in 0..length {
        data[i] = u16::from_be_bytes(digest[i * 2..(i + 1) * 2].try_into().unwrap());
    }
    AmtId(ArrayVec::from_array_len(data, length))
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
        Ok(Cow::Owned(AmtId(array)))
    }
}

#[cfg(test)]
pub mod tests {
    use crate::lvmt::types::test_utils;

    use super::*;
    use proptest::array::uniform32;
    use proptest::collection::vec;
    use proptest::prelude::*;

    impl Arbitrary for AmtId {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            vec(0u16..u16::MAX, 0..=16)
                .prop_map(|x| AmtId(x.as_slice().try_into().unwrap()))
                .boxed()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        // Test that encoding and decoding are inverse operations
        #[test]
        fn test_serde(data in any::<AmtId>()) {
            test_utils::test_serde(data)
        }

        #[test]
        fn test_serde_keep_order((a, b) in (any::<AmtId>(), any::<AmtId>())) {
            test_utils::test_serde_keep_order(a, b)
        }

        #[test]
        fn test_amt_id_decode_error(data in vec(0u8..=255, 1..32).prop_filter("Length must be odd", |v| v.len() % 2 != 0)) {
            let result = AmtId::decode(&data);

            // Decode should return an error
            prop_assert_eq!(result, Err(DecodeError::IncorrectLength));
        }

        #[test]
        fn test_amt_id_compute((digest, depth) in (uniform32(0u8..=255), 0usize..=14)) {
            let digest = H256(digest);

            let node_id = compute_amt_node_id(digest, depth);

            prop_assert_eq!(node_id.len(), depth + 1);

            prop_assert_eq!(node_id.encode(), &digest[0..2*(depth + 1)]);
        }
    }
}
