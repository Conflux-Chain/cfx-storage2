use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};
use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationKeyInfo {
    pub(in crate::lvmt) index: u8,
    key: Box<[u8]>,
}

impl AllocationKeyInfo {
    pub fn new(index: u8, key: Box<[u8]>) -> Self {
        Self { index, key }
    }
}

impl Encode for AllocationKeyInfo {
    fn encode(&self) -> Cow<[u8]> {
        let mut raw = vec![self.index as u8];
        raw.extend(self.key.as_ref());
        Cow::Owned(raw)
    }
}

impl Decode for AllocationKeyInfo {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() == 0 {
            return Err(DecodeError::IncorrectLength);
        }
        let index = input[0];
        let key = input[1..].to_vec().into_boxed_slice();
        Ok(Cow::Owned(Self { index, key }))
    }
}

#[cfg(test)]
mod tests {
    use crate::lvmt::types::test_utils;

    use super::super::KEY_SLOT_SIZE;
    use proptest::arbitrary::Arbitrary;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;

    use super::*;

    impl Arbitrary for AllocationKeyInfo {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            (0..KEY_SLOT_SIZE, vec(0u8..=255, 0..128))
                .prop_map(|(depth, key)| Self {
                    index: depth as u8,
                    key: key.into_boxed_slice(),
                })
                .boxed()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn test_serde(data in any::<AllocationKeyInfo>()) {
            test_utils::test_serde(data)
        }
    }
}
