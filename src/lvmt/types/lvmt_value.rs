use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};
use std::borrow::Cow;

use super::allocation::AllocatePosition;

#[derive(Clone, Debug, PartialEq, Eq)]
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
        if input.len() < 6 {
            return Err(DecodeError::TooShortHeader);
        }
        let (header, body) = input.split_at(6);
        let (allocation_raw, version_raw) = header.split_at(1);
        let allocation = AllocatePosition::decode(allocation_raw)?.into_owned();

        let mut version_bytes = [0u8; 8];
        version_bytes[0..5].copy_from_slice(version_raw);

        let version = u64::from_le_bytes(version_bytes);
        let value = body.to_vec().into_boxed_slice();

        Ok(Cow::Owned(Self {
            allocation,
            version,
            value,
        }))
    }
}

#[cfg(test)]
mod tests {
    use proptest::arbitrary::Arbitrary;
    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;

    use crate::lvmt::types::test_utils::{self, value_strategy, version_strategy};

    use super::*;

    impl Arbitrary for LvmtValue {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            let allocation_strategy = any::<AllocatePosition>();

            (allocation_strategy, version_strategy(), value_strategy())
                .prop_map(|(allocation, version, value)| LvmtValue {
                    allocation,
                    version,
                    value,
                })
                .boxed()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn test_serde(data in any::<LvmtValue>()) {
            test_utils::test_serde(data)
        }
    }
}
