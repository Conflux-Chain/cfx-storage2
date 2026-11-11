use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};
use std::borrow::Cow;

use super::KEY_SLOT_SIZE;

/// Keys allocated to the key slots of one AMT leaf node, in slot order.
///
/// `keys[i]` is the key in slot `i`; `index` is the last allocated slot, so
/// `keys.len() == index + 1`. Allocations are append-only: a key keeps its slot
/// forever (a deleted key keeps an `LvmtValue` with `value: None`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationKeyInfo {
    pub(in crate::lvmt) index: u8,
    keys: Vec<Box<[u8]>>,
}

impl AllocationKeyInfo {
    /// The info of a leaf whose first slot is just allocated to `key`.
    pub fn first(key: Box<[u8]>) -> Self {
        Self {
            index: 0,
            keys: vec![key],
        }
    }

    /// Returns the info after allocating the next slot to `key`.
    ///
    /// Panics if all key slots are already allocated.
    pub fn with_next(&self, key: Box<[u8]>) -> Self {
        assert!(self.keys.len() < KEY_SLOT_SIZE, "all key slots allocated");
        let mut keys = self.keys.clone();
        keys.push(key);
        Self {
            index: self.index + 1,
            keys,
        }
    }

    /// The keys in slot order (`keys()[i]` is in slot `i`).
    pub fn keys(&self) -> &[Box<[u8]>] {
        &self.keys
    }
}

/// Format tag in the first byte, above the slot index (< `KEY_SLOT_SIZE`).
///
/// The previous format stored `[index][last key]` with `index < KEY_SLOT_SIZE`, so a
/// first byte without this bit is rejected instead of being misread.
const FORMAT_TAG: u8 = 0x80;

// Encoding: [FORMAT_TAG | index] then, for each of the `index + 1` keys,
// [u32 little-endian length][key bytes].
impl Encode for AllocationKeyInfo {
    fn encode(&self) -> Cow<[u8]> {
        let total: usize = self.keys.iter().map(|k| 4 + k.len()).sum();
        let mut raw = Vec::with_capacity(1 + total);
        raw.push(FORMAT_TAG | self.index);
        for key in &self.keys {
            raw.extend_from_slice(&(key.len() as u32).to_le_bytes());
            raw.extend_from_slice(key);
        }
        Cow::Owned(raw)
    }
}

impl Decode for AllocationKeyInfo {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        let (&head, mut rest) = input.split_first().ok_or(DecodeError::IncorrectLength)?;
        if head & FORMAT_TAG == 0 {
            return Err(DecodeError::Custom(
                "legacy slot allocation format (single key); rebuild the database",
            ));
        }
        let index = head & !FORMAT_TAG;
        if index as usize >= KEY_SLOT_SIZE {
            return Err(DecodeError::Custom("slot index overflow"));
        }

        let mut keys = Vec::with_capacity(index as usize + 1);
        for _ in 0..=index {
            if rest.len() < 4 {
                return Err(DecodeError::IncorrectLength);
            }
            let (len, tail) = rest.split_at(4);
            let len = u32::from_le_bytes(len.try_into().unwrap()) as usize;
            if tail.len() < len {
                return Err(DecodeError::IncorrectLength);
            }
            let (key, tail) = tail.split_at(len);
            keys.push(key.into());
            rest = tail;
        }
        if !rest.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        Ok(Cow::Owned(Self { index, keys }))
    }
}

#[cfg(test)]
mod tests {
    use crate::lvmt::types::test_utils;

    use proptest::arbitrary::Arbitrary;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;

    use super::*;

    impl Arbitrary for AllocationKeyInfo {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            vec(vec(0u8..=255, 0..128), 1..=KEY_SLOT_SIZE)
                .prop_map(|keys| Self {
                    index: (keys.len() - 1) as u8,
                    keys: keys.into_iter().map(Vec::into_boxed_slice).collect(),
                })
                .boxed()
        }
    }

    #[test]
    fn test_reject_legacy_format() {
        // Old format: [index][last key]
        assert!(AllocationKeyInfo::decode(&[2, b'k', b'e', b'y']).is_err());
        assert!(AllocationKeyInfo::decode(&[]).is_err());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn test_serde(data in any::<AllocationKeyInfo>()) {
            test_utils::test_serde(data)
        }

        #[test]
        fn test_with_next(keys in vec(vec(0u8..=255, 0..16), 1..=KEY_SLOT_SIZE)) {
            let mut keys = keys.into_iter().map(Vec::into_boxed_slice);
            let mut info = AllocationKeyInfo::first(keys.next().unwrap());
            for key in keys {
                info = info.with_next(key);
            }
            prop_assert_eq!(info.index as usize + 1, info.keys().len());
        }
    }
}
