use std::borrow::Cow;

use super::{DecResult, Decode, DecodeError, Encode, PendingKeyValueSchema};

/// Meta should be logically smaller than the corresponding other entries.
#[derive(Clone, Debug)]
pub enum WalKeySpecificPart<S: PendingKeyValueSchema> {
    AddNodeMeta,
    AddNodeMapKey(S::Key),
    ChangeRootMeta,
    MakePivotMeta,
    DiscardMeta,
}

impl<S: PendingKeyValueSchema> PartialEq for WalKeySpecificPart<S>
where
    S::Key: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::AddNodeMapKey(l0), Self::AddNodeMapKey(r0)) => l0 == r0,
            (Self::AddNodeMeta, Self::AddNodeMeta) => true,
            (Self::ChangeRootMeta, Self::ChangeRootMeta) => true,
            (Self::MakePivotMeta, Self::MakePivotMeta) => true,
            (Self::DiscardMeta, Self::DiscardMeta) => true,
            _ => false,
        }
    }
}

impl<S: PendingKeyValueSchema> Eq for WalKeySpecificPart<S> where S::Key: Eq {}

impl<S: PendingKeyValueSchema> PartialOrd for WalKeySpecificPart<S>
where
    S::Key: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: PendingKeyValueSchema> Ord for WalKeySpecificPart<S>
where
    S::Key: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        fn variant_order<T: PendingKeyValueSchema>(v: &WalKeySpecificPart<T>) -> u8 {
            match v {
                WalKeySpecificPart::AddNodeMeta => 0,
                WalKeySpecificPart::AddNodeMapKey(_) => 1,
                WalKeySpecificPart::ChangeRootMeta => 2,
                WalKeySpecificPart::MakePivotMeta => 3,
                WalKeySpecificPart::DiscardMeta => 4,
            }
        }

        let order_cmp = variant_order(self).cmp(&variant_order(other));
        if order_cmp != std::cmp::Ordering::Equal {
            return order_cmp;
        }

        match (self, other) {
            (WalKeySpecificPart::AddNodeMapKey(key1), WalKeySpecificPart::AddNodeMapKey(key2)) => {
                key1.cmp(key2)
            }
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl<S: PendingKeyValueSchema> Encode for WalKeySpecificPart<S> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            WalKeySpecificPart::AddNodeMeta => Cow::Borrowed(&[0x00]),
            WalKeySpecificPart::AddNodeMapKey(key) => {
                let encoded_key = key.encode();
                let mut vec = Vec::with_capacity(1 + encoded_key.len());
                vec.push(0x01);
                vec.extend_from_slice(encoded_key.as_ref());
                Cow::Owned(vec)
            }
            WalKeySpecificPart::ChangeRootMeta => Cow::Borrowed(&[0x02]),
            WalKeySpecificPart::MakePivotMeta => Cow::Borrowed(&[0x03]),
            WalKeySpecificPart::DiscardMeta => Cow::Borrowed(&[0x04]),
        }
    }
}

impl<S: PendingKeyValueSchema> Decode for WalKeySpecificPart<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input[0];
        let data = &input[1..];

        let require_empty = |key_variant: WalKeySpecificPart<S>| {
            if !data.is_empty() {
                Err(DecodeError::IncorrectLength)
            } else {
                Ok(Cow::Owned(key_variant))
            }
        };

        match tag {
            0x00 => require_empty(WalKeySpecificPart::AddNodeMeta),
            0x01 => {
                let key = S::Key::decode(data)?;
                Ok(Cow::Owned(WalKeySpecificPart::AddNodeMapKey(
                    key.into_owned(),
                )))
            }
            0x02 => require_empty(WalKeySpecificPart::ChangeRootMeta),
            0x03 => require_empty(WalKeySpecificPart::MakePivotMeta),
            0x04 => require_empty(WalKeySpecificPart::DiscardMeta),
            _ => Err(DecodeError::Custom("Invalid WalKey variant prefix")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::super::test_util::*;
    use super::*;
    use std::cmp::Ordering;

    type W = WalKeySpecificPart<TestSchema>;

    #[test]
    fn eq_and_ord_basic() {
        // Equality among identical unit variants
        assert_eq!(W::AddNodeMeta, W::AddNodeMeta);
        assert_eq!(W::ChangeRootMeta, W::ChangeRootMeta);
        assert_eq!(W::MakePivotMeta, W::MakePivotMeta);
        assert_eq!(W::DiscardMeta, W::DiscardMeta);

        // Different variants are not equal
        assert_ne!(W::AddNodeMeta, W::ChangeRootMeta);

        // Ord: by variant order 0..4
        assert!(W::AddNodeMeta < W::AddNodeMapKey(k(&[0x00])));
        assert!(W::AddNodeMapKey(k(&[0x00])) < W::ChangeRootMeta);
        assert!(W::ChangeRootMeta < W::MakePivotMeta);
        assert!(W::MakePivotMeta < W::DiscardMeta);

        // Ord within AddNodeMapKey uses key ordering
        assert_eq!(
            W::AddNodeMapKey(k(&[1, 2, 3])).cmp(&W::AddNodeMapKey(k(&[1, 2, 3]))),
            Ordering::Equal
        );
        assert_eq!(
            W::AddNodeMapKey(k(&[1, 2, 3])).cmp(&W::AddNodeMapKey(k(&[1, 2, 4]))),
            Ordering::Less
        );
        assert_eq!(
            W::AddNodeMapKey(k(&[1, 3])).cmp(&W::AddNodeMapKey(k(&[1, 2, 255]))),
            Ordering::Greater
        );
    }

    #[test]
    fn encode_tags_and_payloads() {
        assert_eq!(W::AddNodeMeta.encode().as_ref(), &[0x00]);
        assert_eq!(W::ChangeRootMeta.encode().as_ref(), &[0x02]);
        assert_eq!(W::MakePivotMeta.encode().as_ref(), &[0x03]);
        assert_eq!(W::DiscardMeta.encode().as_ref(), &[0x04]);

        // AddNodeMapKey: 0x01 + key bytes
        let w = W::AddNodeMapKey(k(&[0xAA, 0xBB, 0xCC]));
        let enc = w.encode();
        assert_eq!(enc.as_ref(), &[0x01, 0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn decode_unit_variants() {
        // Exact single-byte tags decode successfully
        assert!(matches!(
            W::decode(&[0x00]),
            Ok(c) if matches!(c.as_ref(), W::AddNodeMeta)
        ));
        assert!(matches!(
            W::decode(&[0x02]),
            Ok(c) if matches!(c.as_ref(), W::ChangeRootMeta)
        ));
        assert!(matches!(
            W::decode(&[0x03]),
            Ok(c) if matches!(c.as_ref(), W::MakePivotMeta)
        ));
        assert!(matches!(
            W::decode(&[0x04]),
            Ok(c) if matches!(c.as_ref(), W::DiscardMeta)
        ));

        // Extra payload after unit tag is an error
        assert_eq!(W::decode(&[0x00, 0xFF]), Err(DecodeError::IncorrectLength));
        assert_eq!(W::decode(&[0x02, 0x01]), Err(DecodeError::IncorrectLength));
        assert_eq!(
            W::decode(&[0x03, 0x01, 0x02]),
            Err(DecodeError::IncorrectLength)
        );
        assert_eq!(W::decode(&[0x04, 0x99]), Err(DecodeError::IncorrectLength));
    }

    #[test]
    fn decode_add_node_map_key() {
        // Empty key is allowed (encodes to tag only with no payload)
        let decoded = W::decode(&[0x01]).unwrap();
        assert!(matches!(decoded.as_ref(), W::AddNodeMapKey(k) if k.is_empty()));

        // Non-empty key
        let decoded = W::decode(&[0x01, 0x10, 0x20]).unwrap();
        assert!(matches!(decoded.as_ref(), W::AddNodeMapKey(k) if k.as_ref() == [0x10, 0x20]));
    }

    #[test]
    fn decode_errors() {
        // Empty input
        assert_eq!(W::decode(&[]), Err(DecodeError::IncorrectLength));

        // Unknown tag
        assert_eq!(
            W::decode(&[0xFF]),
            Err(DecodeError::Custom("Invalid WalKey variant prefix"))
        );
    }

    #[test]
    fn round_trip_encode_decode() {
        let cases = vec![
            W::AddNodeMeta,
            W::AddNodeMapKey(k(&[])),
            W::AddNodeMapKey(k(&[1, 2, 3])),
            W::ChangeRootMeta,
            W::MakePivotMeta,
            W::DiscardMeta,
        ];

        for case in cases {
            let enc = case.encode();
            let dec = W::decode(enc.as_ref()).expect("decode should succeed");
            assert_eq!(dec.as_ref(), &case);
        }
    }

    #[test]
    fn ordering_sort_sequence() {
        let mut items = vec![
            W::DiscardMeta,
            W::AddNodeMapKey(k(&[5])),
            W::AddNodeMeta,
            W::AddNodeMapKey(k(&[1, 2])),
            W::ChangeRootMeta,
            W::MakePivotMeta,
            W::AddNodeMapKey(k(&[1, 2, 3])),
            W::AddNodeMapKey(k(&[1])),
        ];

        items.sort();

        assert_eq!(
            items,
            vec![
                W::AddNodeMeta,            // tag 0
                W::AddNodeMapKey(k(&[1])), // tag 1, key order
                W::AddNodeMapKey(k(&[1, 2])),
                W::AddNodeMapKey(k(&[1, 2, 3])),
                W::AddNodeMapKey(k(&[5])),
                W::ChangeRootMeta, // tag 2
                W::MakePivotMeta,  // tag 3
                W::DiscardMeta,    // tag 4
            ]
        );
    }
}
