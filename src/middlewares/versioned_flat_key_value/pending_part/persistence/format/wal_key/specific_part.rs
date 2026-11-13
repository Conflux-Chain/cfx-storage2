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
