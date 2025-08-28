use std::borrow::Cow;

use super::{
    super::{
        DecResult, Decode, DecodeError, Encode, FixedLengthEncoded, PendingKeyValueSchema,
        RecoverRecord, ValueEntry,
    },
    codec::{decode_option, decode_option_owned, encode_option},
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum WalValue<S: PendingKeyValueSchema> {
    MetaValue {
        commit_id: S::CommitId,
        maybe_parent_cid: Option<S::CommitId>,
        /// The total number of MapValue records associated with this commit. This is only for checking consistency.
        map_value_count: u64,
    },
    MapValue(RecoverRecord<S>),
}

impl<T: Encode + Clone> Encode for ValueEntry<T> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            ValueEntry::Deleted => Cow::Borrowed(&[0x00]),
            ValueEntry::Value(value) => {
                let encoded_val = value.encode();
                let mut vec = Vec::with_capacity(1 + encoded_val.len());
                vec.push(0x01); // 'Value' tag
                vec.extend_from_slice(encoded_val.as_ref());
                Cow::Owned(vec)
            }
        }
    }
}

impl<T: Decode + Clone + ToOwned<Owned = T>> Decode for ValueEntry<T> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input[0];
        let data = &input[1..];

        match tag {
            0x00 => {
                if !data.is_empty() {
                    return Err(DecodeError::IncorrectLength);
                }
                Ok(Cow::Owned(ValueEntry::Deleted))
            }
            0x01 => {
                let value = T::decode(data)?;
                Ok(Cow::Owned(ValueEntry::Value(value.into_owned())))
            }
            _ => Err(DecodeError::Custom("Invalid ValueEntry variant prefix")),
        }
    }
}

impl<S: PendingKeyValueSchema> Encode for RecoverRecord<S> {
    fn encode(&self) -> Cow<[u8]> {
        let encoded_value_entry = self.value.encode();
        let encoded_last_cid = encode_option(&self.last_commit_id);

        let len_prefix = (encoded_value_entry.len() as u32).to_be_bytes();

        let mut vec = Vec::with_capacity(4 + encoded_value_entry.len() + encoded_last_cid.len());
        vec.extend_from_slice(&len_prefix);
        vec.extend_from_slice(encoded_value_entry.as_ref());
        vec.extend_from_slice(encoded_last_cid.as_ref());

        Cow::Owned(vec)
    }
}

impl<S: PendingKeyValueSchema> Decode for RecoverRecord<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() < 4 {
            return Err(DecodeError::IncorrectLength);
        }
        let value_entry_len = u32::from_be_bytes(input[0..4].try_into().unwrap()) as usize;

        let cursor = 4 + value_entry_len;
        if input.len() < cursor {
            return Err(DecodeError::IncorrectLength);
        }

        let value_entry_data = &input[4..cursor];
        let value = ValueEntry::<S::Value>::decode(value_entry_data)?;

        let remaining_data = &input[cursor..];
        let last_commit_id = decode_option::<S::CommitId>(remaining_data)?;

        Ok(Cow::Owned(RecoverRecord {
            value: value.into_owned(),
            last_commit_id: last_commit_id.map(|cow| cow.into_owned()),
        }))
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.len() < 4 {
            return Err(DecodeError::IncorrectLength);
        }
        let value_entry_len = u32::from_be_bytes(input[0..4].try_into().unwrap()) as usize;

        let total_prefix_len = 4 + value_entry_len;
        if input.len() < total_prefix_len {
            return Err(DecodeError::IncorrectLength);
        }

        let last_cid_data = input.split_off(total_prefix_len);

        let value_entry_data = input.split_off(4);

        let value = ValueEntry::<S::Value>::decode_owned(value_entry_data)?;
        let last_commit_id = decode_option_owned::<S::CommitId>(last_cid_data)?;

        Ok(RecoverRecord {
            value,
            last_commit_id,
        })
    }
}

impl<S: PendingKeyValueSchema> Encode for WalValue<S> {
    fn encode(&self) -> Cow<[u8]> {
        match self {
            WalValue::MetaValue {
                commit_id,
                maybe_parent_cid,
                map_value_count,
            } => {
                let encoded_cid = commit_id.encode();
                let encoded_parent_opt = encode_option(maybe_parent_cid);
                let encoded_count = map_value_count.encode();

                let mut vec = Vec::with_capacity(
                    1 // Tag
                    + S::CommitId::LENGTH
                    + encoded_parent_opt.len()
                    + u64::LENGTH,
                );

                vec.push(0x00); // MetaValue tag
                vec.extend_from_slice(encoded_cid.as_ref());
                vec.extend_from_slice(encoded_parent_opt.as_ref());
                vec.extend_from_slice(encoded_count.as_ref());
                Cow::Owned(vec)
            }
            WalValue::MapValue(record) => {
                let encoded_record = record.encode();
                let mut vec = Vec::with_capacity(1 + encoded_record.len());
                vec.push(0x01); // MapValue tag
                vec.extend_from_slice(encoded_record.as_ref());
                Cow::Owned(vec)
            }
        }
    }
}

impl<S: PendingKeyValueSchema> Decode for WalValue<S> {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input[0];
        let data = &input[1..];

        match tag {
            0x00 => {
                // MetaValue
                let cid_len = S::CommitId::LENGTH;
                let count_len = u64::LENGTH;
                let fixed_parts_len = cid_len + count_len;

                if data.len() < fixed_parts_len {
                    return Err(DecodeError::IncorrectLength);
                }

                let (commit_id_data, rest) = data.split_at(cid_len);

                let (parent_cid_data, count_data) = rest.split_at(rest.len() - count_len);

                let commit_id = S::CommitId::decode(commit_id_data)?;
                let maybe_parent_cid = decode_option::<S::CommitId>(parent_cid_data)?;
                let map_value_count = u64::decode(count_data)?;

                Ok(Cow::Owned(WalValue::MetaValue {
                    commit_id: commit_id.into_owned(),
                    maybe_parent_cid: maybe_parent_cid.map(|cow| cow.into_owned()),
                    map_value_count: map_value_count.into_owned(),
                }))
            }
            0x01 => {
                // MapValue
                let record = RecoverRecord::<S>::decode(data)?;
                Ok(Cow::Owned(WalValue::MapValue(record.into_owned())))
            }
            _ => Err(DecodeError::Custom("Invalid WalValue variant prefix")),
        }
    }

    fn decode_owned(mut input: Vec<u8>) -> DecResult<Self> {
        if input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }

        let tag = input.remove(0);

        match tag {
            0x00 => {
                // MetaValue
                let cid_len: usize = S::CommitId::LENGTH;
                let count_len = u64::LENGTH;
                let fixed_parts_len = cid_len + count_len;

                if input.len() < fixed_parts_len {
                    return Err(DecodeError::IncorrectLength);
                }

                let mut rest = input.split_off(cid_len);
                let commit_id_data = input;

                let count_data = rest.split_off(rest.len() - count_len);
                let parent_cid_data = rest;

                let commit_id = S::CommitId::decode_owned(commit_id_data)?;
                let maybe_parent_cid = decode_option_owned::<S::CommitId>(parent_cid_data)?;
                let map_value_count = u64::decode_owned(count_data)?;

                Ok(WalValue::MetaValue {
                    commit_id,
                    maybe_parent_cid,
                    map_value_count,
                })
            }
            0x01 => {
                // MapValue
                let record = RecoverRecord::<S>::decode_owned(input)?;
                Ok(WalValue::MapValue(record))
            }
            _ => Err(DecodeError::Custom("Invalid WalValue variant prefix")),
        }
    }
}
