// Naive history index implementation: one record per (key, version) write.
//
// Each write produces one `HistoryIndexKey(key, version)` entry whose value is
// an empty `HistoryIndices`. Lookups use the table's seek-geq iterator with the
// version portion reverse-encoded (see `serde.rs`) so the first match for
// `(key, v_query)` is the record with the largest `v' <= v_query` for that key.

use std::borrow::Cow;

use crate::backends::serde::{Decode, Encode};
use crate::errors::{DecResult, DecodeError};

#[derive(Clone, Debug)]
pub struct HistoryIndices;

#[cfg(test)]
impl PartialEq for HistoryIndices {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

const EMPTY: &[u8] = &[];
impl Encode for HistoryIndices {
    fn encode(&self) -> Cow<[u8]> {
        Cow::Borrowed(EMPTY)
    }
}

impl Decode for HistoryIndices {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if !input.is_empty() {
            return Err(DecodeError::IncorrectLength);
        }
        Ok(Cow::Owned(HistoryIndices))
    }
}
