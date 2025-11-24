#![no_main]

use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;
use cfx_storage2::middlewares::commit_id_schema::HistoryNumber;
use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::HistoryIndices;
use cfx_storage2::backends::serde::{Decode, Encode};

mod offset_based_version_range_arbitrary_impl;
use offset_based_version_range_arbitrary_impl::{FuzzOffsetBasedVersionRange, FuzzNonEmptyOffsetBasedVersionRange};

#[path = "bounded_vec.rs"]
mod bounded_vec;
use bounded_vec::BoundedVec;

#[derive(Debug, Arbitrary)]
pub enum FuzzHistoryIndices {
    /// Active record tracking ongoing modifications.
    Latest {
        /// Starting version number for this record
        start_version_number: HistoryNumber,
        /// Range encoding structure (may be empty)
        range_encoding: FuzzOffsetBasedVersionRange,
        /// Current value (None indicates deletion)
        latest_value: Option<BoundedVec>,
    },

    /// Immutable historical record. Contains:
    /// - Non-empty range encoding ensuring valid version ranges
    Previous(FuzzNonEmptyOffsetBasedVersionRange),
}

impl From<FuzzHistoryIndices> for HistoryIndices<BoundedVec> {
    fn from(arb: FuzzHistoryIndices) -> Self {
        match arb {
            FuzzHistoryIndices::Latest{start_version_number, range_encoding, latest_value} => HistoryIndices::<BoundedVec>::Latest{start_version_number, range_encoding: range_encoding.into(), latest_value},
            FuzzHistoryIndices::Previous(r) => HistoryIndices::<BoundedVec>::Previous(r.into()),
        }
    }
}

fuzz_target!(|fuzz: FuzzHistoryIndices| {
    let original: HistoryIndices<BoundedVec> = fuzz.into();
    let encoded_data = original.encode();
    
    match HistoryIndices::<BoundedVec>::decode(&encoded_data) {
        Ok(decoded_cow) => {
            let decoded = decoded_cow.into_owned();
            assert_eq!(original, decoded, "Roundtrip failed for HistoryIndices");
        }
        Err(e) => {
            panic!("Failed to decode a validly encoded HistoryIndices: {:?}", e);
        }
    }
});