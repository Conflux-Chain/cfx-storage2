#![no_main]

use libfuzzer_sys::fuzz_target;
use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::version_range::OffsetBasedVersionRange;
use cfx_storage2::middlewares::commit_id_schema::HistoryNumber;

fuzz_target!(|version_seqs: Vec<HistoryNumber>| {
    if version_seqs.len() < 2 {
        return;
    }

    let mut sorted_unique_versions = version_seqs;
    sorted_unique_versions.sort_unstable();
    sorted_unique_versions.dedup();

    if sorted_unique_versions.len() < 2 {
        return;
    }

    // Initialize state
    let mut current_start_version = sorted_unique_versions[0];
    let mut range = OffsetBasedVersionRange::new();

    for &version in &sorted_unique_versions[1..] {
        assert!(version > current_start_version, "The input sequence is strictly increasing. There is something wrong with current_start_version");

        let offset = version - current_start_version;

        match range.try_push_or_new(offset) {
            Ok(Some(new_range)) => {
                // Range split occurs.
                assert!(range.validate().is_ok(), "Original range became invalid after split. Original: {:?}", range);

                current_start_version += range.max_offset();
                range = new_range;

                assert!(range.validate().is_ok(), "New range from split is invalid. New: {:?}", range);
            }
            Ok(None) => {
                assert!(range.validate().is_ok(), "Range became invalid after push. Range: {:?}", range);
            }
            Err(e) => {
                panic!("try_push_or_new failed unexpectedly: {:?}, on range: {:?}", e, range);
            }
        }
    }
});