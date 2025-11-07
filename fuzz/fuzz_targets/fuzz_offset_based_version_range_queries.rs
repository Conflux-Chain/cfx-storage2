#![no_main]

use libfuzzer_sys::fuzz_target;
use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::version_range::{error::VersionError, OffsetBasedVersionRange, bitmap::Bitmap};
use cfx_storage2::middlewares::commit_id_schema::HistoryNumber;

mod offset_based_version_range_arbitrary_impl;
use offset_based_version_range_arbitrary_impl::FuzzOffsetBasedVersionRange;

mod oracle {
    use super::*;

    fn decode_to_versions(range: &OffsetBasedVersionRange, start_version: HistoryNumber) -> Result<Vec<HistoryNumber>, VersionError> {
        let mut versions = vec![start_version];

        match range {
            OffsetBasedVersionRange::OnlyEnd(offset) => {
                versions.push(start_version.checked_add(*offset).ok_or(VersionError::Overflow)?);
            }
            OffsetBasedVersionRange::U32Vector(vec) => {
                for &offset in vec {
                    versions.push(start_version.checked_add(offset as u64).ok_or(VersionError::Overflow)?);
                }
            }
            OffsetBasedVersionRange::U16Vector(vec) => {
                for &offset in vec {
                    versions.push(start_version.checked_add(offset as u64).ok_or(VersionError::Overflow)?);
                }
            }
            OffsetBasedVersionRange::Bitmap(bitmap) => {
                let collected_versions = bitmap
                    .to_vec()
                    .into_iter()
                    .map(|o| start_version.checked_add(o as u64).ok_or(VersionError::Overflow))
                    .collect::<Result<Vec<_>, _>>()?;

                versions.clear();
                versions.extend(collected_versions);
            }
        }

        let original_versions = versions.clone();

        versions.sort_unstable();
        versions.dedup();

        if versions != original_versions {
            panic!(
                "Panic after sort and dedup: The versions vector was modified. \
                This implies the initial set of versions was not already sorted and unique. \
                Original: {:?}, After dedup: {:?}",
                original_versions, versions
            );
        }

        Ok(versions)
    }

    pub fn last_le(range: &OffsetBasedVersionRange, start_version: HistoryNumber, upper_bound: HistoryNumber) -> Result<Option<HistoryNumber>, VersionError> {
        Ok(decode_to_versions(range, start_version)?
            .into_iter()
            .filter(|&v| v <= upper_bound)
            .last())
    }

    pub fn collect_versions_le(range: &OffsetBasedVersionRange, start_version: HistoryNumber, upper_bound: HistoryNumber) -> Result<Vec<HistoryNumber>, VersionError> {
        Ok(decode_to_versions(range, start_version)?
            .into_iter()
            .filter(|&v| v <= upper_bound)
            .collect())
    }
}

fuzz_target!(|data: (FuzzOffsetBasedVersionRange, HistoryNumber, HistoryNumber)| {
    let (arbitrary_range, start_version, upper_bound) = data;
    let range: OffsetBasedVersionRange = arbitrary_range.into();

    if range.validate().is_err() {
        panic!("FuzzOffsetBasedVersionRange's Arbitrary impl should always produce a valid range, but validate() failed.");
    }

    let expected_last_le = oracle::last_le(&range, start_version, upper_bound);
    let expected_collect_le = oracle::collect_versions_le(&range, start_version, upper_bound);

    let actual_last_le = range.last_le(start_version, upper_bound);
    let actual_collect_le = range.collect_versions_le(start_version, upper_bound);

    assert_eq!(actual_last_le, expected_last_le, "last_le mismatch. Range: {:?}, Start: {}, UpperBound: {}", range, start_version, upper_bound);
    assert_eq!(actual_collect_le, expected_collect_le, "collect_versions_le mismatch. Range: {:?}, Start: {}, UpperBound: {}", range, start_version, upper_bound);
});