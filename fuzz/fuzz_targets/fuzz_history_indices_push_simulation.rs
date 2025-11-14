#![no_main]

use libfuzzer_sys::fuzz_target;
use arbitrary::{Arbitrary, Unstructured, Result};
// Import the NonZero integer types.
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::HistoryIndices;
use cfx_storage2::middlewares::commit_id_schema::HistoryNumber;

/// Defines different version stepping strategies, allowing the fuzzer to explore various types of inputs.
#[derive(Debug, Arbitrary)]
enum VersionStep {
    /// An offset of zero, which is an invalid input for a version increment.
    ZeroOffset,
    /// Generates a small, non-zero offset (1-65535).
    SmallOffset(NonZeroU16),
    /// Generates a medium-sized, non-zero offset.
    MediumOffset(NonZeroU32),
    /// Generates a large, non-zero offset.
    LargeOffset(NonZeroU64),
    /// Directly specifies the next version number, making it easy to test invalid inputs (like time going backwards).
    AbsoluteVersion(HistoryNumber),
}

#[derive(Debug, Arbitrary, Clone, Copy)]
enum StartVersion {
    Small(u16),
    Medium(u32),
    Large(u64),
}

impl From<StartVersion> for HistoryNumber {
    fn from(arb: StartVersion) -> Self {
        match arb {
            StartVersion::Small(s) => s as HistoryNumber,
            StartVersion::Medium(m) => m as HistoryNumber,
            StartVersion::Large(l) => l as HistoryNumber,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BoundedVec(pub Vec<u8>);

impl<'a> Arbitrary<'a> for BoundedVec {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let len = u.int_in_range(0..=32)?;
        let bytes = u.bytes(len)?.to_vec();
        Ok(BoundedVec(bytes))
    }
}

#[derive(Debug, Arbitrary)]
struct PushSimulationInput {
    start_version: StartVersion,
    initial_value: Option<BoundedVec>,
    /// A series of version stepping actions.
    steps: Vec<VersionStep>,
    /// A sequence of random values that can be cycled through.
    values: Vec<Option<BoundedVec>>,
}

fuzz_target!(|input: PushSimulationInput| {
    if input.steps.is_empty() {
        return;
    }

    let mut history = HistoryIndices::new(input.start_version.into(), input.initial_value.clone());
    let mut current_version: HistoryNumber = input.start_version.into();
    // If the value sequence is empty, always use None.
    let default_values: Vec<Option<BoundedVec>> = vec![None];
    let mut value_iter = if input.values.is_empty() {
        default_values.iter().cycle()
    } else {
        input.values.iter().cycle()
    };


    for step in input.steps {
        // Calculate the next target version number based on the VersionStep.
        let next_version = match step {
            VersionStep::ZeroOffset => current_version, // This will always trigger the invalid path check.
            // Use .get() to retrieve the value from NonZero types.
            VersionStep::SmallOffset(offset) => current_version.saturating_add(offset.get() as u64),
            VersionStep::MediumOffset(offset) => current_version.saturating_add(offset.get() as u64),
            VersionStep::LargeOffset(offset) => current_version.saturating_add(offset.get()),
            VersionStep::AbsoluteVersion(version) => version,
        };

        let new_value = value_iter.next().cloned().unwrap();

        // Before calling push, record the current state for verification.
        let (start_version_before, range_before) = if let HistoryIndices::Latest { start_version_number, range_encoding, .. } = &history {
            (*start_version_number, range_encoding.clone())
        } else {
            // In a valid simulation sequence, the state must be Latest before calling push.
            panic!("HistoryIndices should be Latest before a valid push. Current version: {}", current_version);
        };

        if next_version <= current_version {
            // --- Test invalid input path ---
            // This is an expected error condition (pushing an older or same version).
            // We assert that the `push` method must return an error and must not panic.
            let history_backup = history.clone();
            let result = history.push(next_version, new_value);
            assert!(
                result.is_err(),
                "push() should return an error for non-increasing version numbers. current: {}, next: {}",
                current_version,
                next_version
            );
            assert_eq!(
                history_backup.collect_versions_le(start_version_before, u64::MAX).unwrap(),
                history.collect_versions_le(start_version_before, u64::MAX).unwrap(),
                "The state should not have changed if push() returned an error"
            );

            if current_version == u64::MAX {
                return;
            } else {
                continue; // Continue to the next step.
            }
        }

        // --- Test valid input path ---
        match history.push(next_version, new_value) {
            Ok(Some(previous_record)) => {
                // A split occurred, perform verification.
                let expected_end_version = start_version_before + range_before.max_offset();
                
                // 1. Verify the end_version_number of the split-off PreviousRecord.
                assert_eq!(
                    previous_record.end_version_number, expected_end_version,
                    "Previous record end_version_number is incorrect"
                );

                // 2. Verify that the range_encoding of the PreviousRecord matches the state before the split.
                assert_eq!(
                    previous_record.range_encoding, range_before,
                    "Previous record range_encoding is incorrect"
                );

                // 3. Verify that the range_encoding of the PreviousRecord is non-empty
                assert!(
                    previous_record.range_encoding.max_offset() > 0,
                    "Previous record range_encoding should be non-empty"
                );

                // 4. Verify that the start_version_number of the new Latest record correctly follows the previous one.
                if let HistoryIndices::Latest { start_version_number, range_encoding, .. } = &history {
                    assert_eq!(
                        *start_version_number, expected_end_version,
                        "New latest record's start_version_number should be the old latest's end version"
                    );
                    assert_eq!(
                        range_encoding.collect_versions_le(*start_version_number, u64::MAX).unwrap(), vec![*start_version_number, next_version],
                        "If split, new latest record's versions should contain the start version number and the pushed version number"
                    );
                } else {
                    panic!("After a split, state should still be Latest");
                }
            }
            Ok(None) => {
                // No split occurred, the state is still Latest.
                if let HistoryIndices::Latest { start_version_number, range_encoding, .. } = &history {
                    assert_eq!(
                        *start_version_number, start_version_before,
                        "The latest record after pushing should maintain the same start version number"
                    );
                    let mut versions_before_pushing = range_before.collect_versions_le(start_version_before, u64::MAX).unwrap();
                    versions_before_pushing.push(next_version);
                    let versions_after_pushing = range_encoding.collect_versions_le(*start_version_number, u64::MAX).unwrap();
                    assert_eq!(
                        versions_before_pushing, versions_after_pushing,
                        "If not split, new latest record's versions should contain the old versions and the pushed version number"
                    );
                } else {
                    panic!("After a push instead of a split, state should still be Latest");
                }
            }
            Err(e) => {
                // When next_version > current_version, push should not return an error.
                // If this happens, it indicates a flaw in the internal logic of push or in our preconditions.
                panic!(
                    "push() returned an unexpected error on a valid increasing version. current: {}, next: {}. Error: {:?}",
                    current_version, next_version, e
                );
            }
        }
        
        // Update the current version number for the next iteration.
        current_version = next_version;
    }
});