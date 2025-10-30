#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::{VERSION_RANGE_BYTES, version_range::bitmap::{Bitmap, BITMAP_MAX_INDEX}};

#[derive(Debug, Arbitrary)]
enum ConstructorInput {
    FromBytes([u8; VERSION_RANGE_BYTES]),
    FromVec(Vec<u16>),
}

fuzz_target!(|data: ConstructorInput| {
    match data {
        ConstructorInput::FromBytes(bytes) => {
            let result = Bitmap::try_new(bytes);

            // Oracle
            let should_be_valid = (bytes[0] & 1) != 0;

            if should_be_valid {
                let valid_bitmap = result.expect("Constructor failed on a seemingly valid byte array");
                assert_eq!(valid_bitmap.as_slice(), &bytes);
            } else {
                assert!(result.is_err(), "Constructor succeeded on an invalid byte array");
            }
        }
        ConstructorInput::FromVec(vec) => {
            let result = Bitmap::try_new_from_vec(&vec);

            // Oracle
            let has_out_of_bounds = vec.iter().any(|&x| x > BITMAP_MAX_INDEX);

            if has_out_of_bounds {
                assert!(result.is_err(), "Constructor succeeded with out-of-bounds values in vec");
            } else {
                let valid_bitmap = result.expect("Constructor failed on a valid vec");
                
                // Oracle
                let mut oracle_vec: Vec<u16> = vec.clone();
                oracle_vec.push(0);
                oracle_vec.sort_unstable();
                oracle_vec.dedup();

                assert_eq!(valid_bitmap.to_vec(), oracle_vec);
            }
        }
    }
});