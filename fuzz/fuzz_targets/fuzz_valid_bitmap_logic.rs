#![no_main]

use arbitrary::{Arbitrary, Result, Unstructured};

use libfuzzer_sys::fuzz_target;

mod bitmap_oracle;
use bitmap_oracle::BitmapOracle;

use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::version_range::bitmap::{Bitmap, ValidBitmap, BITMAP_MAX_INDEX};

#[derive(Debug, Clone)]
pub struct FuzzValidBitmap(pub ValidBitmap);

impl<'a> Arbitrary<'a> for FuzzValidBitmap {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let vec: Vec<u16> = Arbitrary::arbitrary(u)?;

        let filtered_vec: Vec<u16> = vec.into_iter().filter(|&i| i <= BITMAP_MAX_INDEX).collect();
        
        let valid_bitmap = Bitmap::try_new_from_vec(&filtered_vec).unwrap();
        
        Ok(FuzzValidBitmap(valid_bitmap))
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    bitmap: FuzzValidBitmap,
    
    // for `last_le` and `collect_le`
    upper_bound: u64,

    // for `set_unchecked`
    index_to_set: u64,
}

fuzz_target!(|input: FuzzInput| {
    let FuzzInput {
        bitmap: fuzz_bitmap,
        upper_bound,
        index_to_set,
    } = input;

    let mut bitmap = fuzz_bitmap.0;

    // Oracle
    let initial_indices = bitmap.to_vec();
    let mut oracle = BitmapOracle::new(initial_indices);

    assert_eq!(
        bitmap.max_bit(),
        oracle.max_bit(),
        "max_bit() mismatch"
    );

    assert_eq!(
        bitmap.count_ones(),
        oracle.count_ones(),
        "count_ones() mismatch"
    );

    assert_eq!(
        bitmap.last_le(upper_bound),
        oracle.last_le(upper_bound),
        "last_le(upper_bound={}) mismatch", upper_bound
    );
    
    assert_eq!(
        bitmap.collect_le(upper_bound),
        oracle.collect_le(upper_bound),
        "collect_le(upper_bound={}) mismatch", upper_bound
    );

    let mut bitmap_for_set = bitmap.clone();
    let mut oracle_for_set = oracle.clone();
    let bitmap_ret = bitmap_for_set.set_unchecked(index_to_set);
    let oracle_ret = oracle_for_set.set_unchecked(index_to_set);
    assert_eq!(
        bitmap_ret,
        oracle_ret,
        "set_unchecked(index={}) return value mismatch", index_to_set
    );
    if bitmap_ret {
        assert_eq!(
            bitmap_for_set.to_vec(),
            oracle_for_set.to_vec(),
            "state mismatch after set_unchecked(index={})", index_to_set
        );
    }
});