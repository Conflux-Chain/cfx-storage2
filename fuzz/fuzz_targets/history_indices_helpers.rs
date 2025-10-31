use cfx_storage2::middlewares::versioned_flat_key_value::history_indices::version_range::bitmap::{Bitmap, ValidBitmap, BITMAP_MAX_INDEX};

use arbitrary::{Arbitrary, Result, Unstructured};

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