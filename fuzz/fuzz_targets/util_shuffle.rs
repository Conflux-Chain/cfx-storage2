use arbitrary::{Error, Unstructured};

/// Shuffles a slice in-place using the Fisher-Yates algorithm and an `Unstructured` data source.
///
/// # Arguments
/// * `u`: A mutable `Unstructured` instance, used as a source of randomness.
/// * `slice`: The mutable slice to be shuffled.
///
/// # Returns
/// Returns `Ok(())` if the `Unstructured` source has enough data to perform the shuffle.
/// Returns `Err(arbitrary::Error::NotEnoughData)` if the source runs out of data.
///
/// # Generics
/// * `'a`: The lifetime of the `Unstructured` reference.
/// * `T`: The type of the elements in the slice.
pub fn shuffle_slice<'a, T>(
    u: &mut Unstructured<'a>,
    slice: &mut [T],
) -> Result<(), Error> {
    // If the slice has 1 or 0 elements, there's nothing to shuffle.
    if slice.len() <= 1 {
        return Ok(());
    }

    // Implement the Fisher-Yates shuffle algorithm.
    for i in (1..slice.len()).rev() {
        // Choose a random index from the range [0, i] (inclusive).
        let swap_idx = u.int_in_range(0..=i)?;
        
        // Swap the current element with the randomly chosen one.
        slice.swap(i, swap_idx);
    }

    Ok(())
}