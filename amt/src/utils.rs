use ark_ec::pairing::Pairing;
use ark_std::cfg_into_iter;
#[cfg(feature = "parallel")]
use rayon::prelude::*;
use std::{
    any::Any,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

pub(crate) fn type_hash<T: Any>() -> String {
    use base64::prelude::*;

    let type_name = std::any::type_name::<T>().to_string();
    let mut s = DefaultHasher::new();
    type_name.hash(&mut s);
    BASE64_STANDARD.encode(s.finish().to_be_bytes())
}

fn file_name<PE: Pairing>(
    prefix: &str, depth: usize, sub_depth: Option<usize>,
) -> String {
    let suffix = if let Some(x) = sub_depth {
        format!("{:02}-{:02}.bin", x, depth)
    } else {
        format!("{:02}.bin", depth)
    };
    format!("{}-{}-{}", prefix, &type_hash::<PE>()[..6], suffix)
}

pub fn ptau_file_name<PE: Pairing>(depth: usize, mont: bool) -> String {
    let prefix = format!("power-tau{}", if mont { "-mont" } else { "" });
    file_name::<PE>(&prefix, depth, None)
}

pub fn amtp_file_name<PE: Pairing>(
    depth: usize, prove_depth: usize, coset: usize, mont: bool,
) -> String {
    let prefix = format!(
        "amt-prove-coset{}{}",
        coset,
        if mont { "-mont" } else { "" }
    );
    file_name::<PE>(&prefix, depth, Some(prove_depth))
}

pub fn amtp_verify_file_name<PE: Pairing>(
    depth: usize, verify_depth: usize, coset: usize,
) -> String {
    let prefix = format!("amt-verify-coset{}", coset);
    file_name::<PE>(&prefix, depth, Some(verify_depth))
}

#[inline]
pub fn bitreverse(n: usize, l: usize) -> usize {
    n.reverse_bits() >> (usize::BITS as usize - l)
}

/// Swap the lowest `lo` bits with the
/// next `hi` bits in a given number,
/// and clear the rest part.
#[inline]
pub fn swap_bits(n: usize, lo: usize, hi: usize) -> usize {
    let lowest = n & ((1 << lo) - 1);
    let next = (n >> lo) & ((1 << hi) - 1);

    (lowest << hi) | next
}

pub fn index_reverse<T: Sync>(input: &mut [T]) {
    let n = input.len();
    assert!(n.is_power_of_two());
    let depth = ark_std::log2(n) as usize;
    assert!(depth <= 32);

    cfg_into_iter!(0..input.len(), 1 << 14).for_each(|i| {
        let ri = bitreverse(i, depth);
        if i < ri {
            let x = &input[i] as *const T;
            let y = &input[ri] as *const T;
            unsafe {
                let x = x as *mut T;
                let y = y as *mut T;
                std::ptr::swap(x, y);
            }
        }
    })
}

pub fn change_matrix_direction<T: Clone>(
    input: &mut Vec<T>, log_current: usize, log_next: usize,
) {
    let n = input.len();
    assert_eq!(n, 1 << (log_current + log_next));
    if log_current == log_next {
        return transpose_square_matrix(input, log_current);
    }

    let mut output = input.clone();

    #[allow(clippy::needless_range_loop)]
    for i in 0..input.len() {
        let ri = swap_bits(i, log_current, log_next);
        output[ri] = input[i].clone();
    }
    std::mem::swap(input, &mut output);
}

fn transpose_square_matrix<T>(input: &mut [T], k: usize) {
    for i in 0..input.len() {
        let ri = swap_bits(i, k, k);
        if i < ri {
            input.swap(i, ri);
        }
    }
}
