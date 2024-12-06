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

fn file_name<PE: Pairing>(prefix: &str, depth: usize, sub_depth: Option<usize>) -> String {
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

pub fn amtp_file_name<PE: Pairing>(depth: usize, prove_depth: usize, mont: bool) -> String {
    let prefix = format!("amt-prove{}", if mont { "-mont" } else { "" });
    file_name::<PE>(&prefix, depth, Some(prove_depth))
}

#[inline]
pub fn bitreverse(n: usize, l: usize) -> usize {
    n.reverse_bits() >> (usize::BITS as usize - l)
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
