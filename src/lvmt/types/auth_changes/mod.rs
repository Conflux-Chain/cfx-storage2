mod key;
mod node;

pub use key::AuthChangeKey;
pub use node::AuthChangeNode;
use static_assertions::const_assert;

pub const MAX_NODE_SIZE_LOG: usize = 3;
pub const MAX_NODE_SIZE: usize = 1 << MAX_NODE_SIZE_LOG;

const_assert!(MAX_NODE_SIZE <= 8 * std::mem::size_of::<u8>());

pub fn log2_floor(n: usize) -> usize {
    63 - (n as u64).leading_zeros() as usize
}

pub fn log2_ceil(n: usize) -> usize {
    64 - (n as u64 - 1).leading_zeros() as usize
}

fn bit_ones(size: usize) -> u8 {
    1u8.overflowing_shl(size as u32).0.overflowing_sub(1).0
}

#[test]
fn test_log2() {
    assert_eq!(log2_ceil(1), 0);
    assert_eq!(log2_ceil(2), 1);
    assert_eq!(log2_ceil(3), 2);
    assert_eq!(log2_ceil(4), 2);
    for d in 3..=8 {
        for i in (1 << (d - 1) + 1)..=(1 << d) {
            assert_eq!(log2_ceil(i), d);
        }
    }
    assert_eq!(log2_floor(1), 0);
    assert_eq!(log2_floor(2), 1);
    assert_eq!(log2_floor(3), 1);
    assert_eq!(log2_floor(4), 2);
    assert_eq!(log2_floor(5), 2);
    assert_eq!(log2_floor(6), 2);
    assert_eq!(log2_floor(7), 2);
    for d in 3..=8 {
        for i in (1 << d)..(1 << (d + 1)) {
            assert_eq!(log2_floor(i), d);
        }
    }
}
