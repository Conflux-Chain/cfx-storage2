mod key;
mod node;

pub use key::AuthChangeKey;
pub use node::AuthChangeNode;
use static_assertions::const_assert;

pub const MAX_NODE_SIZE_LOG: usize = 3;
pub const MAX_NODE_SIZE: usize = 1 << MAX_NODE_SIZE_LOG;

const_assert!(MAX_NODE_SIZE <= 8 * std::mem::size_of::<u8>());

pub fn log2_floor(n: usize) -> usize {
    if n == 0 {
        panic!("log2_floor(n): n cannot be zero.")
    }

    (usize::BITS - 1 - n.leading_zeros()) as usize
}

pub fn log2_ceil(n: usize) -> usize {
    if n == 0 {
        return 0;
    }

    (usize::BITS - (n - 1).leading_zeros()) as usize
}

fn bit_ones(size: usize) -> u8 {
    1u8.overflowing_shl(size as u32).0.overflowing_sub(1).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log2_ceil_around_powers_of_two() {
        let power: u32 = 20;
        let val = 1usize << power;
        assert_eq!(
            log2_ceil(val - 1),
            power as usize,
            "Just below a power of two"
        );
        assert_eq!(log2_ceil(val), power as usize, "Exactly a power of two");
        assert_eq!(
            log2_ceil(val + 1),
            power as usize + 1,
            "Just above a power of two"
        );
    }

    #[test]
    fn test_log2_ceil_large_values_and_boundaries() {
        const BITS: u32 = usize::BITS;

        let half_max_plus_one = 1usize << (BITS - 1);
        assert_eq!(log2_ceil(half_max_plus_one), (BITS - 1) as usize);

        let just_over_half_max = half_max_plus_one + 1;
        assert_eq!(log2_ceil(just_over_half_max), BITS as usize);

        assert_eq!(log2_ceil(usize::MAX), BITS as usize);
    }

    #[test]
    #[should_panic(expected = "log2_floor(n): n cannot be zero.")]
    fn test_log2_floor_panics_on_zero() {
        log2_floor(0);
    }

    #[test]
    fn test_log2_floor_large_values() {
        const BITS: u32 = usize::BITS;
        let half_max_plus_one = 1usize << (BITS - 1);

        assert_eq!(log2_floor(half_max_plus_one - 1), (BITS - 2) as usize);
        assert_eq!(log2_floor(half_max_plus_one), (BITS - 1) as usize);
        assert_eq!(log2_floor(usize::MAX), (BITS - 1) as usize);
    }

    #[test]
    fn test_log2() {
        assert_eq!(log2_ceil(0), 0);
        assert_eq!(log2_ceil(1), 0);
        assert_eq!(log2_ceil(2), 1);
        assert_eq!(log2_ceil(3), 2);
        assert_eq!(log2_ceil(4), 2);
        for d in 3..=8 {
            for i in (1 << ((d - 1) + 1))..=(1 << d) {
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
}
