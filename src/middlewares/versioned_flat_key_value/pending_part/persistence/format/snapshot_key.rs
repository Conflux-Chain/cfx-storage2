use std::borrow::Cow;

use super::super::{subkey_not_support, DecResult, Decode, Encode, SeekKey};

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct SnapshotKey(pub u64);

impl SnapshotKey {
    /// Generates the `SeekKey` to start an iteration from the entry at a
    /// specific `height`.
    ///
    /// An iterator starting from this key will first yield the entry for `height`
    /// (if it exists) and then proceed to subsequent heights (`height + 1`, etc.).
    ///
    /// **Note**: This only provides a starting point. If the caller is only
    /// interested in the single entry at the specified `height`, they are
    /// responsible for stopping the iteration after the first item.
    pub fn seek_key_for_height(height: u64) -> SeekKey<Self> {
        let start_key = SnapshotKey(height);

        SeekKey { key: start_key }
    }
}

impl Encode for SnapshotKey {
    fn encode(&self) -> Cow<[u8]> {
        self.0.encode()
    }
}

subkey_not_support!(SnapshotKey);

impl Decode for SnapshotKey {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(SnapshotKey(u64::decode(input)?.into_owned())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn encode_u64_big_endian_compat() {
        let k = SnapshotKey(0x0102030405060708);
        let enc = k.encode();
        // Expect big-endian bytes of 0x0102030405060708
        assert_eq!(
            enc.as_ref(),
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn encode_matches_u64_encode() {
        let val = 123456789u64;
        let k = SnapshotKey(val);
        assert_eq!(k.encode().as_ref(), val.encode().as_ref());
    }

    #[test]
    fn decode_round_trip() {
        let values = [
            0u64,
            1,
            42,
            u32::MAX as u64,
            u64::from(u32::MAX) + 1,
            u64::MAX - 1,
            u64::MAX,
        ];

        for &v in &values {
            let k = SnapshotKey(v);
            let enc = k.encode();
            let dec = SnapshotKey::decode(&enc).unwrap();
            assert_eq!(dec, Cow::Owned(k));
            assert_eq!(dec.into_owned(), k);
        }
    }

    #[test]
    fn decode_invalid_length_fails() {
        // Assuming u64::decode rejects non-8-byte inputs.
        let too_short = [0u8; 7];
        let too_long = [0u8; 9];

        assert!(SnapshotKey::decode(&too_short).is_err());
        assert!(SnapshotKey::decode(&too_long).is_err());
    }

    #[test]
    fn ord_and_partial_ord_match_inner() {
        let a = SnapshotKey(10);
        let b = SnapshotKey(20);

        assert!(a < b);
        assert!(a <= b);
        assert!(b > a);
        assert!(b >= a);
        assert_eq!(a.partial_cmp(&b), Some(core::cmp::Ordering::Less));
        assert_eq!(b.partial_cmp(&a), Some(core::cmp::Ordering::Greater));
        assert_eq!(a.cmp(&a), core::cmp::Ordering::Equal);
    }

    #[test]
    fn eq_and_ne() {
        let a = SnapshotKey(7);
        let b = SnapshotKey(7);
        let c = SnapshotKey(8);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn debug_fmt_contains_value() {
        let k = SnapshotKey(123);
        let s = format!("{:?}", k);
        assert!(s.contains("SnapshotKey"));
        assert!(s.contains("123"));
    }

    #[allow(clippy::clone_on_copy)]
    #[test]
    fn clone_and_copy() {
        let a = SnapshotKey(5);
        let b = a; // Copy
        let c = a.clone();
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn seek_key_starts_at_exact_height() {
        // This test only ensures the starting key; actual iteration is tested elsewhere.
        let start = SnapshotKey::seek_key_for_height(999);
        assert_eq!(start.key.0, 999);
    }
}
