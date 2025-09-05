use std::borrow::Cow;

use super::super::{DecResult, Decode, Encode, FixedLengthEncoded};

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct SnapshotId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct ModificationId(pub u64);

impl Encode for SnapshotId {
    fn encode(&self) -> Cow<[u8]> {
        self.0.encode()
    }
}

impl FixedLengthEncoded for SnapshotId {
    const LENGTH: usize = u64::LENGTH;
}

impl Decode for SnapshotId {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(SnapshotId(u64::decode(input)?.into_owned())))
    }
}

impl Encode for ModificationId {
    fn encode(&self) -> Cow<[u8]> {
        self.0.encode()
    }
}

impl FixedLengthEncoded for ModificationId {
    const LENGTH: usize = u64::LENGTH;
}

impl Decode for ModificationId {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        Ok(Cow::Owned(ModificationId(u64::decode(input)?.into_owned())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    fn sample_values() -> Vec<u64> {
        vec![
            0,
            1,
            42,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX - 1,
            u64::MAX,
        ]
    }

    #[test]
    fn snapshot_id_encode_decode_roundtrip() {
        for v in sample_values() {
            let id = SnapshotId(v);
            let enc = id.encode();
            assert_eq!(enc.len(), SnapshotId::LENGTH);

            let dec = SnapshotId::decode(&enc).expect("decode ok");
            assert_eq!(dec.into_owned(), id);
        }
    }

    #[test]
    fn modification_id_encode_decode_roundtrip() {
        for v in sample_values() {
            let id = ModificationId(v);
            let enc = id.encode();
            assert_eq!(enc.len(), ModificationId::LENGTH);

            let dec = ModificationId::decode(&enc).expect("decode ok");
            assert_eq!(dec.into_owned(), id);
        }
    }

    #[test]
    fn length_matches_u64() {
        assert_eq!(SnapshotId::LENGTH, <u64 as FixedLengthEncoded>::LENGTH);
        assert_eq!(ModificationId::LENGTH, <u64 as FixedLengthEncoded>::LENGTH);
    }

    #[test]
    fn ordering_and_equality_snapshot_id() {
        let a = SnapshotId(1);
        let b = SnapshotId(2);
        assert!(a < b);
        assert!(a != b);
        assert_eq!(a, SnapshotId(1));
    }

    #[test]
    fn ordering_and_equality_modification_id() {
        let a = ModificationId(10);
        let b = ModificationId(10);
        let c = ModificationId(11);
        assert_eq!(a, b);
        assert!(a < c);
        assert!(c > b);
    }

    #[test]
    fn decode_rejects_wrong_length_snapshot() {
        // Construct a buffer with wrong length by trimming/expanding.
        let id = SnapshotId(123);
        let enc = id.encode().into_owned();

        if !enc.is_empty() {
            let trimmed = &enc[..enc.len() - 1];
            assert!(SnapshotId::decode(trimmed).is_err());
        }

        let mut extended = enc.clone();
        extended.push(0);
        assert!(SnapshotId::decode(&extended).is_err());
    }

    #[test]
    fn decode_rejects_wrong_length_modification() {
        let id = ModificationId(456);
        let enc = id.encode().into_owned();

        if !enc.is_empty() {
            let trimmed = &enc[..enc.len() - 1];
            assert!(ModificationId::decode(trimmed).is_err());
        }

        let mut extended = enc.clone();
        extended.push(0);
        assert!(ModificationId::decode(&extended).is_err());
    }

    #[test]
    fn encode_returns_cow_slice() {
        let s = SnapshotId(7).encode();
        match s {
            Cow::Borrowed(_) | Cow::Owned(_) => {}
        }
        let m = ModificationId(8).encode();
        match m {
            Cow::Borrowed(_) | Cow::Owned(_) => {}
        }
    }

    #[test]
    fn decode_returns_owned_wrappers() {
        let s = SnapshotId(999);
        let enc = s.encode();
        let decoded = SnapshotId::decode(&enc).unwrap();
        assert!(matches!(decoded, Cow::Owned(_)));
        assert_eq!(decoded.into_owned(), s);

        let m = ModificationId(1000);
        let enc = m.encode();
        let decoded = ModificationId::decode(&enc).unwrap();
        assert!(matches!(decoded, Cow::Owned(_)));
        assert_eq!(decoded.into_owned(), m);
    }
}
