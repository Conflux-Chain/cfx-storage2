use std::{
    borrow::Cow,
    ops::{Add, AddAssign},
};

use ethereum_types::H256;

use crate::{
    backends::serde::{Decode, Encode},
    errors::{DecResult, DecodeError},
    utils::hash::blake2s,
};

use super::super::crypto::{CanonicalDeserialize, CanonicalSerialize, CurveGroup, G1Aff, G1};

#[derive(Clone, Copy, Debug)]
pub enum CurvePoint {
    Projective(G1),
    Affine(G1Aff),
}

impl PartialEq for CurvePoint {
    fn eq(&self, other: &Self) -> bool {
        self.affine() == other.affine()
    }
}

impl Eq for CurvePoint {}

impl Default for CurvePoint {
    fn default() -> Self {
        Self::Affine(Default::default())
    }
}

impl Add<G1> for CurvePoint {
    type Output = Self;

    fn add(self, rhs: G1) -> Self::Output {
        use CurvePoint::*;
        match self {
            Projective(lhs) => Projective(lhs + rhs),
            Affine(lhs) => Projective(rhs + lhs),
        }
    }
}

impl AddAssign<G1> for CurvePoint {
    fn add_assign(&mut self, rhs: G1) {
        use CurvePoint::*;
        match self {
            Projective(lhs) => lhs.add_assign(rhs),
            Affine(_) => {
                *self = *self + rhs;
            }
        }
    }
}

impl CurvePoint {
    pub fn hash(&self) -> H256 {
        let mut writer = vec![];
        self.affine().serialize_uncompressed(&mut writer).unwrap();
        blake2s(&writer)
    }

    pub fn affine(&self) -> Cow<G1Aff> {
        match &self {
            CurvePoint::Projective(proj) => Cow::Owned(proj.into_affine()),
            CurvePoint::Affine(affine) => Cow::Borrowed(affine),
        }
    }
}

pub fn batch_normalize<'a>(
    curve_point_iter_mut: impl Iterator<Item = &'a mut CurvePoint> + ExactSizeIterator,
) {
    let mut pointers = Vec::with_capacity(curve_point_iter_mut.len());
    let mut proj_points = Vec::with_capacity(curve_point_iter_mut.len());

    for curve_point in curve_point_iter_mut {
        if let CurvePoint::Projective(proj_point) = curve_point {
            proj_points.push(*proj_point);
            pointers.push(curve_point);
        }
    }

    let affine_points = G1::normalize_batch(&proj_points);
    pointers
        .into_iter()
        .zip(affine_points)
        .for_each(|(pointer, affine)| *pointer = CurvePoint::Affine(affine))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CurvePointWithVersion {
    pub(in crate::lvmt) version: u64,
    pub(in crate::lvmt) point: CurvePoint,
}

impl Encode for CurvePointWithVersion {
    fn encode(&self) -> Cow<[u8]> {
        let mut writer = self.version.to_be_bytes()[3..].to_vec();
        self.point
            .affine()
            .serialize_uncompressed(&mut writer)
            .unwrap();
        Cow::Owned(writer)
    }
}

impl Decode for CurvePointWithVersion {
    fn decode(input: &[u8]) -> DecResult<Cow<Self>> {
        if input.len() < 5 {
            return Err(DecodeError::IncorrectLength);
        }
        let (input_version, input_point) = input.split_at(5);
        let mut raw_version = [0u8; 8];
        raw_version[3..].copy_from_slice(input_version);
        let version = u64::from_be_bytes(raw_version);

        let affine_point = G1Aff::deserialize_uncompressed_unchecked(input_point)?;

        Ok(Cow::Owned(Self {
            version,
            point: CurvePoint::Affine(affine_point),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lvmt::crypto::Fq;
    use crate::lvmt::types::test_utils::{self, version_strategy};
    use ark_ec::AffineRepr;
    use ark_ff::{BigInt, BigInteger, PrimeField};
    use proptest::array::uniform;
    use proptest::collection::vec;
    use proptest::prelude::*;

    fn fq_strategy() -> impl Strategy<Value = Fq> {
        const NUM_LIMBS: usize = <<Fq as PrimeField>::BigInt as BigInteger>::NUM_LIMBS;
        uniform::<_, NUM_LIMBS>(0u64..=255).prop_filter_map("Fq out of range", |raw| {
            let big_int = BigInt(raw);
            Fq::from_bigint(big_int)
        })
    }
    fn g1aff_strategy() -> impl Strategy<Value = G1Aff> {
        (fq_strategy(), any::<bool>()).prop_filter_map("not on curve", |(x, greatest)| {
            G1Aff::get_point_from_x_unchecked(x, greatest).map(|p| p.mul_by_cofactor())
        })
    }

    fn g1_strategy() -> impl Strategy<Value = G1> {
        prop_oneof![
            g1aff_strategy().prop_map(|x| x.into()),
            (g1aff_strategy(), g1aff_strategy()).prop_map(|(x, y)| x + y)
        ]
    }

    impl Arbitrary for CurvePoint {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                g1aff_strategy().prop_map(CurvePoint::Affine),
                g1_strategy().prop_map(CurvePoint::Projective),
            ]
            .boxed()
        }
    }

    impl Arbitrary for CurvePointWithVersion {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            (any::<CurvePoint>(), version_strategy())
                .prop_map(|(point, version)| CurvePointWithVersion { point, version })
                .boxed()
        }
    }

    proptest! {
        #[test]
        fn test_add(a in any::<CurvePoint>(), b in g1_strategy()) {
            let c = a + b;
            let d = c + (-b);
            prop_assert_eq!(a, d);

            let mut e = a;
            e += b;
            e += -b;
            prop_assert_eq!(a, e);
        }

        #[test]
        fn test_serde(point in any::<CurvePointWithVersion>()) {
            test_utils::test_serde(point)
        }

        #[test]
        fn test_normalize(input in vec(any::<CurvePoint>(), 0..100)) {
            let mut output = input.clone();
            batch_normalize(output.iter_mut());

            let all_affine = output.iter().all(|x|matches!(x, CurvePoint::Affine(_)));
            prop_assert!(all_affine);

            let all_equal = input.into_iter().zip(output.into_iter()).all(|(a, b)| a.hash() == b.hash() && a == b);
            prop_assert!(all_equal);
        }
    }
}
