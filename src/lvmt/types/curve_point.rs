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
    mut curve_point_iter_mut: impl Iterator<Item = &'a mut CurvePoint> + ExactSizeIterator,
) {
    let mut pointers = Vec::with_capacity(curve_point_iter_mut.len());
    let mut proj_points = Vec::with_capacity(curve_point_iter_mut.len());

    while let Some(curve_point) = curve_point_iter_mut.next() {
        if let CurvePoint::Projective(proj_point) = curve_point {
            proj_points.push(*proj_point);
            pointers.push(curve_point);
        }
    }

    let affine_points = G1::normalize_batch(&proj_points);
    pointers
        .into_iter()
        .zip(affine_points.into_iter())
        .for_each(|(pointer, affine)| *pointer = CurvePoint::Affine(affine))
}

#[derive(Clone, Debug, Default)]
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
