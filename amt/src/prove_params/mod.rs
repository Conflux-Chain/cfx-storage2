#[cfg(not(feature = "bls12-381"))]
pub mod fast_serde_bn254;
mod generate;
mod prove;
mod serde;
mod verify;

#[cfg(test)]
pub mod tests;

use crate::ec_algebra::{G1Aff, G2Aff, G2};

use ark_ec::{pairing::Pairing, CurveGroup};

pub struct AMTParams<PE: Pairing> {
    pub(super) basis: Vec<G1Aff<PE>>,
    pub(super) quotients: Vec<Vec<G1Aff<PE>>>,
    pub(super) vanishes: Vec<Vec<G2Aff<PE>>>,
    pub(super) g2: G2Aff<PE>,
}

impl<PE: Pairing> AMTParams<PE> {
    pub fn new(
        basis: Vec<G1Aff<PE>>,
        quotients: Vec<Vec<G1Aff<PE>>>,
        vanishes: Vec<Vec<G2Aff<PE>>>,
        g2: G2<PE>,
    ) -> Self {
        Self {
            basis,
            quotients,
            vanishes,
            g2: g2.into_affine(),
        }
    }

    pub fn reduce_prove_depth(&self, depth: usize) -> Self {
        Self::new(
            self.basis.clone(),
            self.quotients[..depth].to_vec(),
            self.vanishes[..depth].to_vec(),
            self.g2.into(),
        )
    }
}

impl<PE: Pairing> PartialEq for AMTParams<PE> {
    fn eq(&self, other: &Self) -> bool {
        self.basis == other.basis
            && self.quotients == other.quotients
            && self.vanishes == other.vanishes
            && self.g2 == other.g2
    }
}

impl<PE: Pairing> Eq for AMTParams<PE> {}
