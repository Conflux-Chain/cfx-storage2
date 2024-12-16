#[cfg(feature = "bls12-381")]
pub mod fast_serde_bls12_381;

#[cfg(not(feature = "bls12-381"))]
pub mod fast_serde_bn254;

mod generate;
mod serde;

#[cfg(test)]
pub mod tests;

pub use generate::CreateMode;

use super::ec_algebra::{G1Aff, G2Aff, G2};

use ark_ec::{pairing::Pairing, CurveGroup};

const SLOT_SIZE: usize = 6;
const SLOT_SIZE_MINUS_1: usize = SLOT_SIZE - 1;

pub struct AmtParams<PE: Pairing> {
    pub(super) basis: Vec<G1Aff<PE>>,
    pub(super) quotients: Vec<Vec<G1Aff<PE>>>,
    pub(super) vanishes: Vec<Vec<G2Aff<PE>>>,
    pub(super) g2: G2Aff<PE>,
    /// basis_power(i, j) = basis(i) * 2^{40 * (j + 1)}, j in 0..=4
    basis_power: Vec<[G1Aff<PE>; SLOT_SIZE_MINUS_1]>,
}

impl<PE: Pairing> AmtParams<PE> {
    pub fn new(
        basis: Vec<G1Aff<PE>>,
        quotients: Vec<Vec<G1Aff<PE>>>,
        vanishes: Vec<Vec<G2Aff<PE>>>,
        g2: G2<PE>,
        basis_power: Vec<[G1Aff<PE>; SLOT_SIZE_MINUS_1]>,
    ) -> Self {
        Self {
            basis,
            quotients,
            vanishes,
            g2: g2.into_affine(),
            basis_power,
        }
    }

    pub fn reduce_prove_depth(&self, depth: usize) -> Self {
        Self::new(
            self.basis.clone(),
            self.quotients[..depth].to_vec(),
            self.vanishes[..depth].to_vec(),
            self.g2.into(),
            self.basis_power[..depth].to_vec(),
        )
    }
}

impl<PE: Pairing> AmtParams<PE> {
    pub fn get_basis_power_at(&self, idx: usize) -> [G1Aff<PE>; SLOT_SIZE] {
        let mut basis_power: [G1Aff<PE>; SLOT_SIZE] = Default::default();
        basis_power[0] = self.basis[idx];
        basis_power[1..].copy_from_slice(&self.basis_power[idx]);
        // todo if cfg!(test)
        basis_power
    }
}

impl<PE: Pairing> PartialEq for AmtParams<PE> {
    fn eq(&self, other: &Self) -> bool {
        self.basis == other.basis
            && self.quotients == other.quotients
            && self.vanishes == other.vanishes
            && self.g2 == other.g2
    }
}

impl<PE: Pairing> Eq for AmtParams<PE> {}
