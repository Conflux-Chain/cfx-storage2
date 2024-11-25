use super::super::{
    ec_algebra::{EvaluationDomain, Fr, Radix2EvaluationDomain, UniformRand, G1},
    PowerTau,
};
use super::*;
use once_cell::sync::Lazy;

pub const TEST_LEVEL: usize = 5;
pub const TEST_LENGTH: usize = 1 << TEST_LEVEL;

#[cfg(not(feature = "bls12-381"))]
pub type PE = ark_bn254::Bn254;
#[cfg(feature = "bls12-381")]
pub type PE = ark_bls12_381::Bls12_381;

pub type TestParams = AMTParams<PE>;

#[cfg(not(feature = "bls12-381"))]
pub static PP: Lazy<PowerTau<PE>> =
    Lazy::new(|| PowerTau::<PE>::from_dir_mont("./pp", TEST_LEVEL, true));
#[cfg(feature = "bls12-381")]
pub static PP: Lazy<PowerTau<PE>> =
    Lazy::new(|| PowerTau::<PE>::from_dir("./pp", TEST_LEVEL, true));

pub static G1PP: Lazy<Vec<G1<PE>>> =
    Lazy::new(|| PP.g1pp.iter().copied().map(G1::<PE>::from).collect());

pub static G2PP: Lazy<Vec<G2<PE>>> =
    Lazy::new(|| PP.g2pp.iter().copied().map(G2::<PE>::from).collect());

pub static AMT: Lazy<AMTParams<PE>> = Lazy::new(|| AMTParams::from_pp(PP.clone(), TEST_LEVEL));

pub static DOMAIN: Lazy<Radix2EvaluationDomain<Fr<PE>>> =
    Lazy::new(|| Radix2EvaluationDomain::new(TEST_LENGTH).unwrap());

pub static W: Lazy<Fr<PE>> = Lazy::new(|| DOMAIN.group_gen);

pub fn random_scalars(length: usize) -> Vec<Fr<PE>> {
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| Fr::<PE>::rand(&mut rng))
        .collect::<Vec<_>>()
}
