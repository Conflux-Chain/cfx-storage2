use self::generate::CreateMode;

use super::super::{
    ec_algebra::{EvaluationDomain, Fr, Radix2EvaluationDomain, G1},
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

pub type TestParams = AmtParams<PE>;

pub static PP: Lazy<PowerTau<PE>> =
    Lazy::new(|| PowerTau::<PE>::from_dir_mont("./pp", TEST_LEVEL, true));

pub static G1PP: Lazy<Vec<G1<PE>>> =
    Lazy::new(|| PP.g1pp.iter().copied().map(G1::<PE>::from).collect());

pub static G2PP: Lazy<Vec<G2<PE>>> =
    Lazy::new(|| PP.g2pp.iter().copied().map(G2::<PE>::from).collect());

pub static AMT: Lazy<AmtParams<PE>> = Lazy::new(|| {
    AmtParams::from_dir_mont(
        "./pp",
        TEST_LEVEL,
        TEST_LEVEL,
        CreateMode::AmtOnly,
        Some(&PP),
    )
});

pub static DOMAIN: Lazy<Radix2EvaluationDomain<Fr<PE>>> =
    Lazy::new(|| Radix2EvaluationDomain::new(TEST_LENGTH).unwrap());

pub static W: Lazy<Fr<PE>> = Lazy::new(|| DOMAIN.group_gen);
