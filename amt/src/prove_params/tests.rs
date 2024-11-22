use super::*;
use crate::{
    ec_algebra::{EvaluationDomain, Field, Fr, Radix2EvaluationDomain, UniformRand, Zero, G1},
    PowerTau,
};
use once_cell::sync::Lazy;

pub const TEST_LEVEL: usize = 4;
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
    Lazy::new(|| PP.g1pp.iter().copied().map(|x| G1::<PE>::from(x)).collect());

pub static G2PP: Lazy<Vec<G2<PE>>> =
    Lazy::new(|| PP.g2pp.iter().copied().map(|x| G2::<PE>::from(x)).collect());

pub static AMT: Lazy<AMTParams<PE>> = Lazy::new(|| AMTParams::from_pp(PP.clone(), TEST_LEVEL, 0));

pub static DOMAIN: Lazy<Radix2EvaluationDomain<Fr<PE>>> =
    Lazy::new(|| Radix2EvaluationDomain::new(TEST_LENGTH).unwrap());

pub static W: Lazy<Fr<PE>> = Lazy::new(|| DOMAIN.group_gen);

pub fn random_scalars(length: usize) -> Vec<Fr<PE>> {
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| Fr::<PE>::rand(&mut rng))
        .collect::<Vec<_>>()
}

#[test]
fn test_coset_consistency() {
    let input = random_scalars(TEST_LENGTH);

    let fft_domain = Radix2EvaluationDomain::<Fr<PE>>::new(TEST_LENGTH).unwrap();
    let fft2_domain = Radix2EvaluationDomain::<Fr<PE>>::new(TEST_LENGTH * 2).unwrap();

    let mut coeff = fft_domain.ifft(&input);
    let mut coset_coeff = coeff.clone();
    coeff.extend(vec![Fr::<PE>::zero(); TEST_LENGTH]);

    let answer = fft2_domain.fft(&coeff[..]);

    for i in 0..TEST_LENGTH {
        assert_eq!(answer[i * 2], input[i]);
    }

    coset_coeff.iter_mut().enumerate().for_each(|(idx, x)| {
        *x *= &fft2_domain.group_gen.pow(&[idx as u64]);
    });
    let coset = fft_domain.fft(&coset_coeff);
    for i in 0..TEST_LENGTH {
        assert_eq!(answer[i * 2 + 1], coset[i]);
    }
}
