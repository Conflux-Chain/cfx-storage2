use ark_bn254::Bn254;
use pairing_ce::bn256::Bn256;
pub type PowerTau = amt::PowerTau<Bn254>;
pub type PePpot = Bn256;
pub type PeArk = Bn254;

use std::marker::PhantomData;

use ark_ec::AffineRepr;
use ff::PrimeField;
use pairing_ce::CurveAffine;

use super::Adapter;

mod ppot {
    pub use pairing_ce::bn256::{Fq, Fq2, FqRepr, Fr, FrRepr, G1Affine, G2Affine};
}

mod ark {
    pub use ark_ff::BigInt;
    use ark_ff::MontBackend;

    pub use ark_bn254::{Fq, Fq2, Fr, G1Affine, G2Affine};

    pub use ark_ff::fields::Fp;

    pub type FrParameters = MontBackend<ark_bn254::FrConfig, 4>;
    pub type FqParameters = MontBackend<ark_bn254::FqConfig, 4>;

    pub type FqRepr = ark_ff::BigInt<4>;
    pub type FrRepr = ark_ff::BigInt<4>;
}

impl Adapter for ppot::FqRepr {
    type Output = ark::FqRepr;

    fn adapt(self) -> Self::Output {
        ark::BigInt(self.0)
    }
}

impl Adapter for ppot::FrRepr {
    type Output = ark::FrRepr;

    fn adapt(self) -> Self::Output {
        ark::BigInt(self.0)
    }
}

impl Adapter for ppot::Fq {
    type Output = ark::Fq;

    fn adapt(self) -> Self::Output {
        ark::Fp::<ark::FqParameters, 4>(self.into_raw_repr().adapt(), PhantomData)
    }
}

impl Adapter for ppot::Fr {
    type Output = ark::Fr;

    fn adapt(self) -> Self::Output {
        ark::Fp::<ark::FrParameters, 4>(self.into_raw_repr().adapt(), PhantomData)
    }
}

impl Adapter for ppot::Fq2 {
    type Output = ark::Fq2;

    fn adapt(self) -> Self::Output {
        ark::Fq2::new(self.c0.adapt(), self.c1.adapt())
    }
}

impl Adapter for ppot::G1Affine {
    type Output = ark::G1Affine;

    fn adapt(self) -> Self::Output {
        if self.is_zero() {
            ark::G1Affine::zero()
        } else {
            ark::G1Affine::new(self.get_x().adapt(), self.get_y().adapt())
        }
    }
}

impl Adapter for ppot::G2Affine {
    type Output = ark::G2Affine;

    fn adapt(self) -> Self::Output {
        if self.is_zero() {
            ark::G2Affine::zero()
        } else {
            ark::G2Affine::new(self.get_x().adapt(), self.get_y().adapt())
        }
    }
}
