// Re-export all the required components
// in Arkworks's repo (original Zexe).

// Since Zexe's repo doesn't have a
// stable implementation and could be
// refactored in the future,
// we import all the required objects in
// one place and all its usage for this
// repo should import from here.

#[cfg(test)]
pub use ark_ec::VariableBaseMSM;
#[cfg(test)]
pub use ark_ff::{Field, One};

pub use ark_ec::{pairing::Pairing, AffineRepr, CurveGroup};
pub use ark_ff::{utils::k_adicity, BigInt, BigInteger, PrimeField, UniformRand, Zero};
pub use ark_poly::{EvaluationDomain, Radix2EvaluationDomain};
pub use ark_serialize::{CanonicalDeserialize, CanonicalSerialize, Read, Write};

pub type G1<PE> = <PE as ark_ec::pairing::Pairing>::G1;
pub type G1Aff<PE> = <PE as ark_ec::pairing::Pairing>::G1Affine;
pub type G2<PE> = <PE as ark_ec::pairing::Pairing>::G2;
pub type G2Aff<PE> = <PE as ark_ec::pairing::Pairing>::G2Affine;
pub type Fr<PE> = <PE as ark_ec::pairing::Pairing>::ScalarField;
pub type Fq<PE> = <PE as ark_ec::pairing::Pairing>::BaseField;
pub type FrInt<PE> = <Fr<PE> as PrimeField>::BigInt;
pub type FqInt<PE> = <Fq<PE> as PrimeField>::BigInt;
pub type Fq2<PE> = <G2Aff<PE> as AffineRepr>::BaseField;
