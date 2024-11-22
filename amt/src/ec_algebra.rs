// Re-export all the required components
// in Arkworks's repo (original Zexe).

// Since Zexe's repo doesn't have a
// stable implementation and could be
// refactored in the future,
// we import all the required objects in
// one place and all its usage for this
// repo should import from here.

pub use ark_ec::{
    pairing::Pairing as ArkPairing, AffineRepr, CurveGroup, Group,
    VariableBaseMSM,
};
pub use ark_ff::{
    utils::k_adicity, BigInt, BigInteger, FftField, Field, One, PrimeField,
    UniformRand, Zero,
};
pub use ark_poly::{EvaluationDomain, Radix2EvaluationDomain};
pub use ark_serialize::{
    CanonicalDeserialize, CanonicalSerialize, Read, SerializationError, Write,
};

pub type G1<PE> = <PE as ark_ec::pairing::Pairing>::G1;
pub type G1Aff<PE> = <PE as ark_ec::pairing::Pairing>::G1Affine;
pub type G2<PE> = <PE as ark_ec::pairing::Pairing>::G2;
pub type G2Aff<PE> = <PE as ark_ec::pairing::Pairing>::G2Affine;
pub type Fr<PE> = <PE as ark_ec::pairing::Pairing>::ScalarField;
pub type Fq<PE> = <PE as ark_ec::pairing::Pairing>::BaseField;
pub type FrInt<PE> = <Fr<PE> as PrimeField>::BigInt;
pub type FqInt<PE> = <Fq<PE> as PrimeField>::BigInt;
pub type Fq2<PE> = <G2Aff<PE> as AffineRepr>::BaseField;