#![allow(unused)]
// Re-export all the required components
pub use ark_ec::{pairing::Pairing, AdditiveGroup, AffineRepr, CurveGroup, VariableBaseMSM};
pub use ark_ff::{
    utils::k_adicity, BigInt, BigInteger, FftField, Field, One, PrimeField, UniformRand, Zero,
};
pub use ark_poly::{EvaluationDomain, Radix2EvaluationDomain};
pub use ark_serialize::{
    CanonicalDeserialize, CanonicalSerialize, Read, SerializationError, Write,
};

pub type PE = ark_bls12_381::Bls12_381;

pub type G1 = <PE as Pairing>::G1;
pub type G1Aff = <PE as Pairing>::G1Affine;
pub type G2 = <PE as Pairing>::G2;
pub type G2Aff = <PE as Pairing>::G2Affine;
pub type Fr = <PE as Pairing>::ScalarField;
pub type Fq = <PE as Pairing>::BaseField;
pub type FrInt = <Fr as PrimeField>::BigInt;
pub type FqInt = <Fq as PrimeField>::BigInt;
pub type Fq2 = <G2Aff as AffineRepr>::BaseField;
