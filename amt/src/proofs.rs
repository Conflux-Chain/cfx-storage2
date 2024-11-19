use std::ops::Deref;

use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};

use crate::ec_algebra::{G1Aff, Pairing};

#[derive(CanonicalSerialize, CanonicalDeserialize, Debug)]
pub struct AllProofs<PE: Pairing> {
    pub(crate) commitments: Vec<Vec<G1Aff<PE>>>,
    pub(crate) proofs: Vec<Vec<G1Aff<PE>>>,
    pub(crate) input_len: usize,
    pub(crate) batch_size: usize,
    pub(crate) high_commitment: G1Aff<PE>,
}

impl<PE: Pairing> AllProofs<PE> {
    pub fn get_proof(&self, reversed_index: usize) -> (Proof<PE>, G1Aff<PE>) {
        assert!(reversed_index * self.batch_size < self.input_len);
        let index_bits = self.commitments.len();

        let mut answer = vec![];

        for d in 0..index_bits {
            let lv_index = reversed_index >> (index_bits - 1 - d);
            let commitment = self.commitments[d][lv_index ^ 1];
            let proof = self.proofs[d][lv_index ^ 1];
            answer.push((commitment, proof));
        }
        (Proof(answer), self.high_commitment)
    }
}

#[derive(Debug, CanonicalSerialize, CanonicalDeserialize, Clone)]
pub struct Proof<PE: Pairing>(Vec<(G1Aff<PE>, G1Aff<PE>)>);

impl<PE: Pairing> PartialEq for Proof<PE> {
    fn eq(&self, other: &Self) -> bool { self.0 == other.0 }
}

impl<PE: Pairing> Deref for Proof<PE> {
    type Target = Vec<(G1Aff<PE>, G1Aff<PE>)>;

    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AmtProofError {
    UnexpectedDataLength,
    IncorrectPosition,
    KzgError(usize),
    InconsistentCommitment,
    FailedLowDegreeTest,
}
