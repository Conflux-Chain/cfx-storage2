use std::collections::VecDeque;

use crate::{
    ec_algebra::{AffineRepr, Fr, G1Aff, Pairing, G1},
    proofs::AllProofs,
};

use super::AMTParams;
use ark_ec::{CurveGroup, VariableBaseMSM};
use tracing::instrument;

impl<PE: Pairing> AMTParams<PE>
where G1<PE>: VariableBaseMSM<MulBase = G1Aff<PE>>
{
    #[allow(unused)]
    pub(crate) fn commitment(&self, ri_data: &[Fr<PE>]) -> G1<PE> {
        VariableBaseMSM::msm(self.basis.as_slice(), ri_data).unwrap()
    }

    pub fn gen_commitment_tree(
        &self, ri_data: &[Fr<PE>], batch_size: usize,
    ) -> (G1Aff<PE>, Vec<Vec<G1Aff<PE>>>) {
        assert!(batch_size.is_power_of_two());
        assert!(batch_size <= self.len());

        let scalars = ri_data.chunks_exact(batch_size);
        let last_layer = self
            .basis
            .chunks_exact(batch_size)
            .zip(scalars)
            .map(|(base, scalar)| VariableBaseMSM::msm(base, scalar).unwrap())
            .collect::<Vec<G1<PE>>>();

        self.build_commitment_tree(&last_layer)
    }

    pub(crate) fn build_commitment_tree(
        &self, last_layer: &[G1<PE>],
    ) -> (G1Aff<PE>, Vec<Vec<G1Aff<PE>>>) {
        let mut answer = VecDeque::new();
        let mut last_layer = CurveGroup::normalize_batch(last_layer);
        while last_layer.len() > 1 {
            let next_layer = last_layer
                .chunks_exact(2)
                .map(|x| x[0] + x[1])
                .collect::<Vec<_>>();
            let mut to_push_layer = CurveGroup::normalize_batch(&next_layer);
            std::mem::swap(&mut last_layer, &mut to_push_layer);
            answer.push_front(to_push_layer);
        }
        (last_layer[0], answer.into())
    }

    pub fn gen_high_commitment(&self, ri_data: &[Fr<PE>]) -> G1<PE> {
        VariableBaseMSM::msm(&self.high_basis, ri_data).unwrap()
    }

    pub fn gen_prove_tree(
        &self, ri_data: &[Fr<PE>], batch_size: usize,
    ) -> Vec<Vec<G1Aff<PE>>> {
        assert!(batch_size.is_power_of_two());
        assert!(batch_size < self.len());
        assert_eq!(ri_data.len(), self.len());

        let last_depth = ark_std::log2(self.len() / batch_size) as usize;

        let mut answer = vec![];
        for depth in 1..=last_depth {
            let items = 1 << depth;
            let chunk_size = self.len() / items;
            let scalars = ri_data.chunks_exact(chunk_size);

            let quotients = self.quotients[depth - 1]
                .chunks_exact(chunk_size)
                .zip(scalars)
                .map(|(base, scalar)| {
                    VariableBaseMSM::msm(base, scalar).unwrap()
                })
                .collect::<Vec<G1<PE>>>();
            answer.push(CurveGroup::normalize_batch(quotients.as_slice()))
        }
        answer
    }

    #[instrument(skip_all, name = "gen_amt_proofs", level = 2)]
    pub fn gen_all_proofs(
        &self, ri_data: &[Fr<PE>],
    ) -> (G1<PE>, AllProofs<PE>) {
        let batch_size = self.len() / (1 << self.quotients.len());
        let proofs = self.gen_prove_tree(ri_data, batch_size);
        let (commitment, commitments) =
            self.gen_commitment_tree(ri_data, batch_size);
        let high_commitment = self.gen_high_commitment(ri_data).into_affine();
        let all_proofs = AllProofs {
            commitments,
            proofs,
            input_len: self.len(),
            batch_size,
            high_commitment,
        };
        (commitment.into_group(), all_proofs)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        random_scalars, AMT, G2PP, PE, TEST_LENGTH, TEST_LEVEL,
    };
    use crate::ec_algebra::{AffineRepr, ArkPairing, Fr};

    #[test]
    fn test_commitment_tree() {
        let ri_data = &random_scalars(TEST_LENGTH);
        for log_batch in 0..=TEST_LEVEL {
            assert_eq!(
                AMT.commitment(ri_data),
                AMT.gen_commitment_tree(ri_data, 1 << log_batch)
                    .0
                    .into_group()
            )
        }
    }

    fn check_proof_tree(ri_data: &[Fr<PE>], batch: usize) {
        let (_, commitments) = AMT.gen_commitment_tree(ri_data, batch);
        let proofs = AMT.gen_prove_tree(ri_data, batch);
        for d in 0..(TEST_LEVEL - ark_std::log2(batch) as usize) {
            assert_eq!(commitments[d].len(), 1 << (d + 1));
            assert_eq!(proofs[d].len(), 1 << (d + 1));
            assert_eq!(AMT.vanishes[d].len(), 1 << (d + 1));
            for i in 0..1usize << (d + 1) {
                assert_eq!(
                    PE::pairing(commitments[d][i], G2PP[0]),
                    PE::pairing(proofs[d][i], AMT.vanishes[d][i])
                );
            }
        }
    }

    #[test]
    fn test_proof_tree() {
        let ri_data = &random_scalars(TEST_LENGTH);
        for d in 1..TEST_LEVEL {
            check_proof_tree(ri_data.as_slice(), 1 << d);
        }
    }

    #[test]
    fn test_proof_verify() {
        let ri_data = &random_scalars(TEST_LENGTH);
        let commitment = AMT.commitment(ri_data);

        for log_batch in 0..TEST_LEVEL {
            let prove_depth = TEST_LEVEL - log_batch;
            let batch = 1 << log_batch;
            let all_proofs = AMT
                .reduce_prove_depth(prove_depth)
                .gen_all_proofs(ri_data)
                .1;
            for (index, data) in ri_data.chunks_exact(batch).enumerate() {
                let (proof, high_commitment) = all_proofs.get_proof(index);
                AMT.verify_proof(
                    &data,
                    index,
                    &proof,
                    high_commitment.into(),
                    commitment,
                )
                .unwrap();
            }
        }
    }
}
