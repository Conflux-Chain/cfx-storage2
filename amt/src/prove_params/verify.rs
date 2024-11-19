use crate::{
    ec_algebra::{Fr, G1Aff, Pairing, G1},
    proofs::{AmtProofError, Proof},
    verify_params::verify_amt_proof,
};

use ark_ec::VariableBaseMSM;

use super::AMTParams;

impl<PE: Pairing> AMTParams<PE>
where G1<PE>: VariableBaseMSM<MulBase = G1Aff<PE>>
{
    pub fn verify_proof(
        &self, ri_data: &[Fr<PE>], batch_index: usize, proof: &Proof<PE>,
        high_commitment: G1<PE>, commitment: G1<PE>,
    ) -> Result<(), AmtProofError> {
        verify_amt_proof(
            &self.basis,
            &self.vanishes,
            ri_data,
            batch_index,
            proof,
            commitment,
            &self.g2,
            high_commitment,
            &self.high_g2,
            None,
        )
    }
}
