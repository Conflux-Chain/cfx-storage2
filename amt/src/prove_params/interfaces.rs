use ark_ec::pairing::Pairing;

use crate::{
    ec_algebra::{Fr, G1},
    proofs::AllProofs,
    AMTParams,
};

pub trait AMTProofs {
    type PE: Pairing;

    fn gen_amt_proofs(
        &self, ri_data: &[Fr<Self::PE>],
    ) -> (G1<Self::PE>, AllProofs<Self::PE>);
}

impl<PE: Pairing> AMTProofs for AMTParams<PE> {
    type PE = PE;

    fn gen_amt_proofs(
        &self, ri_data: &[Fr<Self::PE>],
    ) -> (G1<Self::PE>, AllProofs<Self::PE>) {
        self.gen_all_proofs(ri_data)
    }
}