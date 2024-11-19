use std::{fs::File, io::BufReader, path::Path};

use crate::{
    amtp_verify_file_name,
    deferred_verification::{DeferredVerifier, PairingTask},
    error, AMTParams,
};

use crate::ec_algebra::{Fr, G1Aff, G2Aff, Pairing, G1};

use ark_ff::Zero;
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};
use tracing::{debug, info, instrument};

use crate::proofs::{AmtProofError, Proof};

use ark_ec::{AffineRepr, VariableBaseMSM};

#[cfg(not(feature = "bls12-381"))]
use ark_bn254::Bn254;

#[derive(CanonicalSerialize, CanonicalDeserialize)]
pub struct AMTVerifyParams<PE: Pairing> {
    pub basis: Vec<G1Aff<PE>>,
    pub vanishes: Vec<Vec<G2Aff<PE>>>,
    pub g2: G2Aff<PE>,
    pub high_g2: G2Aff<PE>,
}

#[cfg(not(feature = "bls12-381"))]
impl AMTVerifyParams<Bn254> {
    pub fn from_dir_mont(
        dir: impl AsRef<Path>, depth: usize, verify_depth: usize, coset: usize,
    ) -> Self {
        Self::from_dir_inner(&dir, depth, verify_depth, coset, || {
            AMTParams::<Bn254>::from_dir_mont(
                &dir,
                depth,
                verify_depth,
                coset,
                false,
                None,
            )
        })
    }
}

impl<PE: Pairing> AMTVerifyParams<PE> {
    pub fn from_dir(
        dir: impl AsRef<Path>, expected_depth: usize, verify_depth: usize,
        coset: usize,
    ) -> Self {
        Self::from_dir_inner(&dir, expected_depth, verify_depth, coset, || {
            AMTParams::<PE>::from_dir(
                &dir,
                expected_depth,
                verify_depth,
                coset,
                false,
                None,
            )
        })
    }

    #[instrument(skip_all, name = "load_amt_verify_params", level = 2, parent = None, fields(depth=expected_depth, verify_depth, coset))]
    fn from_dir_inner(
        dir: impl AsRef<Path>, expected_depth: usize, verify_depth: usize,
        coset: usize, make_prover_params: impl Fn() -> AMTParams<PE>,
    ) -> Self {
        debug!(
            depth = expected_depth,
            verify_depth, coset, "Load AMT verify params"
        );

        let file_name =
            amtp_verify_file_name::<PE>(expected_depth, verify_depth, coset);
        let path = dir.as_ref().join(file_name);

        match Self::load_cached(&path) {
            Ok(loaded) => {
                return loaded;
            }
            Err(e) => {
                info!(?path, error = ?e, "Fail to load AMT verify params, recover from AMT params");
            }
        }

        let amt_params = make_prover_params();
        let verify_params = Self {
            basis: amt_params.basis.clone(),
            vanishes: amt_params.vanishes[0..verify_depth].to_vec(),
            g2: amt_params.g2,
            high_g2: amt_params.high_g2,
        };

        let buffer = File::create(&path).unwrap();

        info!(file = ?path, "Save recovered AMT verify params");
        verify_params.serialize_uncompressed(&buffer).unwrap();

        verify_params
    }

    fn load_cached(file: impl AsRef<Path>) -> Result<Self, error::Error> {
        let mut buffer = BufReader::new(File::open(file)?);
        Ok(CanonicalDeserialize::deserialize_uncompressed_unchecked(
            &mut buffer,
        )?)
    }
}

impl<PE: Pairing> AMTVerifyParams<PE>
where G1<PE>: VariableBaseMSM<MulBase = G1Aff<PE>>
{
    pub fn verify_proof(
        &self, ri_data: &[Fr<PE>], batch_index: usize, proof: &Proof<PE>,
        high_commitment: G1<PE>, commitment: G1<PE>,
        deferred_verifier: Option<DeferredVerifier<PE>>,
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
            deferred_verifier,
        )
    }
}

#[allow(clippy::too_many_arguments)]
pub fn verify_amt_proof<PE: Pairing>(
    basis: &[G1Aff<PE>], vanishes: &[Vec<G2Aff<PE>>], ri_data: &[Fr<PE>],
    batch_index: usize, proof: &Proof<PE>, commitment: G1<PE>, g2: &G2Aff<PE>,
    high_commitment: G1<PE>, high_g2: &G2Aff<PE>,
    deferred_verifier: Option<DeferredVerifier<PE>>,
) -> Result<(), AmtProofError>
where
    G1<PE>: VariableBaseMSM<MulBase = G1Aff<PE>>,
{
    use AmtProofError::*;

    let mut task_collector = deferred_verifier.is_some().then_some(vec![]);

    let proof_depth = proof.len();
    let num_batch = 1 << proof_depth;
    let batch = basis.len() / num_batch;

    if batch != ri_data.len() {
        return Err(UnexpectedDataLength);
    }
    if batch_index >= num_batch {
        return Err(IncorrectPosition);
    }
    assert!(batch_index < num_batch);

    let mut acc_commitment = G1::<PE>::zero();
    for (d, (commitment, quotient)) in proof.iter().enumerate().rev() {
        let vanish_index = batch_index >> (proof_depth - 1 - d);
        let vanish = vanishes[d][vanish_index ^ 1];
        pairing_check::<PE>(
            &mut task_collector,
            commitment.into_group(),
            *g2,
            quotient.into_group(),
            vanish,
            KzgError(d),
        )?;
        acc_commitment += commitment;
    }


    let self_commitment: G1<PE> = VariableBaseMSM::msm(
        &basis[batch_index * batch..(batch_index + 1) * batch],
        ri_data,
    )
    .unwrap();
    if acc_commitment + self_commitment != commitment {
        return Err(InconsistentCommitment);
    }

    pairing_check::<PE>(
        &mut task_collector,
        high_commitment,
        *g2,
        commitment,
        *high_g2,
        FailedLowDegreeTest,
    )?;

    if let Some(verifier) = deferred_verifier {
        verifier.record_pairing(task_collector.unwrap());
    }

    Ok(())
}

fn pairing_check<PE: Pairing>(
    task_collector: &mut Option<Vec<PairingTask<PE>>>, a: G1<PE>, b: G2Aff<PE>,
    c: G1<PE>, d: G2Aff<PE>, error: AmtProofError,
) -> Result<(), AmtProofError> {
    if let Some(collector) = task_collector {
        collector.push((a, b, c, d, error));
        Ok(())
    } else if PE::pairing(a, b) != PE::pairing(c, d) {
        Err(error)
    } else {
        Ok(())
    }
}
