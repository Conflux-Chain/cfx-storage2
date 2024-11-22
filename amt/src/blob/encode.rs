use ark_std::cfg_chunks_mut;
use std::path::Path;

#[cfg(not(feature = "bls12-381"))]
use ark_bn254::Bn254;
use ark_ec::{pairing::Pairing, CurveGroup};
use ark_poly::{EvaluationDomain, Radix2EvaluationDomain};
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};
#[cfg(feature = "parallel")]
use rayon::prelude::*;

use crate::{
    deferred_verification::DeferredVerifier,
    ec_algebra::{FftField, Field, Fr, G1Aff, G1},
    proofs::{AllProofs, AmtProofError, Proof},
    prove_params::AMTProofs,
    utils::{bitreverse, change_matrix_direction, index_reverse},
    AMTParams, AMTVerifyParams, PowerTau,
};

pub struct EncoderParams<
    PE: Pairing,
    const COSET_N: usize,
    const LOG_COL: usize,
    const LOG_ROW: usize,
> {
    pub amt_list: [AMTParams<PE>; COSET_N],
}

impl<
        PE: Pairing,
        const COSET_N: usize,
        const LOG_COL: usize,
        const LOG_ROW: usize,
    > EncoderParams<PE, COSET_N, LOG_COL, LOG_ROW>
where AMTParams<PE>: AMTProofs<PE = PE>
{
    pub fn new(amt_list: [AMTParams<PE>; COSET_N]) -> Self {
        Self::assert_validity();
        Self { amt_list }
    }

    pub fn from_dir(
        dir: impl AsRef<Path> + Clone, create_mode: bool,
        pp: Option<&PowerTau<PE>>,
    ) -> Self {
        Self::from_builder(|coset| {
            AMTParams::from_dir(
                dir.clone(),
                LOG_COL + LOG_ROW,
                LOG_ROW,
                coset,
                create_mode,
                pp,
            )
        })
    }

    pub fn from_builder<F: Fn(usize) -> AMTParams<PE>>(f: F) -> Self {
        Self::assert_validity();

        let mut amt_list = vec![];
        for coset in 0..COSET_N {
            let amt = f(coset);
            amt_list.push(amt);
        }

        let amt_list = match amt_list.try_into() {
            Ok(x) => x,
            Err(_) => unreachable!(),
        };

        Self { amt_list }
    }

    const fn assert_validity() {
        assert!(
            (1 << (LOG_COL + LOG_ROW)) * COSET_N
                <= 1 << <Fr<PE> as FftField>::TWO_ADICITY as usize
        );
    }

    pub const fn len() -> usize { 1 << (LOG_COL + LOG_ROW) }

    pub fn process_blob(
        &self, raw_blob: &[Fr<PE>],
    ) -> [HalfBlob<PE, LOG_COL, LOG_ROW>; COSET_N] {
        assert_eq!(Self::len(), raw_blob.len());

        let mut points = raw_blob.to_vec();
        change_matrix_direction(&mut points, LOG_COL, LOG_ROW);

        let mut blobs = vec![];
        for (idx, amt) in self.amt_list.iter().enumerate() {
            blobs.push(HalfBlob::<PE, LOG_COL, LOG_ROW>::generate(
                to_coset_blob::<PE>(&points, idx),
                amt,
            ))
        }

        blobs.try_into().unwrap()
    }
}

#[cfg(not(feature = "bls12-381"))]
impl<const COSET_N: usize, const LOG_COL: usize, const LOG_ROW: usize>
    EncoderParams<Bn254, COSET_N, LOG_COL, LOG_ROW>
{
    #[instrument(skip_all, level = 3)]
    pub fn from_dir_mont(
        dir: impl AsRef<Path> + Clone, create_mode: bool,
        pp: Option<&PowerTau<Bn254>>,
    ) -> Self {
        info!("Load AMT params");

        Self::from_builder(|coset| {
            AMTParams::from_dir_mont(
                dir.clone(),
                LOG_COL + LOG_ROW,
                LOG_ROW,
                coset,
                create_mode,
                pp,
            )
        })
    }
}

pub fn to_coset_blob<PE: Pairing>(
    data: &[Fr<PE>], coset: usize,
) -> Vec<Fr<PE>> {
    if coset == 0 {
        return data.to_vec();
    }

    let fft_domain = Radix2EvaluationDomain::<Fr<PE>>::new(data.len()).unwrap();

    let coset_w = AMTParams::<PE>::coset_factor(data.len(), coset);

    let mut coeff = fft_domain.ifft(data);
    cfg_chunks_mut!(coeff, 16)
        .enumerate()
        .for_each(|(idx, chunks)| {
            let mut base = coset_w.pow([idx as u64 * 16]);
            for x in chunks.iter_mut() {
                *x *= base;
                base *= coset_w;
            }
        });

    fft_domain.fft(&coeff)
}

#[derive(Debug)]
pub struct HalfBlob<PE: Pairing, const LOG_COL: usize, const LOG_ROW: usize> {
    pub blob: Vec<Fr<PE>>,
    pub commitment: G1Aff<PE>,
    pub proofs: AllProofs<PE>,
}

impl<PE: Pairing, const LOG_COL: usize, const LOG_ROW: usize>
    HalfBlob<PE, LOG_COL, LOG_ROW>
where AMTParams<PE>: AMTProofs<PE = PE>
{
    fn generate(mut points: Vec<Fr<PE>>, amt: &AMTParams<PE>) -> Self {
        index_reverse(&mut points);
        let (commitment, proofs) = amt.gen_amt_proofs(&points);

        index_reverse(&mut points);
        change_matrix_direction(&mut points, LOG_ROW, LOG_COL);

        Self {
            blob: points,
            commitment: commitment.into_affine(),
            proofs,
        }
    }

    pub fn get_row(&self, index: usize) -> BlobRow<PE, LOG_COL, LOG_ROW> {
        assert!(index < 1 << LOG_ROW);

        let row_size = 1 << LOG_COL;
        let row = self.blob[row_size * index..row_size * (index + 1)].to_vec();

        let reversed_index = bitreverse(index, LOG_ROW);
        let (proof, high_commitment) = self.proofs.get_proof(reversed_index);

        BlobRow::<PE, LOG_COL, LOG_ROW> {
            row,
            proof,
            high_commitment,
            index,
        }
    }
}

#[derive(Debug, CanonicalSerialize, CanonicalDeserialize)]
pub struct BlobRow<PE: Pairing, const LOG_COL: usize, const LOG_ROW: usize> {
    pub index: usize,
    pub row: Vec<Fr<PE>>,
    pub proof: Proof<PE>,
    pub high_commitment: G1Aff<PE>,
}

impl<PE: Pairing, const LOG_COL: usize, const LOG_ROW: usize> PartialEq
    for BlobRow<PE, LOG_COL, LOG_ROW>
{
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
            && self.row == other.row
            && self.proof == other.proof
            && self.high_commitment == other.high_commitment
    }
}

impl<PE: Pairing, const LOG_COL: usize, const LOG_ROW: usize>
    BlobRow<PE, LOG_COL, LOG_ROW>
{
    pub fn verify(
        &self, amt: &AMTVerifyParams<PE>, commitment: G1<PE>,
        deferred_verifier: Option<DeferredVerifier<PE>>,
    ) -> Result<(), AmtProofError> {
        let mut data = self.row.clone();

        index_reverse(&mut data);
        let batch_index = bitreverse(self.index, LOG_ROW);
        amt.verify_proof(
            &data,
            batch_index,
            &self.proof,
            self.high_commitment.into(),
            commitment,
            deferred_verifier,
        )
    }
}

#[cfg(test)]
mod tests {
    use ark_ff::FftField;
    use ark_poly::Radix2EvaluationDomain;
    use once_cell::sync::Lazy;

    use crate::{
        ec_algebra::{Fr, UniformRand},
        prove_params::tests::PP,
        utils::change_matrix_direction,
        AMTParams, VerifierParams,
    };

    use super::EncoderParams;

    const LOG_ROW: usize = 3;
    const LOG_COL: usize = 5;
    const COSET_N: usize = 2;

    type TestEncoderContext = EncoderParams<PE, COSET_N, LOG_COL, LOG_ROW>;
    #[cfg(not(feature = "bls12-381"))]
    type PE = ark_bn254::Bn254;
    #[cfg(feature = "bls12-381")]
    type PE = ark_bls12_381::Bls12_381;

    static ENCODER: Lazy<TestEncoderContext> = Lazy::new(|| {
        #[cfg(not(feature = "bls12-381"))]
        return TestEncoderContext::from_dir_mont("./pp", true, Some(&*PP));
        #[cfg(feature = "bls12-381")]
        return TestEncoderContext::from_dir("./pp", true, Some(&*PP));
    });

    type TestVerifierContext = VerifierParams<PE, COSET_N, LOG_COL, LOG_ROW>;
    static VERIFIER: Lazy<TestVerifierContext> = Lazy::new(|| {
        // Guarantee encoder has complete before loading verifier
        Lazy::force(&ENCODER);
        #[cfg(not(feature = "bls12-381"))]
        return TestVerifierContext::from_dir_mont("./pp");
        #[cfg(feature = "bls12-381")]
        return TestVerifierContext::from_dir("./pp");
    });

    fn random_scalars(length: usize) -> Vec<Fr<PE>> {
        let mut rng = rand::thread_rng();
        (0..length)
            .map(|_| Fr::<PE>::rand(&mut rng))
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_encode_and_prove() {
        let scalars = random_scalars(1 << (LOG_ROW + LOG_COL));
        let [primary_blob, coset_blob] =
            ENCODER.process_blob(scalars.as_slice());
        assert_eq!(primary_blob.blob, scalars);
        assert_eq!(primary_blob.commitment, coset_blob.commitment);

        for index in 0..(1 << LOG_ROW) {
            let commitment = primary_blob.commitment;
            let row = primary_blob.get_row(index);
            row.verify(&VERIFIER.amt_list[0], commitment.into(), None)
                .unwrap();
        }

        for index in 0..(1 << LOG_ROW) {
            let commitment = coset_blob.commitment;
            let row = coset_blob.get_row(index);
            row.verify(&VERIFIER.amt_list[1], commitment.into(), None)
                .unwrap();
        }
    }

    #[test]
    fn test_erasure_encoding() {
        use ark_poly::EvaluationDomain;
        use ark_std::Zero;
        const LENGTH: usize = 1 << (LOG_ROW + LOG_COL);
        let scalars = random_scalars(LENGTH);
        let [primary_blob, coset_blob] =
            ENCODER.process_blob(scalars.as_slice());
        assert_eq!(primary_blob.blob, scalars);

        let fft_domain = Radix2EvaluationDomain::<Fr<PE>>::new(LENGTH).unwrap();
        let fft2_domain =
            Radix2EvaluationDomain::<Fr<PE>>::new(LENGTH * 2).unwrap();

        let mut fft_input = scalars.clone();
        change_matrix_direction(&mut fft_input, LOG_COL, LOG_ROW);

        let mut coeff = fft_domain.ifft(&fft_input);
        coeff.extend(vec![Fr::<PE>::zero(); LENGTH]);

        let answer = fft2_domain.fft(&coeff[..]);
        let mut primary_half: Vec<Fr<PE>> =
            answer.iter().step_by(2).cloned().collect();
        let mut secondary_half: Vec<Fr<PE>> =
            answer.iter().skip(1).step_by(2).cloned().collect();

        change_matrix_direction(&mut primary_half, LOG_ROW, LOG_COL);
        change_matrix_direction(&mut secondary_half, LOG_ROW, LOG_COL);

        assert_eq!(primary_half, primary_blob.blob);
        assert_eq!(secondary_half, coset_blob.blob);
    }

    #[test]
    fn test_coset_factor() {
        assert_eq!(
            AMTParams::<PE>::coset_factor(16, 1),
            <Fr<PE> as FftField>::get_root_of_unity(32).unwrap()
        );
    }
}
