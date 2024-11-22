use ark_ec::{pairing::{Pairing, PairingOutput}, CurveGroup, VariableBaseMSM};
use ark_ff::Field;
use ark_std::{cfg_into_iter, cfg_iter, UniformRand, Zero};
use rand::rngs::OsRng;
use std::sync::{Arc, Mutex};

use crate::{
    ec_algebra::{Fr, FrInt, G1Aff, G2Aff, G1},
    AmtProofError,
};

#[cfg(feature = "parallel")]
use rayon::prelude::*;

pub type PairingTask<PE> =
    (G1<PE>, G2Aff<PE>, G1<PE>, G2Aff<PE>, AmtProofError);

type PairingVerifier<PE> = (
    Vec<G1<PE>>,
    Vec<G2Aff<PE>>,
    Vec<G1<PE>>,
    Vec<G2Aff<PE>>,
    Vec<AmtProofError>,
);

#[derive(Clone, Default)]
struct MsmVerifier<PE: Pairing> {
    basis: Vec<G1Aff<PE>>,
    bigint: Vec<FrInt<PE>>,
    answer: G1<PE>,
}

#[derive(Clone)]
pub struct DeferredVerifier<PE: Pairing> {
    pairing: Arc<Mutex<PairingVerifier<PE>>>,
    msm: Arc<Mutex<MsmVerifier<PE>>>,
}

impl<PE: Pairing> Default for DeferredVerifier<PE> {
    fn default() -> Self { Self::new() }
}

impl<PE: Pairing> DeferredVerifier<PE> {
    pub fn new() -> Self {
        Self {
            pairing: Arc::new(Mutex::new(Default::default())),
            msm: Arc::new(Mutex::new(MsmVerifier {
                basis: vec![],
                bigint: vec![],
                answer: G1::<PE>::zero(),
            })),
        }
    }

    pub fn record_pairing(&self, tasks: Vec<PairingTask<PE>>) {
        let mut lock_guard = self.pairing.lock().unwrap();
        let (va, vb, vc, vd, verror) = &mut *lock_guard;
        for (a, b, c, d, error) in tasks.into_iter() {
            va.push(a);
            vb.push(b);
            vc.push(c);
            vd.push(d);
            verror.push(error);
        }
    }

    pub fn fast_check(&self) -> bool {
        self.fast_check_pairing() && self.fast_check_msm()
    }

    fn fast_check_pairing(&self) -> bool {
        let lock_guard = self.pairing.lock().unwrap();
        let (va, vb, vc, vd, _) = &*lock_guard;

        let n = va.len() as u64;
        if n == 0 {
            return true;
        }

        let tau = Fr::<PE>::rand(&mut OsRng);
        let coeff: Vec<Fr<PE>> =
            cfg_into_iter!(0..n).map(|pow| tau.pow([pow])).collect();

        let left = pairing_rlc::<PE>(va, vb, &coeff);
        let right = pairing_rlc::<PE>(vc, vd, &coeff);

        left == right
    }

    fn fast_check_msm(&self) -> bool {
        let lock_guard = self.msm.lock().unwrap();
        let MsmVerifier {
            basis,
            bigint,
            answer,
        } = &*lock_guard;

        if basis.is_empty() {
            return true;
        }

        G1::<PE>::msm_bigint(&basis[..], &bigint[..]) == *answer
    }

    pub fn check_pairing(&self) -> Result<(), AmtProofError> {
        let lock_guard = self.pairing.lock().unwrap();
        let (va, vb, vc, vd, ve) = &*lock_guard;

        let n = va.len();
        if n == 0 {
            return Ok(());
        }

        for i in 0..n {
            if PE::pairing(va[i], vb[i]) != PE::pairing(vc[i], vd[i]) {
                return Err(ve[i]);
            }
        }

        Ok(())
    }
}

fn all_same<T: Eq>(input: &[T]) -> bool { input.iter().all(|x| *x == input[0]) }

fn pairing_rlc<PE: Pairing>(
    g1: &[G1<PE>], g2: &[G2Aff<PE>], coeff: &[Fr<PE>],
) -> PairingOutput<PE> {
    if all_same(g2) {
        let g1 = CurveGroup::normalize_batch(g1);
        let combined: G1<PE> = VariableBaseMSM::msm(&g1[..], coeff).unwrap();
        PE::pairing(combined, g2[0])
    } else {
        let g1: Vec<_> = cfg_iter!(g1)
            .zip(cfg_iter!(coeff))
            .map(|(x, y)| *x * *y)
            .collect();
        let g1 = CurveGroup::normalize_batch(&g1[..]);

        #[cfg(not(feature = "parallel"))]
        let ans = PE::multi_pairing(g1, g2);
        #[cfg(feature = "parallel")]
        let ans = multi_pairing_parallel(&g1, g2);
        ans
    }
}

#[cfg(feature = "parallel")]
pub fn multi_pairing_parallel<PE: Pairing>(
    g1: &[G1Aff<PE>], g2: &[G2Aff<PE>],
) -> PairingOutput<PE> {
    let min_elements_per_thread = 1;
    let num_cpus_available = rayon::current_num_threads();
    let num_elems = g1.len();
    let num_elem_per_thread =
        std::cmp::max(num_elems / num_cpus_available, min_elements_per_thread);

    let thread_outputs: Vec<_> = g1
        .par_chunks(num_elem_per_thread)
        .zip(g2.par_chunks(num_elem_per_thread))
        .map(|(a, b)| PE::multi_pairing(a, b))
        .collect();

    thread_outputs.into_par_iter().sum()
}