use super::{
    ec_algebra::{
        AffineRepr, CanonicalDeserialize, CanonicalSerialize, CurveGroup, Fr, G1Aff, G2Aff,
        UniformRand, Zero, G1, G2,
    },
    error, ptau_file_name,
};
#[cfg(feature = "bls12-381")]
use ark_bls12_381::Bls12_381;
#[cfg(not(feature = "bls12-381"))]
use ark_bn254::Bn254;
use ark_ec::{pairing::Pairing, VariableBaseMSM};
use ark_ff::{utils::k_adicity, Field};
use ark_std::cfg_into_iter;
use rand::rngs::ThreadRng;
#[cfg(feature = "parallel")]
use rayon::prelude::*;
use std::{
    fs::{create_dir_all, File},
    path::Path,
};
use tracing::{debug, info};

#[derive(CanonicalDeserialize, CanonicalSerialize, Clone)]
pub struct PowerTau<PE: Pairing> {
    pub g1pp: Vec<G1Aff<PE>>,
    pub g2pp: Vec<G2Aff<PE>>,
}

impl<PE: Pairing> PowerTau<PE> {
    pub fn check_powers_of_tau(&self, rng: &mut ThreadRng) -> Result<(), error::Error> {
        let len = self.g1pp.len();
        if self.g2pp.len() != len {
            return Err(error::ErrorKind::InconsistentLength.into());
        }

        let r = (0..len - 1)
            .map(|_| Self::gen_rand_non_zero_fr(rng))
            .collect::<Result<Vec<_>, error::Error>>()?;
        let q = (0..len - 1)
            .map(|_| Self::gen_rand_non_zero_fr(rng))
            .collect::<Result<Vec<_>, error::Error>>()?;

        let g1_low: G1<PE> = VariableBaseMSM::msm(&self.g1pp[..len - 1], &r).unwrap();
        let g1_high: G1<PE> = VariableBaseMSM::msm(&self.g1pp[1..], &r).unwrap();
        let g2_low: G2<PE> = VariableBaseMSM::msm(&self.g2pp[..len - 1], &q).unwrap();
        let g2_high: G2<PE> = VariableBaseMSM::msm(&self.g2pp[1..], &q).unwrap();

        if PE::pairing(g1_low, g2_high) != PE::pairing(g1_high, g2_low) {
            Err(error::ErrorKind::InconsistentPowersOfTau.into())
        } else {
            Ok(())
        }
    }

    fn gen_rand_non_zero_fr(rng: &mut ThreadRng) -> Result<Fr<PE>, error::Error> {
        for _ in 0..10 {
            let fr = Fr::<PE>::rand(rng);
            if !fr.is_zero() {
                return Ok(fr);
            }
        }
        Err(error::ErrorKind::RareZeroGenerationError.into())
    }
}

fn power_tau<'a, G: AffineRepr>(gen: &'a G, tau: &'a G::ScalarField, length: usize) -> Vec<G> {
    let gen: G::Group = gen.into_group();
    cfg_into_iter!(0usize..length)
        .step_by(1024)
        .flat_map(|start| {
            let end = std::cmp::min(start + 1024, length);
            let project_tau: Vec<_> = (start..end)
                .map(|idx| gen * tau.pow([idx as u64]))
                .collect();
            CurveGroup::normalize_batch(&project_tau[..])
        })
        .collect()
}

impl<PE: Pairing> PowerTau<PE> {
    #[cfg(test)]
    fn setup_with_tau(tau: Fr<PE>, depth: usize) -> PowerTau<PE> {
        Self::setup_inner(Some(tau), depth)
    }

    pub fn setup(depth: usize) -> PowerTau<PE> {
        Self::setup_inner(None, depth)
    }

    fn setup_inner(tau: Option<Fr<PE>>, depth: usize) -> PowerTau<PE> {
        info!(random_tau = tau.is_none(), depth, "Setup powers of tau");

        let random_tau = Fr::<PE>::rand(&mut rand::thread_rng());
        let tau = tau.unwrap_or(random_tau);

        let gen1 = G1Aff::<PE>::generator();
        let gen2 = G2Aff::<PE>::generator();

        let g1pp: Vec<G1Aff<PE>> = power_tau(&gen1, &tau, 1 << depth);
        let g2pp: Vec<G2Aff<PE>> = power_tau(&gen2, &tau, 1 << depth);

        PowerTau { g1pp, g2pp }
    }

    fn from_dir_inner(
        file: impl AsRef<Path>,
        expected_depth: usize,
    ) -> Result<PowerTau<PE>, error::Error> {
        let buffer = File::open(file)?;
        let pp: PowerTau<PE> = CanonicalDeserialize::deserialize_compressed_unchecked(buffer)?;

        let (g1_len, g2_len) = (pp.g1pp.len(), pp.g2pp.len());
        let depth = k_adicity(2, g1_len as u64) as usize;

        if g1_len != g2_len || expected_depth > depth {
            Err(error::ErrorKind::InconsistentLength.into())
        } else if expected_depth < g2_len {
            let g1pp = pp.g1pp[..1 << expected_depth].to_vec();
            let g2pp = pp.g2pp[..1 << expected_depth].to_vec();
            Ok(PowerTau { g1pp, g2pp })
        } else {
            Ok(pp)
        }
    }

    pub fn from_dir(
        dir: impl AsRef<Path>,
        expected_depth: usize,
        create_mode: bool,
    ) -> PowerTau<PE> {
        debug!("Load powers of tau");

        let file = &dir
            .as_ref()
            .join(ptau_file_name::<PE>(expected_depth, false));

        match Self::from_dir_inner(file, expected_depth) {
            Ok(loaded) => {
                return loaded;
            }
            Err(e) => {
                info!(path = ?file, error = ?e, "Fail to load powers of tau");
            }
        }

        if !create_mode {
            panic!(
                "Fail to load public parameters for {} at depth {}, read TODO to generate",
                std::any::type_name::<PE>(),
                expected_depth
            );
        }

        let pp = Self::setup(expected_depth);
        create_dir_all(Path::new(file).parent().unwrap()).unwrap();
        let buffer = File::create(file).unwrap();
        info!(?file, "Save generated powers of tau");
        pp.serialize_compressed(&buffer).unwrap();
        pp
    }

    #[allow(clippy::type_complexity)]
    pub fn into_projective(self) -> (Vec<G1<PE>>, Vec<G2<PE>>) {
        let g1pp = self.g1pp.into_iter().map(G1::<PE>::from).collect();
        let g2pp: Vec<<PE as Pairing>::G2> = self.g2pp.into_iter().map(G2::<PE>::from).collect();
        (g1pp, g2pp)
    }
}

#[cfg(not(feature = "bls12-381"))]
impl PowerTau<Bn254> {
    pub fn from_dir_mont(dir: impl AsRef<Path>, expected_depth: usize, create_mode: bool) -> Self {
        debug!("Load powers of tau (mont format)");

        let path = dir
            .as_ref()
            .join(ptau_file_name::<Bn254>(expected_depth, true));

        match Self::load_cached_mont(&path) {
            Ok(loaded) => {
                return loaded;
            }
            Err(e) => {
                info!(?path, error = ?e, "Fail to load powers of tau (mont format)");
            }
        }

        if !create_mode {
            panic!(
                "Fail to load public parameters for {} at depth {}",
                std::any::type_name::<Bn254>(),
                expected_depth
            );
        }

        info!("Recover from unmont format");

        let pp = Self::from_dir(dir, expected_depth, create_mode);
        let writer = File::create(&*path).unwrap();

        info!(file = ?path, "Save generated AMT params (mont format)");
        super::fast_serde_bn254::write_power_tau(&pp, writer).unwrap();

        pp
    }

    fn load_cached_mont(file: impl AsRef<Path>) -> Result<Self, error::Error> {
        let buffer = File::open(file)?;
        super::fast_serde_bn254::read_power_tau(buffer)
    }
}

#[cfg(feature = "bls12-381")]
impl PowerTau<Bls12_381> {
    pub fn from_dir_mont(dir: impl AsRef<Path>, expected_depth: usize, create_mode: bool) -> Self {
        debug!("Load powers of tau (mont format)");

        let path = dir
            .as_ref()
            .join(ptau_file_name::<Bls12_381>(expected_depth, true));

        match Self::load_cached_mont(&path) {
            Ok(loaded) => {
                return loaded;
            }
            Err(e) => {
                info!(?path, error = ?e, "Fail to load powers of tau (mont format)");
            }
        }

        if !create_mode {
            panic!(
                "Fail to load public parameters for {} at depth {}",
                std::any::type_name::<Bls12_381>(),
                expected_depth
            );
        }

        info!("Recover from unmont format");

        let pp = Self::from_dir(dir, expected_depth, create_mode);
        let writer = File::create(&*path).unwrap();

        info!(file = ?path, "Save generated AMT params (mont format)");
        super::fast_serde_bls12_381::write_power_tau(&pp, writer).unwrap();

        pp
    }

    fn load_cached_mont(file: impl AsRef<Path>) -> Result<Self, error::Error> {
        let buffer = File::open(file)?;
        super::fast_serde_bls12_381::read_power_tau(buffer)
    }
}

impl<PE: Pairing> PartialEq for PowerTau<PE> {
    fn eq(&self, other: &Self) -> bool {
        self.g1pp == other.g1pp && self.g2pp == other.g2pp
    }
}

#[test]
fn test_partial_load() {
    #[cfg(not(feature = "bls12-381"))]
    type PE = ark_bn254::Bn254;
    #[cfg(feature = "bls12-381")]
    type PE = ark_bls12_381::Bls12_381;

    let tau = Fr::<PE>::rand(&mut rand::thread_rng());
    let large_pp = PowerTau::<PE>::setup_with_tau(tau, 8);
    let small_pp = PowerTau::<PE>::setup_with_tau(tau, 4);

    assert_eq!(small_pp.g1pp[..], large_pp.g1pp[..(small_pp.g1pp.len())]);
    assert_eq!(small_pp.g2pp[..], large_pp.g2pp[..(small_pp.g2pp.len())]);
}

#[test]
fn test_parallel_build() {
    use super::ec_algebra::CurveGroup;

    const DEPTH: usize = 13;
    type PE = ark_bn254::Bn254;

    let tau = Fr::<PE>::rand(&mut rand::thread_rng());
    let gen1 = G1Aff::<PE>::generator();
    let g1pp_ans = power_tau(&gen1, &tau, 1 << DEPTH);

    let mut g1pp: Vec<G1Aff<PE>> = Vec::with_capacity(1 << DEPTH);
    let mut gen1 = gen1.into_group();
    for _ in 0..1 << DEPTH {
        g1pp.push(gen1.into_affine());
        gen1 *= tau;
    }
    assert_eq!(g1pp, g1pp_ans)
}
