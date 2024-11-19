use crate::{
    ec_algebra::{
        AffineRepr, CanonicalDeserialize, CanonicalSerialize, CurveGroup, Fr,
        G1Aff, G2Aff, Pairing, UniformRand, G1, G2,
    },
    error, ptau_file_name,
};
#[cfg(not(feature = "bls12-381"))]
use ark_bn254::Bn254;
use ark_ff::{utils::k_adicity, Field};
use ark_std::cfg_into_iter;
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
    pub high_g1pp: Vec<G1Aff<PE>>,
    pub high_g2: G2<PE>,
}

fn power_tau<'a, G: AffineRepr>(
    gen: &'a G, tau: &'a G::ScalarField, length: usize,
) -> Vec<G> {
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
        let high_depth = depth + 2;

        let random_tau = Fr::<PE>::rand(&mut rand::thread_rng());
        let tau = tau.unwrap_or(random_tau);

        let gen1 = G1Aff::<PE>::generator();
        let gen2 = G2Aff::<PE>::generator();

        let g1pp: Vec<G1Aff<PE>> = power_tau(&gen1, &tau, 1 << depth);
        let g2pp: Vec<G2Aff<PE>> = power_tau(&gen2, &tau, 1 << depth);

        let high_start = (1 << high_depth) - (1 << depth);
        let high_gen1: G1Aff<PE> =
            (gen1 * tau.pow([high_start as u64])).into_affine();
        let high_g2: G2<PE> = gen2 * tau.pow([high_start as u64]);

        let high_g1pp: Vec<G1Aff<PE>> = power_tau(&high_gen1, &tau, 1 << depth);

        PowerTau {
            g1pp,
            g2pp,
            high_g1pp,
            high_g2,
        }
    }

    fn from_dir_inner(
        file: impl AsRef<Path>, expected_depth: usize,
    ) -> Result<PowerTau<PE>, error::Error> {
        let buffer = File::open(file)?;
        let pp: PowerTau<PE> =
            CanonicalDeserialize::deserialize_compressed_unchecked(buffer)?;

        let (g1_len, g2_len, high_g1_len) =
            (pp.g1pp.len(), pp.g2pp.len(), pp.high_g1pp.len());
        let depth = k_adicity(2, g1_len as u64) as usize;

        if g1_len != g2_len || g1_len != high_g1_len || expected_depth > depth {
            Err(error::ErrorKind::InconsistentLength.into())
        } else if expected_depth < g2_len {
            let g1pp = pp.g1pp[..1 << expected_depth].to_vec();
            let g2pp = pp.g2pp[..1 << expected_depth].to_vec();
            let high_g1pp = pp.high_g1pp[..1 << expected_depth].to_vec();
            Ok(PowerTau {
                g1pp,
                g2pp,
                high_g1pp,
                high_g2: pp.high_g2,
            })
        } else {
            Ok(pp)
        }
    }

    pub fn from_dir(
        dir: impl AsRef<Path>, expected_depth: usize, create_mode: bool,
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
    pub fn into_projective(
        self,
    ) -> (Vec<G1<PE>>, Vec<G2<PE>>, Vec<G1<PE>>, Vec<G2<PE>>) {
        let g1pp = self.g1pp.into_iter().map(G1::<PE>::from).collect();
        let g2pp = self.g2pp.into_iter().map(G2::<PE>::from).collect();
        let high_g1pp =
            self.high_g1pp.into_iter().map(G1::<PE>::from).collect();
        (g1pp, g2pp, high_g1pp, vec![self.high_g2])
    }
}

#[cfg(not(feature = "bls12-381"))]
impl PowerTau<Bn254> {
    pub fn from_dir_mont(
        dir: impl AsRef<Path>, expected_depth: usize, create_mode: bool,
    ) -> Self {
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
        crate::fast_serde_bn254::write_power_tau(&pp, writer).unwrap();

        pp
    }

    fn load_cached_mont(file: impl AsRef<Path>) -> Result<Self, error::Error> {
        let buffer = File::open(file)?;
        crate::fast_serde_bn254::read_power_tau(buffer)
    }
}

impl<PE: Pairing> PartialEq for PowerTau<PE> {
    fn eq(&self, other: &Self) -> bool {
        self.g1pp == other.g1pp
            && self.g2pp == other.g2pp
            && self.high_g1pp == other.high_g1pp
            && self.high_g2 == other.high_g2
    }
}

impl<PE: Pairing> PowerTau<PE> {
    pub fn check_ldt(&self) {
        assert_eq!(self.g1pp.len(), self.g2pp.len());
        assert_eq!(self.g1pp.len(), self.high_g1pp.len());
        let g2: G2<PE> = self.g2pp[0].into();
        let _ =
            self.g1pp
                .iter()
                .zip(self.high_g1pp.iter())
                .map(|(g1, high_g1)| {
                    assert_eq!(
                        PE::pairing(g1, self.high_g2),
                        PE::pairing(high_g1, g2)
                    )
                });
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
    use crate::ec_algebra::CurveGroup;

    const DEPTH: usize = 13;
    type PE = ark_bn254::Bn254;

    let tau = Fr::<PE>::rand(&mut rand::thread_rng());
    let gen1 = G1Aff::<PE>::generator();
    let g1pp_ans = power_tau(&gen1, &tau, 1 << DEPTH);

    let mut g1pp: Vec<G1Aff<PE>> = vec![];
    g1pp.reserve(1 << DEPTH);
    let mut gen1 = gen1.into_group();
    for _ in 0..1 << DEPTH {
        g1pp.push(gen1.into_affine());
        gen1 *= tau.clone();
    }
    assert_eq!(g1pp, g1pp_ans)
}
