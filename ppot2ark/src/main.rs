#[macro_use]
extern crate tracing;

mod adapter;

#[cfg(not(feature = "bls12-381"))]
mod config_bn;
#[cfg(not(feature = "bls12-381"))]
use config_bn::{PeArk, PePpot, PowerTau};
#[cfg(feature = "bls12-381")]
mod config_bls;
#[cfg(feature = "bls12-381")]
use config_bls::{PeArk, PePpot, PowerTau};

pub use adapter::Adapter;

use amt::ptau_file_name;
use ark_serialize::CanonicalSerialize;

pub use powersoftau::{
    batched_accumulator::BatchedAccumulator,
    parameters::{CeremonyParams, CheckForCorrectness, UseCompression},
};

use memmap::MmapOptions;
use std::{
    fs::{File, OpenOptions},
    path::Path,
};

#[cfg(not(feature = "bls12-381"))]
const FEATURE: &str = "bn254";
#[cfg(feature = "bls12-381")]
const FEATURE: &str = "bls12-381";

#[derive(Debug, Clone, Copy)]
pub enum InputType {
    Challenge,
    Response,
}

impl InputType {
    fn file_name(&self, degree: usize) -> String {
        format!(
            "{}_{}_{}",
            match self {
                InputType::Challenge => "challenge",
                InputType::Response => "response",
            },
            degree,
            FEATURE,
        )
    }
}

fn from_ppot_file_inner(
    input_path: &str,
    input_type: InputType,
    file_size: usize,
    read_from: usize,
    read_size_pow: usize,
    chunk_size_pow: usize,
    parameters: &CeremonyParams<PePpot>,
) -> Result<PowerTau, String> {
    use ark_std::cfg_iter;
    #[cfg(feature = "parallel")]
    use rayon::prelude::*;
    // let read_from = (1 << read_from)
    // - 1;
    let read_size = 1 << read_size_pow;
    let chunk_size = 1 << chunk_size_pow;

    if (read_from + read_size) > (1 << file_size) {
        return Err("too long to read".into());
    }

    let input_filename = format!("{}/{}", input_path, input_type.file_name(file_size));

    let reader = OpenOptions::new()
        .read(true)
        .open(&input_filename)
        .map_err(|e| format!("Cannot open {}: {:?}", input_filename, e))?;

    let input_map = unsafe {
        MmapOptions::new()
            .map(&reader)
            .map_err(|e| format!("unable to create a memory map for input, detail: {}", e))?
    };

    let mut accumulator = BatchedAccumulator::<PePpot>::empty(parameters);
    let use_compression = if let InputType::Response = input_type {
        UseCompression::Yes
    } else {
        UseCompression::No
    };

    let mut g1 = Vec::with_capacity(read_size);
    let mut g2 = Vec::with_capacity(read_size);

    let mut read_offset = read_from;
    let mut remaining_size = read_size;
    while remaining_size > 0 {
        debug!(remaining_size, "Load from perpetual power of tau");
        let current_chunk_size = std::cmp::min(chunk_size, remaining_size);
        accumulator
            .read_chunk(
                read_offset,
                current_chunk_size,
                use_compression,
                CheckForCorrectness::Yes,
                &input_map,
            )
            .map_err(|e| format!("failed to read chunk, detail: {}", e))?;

        let next_g1_chunk: Vec<_> = cfg_iter!(accumulator.tau_powers_g1[..current_chunk_size])
            .map(|tau| tau.adapt())
            .collect();
        g1.extend(next_g1_chunk);

        let next_g2_chunk: Vec<_> = cfg_iter!(accumulator.tau_powers_g2[..current_chunk_size])
            .map(|tau| tau.adapt())
            .collect();
        g2.extend(next_g2_chunk);

        read_offset += current_chunk_size;
        remaining_size -= current_chunk_size;
    }

    Ok(PowerTau { g1pp: g1, g2pp: g2 })
}

#[instrument(skip_all, level=3, fields(file_size_pow=file_size_pow, target_size_pow=read_size_pow, read_from=read_from))]
pub fn from_ppot_file(
    input_path: &str,
    input_type: InputType,
    file_size_pow: usize,
    read_from: usize,
    read_size_pow: usize,
    chunk_size_pow: usize,
) -> Result<PowerTau, String> {
    let params = CeremonyParams::<PePpot>::new(file_size_pow, file_size_pow);
    from_ppot_file_inner(
        input_path,
        input_type,
        file_size_pow,
        read_from,
        read_size_pow,
        chunk_size_pow,
        &params,
    )
}

pub fn load_save_power_tau(
    input_path: &str,
    input_type: InputType,
    file_size_pow: usize,
    target_size_pow: usize,
    chunk_size_pow: usize,
    dir: impl AsRef<Path>,
) -> Result<(), String> {
    let power_tau = from_ppot_file(
        input_path,
        input_type,
        file_size_pow,
        0,
        target_size_pow,
        chunk_size_pow,
    )?;

    let rng = &mut rand::thread_rng();
    power_tau.check_powers_of_tau(rng).unwrap();

    let path = &*dir
        .as_ref()
        .join(ptau_file_name::<PeArk>(target_size_pow, false));
    std::fs::create_dir_all(Path::new(path).parent().unwrap()).unwrap();
    let writer = File::create(path).unwrap();
    power_tau.serialize_compressed(writer).unwrap();
    // write_power_tau(&power_tau, writer).unwrap();
    Ok(())
}

use anyhow::{bail, Result};
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;

fn parse_param() -> Result<(InputType, usize, usize, String, String)> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 6 {
        bail!(
            "Usage: {} <challenge_path> <file_size_pow> <target_size_pow> <output_path> <input_type>",
            args[0]
        );
    }

    let file_size_pow = args[2].parse()?;
    let read_size_pow = args[3].parse()?;
    let challenge_path = args[1].parse()?;
    let output_path = args[4].parse()?;

    if file_size_pow < read_size_pow {
        bail!(
            "Usage: {} <challenge_path> <file_size_pow> <target_size_pow> <output_path> <input_type>\n
            <file_size_pow> should be the largest, 
            <target_size_pow> should be the smallest",
            args[0]
        );
    }

    let input_type = &args[5];
    let input_type = if input_type == "Challenge" {
        InputType::Challenge
    } else if input_type == "Response" {
        InputType::Response
    } else {
        bail!(
            "Usage: {} <challenge_path> <file_size_pow> <target_size_pow> <output_path> <input_type>\n
            <input_type> should be either 'Challenge' or 'Response'.",
            args[0]
        );
    };
    Ok((
        input_type,
        file_size_pow,
        read_size_pow,
        challenge_path,
        output_path,
    ))
}

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_span_events(FmtSpan::CLOSE)
        .with_target(false)
        .init();

    let (input_type, file_size_pow, target_size_pow, challenge_path, output_path) =
        match parse_param() {
            Ok(x) => x,
            Err(e) => {
                eprintln!("Cannot parse input: {:?}", e);
                std::process::exit(1);
            }
        };

    let input_path = challenge_path;

    let chunk_size_pow = std::cmp::min(target_size_pow, 16);
    let dir = &output_path;
    load_save_power_tau(
        &input_path,
        input_type,
        file_size_pow,
        target_size_pow,
        chunk_size_pow,
        dir,
    )
    .unwrap();
}

#[cfg(test)]
fn crate_path() -> String {
    let mut p = project_root::get_project_root().unwrap();
    p.push("ppot2ark");
    p.to_str().unwrap().into()
}

#[cfg(test)]
mod tests {
    pub use ark_ec::pairing::Pairing;
    use parking_lot::Mutex;
    use std::process::Command;

    use super::*;

    lazy_static::lazy_static! {
        static ref LOCK: Mutex<()> = Mutex::new(());
    }

    fn data_path() -> String {
        format!("{}/data", crate_path())
    }

    fn prepare_test_file(ty: InputType, degree: usize) {
        let _guard = LOCK.lock();

        let target_file = format!("{}/{}", data_path(), ty.file_name(degree));

        let script = format!("{}/gen_test_ppot.sh", crate_path());

        if std::fs::metadata(target_file.clone()).is_ok() {
            return;
        }

        println!("{} not found, building...", target_file);

        Command::new("bash")
            .arg(script)
            .arg(degree.to_string())
            .arg(FEATURE)
            .output()
            .expect("Failed to execute command");
    }

    #[test]
    fn test_load_from_challenge_nomal() {
        let input_path = format!("{}/data", crate_path());
        let input_type = InputType::Challenge;
        let file_size_pow = 7;
        let read_size_pow = 5;
        let chunk_size_pow = 10;
        let read_from = 2u32.pow(file_size_pow as u32) - 2u32.pow(read_size_pow as u32);

        prepare_test_file(input_type, file_size_pow);
        let pot = from_ppot_file(
            &input_path,
            input_type,
            file_size_pow,
            read_from as usize,
            read_size_pow,
            chunk_size_pow,
        )
        .unwrap();
        assert_eq!(pot.g1pp.len(), 1 << read_size_pow);
        assert_eq!(
            PeArk::pairing(pot.g1pp[0], pot.g2pp[4]),
            PeArk::pairing(pot.g1pp[1], pot.g2pp[3])
        );
    }

    #[test]
    fn test_load_from_challenge_too_long() {
        let input_path = format!("{}/data", crate_path());
        let input_type = InputType::Challenge;
        let file_size_pow = 7;
        let read_size_pow = 5;
        let chunk_size_pow = 10;
        let read_from = 2u32.pow(file_size_pow as u32) - 2u32.pow(read_size_pow as u32) + 1;

        prepare_test_file(input_type, file_size_pow);
        let pot = from_ppot_file(
            &input_path,
            input_type,
            file_size_pow,
            read_from as usize,
            read_size_pow,
            chunk_size_pow,
        );
        assert!(matches!(pot, Err(ref msg) if msg == "too long to read"));
    }

    //#[ignore = "heavy task"]
    #[test]
    fn test_load_from_response_nomal() {
        // expect to deg 28
        let input_path = format!("{}/data", crate_path());
        let input_type = InputType::Response;
        let file_size_pow = 7;
        let read_size_pow = 5;
        let chunk_size_pow = 10;
        let read_from = 2u32.pow(file_size_pow) - 2u32.pow(read_size_pow);

        prepare_test_file(input_type, file_size_pow as usize);
        let pot = from_ppot_file(
            &input_path,
            input_type,
            file_size_pow as usize,
            read_from as usize,
            read_size_pow as usize,
            chunk_size_pow,
        )
        .unwrap();
        println!("powers length: {}", pot.g1pp.len());
        assert_eq!(pot.g1pp.len(), 1 << read_size_pow);
        assert_eq!(
            PeArk::pairing(pot.g1pp[0], pot.g2pp[4]),
            PeArk::pairing(pot.g1pp[1], pot.g2pp[3])
        );
    }
}
