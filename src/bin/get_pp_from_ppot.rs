use anyhow::{bail, Result};
use cfx_storage2::{load_save_power_tau, InputType};
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
