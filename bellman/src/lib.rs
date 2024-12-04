#![allow(unused_imports)]
#![allow(unused_macros)]
#[macro_use]
extern crate cfg_if;
extern crate bit_vec;
extern crate byteorder;
pub extern crate pairing;
extern crate rand;

#[macro_use]
mod log;

pub mod domain;
pub mod groth16;

#[cfg(feature = "gm17")]
pub mod gm17;
#[cfg(feature = "sonic")]
pub mod sonic;

mod group;
mod multiexp;
mod source;

#[cfg(test)]
mod tests;

cfg_if! {
    if #[cfg(feature = "multicore")] {
        #[cfg(feature = "wasm")]
        compile_error!("Multicore feature is not yet compatible with wasm target arch");

        pub mod multicore;
        mod worker {
            pub use crate::multicore::*;
        }
    } else {
        pub mod singlecore;
        mod worker {
            pub use crate::singlecore::*;
        }
    }
}

mod cs;
pub use self::cs::*;

use std::env;
use std::str::FromStr;

fn verbose_flag() -> bool {
    option_env!("BELLMAN_VERBOSE").unwrap_or("0") == "1"
}
