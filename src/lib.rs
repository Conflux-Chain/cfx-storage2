#![allow(dead_code, unused_variables)]

pub mod backends;
pub mod errors;
mod example;
mod lvmt;
mod macros;

#[cfg(fuzzing)]
pub mod middlewares;

#[cfg(not(fuzzing))]
mod middlewares;

pub mod traits;
pub mod types;
mod utils;

pub use errors::{Result, StorageError};
pub use lvmt::{LvmtSnapshot, LvmtStorage, LvmtStore, LvmtValue};
