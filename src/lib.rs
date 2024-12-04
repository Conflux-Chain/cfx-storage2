#![allow(dead_code, unused_variables)]

#[macro_use]
extern crate tracing;

pub mod backends;
pub mod errors;
mod example;
mod lvmt;
mod macros;
mod middlewares;
pub mod traits;
pub mod types;
mod utils;

pub use errors::{Result, StorageError};
pub use lvmt::{AmtParams, CreateMode, PowerTau, InputType, load_save_power_tau};

#[derive(Default)]
struct DummpyIter<T>(std::marker::PhantomData<T>);

impl<T> Iterator for DummpyIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

fn todo_iter<T>() -> DummpyIter<T> {
    DummpyIter(std::marker::PhantomData)
}
