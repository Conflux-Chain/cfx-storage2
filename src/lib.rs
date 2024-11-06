#![allow(dead_code, unused_variables)]

pub mod backends;
pub mod errors;
mod middlewares;
mod storage;
pub mod traits;
pub mod types;
mod utils;

pub use errors::{Result, StorageError};

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
