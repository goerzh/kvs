pub mod kv;
pub mod error;

pub use error::{Result, KvsError};
pub use kv::KvStore;