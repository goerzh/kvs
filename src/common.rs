use crate::KvsError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}
