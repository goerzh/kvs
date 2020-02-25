use crate::common::{Request, Response};
use crate::{Command, KvsError, Result};
use serde::Deserialize;
use serde_json;
use serde_json::de::{Deserializer, IoRead};
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream};

pub struct Client {
    addr: SocketAddr,
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl Client {
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(&addr)?;
        let writer = BufWriter::new(stream.try_clone()?);
        let reader = Deserializer::from_reader(BufReader::new(stream));

        Ok(Client {
            addr,
            reader,
            writer,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };

        serde_json::to_writer(&mut self.writer, &request)?;
        // self.writer.flush()?;
        let resp = Response::deserialize(&mut self.reader)?;
        match resp {
            Response::Ok(_) => Ok(()),
            Response::Err(e) => Err(KvsError::StringErr(e)),
        }
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;

        let response = Response::deserialize(&mut self.reader)?;
        match response {
            Response::Ok(res) => Ok(res),
            Response::Err(e) => Err(e.into()),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let request = Request::Remove { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;

        Ok(())
    }
}
