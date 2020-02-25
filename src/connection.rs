use crate::common::{Request, Response};
use crate::{Command, KvStore, KvsError, Result};
use serde_json;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

macro_rules! send_resp {
    ($resp:expr, $writer:expr) => {{
        serde_json::to_writer(&mut $writer, &$resp)?;
        $writer.flush()?;
    }};
}

pub struct Connection {
    db: Arc<Mutex<KvStore>>,
    stream: TcpStream,
}

impl Connection {
    pub fn new(stream: TcpStream, db: Arc<Mutex<KvStore>>) -> Self {
        Connection { db, stream }
    }

    pub fn run(&mut self) {
        match self.serve() {
            Ok(_) => {}
            Err(e) => {
                println!("serve error: {}", e);
            }
        }
    }

    pub fn serve(&mut self) -> Result<()> {
        let reader = BufReader::new(&self.stream);
        let mut writer = BufWriter::new(&self.stream);
        let req_reader = serde_json::Deserializer::from_reader(reader).into_iter::<Request>();

        for req in req_reader {
            let req = req?;
            match req {
                Request::Set { key, value } => {
                    let mut lock = self.db.lock().unwrap();
                    send_resp!(
                        match lock.set(key, value) {
                            Ok(_) => Response::Ok(None),
                            Err(e) => Response::Err(format!("{}", e)),
                        },
                        writer
                    );
                }
                Request::Get { key } => {
                    let mut lock = self.db.lock().unwrap();
                    send_resp!(
                        match lock.get(key) {
                            Ok(value) => Response::Ok(value),
                            Err(e) => Response::Err(format!("{}", e)),
                        },
                        writer
                    );
                }
                Request::Remove { key } => {
                    let mut lock = self.db.lock().unwrap();
                    send_resp!(
                        match lock.remove(key) {
                            Ok(_) => Response::Ok(None),
                            Err(e) => Response::Err(format!("{}", e)),
                        },
                        writer
                    );
                }
            }
        }

        Ok(())
    }
}

mod tests {
    #[test]
    pub fn test_connection() {
        use std::sync::Arc;

        let five = Arc::new(5);
        let five_clone = Arc::clone(&five);

        println!("{:p}", five.as_ref());
        println!("{:p}", five_clone.as_ref());
        assert_eq!(five.as_ref(), five_clone.as_ref());
    }
}
