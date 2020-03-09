use crate::common::{Request, Response};
use crate::{Command, KvStore, KvsError, Result};
use serde_json;
use slog::Logger;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub struct Connection {
    db: Arc<Mutex<KvStore>>,
    stream: TcpStream,
    log: Logger,
}

impl Connection {
    pub fn new(stream: TcpStream, db: Arc<Mutex<KvStore>>, log: Logger) -> Self {
        let log = log.new(o!("peer-address"=>stream.peer_addr().unwrap()));
        Connection { db, stream, log }
    }

    pub fn run(&mut self) {
        match self.serve() {
            Ok(_) => {}
            Err(e) => {
                error!(self.log, "Error on serving client: {}", e);
            }
        }
    }

    pub fn serve(&mut self) -> Result<()> {
        let reader = BufReader::new(&self.stream);
        let mut writer = BufWriter::new(&self.stream);
        let req_reader = serde_json::Deserializer::from_reader(reader).into_iter::<Request>();

        macro_rules! send_resp {
            ($resp:expr, $writer:expr) => {{
                let resp = $resp;
                serde_json::to_writer(&mut $writer, &resp)?;
                $writer.flush()?;
                debug!(self.log, "Sent response: {:?}", resp);
            }};
        }

        for req in req_reader {
            let req = req?;
            debug!(self.log, "Receive request: {:?}", req);
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
