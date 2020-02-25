use crate::{Config, Connection, KvStore, Result};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct Server {
    config: Config,
    db: Arc<Mutex<KvStore>>,
    listener_threads: Vec<thread::JoinHandle<()>>,
    listener_channels: Vec<Sender<()>>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        Server {
            db: Arc::new(Mutex::new(KvStore::open(config.path()).unwrap())),
            listener_threads: Vec::new(),
            listener_channels: Vec::new(),
            config,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        self.start()?;
        self.join();

        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.config.address())?;
        let (tx, rx) = channel::<()>();
        self.listener_channels.push(tx);

        let db = self.db.clone();
        let th = thread::spawn(move || {
            for stream in listener.incoming() {
                println!("new connection");
                if rx.try_recv().is_ok() {
                    break;
                }
                match stream {
                    Ok(stream) => {
                        let db1 = db.clone();
                        thread::spawn(move || {
                            let mut conn = Connection::new(stream, db1);
                            conn.run();
                        });
                    }
                    Err(e) => {}
                }
            }
        });

        self.listener_threads.push(th);

        Ok(())
    }

    pub fn join(&mut self) {
        if self.listener_threads.len() > 0 {
            self.listener_threads.pop().unwrap().join();
        }
    }

    pub fn stop(&mut self) {}
}
