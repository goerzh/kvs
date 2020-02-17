#![deny(missing_docs)]
//! A simple key/value store.

use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set(String, String),
    Remove(String),
}

struct CommandOps {
    offset: u64,
    len: u64,
}

/// `KvStore` stores key/value pairs in Memory, not in disk.
///
///  Example:
///
/// ```rust
/// # use kvs::kv::KvStore;
/// let mut kv = KvStore::new();
/// kv.set("hello".to_owned(), "world".to_owned());
/// let v = kv.get("hello".to_owned());
/// assert_eq!(v, Some("world".to_owned()));
/// kv.remove("hello".to_owned());
/// ```
pub struct KvStore {
    path: PathBuf,
    writer: BufWriterWithOps<File>,
    reader: BufReaderWithOps<File>,
    index: BTreeMap<String, CommandOps>,
    load: bool,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path.as_path())?;
        let last_gen = last_gen(&path)?;
        let path = path.join(format!("{}.log", last_gen));
        let writer = BufWriterWithOps::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&path)?,
        )?;
        let reader = BufReaderWithOps::new(File::open(&path)?)?;

        let mut kvs = KvStore {
            path,
            reader,
            writer,
            index: BTreeMap::new(),
            load: true,
        };
        kvs.load()?;

        Ok(kvs)
    }

    /// Sets a value from a string key to a string.
    ///
    /// If the key already exists, the value will be overwritten.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        assert!(self.load);

        let ops = self.writer.offset;
        let cmd_set = Command::Set(key, val);
        serde_json::to_writer(&mut self.writer, &cmd_set)?;
        self.writer.flush()?;

        let new_ops = self.writer.offset;
        let cmd_ops = CommandOps {
            offset: ops,
            len: new_ops - ops,
        };
        if let Command::Set(key, ..) = cmd_set {
            self.index.insert(key, cmd_ops);
        }

        Ok(())
    }

    /// Gets the string value from a given string key.
    ///
    /// Return None if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        assert!(self.load);

        match self.index.get(&key) {
            None => Ok(None),
            Some(ops) => {
                self.reader.seek(SeekFrom::Start(ops.offset))?;
                let take = (&mut self.reader).take(ops.len);
                if let Command::Set(.., value) = serde_json::from_reader(take)? {
                    Ok(Some(value))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            }
        }
    }

    /// Removes a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        assert!(self.load);

        match self.index.get(&key) {
            None => Err(KvsError::KeyNotFound),
            Some(_ops) => {
                let old_cmd = self.index.remove(&key);

                let cmd_rm = Command::Remove(key.clone());
                serde_json::to_writer(&mut self.writer, &cmd_rm)?;
                self.writer.flush()?;

                Ok(())
            }
        }
    }

    /// load index to memory map
    pub fn load(&mut self) -> Result<()> {
        let mut offset: u64 = self.reader.seek(SeekFrom::Start(0))?;
        let mut stream =
            serde_json::Deserializer::from_reader(&mut self.reader).into_iter::<Command>();
        while let Some(cmd) = stream.next() {
            let new_ops = stream.byte_offset() as u64;
            match cmd? {
                Command::Set(key, ..) => {
                    self.index.insert(
                        key,
                        CommandOps {
                            offset,
                            len: new_ops - offset,
                        },
                    );
                }
                Command::Remove(key) => {
                    self.index.remove(&key);
                }
            }
            offset = new_ops;
        }

        self.writer.offset = offset;
        self.writer.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

/// Wrapper BufWriter with offset
pub struct BufWriterWithOps<W: Write + Seek> {
    writer: BufWriter<W>,
    offset: u64,
}

impl<W: Write + Seek> BufWriterWithOps<W> {
    /// Create BufWriterWithOps
    pub fn new(mut inner: W) -> Result<Self> {
        let ops = inner.seek(SeekFrom::Start(0))?;
        Ok(BufWriterWithOps {
            writer: BufWriter::new(inner),
            offset: ops,
        })
    }
}

/// impl Write trait for BufWriterWithOps
impl<W: Write + Seek> Write for BufWriterWithOps<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.offset += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// impl Seek trait for BufWriterWithOps
impl<W: Write + Seek> Seek for BufWriterWithOps<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.offset = self.writer.seek(pos)?;
        Ok(self.offset)
    }
}

/// Wrapper BufWriter with offset
pub struct BufReaderWithOps<R: Read + Seek> {
    reader: BufReader<R>,
    offset: u64,
}

impl<R: Read + Seek> BufReaderWithOps<R> {
    /// Create BufReaderWithOps
    pub fn new(mut inner: R) -> Result<Self> {
        let ops = inner.seek(SeekFrom::Start(0))?;
        Ok(BufReaderWithOps {
            reader: BufReader::new(inner),
            offset: ops,
        })
    }
}

/// impl Read trait for BufReaderWithOps
impl<R: Read + Seek> Read for BufReaderWithOps<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

/// impl Seek trait for BufReaderWithOps
impl<R: Read + Seek> Seek for BufReaderWithOps<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.offset = self.reader.seek(pos)?;
        Ok(self.offset)
    }
}

const INIT_GEN: u64 = 1;

/// Log files are named after a generation number with a "log" extension name.
/// Log file with a lock file indicates a compaction failure and is invalid.
/// This function finds the latest valid generation number.
pub fn last_gen(path: impl AsRef<Path>) -> Result<u64> {
    let gen: Option<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<PathBuf> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(str::parse::<u64>)
        })
        .flatten()
        .max();

    Ok(gen.unwrap_or(INIT_GEN))
}

mod tests {
    use crate::kv::last_gen;
    use std::path::Path;

    #[test]
    pub fn test_last_gen() {
        let path = Path::new("/Users/zhangpanpan/workspace");
        println!("{:?}", last_gen(path));
    }
}
