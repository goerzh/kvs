// #![deny(missing_docs)]
//! A simple key/value store.

use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set(String, String),
    Remove(String),
}

#[derive(Debug)]
struct CommandOps {
    gen: u64,
    offset: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandOps {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandOps {
            gen,
            offset: range.start,
            len: range.end - range.start,
        }
    }
}

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

/// `KvStore` stores key/value pairs in Memory, not in disk.
///
///  Example:
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut kv = KvStore::open(current_dir()?)?;
/// kv.set("hello".to_owned(), "world".to_owned());
/// let v = kv.get("hello".to_owned())?;
/// assert_eq!(v, Some("world".to_owned()));
/// kv.remove("hello".to_owned())?;
/// Ok(())
/// # }
/// ```
pub struct KvStore {
    path: PathBuf,
    writer: BufWriterWithOps<File>,
    readers: HashMap<u64, BufReaderWithOps<File>>,
    index: BTreeMap<String, CommandOps>,
    load: bool,
    current_gen: u64,
    uncompacted: u64,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let gen_list = gen_list(&path)?;
        let mut uncompacted: u64 = 0;

        for &gen in &gen_list {
            let mut reader = BufReaderWithOps::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut index, &mut reader)?;
            readers.insert(gen, reader);
        }
        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen, &mut readers)?;

        Ok(KvStore {
            path,
            readers,
            writer,
            index,
            current_gen,
            uncompacted,
            load: true,
        })
    }

    /// Sets a value from a string key to a string.
    ///
    /// If the key already exists, the value will be overwritten.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        assert!(self.load);

        let ops = self.writer.offset;
        let cmd = Command::Set(key, val);
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Command::Set(key, ..) = cmd {
            if let Some(old_ops) = self
                .index
                .insert(key, (self.current_gen, (ops..self.writer.offset)).into())
            {
                self.uncompacted += old_ops.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
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
                let reader = self
                    .readers
                    .get_mut(&ops.gen)
                    .expect("Cannot find log reader");
                reader.seek(SeekFrom::Start(ops.offset))?;
                let cmd_reader = reader.take(ops.len);
                if let Command::Set(.., value) = serde_json::from_reader(cmd_reader)? {
                    Ok(Some(value))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            }
        }
    }

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn remove(&mut self, key: String) -> Result<()> {
        assert!(self.load);

        if self.index.contains_key(&key) {
            let ops = self.writer.offset;
            let cmd = Command::Remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            self.uncompacted += self.writer.offset - ops;

            if let Command::Remove(key) = cmd {
                let old_ops = self.index.remove(&key).expect("Key not found");
                self.uncompacted += old_ops.len;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Clears stale entries in the log.
    pub fn compact(&mut self) -> Result<()> {
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = self.new_log_file(self.current_gen)?;

        let mut compaction_writer = self.new_log_file(compaction_gen)?;

        let mut ops: u64 = 0;
        for (.., cmd_ops) in &mut self.index {
            let reader = self
                .readers
                .get_mut(&cmd_ops.gen)
                .expect("Cannot find log reader");
            if reader.offset != cmd_ops.offset {
                reader.seek(SeekFrom::Start(cmd_ops.offset))?;
            }

            let mut cmd_reader = reader.take(cmd_ops.len);
            io::copy(&mut cmd_reader, &mut compaction_writer)?;
            *cmd_ops = (compaction_gen, (ops..compaction_writer.offset)).into();

            ops = compaction_writer.offset;
        }
        compaction_writer.flush()?;

        let stale_gens: Vec<u64> = self
            .readers
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }

        self.uncompacted = 0;
        Ok(())
    }

    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithOps<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
    }
}

/// load the whole log file and store value location in the index map.
///
/// Return how many bytes can be saved after a compaction.
fn load(
    gen: u64,
    index: &mut BTreeMap<String, CommandOps>,
    reader: &mut BufReaderWithOps<File>,
) -> Result<u64> {
    let mut uncompacted: u64 = 0;
    let mut offset: u64 = reader.seek(SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let new_ops = stream.byte_offset() as u64;
        match cmd? {
            Command::Set(key, ..) => {
                if let Some(ops) = index.insert(key, (gen, (offset..new_ops)).into()) {
                    uncompacted += ops.len;
                }
            }
            Command::Remove(key) => {
                if let Some(ops) = index.remove(&key) {
                    uncompacted += ops.len;
                }

                uncompacted += new_ops - offset;
            }
        }
        offset = new_ops;
    }

    Ok(uncompacted)
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

/// Log files are named after a generation number with a "log" extension name.
/// Log file with a lock file indicates a compaction failure and is invalid.
/// This function returns valid generation numbers.
pub fn gen_list(path: impl AsRef<Path>) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|res| res.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// Return path of the log file
fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn new_log_file(
    dir: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithOps<File>>,
) -> Result<BufWriterWithOps<File>> {
    let path = log_path(dir, gen);
    let writer = BufWriterWithOps::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(gen, BufReaderWithOps::new(File::open(&path)?)?);
    Ok(writer)
}
