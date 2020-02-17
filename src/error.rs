use std::io;
use failure::Fail;
use serde_json;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "Input was invalid UTF-8 at index {}", _0)]
    Utf8Error(usize),
    #[fail(display = "{}", _0)]
    Io(io::Error),
    #[fail(display = "{}", _0)]
    Serde(serde_json::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// UnexpectedCommandType indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KvsError {
    fn from(f: io::Error) -> Self {
        KvsError::Io(f)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(f: serde_json::Error) -> Self {
        KvsError::Serde(f)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;

mod tests{
    use std::path::PathBuf;
    use failure::Error;
    use std::io::Seek;

    #[test]
    pub fn test_failure() {
        let r = read_toolchains(PathBuf::new());
        println!("{:?}", r);
    }

    pub fn read_toolchains(path: PathBuf) -> Result<(), Error>
    {
        use std::fs::File;
        use std::io::Read;
        use std::io::BufReader;;

        let mut string = String::new();
        let f= File::open(path)?;
        let br = BufReader::new(f);

        Ok(())
    }
}