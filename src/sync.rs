use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjectPath};
use std::{
    env, fmt, fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

const CHUNK_SIZE: usize = 1024 * 1024;
const MAX_FILE_COUNT: u32 = 1_000_000;
const STORAGE_PREFIX: &str = "v1/files";

pub async fn run(directory: &Path) -> Result<u32, Error> {
    let source = canonical_source(directory)?;
    let bucket = required_environment("INFINITEFS_S3_BUCKET")?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()?;

    sync_directory(&store, &source).await
}

async fn sync_directory(store: &dyn ObjectStore, source: &Path) -> Result<u32, Error> {
    let files = regular_files(source)?;
    let file_count = u32::try_from(files.len()).map_err(|_| Error::TooManyFiles)?;

    for file in files {
        upload_file(store, source, &file).await?;
    }

    Ok(file_count)
}

async fn upload_file(
    store: &dyn ObjectStore,
    source: &Path,
    file_path: &Path,
) -> Result<(), Error> {
    let relative = file_path
        .strip_prefix(source)
        .map_err(|_| Error::InvalidPath(file_path.to_owned()))?;
    let prefix = object_prefix(relative)?;
    let manifest_path = prefix.clone().join("manifest");
    let snapshot_before = file_snapshot(file_path)?;
    let mut reader = fs::File::open(file_path)?;
    let mut buffer = vec![0; CHUNK_SIZE];
    let mut chunk_index = 0u64;
    let mut confirmed_size = 0u64;

    put_manifest(store, &manifest_path, confirmed_size, "uploading").await?;

    loop {
        let byte_count = read_chunk(&mut reader, &mut buffer)?;
        if byte_count == 0 {
            break;
        }

        let chunk_path = prefix
            .clone()
            .join("chunks")
            .join(format!("{chunk_index:08}"));
        store
            .put(
                &chunk_path,
                Bytes::copy_from_slice(&buffer[..byte_count]).into(),
            )
            .await?;
        confirmed_size += u64::try_from(byte_count).expect("chunk size fits in u64");
        put_manifest(store, &manifest_path, confirmed_size, "uploading").await?;
        chunk_index += 1;
    }

    if file_snapshot(file_path)? != snapshot_before {
        return Err(Error::ChangedDuringUpload(file_path.to_owned()));
    }

    put_manifest(store, &manifest_path, confirmed_size, "complete").await?;
    println!("Synced {} ({confirmed_size} bytes).", relative.display());
    Ok(())
}

async fn put_manifest(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    confirmed_size: u64,
    state: &str,
) -> Result<(), Error> {
    let manifest =
        format!("version=1\nsize={confirmed_size}\nchunk_size={CHUNK_SIZE}\nstate={state}\n");
    store.put(path, manifest.into()).await?;
    Ok(())
}

fn canonical_source(directory: &Path) -> Result<PathBuf, Error> {
    let source = fs::canonicalize(directory)?;
    if source.is_dir() {
        Ok(source)
    } else {
        Err(Error::NotDirectory(source))
    }
}

fn required_environment(name: &str) -> Result<String, Error> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        _ => Err(Error::MissingEnvironment(name.to_owned())),
    }
}

fn regular_files(source: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut directories = vec![source.to_owned()];
    let mut files = Vec::new();

    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();

            if file_type.is_dir() {
                directories.push(path);
            } else if file_type.is_file() {
                files.push(path);
                if files.len() > MAX_FILE_COUNT as usize {
                    return Err(Error::TooManyFiles);
                }
            } else if file_type.is_symlink() {
                return Err(Error::SymbolicLink(path));
            }
        }
    }

    files.sort();
    Ok(files)
}

fn object_prefix(relative: &Path) -> Result<ObjectPath, Error> {
    let path = relative
        .to_str()
        .ok_or_else(|| Error::NonUtf8Path(relative.to_owned()))?;
    Ok(ObjectPath::from(STORAGE_PREFIX).join(path))
}

fn read_chunk(reader: &mut fs::File, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let byte_count = reader.read(&mut buffer[filled..])?;
        if byte_count == 0 {
            break;
        }
        filled += byte_count;
    }
    Ok(filled)
}

#[derive(Debug, PartialEq, Eq)]
struct FileSnapshot {
    length: u64,
    modified: std::time::SystemTime,
}

fn file_snapshot(path: &Path) -> io::Result<FileSnapshot> {
    let metadata = fs::metadata(path)?;
    Ok(FileSnapshot {
        length: metadata.len(),
        modified: metadata.modified()?,
    })
}

#[derive(Debug)]
pub enum Error {
    ChangedDuringUpload(PathBuf),
    InvalidPath(PathBuf),
    Io(io::Error),
    MissingEnvironment(String),
    NonUtf8Path(PathBuf),
    NotDirectory(PathBuf),
    Store(object_store::Error),
    SymbolicLink(PathBuf),
    TooManyFiles,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChangedDuringUpload(path) => {
                write!(formatter, "{} changed during upload", path.display())
            }
            Self::InvalidPath(path) => {
                write!(formatter, "{} is outside the source", path.display())
            }
            Self::Io(error) => error.fmt(formatter),
            Self::MissingEnvironment(name) => write!(formatter, "set {name}"),
            Self::NonUtf8Path(path) => write!(formatter, "{} is not UTF-8", path.display()),
            Self::NotDirectory(path) => write!(formatter, "{} is not a directory", path.display()),
            Self::Store(error) => error.fmt(formatter),
            Self::SymbolicLink(path) => write!(
                formatter,
                "symbolic links are not supported: {}",
                path.display()
            ),
            Self::TooManyFiles => write!(formatter, "source has more than {MAX_FILE_COUNT} files"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<object_store::Error> for Error {
    fn from(error: object_store::Error) -> Self {
        Self::Store(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let count = COUNTER.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after the Unix epoch")
                .as_nanos();
            let path = env::temp_dir().join(format!("infinitefs-sync-{timestamp}-{count}"));
            fs::create_dir_all(&path).expect("test directory should be created");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn uploads_chunks_and_a_complete_manifest() {
        let source = TestDirectory::new();
        let file = source.0.join("project/package.json");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, vec![42; CHUNK_SIZE + 1]).unwrap();
        let store = InMemory::new();

        assert_eq!(sync_directory(&store, &source.0).await.unwrap(), 1);

        let prefix = object_prefix(Path::new("project/package.json")).unwrap();
        assert_eq!(prefix.as_ref(), "v1/files/project%2Fpackage.json");
        let manifest = store
            .get(&prefix.clone().join("manifest"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&manifest).unwrap(),
            format!(
                "version=1\nsize={}\nchunk_size={CHUNK_SIZE}\nstate=complete\n",
                CHUNK_SIZE + 1
            )
        );
        assert_eq!(
            store
                .get(&prefix.clone().join("chunks").join("00000000"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .len(),
            CHUNK_SIZE
        );
        assert_eq!(
            store
                .get(&prefix.join("chunks").join("00000001"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
