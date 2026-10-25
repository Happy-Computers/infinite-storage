use bytes::Bytes;
use notify::{Config, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, NoCache, new_debouncer_opt};
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjectPath};
use std::{
    collections::HashMap,
    env, fmt, fs,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvTimeoutError, TrySendError},
    },
    time::{Duration, Instant, SystemTime},
};

const CHUNK_SIZE: usize = 1024 * 1024;
const DEBOUNCE_DURATION: Duration = Duration::from_millis(200);
const EVENT_BATCH_CAPACITY: usize = 32;
const EVENT_PATH_LIMIT: usize = 4096;
const MAX_FILE_COUNT: u32 = 1_000_000;
const RESCAN_INTERVAL: Duration = Duration::from_secs(5);
const STORAGE_PREFIX: &str = "v1/files";

pub async fn run(directory: &Path, watch: bool) -> Result<u32, Error> {
    load_local_environment()?;
    let source = canonical_source(directory)?;
    let bucket = required_environment("INFINITEFS_S3_BUCKET")?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()?;

    if watch {
        watch_directory(&store, source).await
    } else {
        let mut index = SyncIndex::new();
        index.rescan(&store, &source).await?;
        Ok(index.file_count())
    }
}

fn load_local_environment() -> Result<(), Error> {
    match dotenvy::from_filename(".env.local") {
        Ok(_) => Ok(()),
        Err(dotenvy::Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Dotenv(error)),
    }
}

async fn watch_directory(store: &dyn ObjectStore, source: PathBuf) -> Result<u32, Error> {
    let (sender, receiver) = mpsc::sync_channel(EVENT_BATCH_CAPACITY);
    let rescan_requested = Arc::new(AtomicBool::new(false));
    let _debouncer = create_watcher(&source, sender, Arc::clone(&rescan_requested))?;
    let mut index = SyncIndex::new();
    index.rescan(store, &source).await?;
    let mut rescan_at = Instant::now() + RESCAN_INTERVAL;

    loop {
        if rescan_requested.swap(false, Ordering::AcqRel) || Instant::now() >= rescan_at {
            index.rescan(store, &source).await?;
            rescan_at = Instant::now() + RESCAN_INTERVAL;
        }

        match receiver.recv_timeout(Duration::from_millis(250)) {
            Ok(paths) => index.apply_paths(store, &source, paths).await?,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Err(Error::WatcherStopped),
        }
    }
}

fn create_watcher(
    source: &Path,
    sender: mpsc::SyncSender<Vec<PathBuf>>,
    rescan_requested: Arc<AtomicBool>,
) -> Result<notify_debouncer_full::Debouncer<notify::RecommendedWatcher, NoCache>, Error> {
    let callback = move |result: DebounceEventResult| {
        let paths = match result {
            Ok(events) => event_paths(events, &rescan_requested),
            Err(_) => {
                rescan_requested.store(true, Ordering::Release);
                return;
            }
        };
        if paths.is_empty() {
            return;
        }
        match sender.try_send(paths) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => rescan_requested.store(true, Ordering::Release),
            Err(TrySendError::Disconnected(_)) => {}
        }
    };
    let mut debouncer = new_debouncer_opt(
        DEBOUNCE_DURATION,
        Some(DEBOUNCE_DURATION / 2),
        callback,
        NoCache::new(),
        Config::default().with_follow_symlinks(false),
    )?;
    debouncer.watch(source, RecursiveMode::Recursive)?;
    Ok(debouncer)
}

fn event_paths(
    events: Vec<notify_debouncer_full::DebouncedEvent>,
    rescan_requested: &AtomicBool,
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for event in events {
        if event.event.need_rescan() {
            rescan_requested.store(true, Ordering::Release);
        }
        paths.extend(event.event.paths);
        if paths.len() > EVENT_PATH_LIMIT {
            rescan_requested.store(true, Ordering::Release);
            return Vec::new();
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

struct SyncIndex {
    files: HashMap<PathBuf, FileSnapshot>,
}

impl SyncIndex {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    fn file_count(&self) -> u32 {
        u32::try_from(self.files.len()).expect("file count is bounded")
    }

    async fn rescan(&mut self, store: &dyn ObjectStore, source: &Path) -> Result<(), Error> {
        let current = regular_files(source)?;
        let mut seen = HashMap::new();

        for path in current {
            let snapshot = match file_snapshot(&path) {
                Ok(snapshot) => snapshot,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if self.files.get(&path) != Some(&snapshot) {
                match upload_file(store, source, &path).await {
                    Ok(()) => {
                        seen.insert(path, snapshot);
                    }
                    Err(Error::ChangedDuringUpload(_)) => {
                        if let Some(previous) = self.files.get(&path) {
                            seen.insert(path, previous.clone());
                        }
                    }
                    Err(error) => return Err(error),
                }
            } else {
                seen.insert(path, snapshot);
            }
        }

        for path in self.files.keys() {
            if !seen.contains_key(path) {
                put_tombstone(store, source, path).await?;
            }
        }

        self.files = seen;
        Ok(())
    }

    async fn apply_paths(
        &mut self,
        store: &dyn ObjectStore,
        source: &Path,
        paths: Vec<PathBuf>,
    ) -> Result<(), Error> {
        for path in paths {
            if !path.starts_with(source) || temporary_path(&path) {
                continue;
            }
            if path.is_dir() {
                self.rescan(store, source).await?;
            } else if path.is_file() {
                match file_snapshot(&path) {
                    Ok(snapshot) => {
                        if self.files.get(&path) == Some(&snapshot) {
                            continue;
                        }
                        if upload_file(store, source, &path).await.is_ok() {
                            self.files.insert(path, snapshot);
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        self.rescan(store, source).await?;
                    }
                    Err(error) => return Err(error.into()),
                }
            } else {
                self.rescan(store, source).await?;
            }
        }
        Ok(())
    }
}

async fn upload_file(
    store: &dyn ObjectStore,
    source: &Path,
    file_path: &Path,
) -> Result<(), Error> {
    let relative = relative_path(source, file_path)?;
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

async fn put_tombstone(store: &dyn ObjectStore, source: &Path, path: &Path) -> Result<(), Error> {
    let relative = relative_path(source, path)?;
    let manifest_path = object_prefix(relative)?.join("manifest");
    put_manifest(store, &manifest_path, 0, "deleted").await
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
                if !temporary_path(&path) {
                    files.push(path);
                }
                if files.len() > MAX_FILE_COUNT as usize {
                    return Err(Error::TooManyFiles);
                }
            }
        }
    }

    files.sort();
    Ok(files)
}

fn temporary_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    name.starts_with(".#") || name.ends_with('~') || name.ends_with(".swp")
}

fn relative_path<'a>(source: &Path, path: &'a Path) -> Result<&'a Path, Error> {
    path.strip_prefix(source)
        .map_err(|_| Error::InvalidPath(path.to_owned()))
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileSnapshot {
    length: u64,
    modified: SystemTime,
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
    Dotenv(dotenvy::Error),
    InvalidPath(PathBuf),
    Io(io::Error),
    MissingEnvironment(String),
    NonUtf8Path(PathBuf),
    NotDirectory(PathBuf),
    Store(object_store::Error),
    TooManyFiles,
    Watcher(notify::Error),
    WatcherStopped,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChangedDuringUpload(path) => {
                write!(formatter, "{} changed during upload", path.display())
            }
            Self::Dotenv(error) => error.fmt(formatter),
            Self::InvalidPath(path) => {
                write!(formatter, "{} is outside the source", path.display())
            }
            Self::Io(error) => error.fmt(formatter),
            Self::MissingEnvironment(name) => write!(formatter, "set {name}"),
            Self::NonUtf8Path(path) => write!(formatter, "{} is not UTF-8", path.display()),
            Self::NotDirectory(path) => write!(formatter, "{} is not a directory", path.display()),
            Self::Store(error) => error.fmt(formatter),
            Self::TooManyFiles => write!(formatter, "source has more than {MAX_FILE_COUNT} files"),
            Self::Watcher(error) => error.fmt(formatter),
            Self::WatcherStopped => write!(formatter, "file watcher stopped"),
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

impl From<notify::Error> for Error {
    fn from(error: notify::Error) -> Self {
        Self::Watcher(error)
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
                .unwrap()
                .as_nanos();
            let path = env::temp_dir().join(format!("infinitefs-sync-{timestamp}-{count}"));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    async fn manifest(store: &InMemory, relative: &str) -> String {
        let path = object_prefix(Path::new(relative)).unwrap().join("manifest");
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn rescan_uploads_changes_and_tombstones_deletions() {
        let source = TestDirectory::new();
        let file = source.0.join("project/package.json");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, vec![42; CHUNK_SIZE + 1]).unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();

        index.rescan(&store, &source.0).await.unwrap();
        assert_eq!(
            manifest(&store, "project/package.json").await,
            format!(
                "version=1\nsize={}\nchunk_size={CHUNK_SIZE}\nstate=complete\n",
                CHUNK_SIZE + 1
            )
        );

        fs::remove_file(&file).unwrap();
        index.rescan(&store, &source.0).await.unwrap();
        assert_eq!(
            manifest(&store, "project/package.json").await,
            format!("version=1\nsize=0\nchunk_size={CHUNK_SIZE}\nstate=deleted\n")
        );
    }

    #[test]
    fn skips_editor_temporary_files() {
        assert!(temporary_path(Path::new("notes.swp")));
        assert!(temporary_path(Path::new("draft~")));
        assert!(temporary_path(Path::new(".#draft")));
        assert!(!temporary_path(Path::new("package.json")));
    }

    #[cfg(unix)]
    #[test]
    fn skips_symbolic_links() {
        use std::os::unix::fs::symlink;

        let source = TestDirectory::new();
        let file = source.0.join("package.json");
        let link = source.0.join("node_modules/.bin/tool");
        fs::create_dir_all(link.parent().unwrap()).unwrap();
        fs::write(&file, "{}").unwrap();
        symlink(&file, &link).unwrap();

        assert_eq!(regular_files(&source.0).unwrap(), vec![file]);
    }

    #[test]
    fn watcher_emits_a_created_file() {
        let source = TestDirectory::new();
        let (sender, receiver) = mpsc::sync_channel(EVENT_BATCH_CAPACITY);
        let rescan_requested = Arc::new(AtomicBool::new(false));
        let _watcher = create_watcher(&source.0, sender, rescan_requested).unwrap();
        let file = source.0.join("created.txt");

        fs::write(&file, "created").unwrap();

        let paths = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(paths.contains(&file));
    }
}
