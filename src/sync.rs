use crate::vpath::VPath;
use bytes::Bytes;
use futures_util::StreamExt;
use notify::{Config, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, NoCache, new_debouncer_opt};
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjectPath};
use percent_encoding::percent_decode_str;
use std::{
    collections::{HashMap, HashSet},
    env, fmt, fs,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError, TrySendError},
    },
    time::{Duration, Instant, SystemTime},
};

const CHUNK_SIZE: usize = 1024 * 1024;
pub const DEFAULT_TRANSFER_CONCURRENCY: usize = 8;
pub const MAX_TRANSFER_CONCURRENCY: usize = 64;
const DEBOUNCE_DURATION: Duration = Duration::from_millis(200);
const EVENT_BATCH_CAPACITY: usize = 32;
const EVENT_PATH_LIMIT: usize = 4096;
const MAX_FILE_COUNT: u32 = 1_000_000;
const RESCAN_INTERVAL: Duration = Duration::from_secs(5);
static PARTIAL_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
static UPLOAD_GENERATION_COUNTER: AtomicU64 = AtomicU64::new(0);

pub async fn run(directory: &Path, watch: bool, concurrency: usize) -> Result<u32, Error> {
    assert_transfer_concurrency(concurrency);
    load_local_environment()?;
    let source = canonical_source(directory)?;
    let bucket = required_environment("INFINITEFS_S3_BUCKET")?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()?;

    if watch {
        watch_directory(&store, source, concurrency).await
    } else {
        let mut index = SyncIndex::load(&store, &source).await?;
        index
            .rescan_with_concurrency(&store, &source, concurrency)
            .await?;
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

#[derive(Debug, PartialEq, Eq)]
pub struct PullResult {
    pub file_count: u32,
    pub newer_version_available: bool,
}

pub async fn pull(root: &str, destination: &Path, concurrency: usize) -> Result<PullResult, Error> {
    assert_transfer_concurrency(concurrency);
    load_local_environment()?;
    let bucket = required_environment("INFINITEFS_S3_BUCKET")?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()?;
    pull_directory_with_concurrency(&store, root, destination, concurrency).await
}

fn assert_transfer_concurrency(concurrency: usize) {
    assert!(concurrency > 0);
    assert!(concurrency <= MAX_TRANSFER_CONCURRENCY);
}

#[cfg(test)]
async fn pull_directory(
    store: &dyn ObjectStore,
    root: &str,
    destination: &Path,
) -> Result<PullResult, Error> {
    pull_directory_with_concurrency(store, root, destination, DEFAULT_TRANSFER_CONCURRENCY).await
}

async fn pull_directory_with_concurrency(
    store: &dyn ObjectStore,
    root: &str,
    destination: &Path,
    concurrency: usize,
) -> Result<PullResult, Error> {
    assert_transfer_concurrency(concurrency);
    let prefix = ObjectPath::from(root);
    let mut objects = store.list(Some(&prefix));
    let mut manifest_paths = Vec::new();

    while let Some(object) = objects.next().await {
        let object = object?;
        if object.location.filename() == Some("manifest") {
            manifest_paths.push(object.location);
        }
    }

    let mut complete = Vec::new();
    let mut deleted = Vec::new();
    let prefix = &prefix;
    let mut manifests = futures_util::stream::iter(manifest_paths)
        .map(|path| async move {
            let bytes = store.get(&path).await?.bytes().await?;
            let manifest = Manifest::parse(&bytes)?;
            let relative = manifest_relative(prefix, &path)?;
            Ok::<_, Error>((path, manifest, relative))
        })
        .buffer_unordered(concurrency);

    while let Some(manifest) = manifests.next().await {
        let (path, manifest, relative) = manifest?;
        match &manifest.state {
            ManifestState::Complete => complete.push((path, manifest, relative)),
            ManifestState::Deleted => deleted.push(relative),
            ManifestState::Uploading => {}
        }
    }

    let complete_paths = complete
        .iter()
        .map(|(_, _, relative): &(_, _, PathBuf)| relative.as_path())
        .collect::<HashSet<_>>();
    for relative in deleted {
        pull_deleted(destination, &relative)?;
        let mut ancestor = relative.parent();
        while let Some(parent) = ancestor {
            if complete_paths.contains(parent) {
                pull_prune_empty_ancestors(destination, &relative, parent)?;
                break;
            }
            ancestor = parent.parent();
        }
    }
    let mut downloaded = Vec::new();
    let mut pulls = futures_util::stream::iter(complete)
        .map(|(path, manifest, relative)| async move {
            pull_file(store, &path, &manifest, destination, &relative).await?;
            Ok::<_, Error>((path, manifest))
        })
        .buffer_unordered(concurrency);
    while let Some(pull) = pulls.next().await {
        downloaded.push(pull?);
    }
    let file_count = u32::try_from(downloaded.len()).expect("file count is bounded");

    Ok(PullResult {
        file_count,
        newer_version_available: newer_complete_manifests(store, &downloaded, concurrency).await?,
    })
}

async fn newer_complete_manifests(
    store: &dyn ObjectStore,
    downloaded: &[(ObjectPath, Manifest)],
    concurrency: usize,
) -> Result<bool, Error> {
    assert_transfer_concurrency(concurrency);
    let mut manifests = futures_util::stream::iter(downloaded)
        .map(|(path, manifest)| newer_complete_manifest(store, path, manifest))
        .buffer_unordered(concurrency);
    while let Some(newer) = manifests.next().await {
        if newer? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn newer_complete_manifest(
    store: &dyn ObjectStore,
    manifest_path: &ObjectPath,
    downloaded: &Manifest,
) -> Result<bool, Error> {
    let bytes = store.get(manifest_path).await?.bytes().await?;
    let current = Manifest::parse(&bytes)?;
    Ok(matches!(current.state, ManifestState::Complete) && current != *downloaded)
}

async fn pull_file(
    store: &dyn ObjectStore,
    manifest_path: &ObjectPath,
    manifest: &Manifest,
    destination_root: &Path,
    relative: &Path,
) -> Result<(), Error> {
    let (destination, parent) = pull_destination_parent(destination_root, relative)?;
    reject_symbolic_link(&destination)?;

    let (partial_path, mut output) = create_partial_file(&parent)?;
    let partial = PartialFile(partial_path);
    let chunks = manifest.chunk_count()?;
    let prefix = manifest_path
        .parent()
        .ok_or_else(|| Error::InvalidRemotePath(manifest_path.to_string()))?;
    let mut written = 0u64;

    for index in 0..chunks {
        let mut path = prefix.clone().join("chunks");
        if let Some(generation) = &manifest.generation {
            path = path.join(generation.as_str());
        }
        let path = path.join(format!("{index:08}"));
        let bytes = store.get(&path).await?.bytes().await?;
        let expected = manifest.chunk_size.min(manifest.size - written);
        let length = u64::try_from(bytes.len()).map_err(|_| Error::ChunkTooLarge)?;
        if length != expected {
            return Err(Error::InvalidChunk(path.to_string()));
        }
        use std::io::Write;
        output.write_all(&bytes)?;
        written += length;
    }

    if written != manifest.size {
        return Err(Error::InvalidManifest("chunk sizes do not match file size"));
    }
    output.sync_all()?;
    drop(output);
    pull_destination_parent(destination_root, relative)?;
    reject_symbolic_link(&destination)?;
    if fs::symlink_metadata(&destination).is_ok_and(|metadata| metadata.is_dir()) {
        fs::remove_dir(&destination)?;
    }
    fs::rename(&partial.0, destination)?;
    Ok(())
}

fn pull_deleted(destination_root: &Path, relative: &Path) -> Result<(), Error> {
    let Some(destination) = pull_existing_destination(destination_root, relative)? else {
        return Ok(());
    };
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(Error::SymbolicLink(destination));
        }
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => fs::remove_file(destination)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn pull_prune_empty_ancestors(
    destination_root: &Path,
    deleted: &Path,
    replacement: &Path,
) -> Result<(), Error> {
    let mut ancestor = deleted.parent();
    while let Some(relative) = ancestor {
        if relative == replacement {
            break;
        }
        let Some(directory) = pull_existing_destination(destination_root, relative)? else {
            break;
        };
        reject_symbolic_link(&directory)?;
        match fs::remove_dir(&directory) {
            Ok(()) => ancestor = relative.parent(),
            Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn pull_existing_destination(
    destination_root: &Path,
    relative: &Path,
) -> Result<Option<PathBuf>, Error> {
    let mut parent = destination_root.to_owned();
    match fs::symlink_metadata(&parent) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(Error::SymbolicLink(parent));
        }
        Ok(metadata) if !metadata.is_dir() => return Err(Error::NotDirectory(parent)),
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }

    for component in relative.parent().unwrap_or(Path::new("")).components() {
        let Component::Normal(component) = component else {
            return Err(Error::InvalidPath(relative.to_owned()));
        };
        parent.push(component);
        match fs::symlink_metadata(&parent) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Error::SymbolicLink(parent));
            }
            Ok(metadata) if !metadata.is_dir() => return Ok(None),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }

    let file_name = relative
        .file_name()
        .ok_or_else(|| Error::InvalidPath(relative.to_owned()))?;
    Ok(Some(parent.join(file_name)))
}

fn pull_destination_parent(
    destination_root: &Path,
    relative: &Path,
) -> Result<(PathBuf, PathBuf), Error> {
    ensure_directory(destination_root)?;
    let file_name = relative
        .file_name()
        .ok_or_else(|| Error::InvalidPath(relative.to_owned()))?;
    let mut parent = destination_root.to_owned();

    for component in relative.parent().unwrap_or(Path::new("")).components() {
        let Component::Normal(component) = component else {
            return Err(Error::InvalidPath(relative.to_owned()));
        };
        parent.push(component);
        ensure_directory(&parent)?;
    }

    Ok((parent.join(file_name), parent))
}

fn ensure_directory(path: &Path) -> Result<(), Error> {
    let mut missing = Vec::new();
    let mut current = path;

    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Error::SymbolicLink(current.to_owned()));
            }
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => return Err(Error::NotDirectory(current.to_owned())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current.to_owned());
                let Some(parent) = current.parent() else {
                    break;
                };
                if parent.as_os_str().is_empty() {
                    break;
                }
                current = parent;
            }
            Err(error) => return Err(error.into()),
        }
    }

    for directory in missing.iter().rev() {
        match fs::create_dir(directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let metadata = fs::symlink_metadata(directory)?;
        if metadata.file_type().is_symlink() {
            return Err(Error::SymbolicLink(directory.to_owned()));
        }
        if !metadata.is_dir() {
            return Err(Error::NotDirectory(directory.to_owned()));
        }
    }

    Ok(())
}

fn create_partial_file(parent: &Path) -> Result<(PathBuf, fs::File), Error> {
    for _ in 0..1024 {
        let sequence = PARTIAL_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let partial = parent.join(format!(
            ".infinitefs-partial-{}-{sequence}",
            std::process::id()
        ));
        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial)
        {
            Ok(file) => return Ok((partial, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::PartialFileCollision)
}

struct PartialFile(PathBuf);

impl Drop for PartialFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn reject_symbolic_link(path: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(Error::SymbolicLink(path.to_owned()))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Manifest {
    chunk_size: u64,
    generation: Option<String>,
    modified_ns: Option<i128>,
    size: u64,
    state: ManifestState,
}

impl Manifest {
    fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| Error::InvalidManifest("manifest is not UTF-8"))?;
        let mut chunk_size = None;
        let mut generation = None;
        let mut modified_ns = None;
        let mut size = None;
        let mut state = None;

        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            match key {
                "chunk_size" => chunk_size = Some(parse_manifest_number(value)?),
                "generation" => generation = Some(parse_generation(value)?),
                "modified_ns" => modified_ns = Some(i128::from(parse_manifest_number(value)?)),
                "modified_ns_signed" => {
                    modified_ns = Some(
                        value
                            .parse()
                            .map_err(|_| Error::InvalidManifest("manifest timestamp is invalid"))?,
                    )
                }
                "size" => size = Some(parse_manifest_number(value)?),
                "state" => state = Some(ManifestState::parse(value)?),
                _ => {}
            }
        }

        let chunk_size =
            chunk_size.ok_or(Error::InvalidManifest("manifest is missing chunk_size"))?;
        if chunk_size != CHUNK_SIZE as u64 {
            return Err(Error::InvalidManifest("manifest chunk_size is unsupported"));
        }
        Ok(Self {
            chunk_size,
            generation,
            modified_ns,
            size: size.ok_or(Error::InvalidManifest("manifest is missing size"))?,
            state: state.ok_or(Error::InvalidManifest("manifest is missing state"))?,
        })
    }

    fn chunk_count(&self) -> Result<u64, Error> {
        let adjusted = self
            .size
            .checked_add(self.chunk_size - 1)
            .ok_or(Error::ChunkTooLarge)?;
        Ok(adjusted / self.chunk_size)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ManifestState {
    Complete,
    Deleted,
    Uploading,
}

impl ManifestState {
    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "complete" => Ok(Self::Complete),
            "deleted" => Ok(Self::Deleted),
            "uploading" => Ok(Self::Uploading),
            _ => Err(Error::InvalidManifest("manifest state is invalid")),
        }
    }
}

fn manifest_relative(prefix: &ObjectPath, manifest: &ObjectPath) -> Result<PathBuf, Error> {
    let manifest_parent = manifest
        .parent()
        .ok_or_else(|| Error::InvalidRemotePath(manifest.to_string()))?;
    let parts = manifest_parent
        .prefix_match(prefix)
        .ok_or_else(|| Error::InvalidRemotePath(manifest.to_string()))?;
    let mut relative = PathBuf::new();

    for part in parts {
        let part = percent_decode_str(part.as_ref())
            .decode_utf8()
            .map_err(|_| Error::InvalidRemotePath(manifest.to_string()))?;
        if part.is_empty() || part == "." || part == ".." || part.contains(['/', '\\', ':']) {
            return Err(Error::InvalidRemotePath(manifest.to_string()));
        }
        relative.push(part.as_ref());
    }

    if relative.as_os_str().is_empty() {
        return Err(Error::InvalidRemotePath(manifest.to_string()));
    }
    Ok(relative)
}

fn parse_manifest_number(value: &str) -> Result<u64, Error> {
    value
        .parse()
        .map_err(|_| Error::InvalidManifest("manifest number is invalid"))
}

fn parse_generation(value: &str) -> Result<String, Error> {
    let path = VPath::parse(value).map_err(|_| Error::InvalidManifest("generation is invalid"))?;
    if path.components().len() == 1 {
        Ok(value.to_owned())
    } else {
        Err(Error::InvalidManifest("generation is invalid"))
    }
}

async fn watch_directory(
    store: &dyn ObjectStore,
    source: PathBuf,
    concurrency: usize,
) -> Result<u32, Error> {
    assert_transfer_concurrency(concurrency);
    let (sender, receiver) = mpsc::sync_channel(EVENT_BATCH_CAPACITY);
    let rescan_requested = Arc::new(AtomicBool::new(false));
    let _debouncer = create_watcher(&source, sender, Arc::clone(&rescan_requested))?;
    let mut index = SyncIndex::load(store, &source).await?;
    rescan_watcher(&mut index, store, &source, concurrency).await?;
    let mut rescan_at = Instant::now() + RESCAN_INTERVAL;

    loop {
        if rescan_requested.swap(false, Ordering::AcqRel) || Instant::now() >= rescan_at {
            rescan_watcher(&mut index, store, &source, concurrency).await?;
            rescan_at = Instant::now() + RESCAN_INTERVAL;
        }

        match receiver.recv_timeout(Duration::from_millis(250)) {
            Ok(paths) => match index.apply_paths(store, &source, paths).await {
                Err(Error::ChangedDuringUpload(_)) => {}
                result => result?,
            },
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Err(Error::WatcherStopped),
        }
    }
}

async fn rescan_watcher(
    index: &mut SyncIndex,
    store: &dyn ObjectStore,
    source: &Path,
    concurrency: usize,
) -> Result<(), Error> {
    match index
        .rescan_with_concurrency(store, source, concurrency)
        .await
    {
        Err(Error::ChangedDuringUpload(_)) => Ok(()),
        result => result,
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
    remote_files: HashSet<PathBuf>,
}

impl SyncIndex {
    #[cfg(test)]
    fn new() -> Self {
        Self {
            files: HashMap::new(),
            remote_files: HashSet::new(),
        }
    }

    async fn load(store: &dyn ObjectStore, source: &Path) -> Result<Self, Error> {
        let root = source
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::NonUtf8Path(source.to_owned()))?;
        let prefix = ObjectPath::from(root);
        let mut objects = store.list(Some(&prefix));
        let mut files = HashMap::new();
        let mut remote_files = HashSet::new();

        while let Some(object) = objects.next().await {
            let object = object?;
            if object.location.filename() != Some("manifest") {
                continue;
            }
            let manifest = Manifest::parse(&store.get(&object.location).await?.bytes().await?)?;
            if matches!(manifest.state, ManifestState::Complete) {
                let path = source.join(manifest_relative(&prefix, &object.location)?);
                if let Ok(snapshot) = file_snapshot(&path)
                    && snapshot.length == manifest.size
                    && manifest.modified_ns == Some(snapshot.modified_ns)
                {
                    files.insert(path.clone(), snapshot);
                }
                remote_files.insert(path);
            }
        }

        Ok(Self {
            files,
            remote_files,
        })
    }

    fn file_count(&self) -> u32 {
        u32::try_from(self.files.len()).expect("file count is bounded")
    }

    async fn rescan(&mut self, store: &dyn ObjectStore, source: &Path) -> Result<(), Error> {
        self.rescan_with_concurrency(store, source, DEFAULT_TRANSFER_CONCURRENCY)
            .await
    }

    async fn rescan_with_concurrency(
        &mut self,
        store: &dyn ObjectStore,
        source: &Path,
        concurrency: usize,
    ) -> Result<(), Error> {
        assert_transfer_concurrency(concurrency);
        let current = regular_files(source)?;
        let mut seen = HashMap::new();
        let mut changed_paths = HashSet::new();
        let mut changed_path = None;
        let mut uploads = Vec::new();

        for path in current {
            let snapshot = match file_snapshot(&path) {
                Ok(snapshot) => snapshot,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if self.files.get(&path) != Some(&snapshot) {
                uploads.push((path, snapshot));
            } else {
                seen.insert(path, snapshot);
            }
        }

        let mut uploads = futures_util::stream::iter(uploads)
            .map(|(path, snapshot)| async move {
                let result = upload_file(store, source, &path).await;
                (path, snapshot, result)
            })
            .buffer_unordered(concurrency);
        while let Some((path, snapshot, result)) = uploads.next().await {
            match result {
                Ok(()) => {
                    seen.insert(path, snapshot);
                }
                Err(Error::ChangedDuringUpload(_)) => {
                    changed_paths.insert(path.clone());
                    changed_path.get_or_insert(path);
                }
                Err(error) => return Err(error),
            }
        }

        let previous = self
            .remote_files
            .iter()
            .chain(self.files.keys())
            .collect::<HashSet<_>>();
        for path in previous {
            if !seen.contains_key(path) && !changed_paths.contains(path) {
                if source_path_has_link(source, path)? {
                    changed_paths.insert(path.clone());
                    continue;
                }
                put_tombstone(store, source, path).await?;
            }
        }

        self.remote_files = seen.keys().chain(changed_paths.iter()).cloned().collect();
        self.files = seen;
        match changed_path {
            Some(path) => Err(Error::ChangedDuringUpload(path)),
            None => Ok(()),
        }
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
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    self.rescan(store, source).await?;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if !metadata.is_file() {
                self.rescan(store, source).await?;
            } else {
                if source_path_has_link(source, &path)? {
                    continue;
                }
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
    let prefix = object_prefix(source, relative)?;
    let manifest_path = prefix.clone().join("manifest");
    let generation = upload_generation();
    if source_path_has_link(source, file_path)? {
        return Err(Error::ChangedDuringUpload(file_path.to_owned()));
    }
    let snapshot_before = match file_snapshot(file_path) {
        Ok(snapshot) => snapshot,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(Error::ChangedDuringUpload(file_path.to_owned()));
        }
        Err(error) => return Err(error.into()),
    };
    let mut reader = match fs::File::open(file_path) {
        Ok(reader) => reader,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(Error::ChangedDuringUpload(file_path.to_owned()));
        }
        Err(error) => return Err(error.into()),
    };
    let mut buffer = vec![0; CHUNK_SIZE];
    let mut chunk_index = 0u64;
    let mut confirmed_size = 0u64;

    put_manifest(
        store,
        &manifest_path,
        confirmed_size,
        "uploading",
        Some(&generation),
        Some(&snapshot_before),
    )
    .await?;

    loop {
        let byte_count = read_chunk(&mut reader, &mut buffer)?;
        if byte_count == 0 {
            break;
        }

        let chunk_path = prefix
            .clone()
            .join("chunks")
            .join(generation.as_str())
            .join(format!("{chunk_index:08}"));
        store
            .put(
                &chunk_path,
                Bytes::copy_from_slice(&buffer[..byte_count]).into(),
            )
            .await?;
        confirmed_size += u64::try_from(byte_count).expect("chunk size fits in u64");
        put_manifest(
            store,
            &manifest_path,
            confirmed_size,
            "uploading",
            Some(&generation),
            Some(&snapshot_before),
        )
        .await?;
        chunk_index += 1;
    }

    let snapshot_after = match file_snapshot(file_path) {
        Ok(snapshot) => snapshot,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(Error::ChangedDuringUpload(file_path.to_owned()));
        }
        Err(error) => return Err(error.into()),
    };
    if snapshot_after != snapshot_before {
        return Err(Error::ChangedDuringUpload(file_path.to_owned()));
    }

    put_manifest(
        store,
        &manifest_path,
        confirmed_size,
        "complete",
        Some(&generation),
        Some(&snapshot_before),
    )
    .await?;
    println!("Synced {} ({confirmed_size} bytes).", relative.display());
    Ok(())
}

async fn put_tombstone(store: &dyn ObjectStore, source: &Path, path: &Path) -> Result<(), Error> {
    let relative = relative_path(source, path)?;
    let manifest_path = object_prefix(source, relative)?.join("manifest");
    put_manifest(store, &manifest_path, 0, "deleted", None, None).await
}

async fn put_manifest(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    confirmed_size: u64,
    state: &str,
    generation: Option<&str>,
    snapshot: Option<&FileSnapshot>,
) -> Result<(), Error> {
    let generation = generation
        .map(|value| format!("generation={value}\n"))
        .unwrap_or_default();
    let modified_ns = snapshot
        .map(|value| match u64::try_from(value.modified_ns) {
            Ok(timestamp) => format!("modified_ns={timestamp}\n"),
            Err(_) => format!("modified_ns_signed={}\n", value.modified_ns),
        })
        .unwrap_or_default();
    let manifest = format!(
        "version=2\nsize={confirmed_size}\nchunk_size={CHUNK_SIZE}\n{generation}{modified_ns}state={state}\n"
    );
    store.put(path, manifest.into()).await?;
    Ok(())
}

fn upload_generation() -> String {
    let sequence = UPLOAD_GENERATION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let elapsed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch");
    format!("{}-{}-{sequence}", elapsed.as_nanos(), std::process::id())
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
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
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

fn source_path_has_link(source: &Path, path: &Path) -> Result<bool, Error> {
    let relative = relative_path(source, path)?;
    let mut current = source.to_owned();
    if fs::symlink_metadata(&current)?.file_type().is_symlink() {
        return Ok(true);
    }
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(Error::InvalidPath(path.to_owned()));
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Ok(true),
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(false)
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

fn object_prefix(source: &Path, relative: &Path) -> Result<ObjectPath, Error> {
    let root = source
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::NonUtf8Path(source.to_owned()))?;
    let mut prefix = ObjectPath::from(root);

    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(Error::InvalidPath(relative.to_owned()));
        };
        let component = component
            .to_str()
            .ok_or_else(|| Error::NonUtf8Path(relative.to_owned()))?;
        prefix = prefix.join(component);
    }

    Ok(prefix)
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
    modified_ns: i128,
}

fn file_snapshot(path: &Path) -> io::Result<FileSnapshot> {
    let metadata = fs::metadata(path)?;
    let modified_ns = match metadata.modified()?.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos().try_into(),
        Err(error) => i128::try_from(error.duration().as_nanos()).map(|value| -value),
    }
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file time is too large"))?;
    Ok(FileSnapshot {
        length: metadata.len(),
        modified_ns,
    })
}

#[derive(Debug)]
pub enum Error {
    ChangedDuringUpload(PathBuf),
    ChunkTooLarge,
    Dotenv(dotenvy::Error),
    InvalidChunk(String),
    InvalidManifest(&'static str),
    InvalidPath(PathBuf),
    InvalidRemotePath(String),
    Io(io::Error),
    MissingEnvironment(String),
    NonUtf8Path(PathBuf),
    NotDirectory(PathBuf),
    PartialFileCollision,
    Store(object_store::Error),
    SymbolicLink(PathBuf),
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
            Self::ChunkTooLarge => write!(formatter, "chunk count is too large"),
            Self::Dotenv(error) => error.fmt(formatter),
            Self::InvalidChunk(path) => write!(formatter, "invalid chunk: {path}"),
            Self::InvalidManifest(message) => write!(formatter, "invalid manifest: {message}"),
            Self::InvalidPath(path) => {
                write!(formatter, "{} is outside the source", path.display())
            }
            Self::InvalidRemotePath(path) => write!(formatter, "invalid remote path: {path}"),
            Self::Io(error) => error.fmt(formatter),
            Self::MissingEnvironment(name) => write!(formatter, "set {name}"),
            Self::NonUtf8Path(path) => write!(formatter, "{} is not UTF-8", path.display()),
            Self::NotDirectory(path) => write!(formatter, "{} is not a directory", path.display()),
            Self::PartialFileCollision => {
                write!(formatter, "could not create a unique partial file")
            }
            Self::Store(error) => error.fmt(formatter),
            Self::SymbolicLink(path) => {
                write!(formatter, "refusing symbolic link: {}", path.display())
            }
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
    use object_store::{
        memory::InMemory,
        throttle::{ThrottleConfig, ThrottledStore},
    };
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

    async fn manifest(store: &InMemory, source: &Path, relative: &str) -> String {
        let path = object_prefix(source, Path::new(relative))
            .unwrap()
            .join("manifest");
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    struct TransferTiming {
        pull: Duration,
        push: Duration,
    }

    async fn time_transfer(source: &Path, concurrency: usize) -> TransferTiming {
        let store = ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig {
                wait_get_per_call: Duration::from_millis(20),
                wait_put_per_call: Duration::from_millis(20),
                ..Default::default()
            },
        );
        let mut index = SyncIndex::new();
        let push_started = Instant::now();
        index
            .rescan_with_concurrency(&store, source, concurrency)
            .await
            .unwrap();
        let push = push_started.elapsed();

        let destination = TestDirectory::new();
        let root = source.file_name().unwrap().to_str().unwrap();
        let pull_started = Instant::now();
        let result = pull_directory_with_concurrency(&store, root, &destination.0, concurrency)
            .await
            .unwrap();
        let pull = pull_started.elapsed();

        assert_eq!(result.file_count, 24);
        for index in 0..24 {
            let name = format!("nested/file-{index:02}.bin");
            assert_eq!(
                fs::read(destination.0.join(&name)).unwrap(),
                name.as_bytes()
            );
        }

        TransferTiming { pull, push }
    }

    #[tokio::test]
    #[ignore = "reports transfer timing for each concurrency limit"]
    async fn transfer_benchmark_reports_transfer_window_scaling() {
        let source = TestDirectory::new();
        for index in 0..24 {
            let name = format!("nested/file-{index:02}.bin");
            let path = source.0.join(&name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, name).unwrap();
        }

        let serial = time_transfer(&source.0, 1).await;
        for concurrency in [2, DEFAULT_TRANSFER_CONCURRENCY, 16] {
            let timing = time_transfer(&source.0, concurrency).await;
            println!(
                "concurrency={concurrency}: push={:.3}s pull={:.3}s",
                timing.push.as_secs_f64(),
                timing.pull.as_secs_f64(),
            );
            assert!(timing.push < serial.push);
            assert!(timing.pull < serial.pull);
        }
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
        let root = source.0.file_name().unwrap().to_str().unwrap();
        assert_eq!(
            object_prefix(&source.0, Path::new("project/package.json"))
                .unwrap()
                .as_ref(),
            format!("{root}/project/package.json")
        );
        let uploaded_manifest = manifest(&store, &source.0, "project/package.json").await;
        assert!(uploaded_manifest.contains("version=2\n"));
        assert!(uploaded_manifest.contains(&format!("size={}\n", CHUNK_SIZE + 1)));
        assert!(uploaded_manifest.contains("generation="));
        assert!(uploaded_manifest.ends_with("state=complete\n"));

        fs::remove_file(&file).unwrap();
        index.rescan(&store, &source.0).await.unwrap();
        let root = source.0.file_name().unwrap().to_str().unwrap();
        assert_eq!(
            object_prefix(&source.0, Path::new("project/package.json"))
                .unwrap()
                .as_ref(),
            format!("{root}/project/package.json")
        );
        assert_eq!(
            manifest(&store, &source.0, "project/package.json").await,
            format!("version=2\nsize=0\nchunk_size={CHUNK_SIZE}\nstate=deleted\n")
        );
    }

    #[tokio::test]
    async fn rescan_tombstones_files_deleted_while_stopped() {
        let source = TestDirectory::new();
        let file = source.0.join("package.json");
        fs::write(&file, "{}").unwrap();
        let store = InMemory::new();
        SyncIndex::new().rescan(&store, &source.0).await.unwrap();
        fs::remove_file(&file).unwrap();

        let mut index = SyncIndex::load(&store, &source.0).await.unwrap();
        index.rescan(&store, &source.0).await.unwrap();

        assert_eq!(
            manifest(&store, &source.0, "package.json").await,
            format!("version=2\nsize=0\nchunk_size={CHUNK_SIZE}\nstate=deleted\n")
        );
    }

    #[tokio::test]
    async fn restart_does_not_reupload_unchanged_files() {
        let source = TestDirectory::new();
        fs::write(source.0.join("package.json"), "{}").unwrap();
        let store = InMemory::new();
        SyncIndex::new().rescan(&store, &source.0).await.unwrap();
        let before = manifest(&store, &source.0, "package.json").await;

        let mut index = SyncIndex::load(&store, &source.0).await.unwrap();
        index.rescan(&store, &source.0).await.unwrap();

        assert_eq!(manifest(&store, &source.0, "package.json").await, before);
    }

    #[tokio::test]
    async fn syncs_files_dated_before_the_unix_epoch() {
        let source = TestDirectory::new();
        let file = source.0.join("archive.txt");
        fs::write(&file, "old").unwrap();
        let time = SystemTime::UNIX_EPOCH - Duration::from_secs(1);
        fs::File::open(&file)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(time))
            .unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();

        index.rescan(&store, &source.0).await.unwrap();
        let before = manifest(&store, &source.0, "archive.txt").await;
        assert!(before.contains("modified_ns_signed=-1000000000\n"));
        assert!(!before.contains("modified_ns=-"));

        let mut restarted = SyncIndex::load(&store, &source.0).await.unwrap();
        restarted.rescan(&store, &source.0).await.unwrap();
        assert_eq!(manifest(&store, &source.0, "archive.txt").await, before);
    }

    #[test]
    fn manifest_accepts_signed_and_legacy_large_timestamps() {
        for (key, timestamp) in [
            ("modified_ns_signed", "-1000000000"),
            ("modified_ns", "18446744073709551615"),
        ] {
            let text = format!(
                "version=2\nsize=1\nchunk_size={CHUNK_SIZE}\n{key}={timestamp}\nstate=complete\n"
            );
            assert_eq!(
                Manifest::parse(text.as_bytes()).unwrap().modified_ns,
                Some(timestamp.parse::<i128>().unwrap())
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn watcher_does_not_upload_symbolic_links() {
        use std::os::unix::fs::symlink;

        let source = TestDirectory::new();
        let outside = TestDirectory::new();
        fs::write(outside.0.join("secret.txt"), "secret").unwrap();
        let link = source.0.join("linked.txt");
        symlink(outside.0.join("secret.txt"), &link).unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();

        index
            .apply_paths(&store, &source.0, vec![link])
            .await
            .unwrap();

        let path = object_prefix(&source.0, Path::new("linked.txt"))
            .unwrap()
            .join("manifest");
        assert!(store.get(&path).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn watcher_ignores_files_beneath_symbolic_linked_directories() {
        use std::os::unix::fs::symlink;

        let source = TestDirectory::new();
        let outside = TestDirectory::new();
        fs::write(outside.0.join("secret.txt"), "secret").unwrap();
        symlink(&outside.0, source.0.join("linked")).unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();

        index
            .apply_paths(&store, &source.0, vec![source.0.join("linked/secret.txt")])
            .await
            .unwrap();

        let path = object_prefix(&source.0, Path::new("linked/secret.txt"))
            .unwrap()
            .join("manifest");
        assert!(store.get(&path).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn watcher_link_event_does_not_tombstone_existing_file() {
        use std::os::unix::fs::symlink;

        let source = TestDirectory::new();
        let outside = TestDirectory::new();
        let file = source.0.join("nested/file");
        fs::create_dir(file.parent().unwrap()).unwrap();
        fs::write(&file, "original").unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();
        index.rescan(&store, &source.0).await.unwrap();
        let previous = manifest(&store, &source.0, "nested/file").await;

        fs::rename(source.0.join("nested"), outside.0.join("saved")).unwrap();
        symlink(outside.0.join("saved"), source.0.join("nested")).unwrap();
        index
            .apply_paths(&store, &source.0, vec![file])
            .await
            .unwrap();

        index.rescan(&store, &source.0).await.unwrap();
        assert_eq!(manifest(&store, &source.0, "nested/file").await, previous);
    }

    #[test]
    fn regular_files_tolerates_a_vanished_directory() {
        let source = TestDirectory::new();
        let vanished = source.0.join("vanished");

        assert!(regular_files(&vanished).unwrap().is_empty());
    }

    #[tokio::test]
    async fn pulls_complete_files_into_a_second_directory() {
        let source = TestDirectory::new();
        let file = source.0.join("nested/hello.txt");
        let contents = b"Hello from InfiniteFS.\n";
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, contents).unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();
        index.rescan(&store, &source.0).await.unwrap();

        let destination = TestDirectory::new();
        let root = source.0.file_name().unwrap().to_str().unwrap();
        assert_eq!(
            pull_directory(&store, root, &destination.0).await.unwrap(),
            PullResult {
                file_count: 1,
                newer_version_available: false,
            }
        );
        assert_eq!(
            fs::read(destination.0.join("nested/hello.txt")).unwrap(),
            contents
        );
        assert!(
            !destination
                .0
                .join("nested/hello.txt.infinitefs-partial")
                .exists()
        );
    }

    #[tokio::test]
    async fn pulls_dotfiles_into_nested_destinations() {
        let source = TestDirectory::new();
        let file = source.0.join(".gitignore");
        fs::write(&file, "target\n").unwrap();
        let store = InMemory::new();
        SyncIndex::new().rescan(&store, &source.0).await.unwrap();

        let destination = TestDirectory::new().0.join("missing/parents/root");
        let root = source.0.file_name().unwrap().to_str().unwrap();
        pull_directory(&store, root, &destination).await.unwrap();

        assert_eq!(
            fs::read_to_string(destination.join(".gitignore")).unwrap(),
            "target\n"
        );
        assert!(SyncIndex::load(&store, &source.0).await.is_ok());
    }

    #[tokio::test]
    async fn pulls_maximum_length_filenames() {
        let source = TestDirectory::new();
        let name = "a".repeat(255);
        let file = source.0.join(&name);
        fs::write(&file, "contents").unwrap();
        let store = InMemory::new();
        SyncIndex::new().rescan(&store, &source.0).await.unwrap();

        let destination = TestDirectory::new();
        let root = source.0.file_name().unwrap().to_str().unwrap();
        pull_directory(&store, root, &destination.0).await.unwrap();

        assert_eq!(
            fs::read_to_string(destination.0.join(name)).unwrap(),
            "contents"
        );
    }

    #[tokio::test]
    async fn pull_replaces_directories_with_files() {
        let source = TestDirectory::new();
        fs::create_dir_all(source.0.join("a/b")).unwrap();
        fs::write(source.0.join("a/b/c"), "old").unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();
        index.rescan(&store, &source.0).await.unwrap();
        let destination = TestDirectory::new();
        let root = source.0.file_name().unwrap().to_str().unwrap();
        pull_directory(&store, root, &destination.0).await.unwrap();

        fs::remove_dir_all(source.0.join("a")).unwrap();
        fs::write(source.0.join("a"), "remote file").unwrap();
        index.rescan(&store, &source.0).await.unwrap();
        pull_directory(&store, root, &destination.0).await.unwrap();

        assert_eq!(
            fs::read_to_string(destination.0.join("a")).unwrap(),
            "remote file"
        );
    }

    #[tokio::test]
    async fn pull_keeps_unrelated_local_directory_files() {
        let source = TestDirectory::new();
        fs::write(source.0.join("a"), "remote file").unwrap();
        let store = InMemory::new();
        SyncIndex::new().rescan(&store, &source.0).await.unwrap();

        let destination = TestDirectory::new();
        fs::create_dir(destination.0.join("a")).unwrap();
        fs::write(destination.0.join("a/notes.txt"), "local").unwrap();
        let root = source.0.file_name().unwrap().to_str().unwrap();

        assert!(pull_directory(&store, root, &destination.0).await.is_err());
        assert_eq!(
            fs::read_to_string(destination.0.join("a/notes.txt")).unwrap(),
            "local"
        );
    }

    #[tokio::test]
    async fn failed_pull_removes_its_partial_file() {
        let source = TestDirectory::new();
        fs::write(source.0.join("a"), "remote file").unwrap();
        let store = InMemory::new();
        SyncIndex::new().rescan(&store, &source.0).await.unwrap();
        let manifest_path = object_prefix(&source.0, Path::new("a"))
            .unwrap()
            .join("manifest");
        let manifest = Manifest::parse(
            &store
                .get(&manifest_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        let chunk_path = manifest_path
            .parent()
            .unwrap()
            .join("chunks")
            .join(manifest.generation.as_deref().unwrap())
            .join("00000000");
        store.delete(&chunk_path).await.unwrap();

        let destination = TestDirectory::new();
        assert!(
            pull_file(
                &store,
                &manifest_path,
                &manifest,
                &destination.0,
                Path::new("a")
            )
            .await
            .is_err()
        );
        assert!(fs::read_dir(&destination.0).unwrap().next().is_none());
    }

    #[tokio::test]
    async fn pull_uses_the_generation_named_by_its_manifest() {
        let source = TestDirectory::new();
        let file = source.0.join("hello.txt");
        fs::write(&file, "first").unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();
        index.rescan(&store, &source.0).await.unwrap();
        let manifest_path = object_prefix(&source.0, Path::new("hello.txt"))
            .unwrap()
            .join("manifest");
        let first_manifest = Manifest::parse(
            &store
                .get(&manifest_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        fs::write(&file, "second-version").unwrap();
        index.rescan(&store, &source.0).await.unwrap();

        let destination = TestDirectory::new();
        pull_file(
            &store,
            &manifest_path,
            &first_manifest,
            &destination.0,
            Path::new("hello.txt"),
        )
        .await
        .unwrap();

        assert_eq!(
            fs::read_to_string(destination.0.join("hello.txt")).unwrap(),
            "first"
        );
    }

    #[tokio::test]
    async fn detects_a_newer_complete_generation_after_pull() {
        let source = TestDirectory::new();
        let file = source.0.join("hello.txt");
        fs::write(&file, "first").unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();
        index.rescan(&store, &source.0).await.unwrap();
        let manifest_path = object_prefix(&source.0, Path::new("hello.txt"))
            .unwrap()
            .join("manifest");
        let downloaded = Manifest::parse(
            &store
                .get(&manifest_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(
            !newer_complete_manifest(&store, &manifest_path, &downloaded)
                .await
                .unwrap()
        );

        fs::write(&file, "second-version").unwrap();
        index.rescan(&store, &source.0).await.unwrap();

        assert!(
            newer_complete_manifest(&store, &manifest_path, &downloaded)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn checks_every_downloaded_manifest_after_the_pull() {
        let source = TestDirectory::new();
        fs::write(source.0.join("a.txt"), "first").unwrap();
        fs::write(source.0.join("b.txt"), "later").unwrap();
        let store = InMemory::new();
        let mut index = SyncIndex::new();
        index.rescan(&store, &source.0).await.unwrap();
        let a_path = object_prefix(&source.0, Path::new("a.txt"))
            .unwrap()
            .join("manifest");
        let b_path = object_prefix(&source.0, Path::new("b.txt"))
            .unwrap()
            .join("manifest");
        let a_manifest =
            Manifest::parse(&store.get(&a_path).await.unwrap().bytes().await.unwrap()).unwrap();
        let b_manifest =
            Manifest::parse(&store.get(&b_path).await.unwrap().bytes().await.unwrap()).unwrap();

        fs::write(source.0.join("a.txt"), "second-version").unwrap();
        index.rescan(&store, &source.0).await.unwrap();

        assert!(
            newer_complete_manifests(
                &store,
                &[(a_path, a_manifest), (b_path, b_manifest)],
                DEFAULT_TRANSFER_CONCURRENCY,
            )
            .await
            .unwrap()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pull_rejects_symbolic_link_ancestors() {
        use std::os::unix::fs::symlink;

        let source = TestDirectory::new();
        let file = source.0.join("nested/hello.txt");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, "remote").unwrap();
        let store = InMemory::new();
        SyncIndex::new().rescan(&store, &source.0).await.unwrap();

        let destination = TestDirectory::new();
        let outside = TestDirectory::new();
        symlink(&outside.0, destination.0.join("nested")).unwrap();
        let root = source.0.file_name().unwrap().to_str().unwrap();

        assert!(matches!(
            pull_directory(&store, root, &destination.0).await,
            Err(Error::SymbolicLink(_))
        ));
        assert!(!outside.0.join("hello.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn pull_delete_rejects_symbolic_link_ancestors() {
        use std::os::unix::fs::symlink;

        let destination = TestDirectory::new();
        let outside = TestDirectory::new();
        let outside_file = outside.0.join("delete-me.txt");
        fs::write(&outside_file, "keep").unwrap();
        symlink(&outside.0, destination.0.join("nested")).unwrap();

        assert!(matches!(
            pull_deleted(&destination.0, Path::new("nested/delete-me.txt")),
            Err(Error::SymbolicLink(_))
        ));
        assert_eq!(fs::read_to_string(outside_file).unwrap(), "keep");
    }

    #[test]
    fn pull_delete_keeps_a_replacement_directory() {
        let destination = TestDirectory::new();
        let replacement = destination.0.join("a");
        fs::create_dir_all(&replacement).unwrap();
        fs::write(replacement.join("b"), "remote child").unwrap();

        pull_deleted(&destination.0, Path::new("a")).unwrap();

        assert_eq!(
            fs::read_to_string(replacement.join("b")).unwrap(),
            "remote child"
        );
    }

    #[test]
    fn manifest_relative_rejects_windows_prefixes() {
        let prefix = ObjectPath::from("root");
        let manifest = ObjectPath::from("root/C:/victim.txt/manifest");

        assert!(manifest_relative(&prefix, &manifest).is_err());
    }

    #[test]
    fn manifest_relative_accepts_dotfiles() {
        let prefix = ObjectPath::from("root");
        let manifest = ObjectPath::from("root/.gitignore/manifest");

        assert_eq!(
            manifest_relative(&prefix, &manifest).unwrap(),
            Path::new(".gitignore")
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
