use crate::sync::{Manifest, ManifestState};
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

const CACHE_LIMIT: usize = 16 * 1024 * 1024;
const MAX_MANIFEST_SIZE: usize = 1024 * 1024;
const GET_CONCURRENCY: usize = 4;
const MAX_IN_FLIGHT_CHUNKS: usize = 8;
const MAX_COALESCED_WAITERS_PER_CHUNK: usize = 16;

#[derive(Debug, Clone)]
pub enum ReaderError {
    MissingManifest,
    ManifestTooLarge,
    Deleted,
    Incomplete,
    InvalidManifest(String),
    MissingChunk(String),
    InvalidChunk(String),
    /// Per-reader capacity is full; retry after other reads make progress.
    TooManyPendingRequests,
    Store(Arc<object_store::Error>),
}

impl fmt::Display for ReaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingManifest => write!(formatter, "remote manifest not found"),
            Self::ManifestTooLarge => {
                write!(
                    formatter,
                    "remote manifest exceeds {MAX_MANIFEST_SIZE} bytes"
                )
            }
            Self::Deleted => write!(formatter, "remote file is deleted"),
            Self::Incomplete => write!(formatter, "remote file upload is incomplete"),
            Self::InvalidManifest(message) => write!(formatter, "invalid manifest: {message}"),
            Self::MissingChunk(path) => write!(formatter, "remote chunk not found: {path}"),
            Self::InvalidChunk(path) => write!(formatter, "invalid remote chunk: {path}"),
            Self::TooManyPendingRequests => write!(formatter, "too many pending remote reads"),
            Self::Store(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ReaderError {}

struct Flight {
    updates: watch::Sender<FlightState>,
    waiters: AtomicUsize,
    _admission: OwnedSemaphorePermit,
}

#[derive(Clone)]
enum FlightState {
    Pending,
    Retry,
    Finished(Result<Bytes, ReaderError>),
}

struct FlightOwner {
    cache: Arc<Mutex<CacheState>>,
    index: u64,
    flight: Arc<Flight>,
    completed: bool,
    #[cfg(test)]
    test_changed: Arc<tokio::sync::Notify>,
}

impl FlightOwner {
    fn finish(mut self, result: Result<Bytes, ReaderError>) -> Result<Bytes, ReaderError> {
        {
            let mut state = lock_state(&self.cache);
            if state
                .flights
                .get(&self.index)
                .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
            {
                state.flights.remove(&self.index);
            }
            if let Ok(bytes) = &result {
                insert_cache(&mut state, self.index, bytes.clone());
            }
        }
        self.flight
            .updates
            .send_replace(FlightState::Finished(result.clone()));
        #[cfg(test)]
        self.test_changed.notify_waiters();
        self.completed = true;
        result
    }
}

impl Drop for FlightOwner {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        {
            let mut state = lock_state(&self.cache);
            if state
                .flights
                .get(&self.index)
                .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
            {
                state.flights.remove(&self.index);
            }
        }
        self.flight.updates.send_replace(FlightState::Retry);
        #[cfg(test)]
        self.test_changed.notify_waiters();
    }
}

struct FlightWaiter(Arc<Flight>);

impl FlightWaiter {
    fn try_new(flight: Arc<Flight>) -> Option<Self> {
        flight
            .waiters
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < MAX_COALESCED_WAITERS_PER_CHUNK).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(flight))
    }
}

impl Drop for FlightWaiter {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
struct TestCounterGuard {
    counter: Arc<AtomicUsize>,
    changed: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl TestCounterGuard {
    fn new(counter: Arc<AtomicUsize>, changed: Arc<tokio::sync::Notify>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        changed.notify_waiters();
        Self { counter, changed }
    }
}

#[cfg(test)]
impl Drop for TestCounterGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
struct ActiveGetGuard {
    active: Arc<AtomicUsize>,
    changed: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl Drop for ActiveGetGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[derive(Default)]
struct CacheState {
    chunks: HashMap<u64, Bytes>,
    order: VecDeque<u64>,
    flights: HashMap<u64, Arc<Flight>>,
    bytes: usize,
}

#[derive(Clone)]
/// Reads ranges from a completed remote file.
///
/// Concurrency limits are per reader instance and shared by its clones: at most four
/// chunk GETs run at once, eight unique chunk fetches may be in flight, and each
/// fetch may have at most sixteen coalesced waiters. Excess requests fail with
/// [`ReaderError::TooManyPendingRequests`]. Separate readers have separate limits.
pub struct RemoteFileReader {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    manifest: Arc<Manifest>,
    cache: Arc<Mutex<CacheState>>,
    permits: Arc<Semaphore>,
    pending: Arc<Semaphore>,
    #[cfg(test)]
    get_count: Arc<AtomicUsize>,
    #[cfg(test)]
    active_gets: Arc<AtomicUsize>,
    #[cfg(test)]
    max_active_gets: Arc<AtomicUsize>,
    #[cfg(test)]
    waiting_gets: Arc<AtomicUsize>,
    #[cfg(test)]
    fetch_gates: Arc<Mutex<HashMap<u64, Arc<Semaphore>>>>,
    #[cfg(test)]
    body_gates: Arc<Mutex<HashMap<u64, Arc<Semaphore>>>>,
    #[cfg(test)]
    body_reads_started: Arc<AtomicUsize>,
    #[cfg(test)]
    waiter_gate: Arc<Mutex<Option<Arc<Semaphore>>>>,
    #[cfg(test)]
    paused_waiters: Arc<AtomicUsize>,
    #[cfg(test)]
    test_changed: Arc<tokio::sync::Notify>,
}

impl RemoteFileReader {
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        manifest_path: ObjectPath,
    ) -> Result<Self, ReaderError> {
        let result = store.get(&manifest_path).await.map_err(|error| {
            if matches!(error, object_store::Error::NotFound { .. }) {
                ReaderError::MissingManifest
            } else {
                ReaderError::Store(Arc::new(error))
            }
        })?;
        let bytes = read_manifest_body(result).await?;
        let manifest = Manifest::parse(&bytes)
            .map_err(|error| ReaderError::InvalidManifest(error.to_string()))?;
        match manifest.state {
            ManifestState::Complete => {}
            ManifestState::Deleted => return Err(ReaderError::Deleted),
            ManifestState::Uploading => return Err(ReaderError::Incomplete),
        }
        let prefix = manifest_path
            .parent()
            .ok_or_else(|| ReaderError::InvalidManifest("manifest has no parent".into()))?;
        Ok(Self {
            store,
            prefix,
            manifest: Arc::new(manifest),
            cache: Arc::new(Mutex::new(CacheState::default())),
            permits: Arc::new(Semaphore::new(GET_CONCURRENCY)),
            pending: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CHUNKS)),
            #[cfg(test)]
            get_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            active_gets: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            max_active_gets: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            waiting_gets: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fetch_gates: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            body_gates: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            body_reads_started: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            waiter_gate: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            paused_waiters: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            test_changed: Arc::new(tokio::sync::Notify::new()),
        })
    }

    pub fn len(&self) -> u64 {
        self.manifest.size
    }

    pub fn is_empty(&self) -> bool {
        self.manifest.size == 0
    }

    /// Read bytes from the completed file at `offset`.
    ///
    /// If a later chunk fetch fails, bytes copied from earlier chunks remain in `output`;
    /// the returned error does not report that partial progress.
    pub async fn read_at(&self, offset: u64, output: &mut [u8]) -> Result<usize, ReaderError> {
        if output.is_empty() || offset >= self.manifest.size {
            return Ok(0);
        }
        let available = self.manifest.size - offset;
        let length = output
            .len()
            .min(usize::try_from(available).unwrap_or(usize::MAX));
        let mut copied = 0usize;
        while copied < length {
            let absolute = offset + copied as u64;
            let index = absolute / self.manifest.chunk_size;
            let within = (absolute % self.manifest.chunk_size) as usize;
            let chunk = self.chunk(index).await?;
            let count = (chunk.len() - within).min(length - copied);
            output[copied..copied + count].copy_from_slice(&chunk[within..within + count]);
            copied += count;
        }
        Ok(copied)
    }

    async fn chunk(&self, index: u64) -> Result<Bytes, ReaderError> {
        loop {
            enum Action {
                Wait {
                    flight: Arc<Flight>,
                    _waiter: FlightWaiter,
                    updates: watch::Receiver<FlightState>,
                },
                Fetch(Arc<Flight>),
            }
            let action = {
                let mut state = lock_state(&self.cache);
                if let Some(bytes) = state.chunks.get(&index).cloned() {
                    state.order.retain(|entry| *entry != index);
                    state.order.push_back(index);
                    return Ok(bytes);
                }
                if let Some(flight) = state.flights.get(&index).cloned() {
                    let Some(waiter) = FlightWaiter::try_new(flight.clone()) else {
                        return Err(ReaderError::TooManyPendingRequests);
                    };
                    #[cfg(test)]
                    self.test_changed.notify_waiters();
                    Action::Wait {
                        updates: flight.updates.subscribe(),
                        flight,
                        _waiter: waiter,
                    }
                } else {
                    let admission = self
                        .pending
                        .clone()
                        .try_acquire_owned()
                        .map_err(|_| ReaderError::TooManyPendingRequests)?;
                    let (updates, _) = watch::channel(FlightState::Pending);
                    let flight = Arc::new(Flight {
                        updates,
                        waiters: AtomicUsize::new(0),
                        _admission: admission,
                    });
                    state.flights.insert(index, flight.clone());
                    #[cfg(test)]
                    self.test_changed.notify_waiters();
                    Action::Fetch(flight)
                }
            };
            match action {
                Action::Wait {
                    flight,
                    mut updates,
                    _waiter,
                } => {
                    if updates.changed().await.is_ok() {
                        #[cfg(test)]
                        self.pause_test_waiter().await;
                        match updates.borrow().clone() {
                            FlightState::Finished(result) => return result,
                            FlightState::Retry | FlightState::Pending => {}
                        }
                    }
                    drop(flight);
                }
                Action::Fetch(flight) => {
                    let owner = FlightOwner {
                        cache: self.cache.clone(),
                        index,
                        flight,
                        completed: false,
                        #[cfg(test)]
                        test_changed: self.test_changed.clone(),
                    };
                    let result = self.fetch_chunk(index).await;
                    return owner.finish(result);
                }
            }
        }
    }

    async fn fetch_chunk(&self, index: u64) -> Result<Bytes, ReaderError> {
        #[cfg(test)]
        let queue_guard =
            TestCounterGuard::new(self.waiting_gets.clone(), self.test_changed.clone());
        let _permit = self
            .permits
            .acquire()
            .await
            .expect("reader GET semaphore is never closed");
        #[cfg(test)]
        drop(queue_guard);
        #[cfg(test)]
        self.test_changed.notify_waiters();
        let mut path = self.prefix.clone().join("chunks");
        if let Some(generation) = &self.manifest.generation {
            path = path.join(generation.as_str());
        }
        path = path.join(format!("{index:08}"));
        let offset = index
            .checked_mul(self.manifest.chunk_size)
            .ok_or_else(|| ReaderError::InvalidManifest("chunk offset overflow".into()))?;
        let remaining = self
            .manifest
            .size
            .checked_sub(offset)
            .filter(|_| offset < self.manifest.size)
            .ok_or_else(|| ReaderError::InvalidManifest("chunk index exceeds file size".into()))?;
        let expected_size = self.manifest.chunk_size.min(remaining);
        #[cfg(test)]
        let gate = lock_mutex(&self.fetch_gates).get(&index).cloned();
        #[cfg(test)]
        let _gate_permit = match gate {
            Some(gate) => Some(
                gate.acquire_owned()
                    .await
                    .expect("test fetch gate is never closed"),
            ),
            None => None,
        };
        #[cfg(test)]
        let _active_get = self.record_get_start();
        #[cfg(test)]
        tokio::task::yield_now().await;
        #[cfg(test)]
        self.test_changed.notify_waiters();
        match self.store.get(&path).await {
            Ok(result) => {
                #[cfg(test)]
                let result = self.gate_body_read(index, result);
                read_chunk_body(result, expected_size, path.to_string()).await
            }
            Err(object_store::Error::NotFound { .. }) => {
                Err(ReaderError::MissingChunk(path.to_string()))
            }
            Err(error) => Err(ReaderError::Store(Arc::new(error))),
        }
    }

    #[cfg(test)]
    fn record_get_start(&self) -> ActiveGetGuard {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        let active = self.active_gets.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active_gets.fetch_max(active, Ordering::SeqCst);
        self.test_changed.notify_waiters();
        ActiveGetGuard {
            active: self.active_gets.clone(),
            changed: self.test_changed.clone(),
        }
    }

    #[cfg(test)]
    fn gate_body_read(
        &self,
        index: u64,
        result: object_store::GetResult,
    ) -> object_store::GetResult {
        let Some(gate) = lock_mutex(&self.body_gates).get(&index).cloned() else {
            return result;
        };
        let object_store::GetResult {
            payload,
            meta,
            range,
            attributes,
            extensions,
        } = result;
        let payload = match payload {
            object_store::GetResultPayload::Stream(stream) => {
                let started = self.body_reads_started.clone();
                let changed = self.test_changed.clone();
                let stream = futures_util::stream::unfold(
                    (stream, gate, started, changed, true),
                    |(mut stream, gate, started, changed, first)| async move {
                        let item = stream.next().await?;
                        if first {
                            started.fetch_add(1, Ordering::SeqCst);
                            changed.notify_waiters();
                            let _permit = gate
                                .clone()
                                .acquire_owned()
                                .await
                                .expect("test body gate is never closed");
                        }
                        Some((item, (stream, gate, started, changed, false)))
                    },
                )
                .boxed();
                object_store::GetResultPayload::Stream(stream)
            }
            object_store::GetResultPayload::File(file, path) => {
                object_store::GetResultPayload::File(file, path)
            }
        };
        object_store::GetResult {
            payload,
            meta,
            range,
            attributes,
            extensions,
        }
    }

    #[cfg(test)]
    async fn pause_test_waiter(&self) {
        let gate = lock_mutex(&self.waiter_gate).clone();
        let Some(gate) = gate else {
            return;
        };
        self.paused_waiters.fetch_add(1, Ordering::SeqCst);
        self.test_changed.notify_waiters();
        let _permit = gate
            .acquire_owned()
            .await
            .expect("test waiter gate is never closed");
        self.paused_waiters.fetch_sub(1, Ordering::SeqCst);
        self.test_changed.notify_waiters();
    }
}

async fn read_chunk_body(
    result: object_store::GetResult,
    expected_size: u64,
    path: String,
) -> Result<Bytes, ReaderError> {
    if result.meta.size != expected_size {
        return Err(ReaderError::InvalidChunk(path));
    }

    let expected_size =
        usize::try_from(expected_size).map_err(|_| ReaderError::InvalidChunk(path.clone()))?;
    let mut body = Vec::with_capacity(expected_size);
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ReaderError::Store(Arc::new(error)))?;
        if chunk.len() > expected_size - body.len() {
            return Err(ReaderError::InvalidChunk(path));
        }
        body.extend_from_slice(&chunk);
    }

    if body.len() != expected_size {
        return Err(ReaderError::InvalidChunk(path));
    }
    Ok(Bytes::from(body))
}

async fn read_manifest_body(result: object_store::GetResult) -> Result<Bytes, ReaderError> {
    let advertised_size = result.meta.size;
    if advertised_size > MAX_MANIFEST_SIZE as u64 {
        return Err(ReaderError::ManifestTooLarge);
    }

    let mut body = Vec::with_capacity(advertised_size as usize);
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ReaderError::Store(Arc::new(error)))?;
        if chunk.len() > MAX_MANIFEST_SIZE - body.len() {
            return Err(ReaderError::ManifestTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

fn lock_state(mutex: &Mutex<CacheState>) -> MutexGuard<'_, CacheState> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn insert_cache(state: &mut CacheState, index: u64, bytes: Bytes) {
    if bytes.len() > CACHE_LIMIT {
        return;
    }
    if let Some(previous) = state.chunks.remove(&index) {
        state.bytes -= previous.len();
        state.order.retain(|entry| *entry != index);
    }
    while state.bytes + bytes.len() > CACHE_LIMIT {
        let Some(oldest) = state.order.pop_front() else {
            break;
        };
        if let Some(previous) = state.chunks.remove(&oldest) {
            state.bytes -= previous.len();
        }
    }
    state.bytes += bytes.len();
    state.order.push_back(index);
    state.chunks.insert(index, bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, memory::InMemory};

    const CHUNK: usize = 1024 * 1024;

    async fn make_reader(data: &[u8]) -> (RemoteFileReader, Arc<InMemory>, ObjectPath) {
        let store = Arc::new(InMemory::new());
        let manifest = ObjectPath::from("root/file/manifest");
        let prefix = manifest.parent().unwrap();
        let generation = "generation-1";
        let count = data.len().div_ceil(CHUNK);
        for index in 0..count {
            let begin = index * CHUNK;
            let end = (begin + CHUNK).min(data.len());
            let path = prefix
                .clone()
                .join("chunks")
                .join(generation)
                .join(format!("{index:08}"));
            store
                .put(&path, Bytes::copy_from_slice(&data[begin..end]).into())
                .await
                .unwrap();
        }
        let text = format!(
            "version=2\nsize={}\nchunk_size={}\ngeneration={}\nstate=complete\n",
            data.len(),
            CHUNK,
            generation
        );
        store
            .put(&manifest, Bytes::from(text).into())
            .await
            .unwrap();
        let reader = RemoteFileReader::open(store.clone(), manifest.clone())
            .await
            .unwrap();
        (reader, store, manifest)
    }

    async fn chunk_result(actual: &[u8], advertised_size: u64) -> object_store::GetResult {
        let store = InMemory::new();
        let path = ObjectPath::from("test/chunk");
        store
            .put(&path, Bytes::copy_from_slice(actual).into())
            .await
            .unwrap();
        let mut result = store.get(&path).await.unwrap();
        result.meta.size = advertised_size;
        result
    }

    fn set_stream_body(result: &mut object_store::GetResult, chunks: Vec<Bytes>) {
        result.payload = object_store::GetResultPayload::Stream(
            futures_util::stream::iter(chunks.into_iter().map(Ok::<_, object_store::Error>))
                .boxed(),
        );
    }

    #[tokio::test]
    async fn rejects_advertised_oversized_chunk_before_reading_body() {
        let mut result = chunk_result(b"data", 5).await;
        result.payload = object_store::GetResultPayload::Stream(
            futures_util::stream::once(async {
                panic!("oversized chunk body must not be read");
                #[allow(unreachable_code)]
                Ok(Bytes::new())
            })
            .boxed(),
        );

        assert!(matches!(
            read_chunk_body(result, 4, "chunk".into()).await,
            Err(ReaderError::InvalidChunk(_))
        ));
    }

    #[tokio::test]
    async fn rejects_incorrect_advertised_chunk_size_before_reading_body() {
        let mut result = chunk_result(b"data", 3).await;
        result.payload = object_store::GetResultPayload::Stream(
            futures_util::stream::once(async {
                panic!("incorrectly advertised chunk body must not be read");
                #[allow(unreachable_code)]
                Ok(Bytes::new())
            })
            .boxed(),
        );

        assert!(matches!(
            read_chunk_body(result, 4, "chunk".into()).await,
            Err(ReaderError::InvalidChunk(_))
        ));
    }

    #[tokio::test]
    async fn rejects_oversized_chunk_body_while_streaming() {
        let mut result = chunk_result(b"data", 4).await;
        let polled = Arc::new(AtomicUsize::new(0));
        let body = futures_util::stream::iter([
            Ok::<_, object_store::Error>(Bytes::from_static(b"data")),
            Ok(Bytes::from_static(b"x")),
            Ok(Bytes::from_static(b"must not be read")),
        ])
        .map({
            let polled = polled.clone();
            move |chunk| {
                polled.fetch_add(1, Ordering::SeqCst);
                chunk
            }
        })
        .boxed();
        result.payload = object_store::GetResultPayload::Stream(body);

        assert!(matches!(
            read_chunk_body(result, 4, "chunk".into()).await,
            Err(ReaderError::InvalidChunk(_))
        ));
        assert_eq!(polled.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn rejects_undersized_chunk_body() {
        let mut result = chunk_result(b"data", 4).await;
        set_stream_body(&mut result, vec![Bytes::from_static(b"dat")]);

        assert!(matches!(
            read_chunk_body(result, 4, "chunk".into()).await,
            Err(ReaderError::InvalidChunk(_))
        ));
    }

    #[tokio::test]
    async fn accepts_normal_and_final_partial_chunk_bodies() {
        let normal = chunk_result(b"data", 4).await;
        assert_eq!(
            read_chunk_body(normal, 4, "chunk".into()).await.unwrap(),
            Bytes::from_static(b"data")
        );

        let mut final_partial = chunk_result(b"end", 3).await;
        set_stream_body(&mut final_partial, vec![Bytes::from_static(b"end")]);
        assert_eq!(
            read_chunk_body(final_partial, 3, "chunk".into())
                .await
                .unwrap(),
            Bytes::from_static(b"end")
        );
    }

    #[tokio::test]
    async fn reads_ranges_and_eof() {
        let data: Vec<u8> = (0..CHUNK * 2 + 37)
            .map(|index| (index % 251) as u8)
            .collect();
        let (reader, _, _) = make_reader(&data).await;
        let mut first = vec![0; CHUNK];
        assert_eq!(reader.read_at(0, &mut first).await.unwrap(), CHUNK);
        assert_eq!(first, data[..CHUNK]);
        let mut small = [0; 9];
        assert_eq!(reader.read_at(15, &mut small).await.unwrap(), 9);
        assert_eq!(small, data[15..24]);
        let mut crossing = [0; 10];
        assert_eq!(
            reader
                .read_at((CHUNK - 5) as u64, &mut crossing)
                .await
                .unwrap(),
            10
        );
        assert_eq!(crossing, data[CHUNK - 5..CHUNK + 5]);
        let mut later = [0; 4];
        assert_eq!(
            reader
                .read_at((CHUNK * 2 + 2) as u64, &mut later)
                .await
                .unwrap(),
            4
        );
        assert_eq!(later, data[CHUNK * 2 + 2..CHUNK * 2 + 6]);
        let mut final_bytes = [0; 100];
        assert_eq!(
            reader
                .read_at((CHUNK * 2) as u64, &mut final_bytes)
                .await
                .unwrap(),
            37
        );
        assert_eq!(&final_bytes[..37], &data[CHUNK * 2..]);
        assert_eq!(
            reader
                .read_at(data.len() as u64, &mut final_bytes)
                .await
                .unwrap(),
            0
        );
        assert_eq!(reader.read_at(u64::MAX, &mut final_bytes).await.unwrap(), 0);
        assert_eq!(reader.read_at(0, &mut []).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn reports_missing_and_short_chunks() {
        let (reader, store, _) = make_reader(&vec![7; CHUNK]).await;
        let path = ObjectPath::from("root/file/chunks/generation-1/00000000");
        store.delete(&path).await.unwrap();
        let mut output = [0; 1];
        assert!(matches!(
            reader.read_at(0, &mut output).await,
            Err(ReaderError::MissingChunk(_))
        ));

        let (reader, store, _) = make_reader(&vec![7; CHUNK]).await;
        store
            .put(&path, Bytes::from_static(b"short").into())
            .await
            .unwrap();
        assert!(matches!(
            reader.read_at(0, &mut output).await,
            Err(ReaderError::InvalidChunk(_))
        ));
        assert!(!lock_state(&reader.cache).chunks.contains_key(&0));
        assert!(matches!(
            reader.read_at(0, &mut output).await,
            Err(ReaderError::InvalidChunk(_))
        ));
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn validates_manifest_state_and_content() {
        let store = Arc::new(InMemory::new());
        let path = ObjectPath::from("root/file/manifest");
        assert!(matches!(
            RemoteFileReader::open(store.clone(), path.clone()).await,
            Err(ReaderError::MissingManifest)
        ));
        for (state, expected) in [("deleted", true), ("uploading", false)] {
            let text = format!("size=0\nchunk_size={CHUNK}\nstate={state}\n");
            store.put(&path, Bytes::from(text).into()).await.unwrap();
            let result = RemoteFileReader::open(store.clone(), path.clone()).await;
            assert_eq!(matches!(result, Err(ReaderError::Deleted)), expected);
            if !expected {
                assert!(matches!(result, Err(ReaderError::Incomplete)));
            }
        }
        store
            .put(&path, Bytes::from_static(b"not a manifest").into())
            .await
            .unwrap();
        assert!(matches!(
            RemoteFileReader::open(store, path).await,
            Err(ReaderError::InvalidManifest(_))
        ));
    }

    #[tokio::test]
    async fn opens_a_valid_manifest() {
        let store = Arc::new(InMemory::new());
        let path = ObjectPath::from("root/file/manifest");
        store
            .put(
                &path,
                Bytes::from(format!(
                    "size=0\nchunk_size={CHUNK}\ngeneration=generation-1\nstate=complete\n"
                ))
                .into(),
            )
            .await
            .unwrap();

        let reader = RemoteFileReader::open(store, path).await.unwrap();
        assert_eq!(reader.len(), 0);
        assert!(reader.is_empty());
    }

    #[tokio::test]
    async fn rejects_manifest_advertised_above_size_limit_before_reading_body() {
        let store = InMemory::new();
        let path = ObjectPath::from("root/file/manifest");
        store
            .put(&path, Bytes::from(vec![b'x'; MAX_MANIFEST_SIZE + 1]).into())
            .await
            .unwrap();
        let mut result = store.get(&path).await.unwrap();
        let stream = futures_util::stream::once(async {
            panic!("oversized advertised body must not be read");
            #[allow(unreachable_code)]
            Ok(Bytes::new())
        })
        .boxed();
        result.payload = object_store::GetResultPayload::Stream(stream);

        assert!(matches!(
            read_manifest_body(result).await,
            Err(ReaderError::ManifestTooLarge)
        ));
    }

    #[tokio::test]
    async fn rejects_oversized_actual_body_with_inaccurate_content_length() {
        let store = InMemory::new();
        let path = ObjectPath::from("root/file/manifest");
        store
            .put(&path, Bytes::from_static(b"x").into())
            .await
            .unwrap();
        let mut result = store.get(&path).await.unwrap();
        result.meta.size = 1;

        let polled = Arc::new(AtomicUsize::new(0));
        let stream = futures_util::stream::iter([
            Ok::<_, object_store::Error>(Bytes::from(vec![b'x'; MAX_MANIFEST_SIZE])),
            Ok(Bytes::from_static(b"x")),
            Ok(Bytes::from_static(b"must not be polled")),
        ])
        .map({
            let polled = polled.clone();
            move |chunk| {
                polled.fetch_add(1, Ordering::SeqCst);
                chunk
            }
        })
        .boxed();
        result.payload = object_store::GetResultPayload::Stream(stream);

        assert!(matches!(
            read_manifest_body(result).await,
            Err(ReaderError::ManifestTooLarge)
        ));
        assert_eq!(polled.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn accepts_valid_manifest_exactly_at_size_limit() {
        let mut body = format!("size=0\nchunk_size={CHUNK}\nstate=complete\nextra=").into_bytes();
        body.resize(MAX_MANIFEST_SIZE - 1, b'x');
        body.push(b'\n');
        assert_eq!(body.len(), MAX_MANIFEST_SIZE);

        let store = Arc::new(InMemory::new());
        let path = ObjectPath::from("root/file/manifest");
        store.put(&path, Bytes::from(body).into()).await.unwrap();

        let reader = RemoteFileReader::open(store, path).await.unwrap();
        assert_eq!(reader.len(), 0);
    }

    #[tokio::test]
    async fn caches_reads_and_evicts_lru() {
        let data = vec![9; CHUNK * 17];
        let (reader, _, _) = make_reader(&data).await;
        let mut byte = [0; 1];
        reader.read_at(0, &mut byte).await.unwrap();
        reader.read_at(0, &mut byte).await.unwrap();
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 1);
        for index in 1..=16 {
            reader
                .read_at((index * CHUNK) as u64, &mut byte)
                .await
                .unwrap();
        }
        assert!(lock_state(&reader.cache).bytes <= CACHE_LIMIT);
        reader.read_at(0, &mut byte).await.unwrap();
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 18);
    }

    #[tokio::test]
    async fn concurrent_reads_share_chunk_fetch() {
        let (reader, _, _) = make_reader(&vec![5; CHUNK]).await;
        let mut first = [0; 32];
        let mut second = [0; 32];
        let (a, b) = tokio::join!(
            reader.read_at(0, &mut first),
            reader.read_at(10, &mut second)
        );
        assert_eq!(a.unwrap(), 32);
        assert_eq!(b.unwrap(), 32);
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 1);
        assert!(reader.max_active_gets.load(Ordering::Relaxed) <= GET_CONCURRENCY);
    }

    #[tokio::test]
    async fn concurrent_chunk_gets_are_bounded() {
        let (reader, _, _) = make_reader(&vec![3; CHUNK * 8]).await;
        let reads = (0..8).map(|index| {
            let reader = reader.clone();
            async move {
                let mut byte = [0; 1];
                reader.read_at((index * CHUNK) as u64, &mut byte).await
            }
        });
        let results = futures_util::future::join_all(reads).await;
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(
            reader.max_active_gets.load(Ordering::Relaxed),
            GET_CONCURRENCY
        );
    }

    #[tokio::test]
    async fn cancelling_fetch_owner_releases_waiting_readers() {
        let (reader, _, _) = make_reader(&vec![6; CHUNK]).await;
        let gate = Arc::new(Semaphore::new(0));
        set_fetch_gate(&reader, 0, gate.clone());

        let owner_reader = reader.clone();
        let owner = tokio::spawn(async move { owner_reader.read_at(0, &mut [0; 1]).await });
        wait_until(&reader, || {
            lock_state(&reader.cache).flights.contains_key(&0)
                && reader.permits.available_permits() == GET_CONCURRENCY - 1
        })
        .await;

        let waiter_reader = reader.clone();
        let waiter = tokio::spawn(async move { waiter_reader.read_at(0, &mut [0; 1]).await });
        wait_until(&reader, || {
            lock_state(&reader.cache)
                .flights
                .get(&0)
                .is_some_and(|flight| flight.waiters.load(Ordering::Relaxed) == 1)
        })
        .await;

        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        gate.add_permits(1);
        assert_eq!(waiter.await.unwrap().unwrap(), 1);
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 1);
        assert_eq!(reader.read_at(0, &mut [0; 1]).await.unwrap(), 1);
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 1);
        assert!(lock_state(&reader.cache).flights.is_empty());
    }

    #[tokio::test]
    async fn cancelling_fetch_owner_waiting_for_get_permit_retries() {
        let (reader, _, _) = make_reader(&vec![4; CHUNK]).await;
        let held_permits = reader
            .permits
            .clone()
            .acquire_many_owned(GET_CONCURRENCY as u32)
            .await
            .unwrap();
        let owner_reader = reader.clone();
        let owner = tokio::spawn(async move { owner_reader.read_at(0, &mut [0; 1]).await });
        wait_until(&reader, || {
            lock_state(&reader.cache).flights.contains_key(&0)
                && reader.waiting_gets.load(Ordering::SeqCst) == 1
        })
        .await;
        let old_flight = lock_state(&reader.cache).flights.get(&0).unwrap().clone();

        let waiter_reader = reader.clone();
        let waiter = tokio::spawn(async move { waiter_reader.read_at(0, &mut [0; 1]).await });
        wait_until(&reader, || old_flight.waiters.load(Ordering::Relaxed) == 1).await;

        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        wait_until(&reader, || {
            lock_state(&reader.cache)
                .flights
                .get(&0)
                .is_some_and(|flight| !Arc::ptr_eq(flight, &old_flight))
                && reader.waiting_gets.load(Ordering::SeqCst) == 1
        })
        .await;
        drop(old_flight);
        drop(held_permits);

        assert_eq!(waiter.await.unwrap().unwrap(), 1);
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 1);
        assert_eq!(reader.waiting_gets.load(Ordering::SeqCst), 0);
        assert_eq!(reader.active_gets.load(Ordering::SeqCst), 0);
        assert!(lock_state(&reader.cache).flights.is_empty());
        assert_eq!(reader.read_at(0, &mut [0; 1]).await.unwrap(), 1);
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cancelling_fetch_owner_during_body_read_retries() {
        let (reader, _, _) = make_reader(&vec![5; CHUNK]).await;
        let gate = Arc::new(Semaphore::new(0));
        set_body_gate(&reader, 0, gate.clone());
        let owner_reader = reader.clone();
        let owner = tokio::spawn(async move { owner_reader.read_at(0, &mut [0; 1]).await });
        wait_until(&reader, || {
            reader.body_reads_started.load(Ordering::SeqCst) == 1
        })
        .await;
        let old_flight = lock_state(&reader.cache).flights.get(&0).unwrap().clone();

        let waiter_reader = reader.clone();
        let waiter = tokio::spawn(async move { waiter_reader.read_at(0, &mut [0; 1]).await });
        wait_until(&reader, || old_flight.waiters.load(Ordering::Relaxed) == 1).await;

        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        wait_until(&reader, || {
            reader.body_reads_started.load(Ordering::SeqCst) == 2
                && lock_state(&reader.cache)
                    .flights
                    .get(&0)
                    .is_some_and(|flight| !Arc::ptr_eq(flight, &old_flight))
        })
        .await;
        drop(old_flight);
        gate.add_permits(1);

        assert_eq!(waiter.await.unwrap().unwrap(), 1);
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 2);
        assert_eq!(reader.active_gets.load(Ordering::SeqCst), 0);
        assert!(lock_state(&reader.cache).flights.is_empty());
        assert_eq!(reader.read_at(0, &mut [0; 1]).await.unwrap(), 1);
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn stale_owner_does_not_remove_replacement_flight() {
        let (reader, _, _) = make_reader(&vec![6; CHUNK]).await;
        let old_admission = reader.pending.clone().try_acquire_owned().unwrap();
        let (old_updates, _) = watch::channel(FlightState::Pending);
        let old_flight = Arc::new(Flight {
            updates: old_updates,
            waiters: AtomicUsize::new(0),
            _admission: old_admission,
        });
        lock_state(&reader.cache)
            .flights
            .insert(0, old_flight.clone());
        let old_owner = FlightOwner {
            cache: reader.cache.clone(),
            index: 0,
            flight: old_flight.clone(),
            completed: false,
            test_changed: reader.test_changed.clone(),
        };

        let new_admission = reader.pending.clone().try_acquire_owned().unwrap();
        let (new_updates, _) = watch::channel(FlightState::Pending);
        let new_flight = Arc::new(Flight {
            updates: new_updates,
            waiters: AtomicUsize::new(0),
            _admission: new_admission,
        });
        lock_state(&reader.cache)
            .flights
            .insert(0, new_flight.clone());

        drop(old_owner);
        let state = lock_state(&reader.cache);
        assert!(Arc::ptr_eq(state.flights.get(&0).unwrap(), &new_flight));
        assert!(matches!(*old_flight.updates.borrow(), FlightState::Retry));
        assert!(matches!(*new_flight.updates.borrow(), FlightState::Pending));
    }

    #[tokio::test]
    async fn bounds_unique_flights_and_coalesced_waiters() {
        let (reader, _, _) = make_reader(&vec![8; CHUNK * (MAX_IN_FLIGHT_CHUNKS + 1)]).await;
        let gate = Arc::new(Semaphore::new(0));
        for index in 0..MAX_IN_FLIGHT_CHUNKS as u64 {
            set_fetch_gate(&reader, index, gate.clone());
        }
        let waiter_gate = Arc::new(Semaphore::new(0));
        *lock_mutex(&reader.waiter_gate) = Some(waiter_gate.clone());
        let mut owners = Vec::new();
        for index in 0..MAX_IN_FLIGHT_CHUNKS {
            let reader = reader.clone();
            owners.push(tokio::spawn(async move {
                reader.read_at((index * CHUNK) as u64, &mut [0; 1]).await
            }));
        }
        wait_until(&reader, || {
            lock_state(&reader.cache).flights.len() == MAX_IN_FLIGHT_CHUNKS
                && reader.permits.available_permits() == 0
        })
        .await;
        assert_eq!(
            lock_state(&reader.cache).flights.len(),
            MAX_IN_FLIGHT_CHUNKS
        );
        assert!(matches!(
            reader
                .read_at((MAX_IN_FLIGHT_CHUNKS * CHUNK) as u64, &mut [0; 1])
                .await,
            Err(ReaderError::TooManyPendingRequests)
        ));

        let mut waiters = Vec::new();
        for index in 0..MAX_IN_FLIGHT_CHUNKS {
            let reader = reader.clone();
            waiters.push(tokio::spawn(async move {
                reader.read_at((index * CHUNK) as u64, &mut [0; 1]).await
            }));
        }
        for _ in 1..MAX_COALESCED_WAITERS_PER_CHUNK {
            let reader = reader.clone();
            waiters.push(tokio::spawn(
                async move { reader.read_at(0, &mut [0; 1]).await },
            ));
        }
        wait_until(&reader, || {
            lock_state(&reader.cache)
                .flights
                .get(&0)
                .is_some_and(|flight| {
                    flight.waiters.load(Ordering::Relaxed) == MAX_COALESCED_WAITERS_PER_CHUNK
                })
        })
        .await;
        assert!(matches!(
            reader.read_at(0, &mut [0; 1]).await,
            Err(ReaderError::TooManyPendingRequests)
        ));

        gate.add_permits(MAX_IN_FLIGHT_CHUNKS);
        for owner in owners {
            assert_eq!(owner.await.unwrap().unwrap(), 1);
        }
        wait_until(&reader, || {
            reader.paused_waiters.load(Ordering::SeqCst)
                == MAX_IN_FLIGHT_CHUNKS + MAX_COALESCED_WAITERS_PER_CHUNK - 1
        })
        .await;
        assert!(lock_state(&reader.cache).flights.is_empty());
        assert_eq!(reader.pending.available_permits(), 0);
        assert!(matches!(
            reader
                .read_at((MAX_IN_FLIGHT_CHUNKS * CHUNK) as u64, &mut [0; 1])
                .await,
            Err(ReaderError::TooManyPendingRequests)
        ));

        waiter_gate.add_permits(MAX_IN_FLIGHT_CHUNKS + MAX_COALESCED_WAITERS_PER_CHUNK - 1);
        for waiter in waiters {
            assert_eq!(waiter.await.unwrap().unwrap(), 1);
        }
        assert_eq!(
            reader.get_count.load(Ordering::Relaxed),
            MAX_IN_FLIGHT_CHUNKS
        );
        assert_eq!(reader.pending.available_permits(), MAX_IN_FLIGHT_CHUNKS);
        assert_eq!(
            reader
                .read_at((MAX_IN_FLIGHT_CHUNKS * CHUNK) as u64, &mut [0; 1])
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            reader.max_active_gets.load(Ordering::Relaxed),
            GET_CONCURRENCY
        );
        assert!(lock_state(&reader.cache).bytes <= CACHE_LIMIT);
    }

    #[tokio::test]
    async fn generation_replacement_during_read_keeps_original_chunks() {
        let mut first = vec![0xA1; CHUNK];
        first.extend(vec![0xA2; CHUNK]);
        let mut second = vec![0xB1; CHUNK];
        second.extend(vec![0xB2; CHUNK]);
        let (reader, store, manifest) = make_reader(&first).await;
        let prefix = manifest.parent().unwrap();
        for index in 0..2 {
            let start = index * CHUNK;
            let path = prefix
                .clone()
                .join("chunks")
                .join("generation-2")
                .join(format!("{index:08}"));
            store
                .put(
                    &path,
                    Bytes::copy_from_slice(&second[start..start + CHUNK]).into(),
                )
                .await
                .unwrap();
        }
        let gate = Arc::new(Semaphore::new(0));
        set_fetch_gate(&reader, 1, gate.clone());
        let read_reader = reader.clone();
        let read = tokio::spawn(async move {
            let mut output = [0; 4];
            let count = read_reader.read_at((CHUNK - 2) as u64, &mut output).await?;
            Ok::<_, ReaderError>((count, output))
        });
        wait_until(&reader, || {
            lock_state(&reader.cache).flights.contains_key(&1)
                && reader.permits.available_permits() == GET_CONCURRENCY - 1
        })
        .await;
        assert_eq!(reader.get_count.load(Ordering::Relaxed), 1);
        let second_manifest = format!(
            "version=2\nsize={}\nchunk_size={CHUNK}\ngeneration=generation-2\nstate=complete\n",
            CHUNK * 2
        );
        store
            .put(&manifest, Bytes::from(second_manifest).into())
            .await
            .unwrap();
        gate.add_permits(1);
        let (count, output) = read.await.unwrap().unwrap();
        assert_eq!(count, 4);
        assert_eq!(output, [0xA1, 0xA1, 0xA2, 0xA2]);
    }

    fn set_fetch_gate(reader: &RemoteFileReader, index: u64, gate: Arc<Semaphore>) {
        lock_mutex(&reader.fetch_gates).insert(index, gate);
    }

    fn set_body_gate(reader: &RemoteFileReader, index: u64, gate: Arc<Semaphore>) {
        lock_mutex(&reader.body_gates).insert(index, gate);
    }

    async fn wait_until(reader: &RemoteFileReader, mut condition: impl FnMut() -> bool) {
        let completed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let notified = reader.test_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if condition() {
                    return;
                }
                notified.await;
            }
        })
        .await;
        assert!(
            completed.is_ok(),
            "timed out waiting for test synchronization"
        );
    }
}
