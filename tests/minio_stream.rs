use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use sha2::{Digest, Sha256};
use std::{
    env,
    ffi::OsString,
    io,
    path::{Path as FilePath, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use store_stream::{
    Storage,
    resumable::{ResumableStore, UploadSpec, UploadState},
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};

const ACCESS_KEY_ENV: &str = "INFINITEFS_MINIO_ACCESS_KEY";
const ENDPOINT_ENV: &str = "INFINITEFS_MINIO_ENDPOINT";
const OUTPUT_PATH_ENV: &str = "INFINITEFS_MINIO_OUTPUT";
const SECRET_KEY_ENV: &str = "INFINITEFS_MINIO_SECRET_KEY";
const SOURCE_PATH_ENV: &str = "INFINITEFS_MINIO_SOURCE";
const BUCKET_NAME: &str = "infinitefs-proof";
const CHUNK_SIZE_BYTES: u64 = 64 * 1024;
const MAX_LOCAL_FILE_BYTES: u64 = 8 * 1024 * 1024;
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct MinioConfig {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

struct FileDigest {
    size_bytes: u64,
    sha256: String,
}

struct LocalFileInput {
    source_path: PathBuf,
    output_path: PathBuf,
}

impl LocalFileInput {
    fn from_environment() -> Result<Option<Self>, io::Error> {
        match (env::var_os(SOURCE_PATH_ENV), env::var_os(OUTPUT_PATH_ENV)) {
            (None, None) => Ok(None),
            (Some(source_path), Some(output_path)) => Ok(Some(Self {
                source_path: PathBuf::from(source_path),
                output_path: PathBuf::from(output_path),
            })),
            _ => Err(invalid_input(
                "INFINITEFS_MINIO_SOURCE and INFINITEFS_MINIO_OUTPUT must be set together",
            )),
        }
    }

    async fn digest(&self) -> TestResult<FileDigest> {
        let metadata = fs::metadata(&self.source_path).await?;
        if !metadata.is_file() {
            return Err(invalid_input("INFINITEFS_MINIO_SOURCE must be a regular file").into());
        }
        if metadata.len() == 0 {
            return Err(invalid_input("INFINITEFS_MINIO_SOURCE must not be empty").into());
        }
        if metadata.len() > MAX_LOCAL_FILE_BYTES {
            return Err(
                invalid_input("INFINITEFS_MINIO_SOURCE exceeds the 8 MiB proof limit").into(),
            );
        }

        let sha256 = digest_file(&self.source_path, metadata.len()).await?;
        let after = fs::metadata(&self.source_path).await?;
        if after.len() != metadata.len() {
            return Err(invalid_input("INFINITEFS_MINIO_SOURCE changed while hashing").into());
        }

        Ok(FileDigest {
            size_bytes: metadata.len(),
            sha256,
        })
    }

    fn partial_path(&self, unique: &str) -> Result<PathBuf, io::Error> {
        let filename = self
            .output_path
            .file_name()
            .ok_or_else(|| invalid_input("INFINITEFS_MINIO_OUTPUT must name a file"))?;
        let mut partial_name = OsString::from(".");
        partial_name.push(filename);
        partial_name.push(format!(".infinitefs-{unique}.partial"));
        Ok(self.output_path.with_file_name(partial_name))
    }
}

impl MinioConfig {
    fn from_environment() -> Result<Self, env::VarError> {
        Ok(Self {
            endpoint: env::var(ENDPOINT_ENV)?,
            access_key: env::var(ACCESS_KEY_ENV)?,
            secret_key: env::var(SECRET_KEY_ENV)?,
        })
    }

    fn validate(&self) -> Result<(), io::Error> {
        if self.endpoint.starts_with("http://127.0.0.1:") {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{ENDPOINT_ENV} must use a loopback endpoint"),
            ))
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reads_confirmed_bytes_before_upload_completion() -> TestResult {
    let config = MinioConfig::from_environment()?;
    config.validate()?;

    tokio::time::timeout(UPLOAD_TIMEOUT, prove_live_read(config)).await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn streams_a_local_file_when_requested() -> TestResult {
    let Some(input) = LocalFileInput::from_environment()? else {
        return Ok(());
    };
    let config = MinioConfig::from_environment()?;
    config.validate()?;

    tokio::time::timeout(UPLOAD_TIMEOUT, prove_local_file(config, input)).await??;
    Ok(())
}

async fn prove_live_read(config: MinioConfig) -> TestResult {
    let unique = unique_id()?;
    ensure_bucket(&config, &unique).await?;

    let storage = object_store(&config)?;
    let root = Path::from(format!("proofs/{unique}/staging"));
    let writer = ResumableStore::new(storage.clone(), storage.clone(), root.clone());
    let reader = ResumableStore::new(storage.clone(), storage.clone(), root);

    let prefix = Bytes::from(vec![b'a'; CHUNK_SIZE_BYTES as usize]);
    let middle = Bytes::from(vec![b'b'; CHUNK_SIZE_BYTES as usize]);
    let suffix = Bytes::from_static(b"stream complete");
    let expected = joined_bytes(&prefix, &middle, &suffix);
    let spec = upload_spec(&unique, expected.len() as u64);
    let session = writer.begin(spec.clone()).await?;

    assert_eq!(session.append(0, prefix.clone()).await?, CHUNK_SIZE_BYTES);
    assert_confirmed_prefix(&reader, &spec, &prefix).await?;
    assert!(
        reader
            .resume(&spec.id, &spec.source_version)
            .await?
            .read_confirmed(CHUNK_SIZE_BYTES - 8, 16)
            .await
            .is_err()
    );

    assert_eq!(
        session.append(CHUNK_SIZE_BYTES, middle.clone()).await?,
        2 * CHUNK_SIZE_BYTES
    );
    assert_cross_chunk_read(&reader, &spec, &prefix, &middle).await?;

    assert_eq!(
        session.append(2 * CHUNK_SIZE_BYTES, suffix.clone()).await?,
        expected.len() as u64
    );
    assert_eq!(session.finish().await?.len(), 64);

    let stored = storage
        .get(&Path::from(spec.destination))
        .await?
        .bytes()
        .await?;
    assert_eq!(stored, expected);
    Ok(())
}

async fn prove_local_file(config: MinioConfig, input: LocalFileInput) -> TestResult {
    reject_existing_output(&input.output_path)?;
    let source = input.digest().await?;
    let unique = unique_id()?;
    ensure_bucket(&config, &unique).await?;

    let storage = object_store(&config)?;
    let root = Path::from(format!("files/{unique}/staging"));
    let writer = ResumableStore::new(storage.clone(), storage.clone(), root.clone());
    let reader = ResumableStore::new(storage.clone(), storage.clone(), root);
    let spec = local_upload_spec(&unique, &source);
    let session = writer.begin(spec.clone()).await?;
    let partial_path = input.partial_path(&unique)?;
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial_path)
        .await?;

    let result = stream_local_chunks(&input, &reader, &session, &spec, &mut output, &source).await;
    let output_digest = match result {
        Ok(digest) => digest,
        Err(error) => {
            drop(output);
            let _ = fs::remove_file(&partial_path).await;
            return Err(error);
        }
    };

    output.sync_all().await?;
    drop(output);

    let result = finalize_local_file(
        &session,
        &storage,
        &spec,
        &source,
        &output_digest,
        &partial_path,
        &input.output_path,
    )
    .await;
    if let Err(error) = result {
        let _ = fs::remove_file(&partial_path).await;
        return Err(error);
    }

    println!(
        "streamed {} bytes to {}",
        source.size_bytes,
        input.output_path.display()
    );
    Ok(())
}

async fn finalize_local_file(
    session: &store_stream::resumable::UploadSession,
    storage: &Arc<dyn ObjectStore>,
    spec: &UploadSpec,
    source: &FileDigest,
    output_digest: &str,
    partial_path: &FilePath,
    output_path: &FilePath,
) -> TestResult {
    let finished_digest = session.finish().await?;
    let remote_digest = hash_remote_object(storage, &spec.destination, source.size_bytes).await?;
    assert_eq!(finished_digest, source.sha256);
    assert_eq!(output_digest, source.sha256);
    assert_eq!(remote_digest, source.sha256);

    std::fs::hard_link(partial_path, output_path)?;
    fs::remove_file(partial_path).await?;
    Ok(())
}

async fn stream_local_chunks(
    input: &LocalFileInput,
    reader: &ResumableStore,
    session: &store_stream::resumable::UploadSession,
    spec: &UploadSpec,
    output: &mut fs::File,
    source: &FileDigest,
) -> TestResult<String> {
    let mut file = fs::File::open(&input.source_path).await?;
    let mut input_hasher = Sha256::new();
    let mut output_hasher = Sha256::new();
    let mut offset = 0u64;

    while offset < source.size_bytes {
        let remaining = source.size_bytes - offset;
        let chunk = read_file_chunk(&mut file, remaining).await?;
        let confirmed_offset = session.append(offset, chunk.clone()).await?;
        assert_eq!(confirmed_offset, offset + chunk.len() as u64);

        let resumed = reader.resume(&spec.id, &spec.source_version).await?;
        let confirmed = resumed.read_confirmed(offset, chunk.len() as u64).await?;
        assert_eq!(confirmed, chunk);
        output.write_all(&confirmed).await?;
        input_hasher.update(&chunk);
        output_hasher.update(&confirmed);
        report_progress(confirmed_offset, source.size_bytes);

        if offset == 0 && confirmed_offset < source.size_bytes {
            assert_eq!(
                resumed.state().await?,
                UploadState::Active { confirmed_offset }
            );
            assert!(resumed.read_confirmed(confirmed_offset, 1).await.is_err());
            println!(
                "live read confirmed {} bytes while {} bytes remain",
                confirmed.len(),
                source.size_bytes - confirmed_offset
            );
        }

        offset = confirmed_offset;
    }

    assert_source_is_exhausted(&mut file).await?;
    assert_eq!(hex(input_hasher.finalize().as_ref()), source.sha256);
    Ok(hex(output_hasher.finalize().as_ref()))
}

async fn read_file_chunk(file: &mut fs::File, remaining: u64) -> io::Result<Bytes> {
    let size = remaining.min(CHUNK_SIZE_BYTES) as usize;
    let mut bytes = vec![0; size];
    file.read_exact(&mut bytes).await?;
    Ok(Bytes::from(bytes))
}

async fn assert_source_is_exhausted(file: &mut fs::File) -> io::Result<()> {
    let mut extra_byte = [0];
    if file.read(&mut extra_byte).await? == 0 {
        Ok(())
    } else {
        Err(invalid_input(
            "INFINITEFS_MINIO_SOURCE grew while streaming",
        ))
    }
}

async fn digest_file(path: &FilePath, size_bytes: u64) -> TestResult<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut offset = 0u64;

    while offset < size_bytes {
        let chunk = read_file_chunk(&mut file, size_bytes - offset).await?;
        hasher.update(&chunk);
        offset += chunk.len() as u64;
    }

    assert_source_is_exhausted(&mut file).await?;
    Ok(hex(hasher.finalize().as_ref()))
}

async fn hash_remote_object(
    storage: &Arc<dyn ObjectStore>,
    destination: &str,
    size_bytes: u64,
) -> TestResult<String> {
    let path = Path::from(destination);
    let mut hasher = Sha256::new();
    let mut offset = 0u64;

    while offset < size_bytes {
        let end = (offset + CHUNK_SIZE_BYTES).min(size_bytes);
        let bytes = storage.get_range(&path, offset..end).await?;
        assert_eq!(bytes.len() as u64, end - offset);
        hasher.update(&bytes);
        offset = end;
    }

    Ok(hex(hasher.finalize().as_ref()))
}

fn local_upload_spec(unique: &str, source: &FileDigest) -> UploadSpec {
    UploadSpec {
        id: format!("local-file-{unique}"),
        source_version: format!("sha256:{};size:{}", source.sha256, source.size_bytes),
        destination: format!("files/{unique}/final.bin"),
        size: source.size_bytes,
        chunk_size: CHUNK_SIZE_BYTES,
        sha256: Some(source.sha256.clone()),
    }
}

fn reject_existing_output(path: &FilePath) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Err(invalid_input("INFINITEFS_MINIO_OUTPUT already exists")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn report_progress(confirmed_bytes: u64, total_bytes: u64) {
    let percent = confirmed_bytes * 100 / total_bytes;
    println!("MinIO confirmed {confirmed_bytes}/{total_bytes} bytes ({percent}%)");
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}

async fn ensure_bucket(config: &MinioConfig, unique: &str) -> TestResult {
    let bootstrap = Storage::new(
        config.endpoint.clone(),
        config.access_key.clone(),
        config.secret_key.clone(),
    );
    let (sender, receiver) = mpsc::channel(1);
    drop(sender);
    bootstrap
        .upload(
            BUCKET_NAME,
            &format!("proofs/{unique}/bootstrap"),
            receiver,
            1,
        )
        .await?;
    Ok(())
}

fn object_store(config: &MinioConfig) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    Ok(Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(BUCKET_NAME)
            .with_region("us-east-1")
            .with_endpoint(&config.endpoint)
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.secret_key)
            .with_allow_http(true)
            .build()?,
    ))
}

fn upload_spec(unique: &str, size: u64) -> UploadSpec {
    UploadSpec {
        id: format!("live-read-{unique}"),
        source_version: "test-input-v1".to_string(),
        destination: format!("proofs/{unique}/final.bin"),
        size,
        chunk_size: CHUNK_SIZE_BYTES,
        sha256: None,
    }
}

async fn assert_confirmed_prefix(
    reader: &ResumableStore,
    spec: &UploadSpec,
    expected: &Bytes,
) -> TestResult {
    let resumed = reader.resume(&spec.id, &spec.source_version).await?;
    assert_eq!(
        resumed.state().await?,
        UploadState::Active {
            confirmed_offset: expected.len() as u64,
        }
    );
    assert_eq!(
        resumed.read_confirmed(0, expected.len() as u64).await?,
        *expected
    );
    Ok(())
}

async fn assert_cross_chunk_read(
    reader: &ResumableStore,
    spec: &UploadSpec,
    prefix: &Bytes,
    middle: &Bytes,
) -> TestResult {
    let resumed = reader.resume(&spec.id, &spec.source_version).await?;
    let expected = Bytes::copy_from_slice(&[&prefix[prefix.len() - 8..], &middle[..8]].concat());

    assert_eq!(
        resumed.read_confirmed(CHUNK_SIZE_BYTES - 8, 16).await?,
        expected
    );
    Ok(())
}

fn joined_bytes(prefix: &Bytes, middle: &Bytes, suffix: &Bytes) -> Bytes {
    let mut output = Vec::with_capacity(prefix.len() + middle.len() + suffix.len());
    output.extend_from_slice(prefix);
    output.extend_from_slice(middle);
    output.extend_from_slice(suffix);
    Bytes::from(output)
}

fn unique_id() -> Result<String, std::time::SystemTimeError> {
    Ok(format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ))
}
