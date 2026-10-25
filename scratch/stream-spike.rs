use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024;
const MAX_CHUNK_SIZE: u64 = 64 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.is_empty() {
        print_usage();
        return Ok(());
    }

    match args[0].as_str() {
        "send" => {
            let options = Options::parse(&args[1..])?;
            let (source, store) = options.two_paths("send")?;
            send(&source, &store, options.chunk_size, options.delay)
        }
        "recv" => {
            let options = Options::parse(&args[1..])?;
            let (store, output) = options.two_paths("recv")?;
            receive(&store, &output, options.follow)
        }
        _ => {
            print_usage();
            Ok(())
        }
    }
}

fn print_usage() {
    eprintln!(
        "usage:\n  \
         stream-spike send <source-dir> <store-dir> [--chunk-size 1M] [--delay-ms 0]\n  \
         stream-spike recv <store-dir> <output-dir> [--follow]\n\
         \n\
         Start recv first, then run send in another terminal with --delay-ms to\n\
         simulate network latency. Chunk files are visible as soon as they land."
    );
}

struct Options {
    chunk_size: u64,
    delay: Duration,
    follow: bool,
    paths: Vec<String>,
}

impl Options {
    fn parse(args: &[String]) -> io::Result<Self> {
        let mut options = Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            delay: Duration::ZERO,
            follow: false,
            paths: Vec::new(),
        };

        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--chunk-size" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| invalid_input("--chunk-size needs a value"))?;
                    options.chunk_size = parse_size(value)?;
                }
                "--delay-ms" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| invalid_input("--delay-ms needs a value"))?;
                    let milliseconds: u64 = value
                        .parse()
                        .map_err(|_| invalid_input("--delay-ms must be an integer"))?;
                    options.delay = Duration::from_millis(milliseconds);
                }
                "--follow" => options.follow = true,
                value if value.starts_with("--") => {
                    return Err(invalid_input(format!("unknown option: {value}")));
                }
                value => options.paths.push(value.to_owned()),
            }
            index += 1;
        }

        if options.chunk_size == 0 || options.chunk_size > MAX_CHUNK_SIZE {
            return Err(invalid_input(format!(
                "chunk size must be between 1 and {MAX_CHUNK_SIZE} bytes"
            )));
        }
        if options.paths.len() != 2 {
            return Err(invalid_input("expected exactly two directory paths"));
        }

        Ok(options)
    }

    fn two_paths(&self, _command: &str) -> io::Result<(PathBuf, PathBuf)> {
        let first = PathBuf::from(&self.paths[0]);
        let second = PathBuf::from(&self.paths[1]);
        fs::create_dir_all(&first)?;
        fs::create_dir_all(&second)?;
        Ok((first, second))
    }
}

fn send(source: &Path, store: &Path, chunk_size: u64, delay: Duration) -> io::Result<()> {
    assert!(chunk_size > 0);
    assert!(chunk_size <= MAX_CHUNK_SIZE);

    let source = fs::canonicalize(source)?;
    let store = fs::canonicalize(store)?;
    if source.starts_with(&store) || store.starts_with(&source) {
        return Err(invalid_input(
            "source and store directories must not contain each other",
        ));
    }

    let mut files: Vec<PathBuf> = fs::read_dir(&source)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect();
    files.sort();

    if files.is_empty() {
        eprintln!("no regular files in {}", source.display());
        return Ok(());
    }

    let send_started = Instant::now();
    let mut total_bytes = 0u64;

    for file_path in files {
        let name = file_name(&file_path)?;
        let directory = store.join(&name);
        if directory.exists() {
            fs::remove_dir_all(&directory)?;
        }
        fs::create_dir_all(&directory)?;

        let started_at = now_milliseconds();
        let file_size = fs::metadata(&file_path)?.len();
        write_manifest(&directory, 0, chunk_size, "uploading", started_at)?;

        let file = File::open(&file_path)?;
        let mut reader = io::BufReader::with_capacity(chunk_size as usize, file);
        let mut buffer = vec![0u8; chunk_size as usize];
        let mut written = 0u64;
        let mut chunk_index = 0u64;
        let mut last_report = Instant::now();
        let file_started = Instant::now();

        loop {
            let read = read_full(&mut reader, &mut buffer)?;
            if read == 0 {
                break;
            }

            write_chunk(&directory, chunk_index, &buffer[..read])?;
            written += read as u64;
            write_manifest(&directory, written, chunk_size, "uploading", started_at)?;

            chunk_index += 1;
            if !delay.is_zero() {
                thread::sleep(delay);
            }

            if last_report.elapsed() >= Duration::from_millis(500) {
                println!(
                    "send {name}: {:.1} / {:.1} MiB ({:.1} MiB/s)",
                    to_mib(written),
                    to_mib(file_size),
                    mib_per_second(written, file_started.elapsed()),
                );
                last_report = Instant::now();
            }
        }

        write_manifest(&directory, written, chunk_size, "complete", started_at)?;
        let elapsed = file_started.elapsed();
        println!(
            "sent {name}: {} bytes in {:.2?} ({:.1} MiB/s)",
            written,
            elapsed,
            mib_per_second(written, elapsed),
        );
        total_bytes += written;
    }

    let elapsed = send_started.elapsed();
    println!(
        "sent total: {} bytes in {:.2?} ({:.1} MiB/s)",
        total_bytes,
        elapsed,
        mib_per_second(total_bytes, elapsed),
    );
    Ok(())
}

struct ReceivedFile {
    output: File,
    read_bytes: u64,
    chunk_size: u64,
    started_at: u64,
    first_seen: Instant,
    first_byte_ms: Option<u64>,
    last_report: Instant,
    announced_first_byte: bool,
    complete: bool,
}

fn receive(store: &Path, output: &Path, follow: bool) -> io::Result<()> {
    let store = fs::canonicalize(store)?;
    let output = fs::canonicalize(output)?;
    if store.starts_with(&output) || output.starts_with(&store) {
        return Err(invalid_input(
            "store and output directories must not contain each other",
        ));
    }

    let mut files: HashMap<String, ReceivedFile> = HashMap::new();

    loop {
        let mut active = 0u32;
        let mut saw_any = false;

        for entry in fs::read_dir(&store)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }

            let directory = entry.path();
            let manifest = match read_manifest(&directory)? {
                Some(manifest) => manifest,
                None => continue,
            };
            saw_any = true;

            if files.get(&name).is_some_and(|file| file.complete) {
                continue;
            }

            if !files.contains_key(&name) {
                let output_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(output.join(&name))?;
                files.insert(
                    name.clone(),
                    ReceivedFile {
                        output: output_file,
                        read_bytes: 0,
                        chunk_size: manifest.chunk_size,
                        started_at: manifest.started_at,
                        first_seen: Instant::now(),
                        first_byte_ms: None,
                        last_report: Instant::now(),
                        announced_first_byte: false,
                        complete: false,
                    },
                );
            }
            let state = files
                .get_mut(&name)
                .ok_or_else(|| invalid_data("receiver file state is missing"))?;

            if manifest.chunk_size != state.chunk_size {
                return Err(invalid_input(format!(
                    "{name}: chunk size changed during transfer"
                )));
            }
            if manifest.size < state.read_bytes {
                state.output.set_len(0)?;
                state.output.seek(SeekFrom::Start(0))?;
                state.read_bytes = 0;
                state.first_byte_ms = None;
                state.announced_first_byte = false;
            }

            while state.read_bytes < manifest.size {
                let chunk_index = state.read_bytes / manifest.chunk_size;
                let chunk_path = directory.join(format!("{chunk_index:08}"));
                let bytes = match fs::read(&chunk_path) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        if follow {
                            break;
                        }
                        return Err(invalid_input(format!(
                            "{name}: missing chunk {chunk_index}"
                        )));
                    }
                    Err(error) => return Err(error),
                };

                let remaining = (manifest.size - state.read_bytes) as usize;
                let take = remaining.min(bytes.len());
                state.output.write_all(&bytes[..take])?;
                state.read_bytes += take as u64;

                if state.first_byte_ms.is_none() {
                    state.first_byte_ms = Some(now_milliseconds().saturating_sub(state.started_at));
                }

                if !state.announced_first_byte {
                    println!(
                        "{name}: first bytes after {} ms ({} the writer)",
                        state.first_byte_ms.unwrap_or_default(),
                        if state.first_byte_ms.unwrap_or_default() < 100 {
                            "inside"
                        } else {
                            "after"
                        },
                    );
                    state.announced_first_byte = true;
                }

                if state.last_report.elapsed() >= Duration::from_millis(500) {
                    println!(
                        "recv {name}: {:.1} / {:.1} MiB ({:.1} MiB/s)",
                        to_mib(state.read_bytes),
                        to_mib(manifest.size),
                        mib_per_second(state.read_bytes, state.first_seen.elapsed()),
                    );
                    state.last_report = Instant::now();
                }
            }

            if manifest.state == "complete" && state.read_bytes >= manifest.size {
                state.output.flush()?;
                state.complete = true;
                println!(
                    "received {name}: {} bytes in {:.2?} ({:.1} MiB/s), first byte {} ms",
                    state.read_bytes,
                    state.first_seen.elapsed(),
                    mib_per_second(state.read_bytes, state.first_seen.elapsed()),
                    state.first_byte_ms.unwrap_or_default(),
                );
            } else {
                active += 1;
            }
        }

        let every_file_complete = !files.is_empty() && files.values().all(|file| file.complete);
        if every_file_complete {
            break;
        }
        if !follow && (!saw_any || active == 0) {
            break;
        }

        thread::sleep(POLL_INTERVAL);
    }

    Ok(())
}

struct Manifest {
    size: u64,
    chunk_size: u64,
    state: String,
    started_at: u64,
}

fn write_manifest(
    directory: &Path,
    size: u64,
    chunk_size: u64,
    state: &str,
    started_at: u64,
) -> io::Result<()> {
    let text = format!(
        "version=1\nsize={size}\nchunk_size={chunk_size}\nstate={state}\nstarted_at={started_at}\n"
    );
    let temporary = directory.join("manifest.tmp");
    let manifest = directory.join("manifest");
    fs::write(&temporary, text)?;
    fs::rename(&temporary, manifest)
}

fn read_manifest(directory: &Path) -> io::Result<Option<Manifest>> {
    let text = match fs::read_to_string(directory.join("manifest")) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    parse_manifest(&text).map(Some)
}

fn parse_manifest(text: &str) -> io::Result<Manifest> {
    let mut size = None;
    let mut chunk_size = None;
    let mut state = String::from("uploading");
    let mut started_at = 0u64;

    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "size" => size = Some(parse_manifest_number(value)?),
            "chunk_size" => chunk_size = Some(parse_manifest_number(value)?),
            "state" => state = value.to_owned(),
            "started_at" => started_at = parse_manifest_number(value)?,
            _ => {}
        }
    }

    Ok(Manifest {
        size: size.ok_or_else(|| invalid_data("manifest is missing size"))?,
        chunk_size: chunk_size.ok_or_else(|| invalid_data("manifest is missing chunk_size"))?,
        state,
        started_at,
    })
}

fn parse_manifest_number(value: &str) -> io::Result<u64> {
    value
        .parse()
        .map_err(|_| invalid_data(format!("invalid manifest number: {value}")))
}

fn write_chunk(directory: &Path, index: u64, bytes: &[u8]) -> io::Result<()> {
    let temporary = directory.join(format!(".{index:08}.tmp"));
    let chunk = directory.join(format!("{index:08}"));
    fs::write(&temporary, bytes)?;
    fs::rename(&temporary, chunk)
}

fn read_full(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = reader.read(&mut buffer[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

fn file_name(path: &Path) -> io::Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| invalid_input(format!("invalid file name: {}", path.display())))
}

fn parse_size(value: &str) -> io::Result<u64> {
    let value = value.trim();
    let (digits, multiplier) = match value.as_bytes().last().copied() {
        Some(b'k' | b'K') => (&value[..value.len() - 1], 1024u64),
        Some(b'm' | b'M') => (&value[..value.len() - 1], 1024u64 * 1024),
        Some(b'g' | b'G') => (&value[..value.len() - 1], 1024u64 * 1024 * 1024),
        _ => (value, 1),
    };
    let base: u64 = digits
        .parse()
        .map_err(|_| invalid_input(format!("invalid size: {value}")))?;
    base.checked_mul(multiplier)
        .ok_or_else(|| invalid_input(format!("size is too large: {value}")))
}

fn now_milliseconds() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn to_mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn mib_per_second(bytes: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        0.0
    } else {
        to_mib(bytes) / seconds
    }
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(name: &str) -> PathBuf {
        let path = env::temp_dir().join(format!(
            "infinitefs-stream-spike-{}-{name}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("test root should be created");
        path
    }

    #[test]
    fn round_trip_streams_all_bytes() {
        let root = test_root("round-trip");
        let source = root.join("source");
        let store = root.join("store");
        let output = root.join("output");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&store).unwrap();
        fs::create_dir_all(&output).unwrap();

        let data: Vec<u8> = (0..(3 * 1024 * 1024 + 12345))
            .map(|index| (index % 251) as u8)
            .collect();
        fs::write(source.join("payload.bin"), &data).unwrap();

        send(&source, &store, 1024 * 1024, Duration::ZERO).unwrap();
        receive(&store, &output, false).unwrap();

        assert_eq!(fs::read(output.join("payload.bin")).unwrap(), data);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn round_trip_handles_empty_file() {
        let root = test_root("empty");
        let source = root.join("source");
        let store = root.join("store");
        let output = root.join("output");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&store).unwrap();
        fs::create_dir_all(&output).unwrap();

        fs::write(source.join("empty.bin"), b"").unwrap();
        send(&source, &store, 1024 * 1024, Duration::ZERO).unwrap();
        receive(&store, &output, false).unwrap();

        assert!(fs::metadata(output.join("empty.bin")).unwrap().is_file());
        assert_eq!(
            fs::read(output.join("empty.bin")).unwrap(),
            Vec::<u8>::new()
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn parses_size_suffixes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("2M").unwrap(), 2 * 1024 * 1024);
        assert!(parse_size("0").is_ok());
        assert!(parse_size("nope").is_err());
    }
}
