# Cloud sync: survey and design

## TL;DR

- Use [`object_store`](https://crates.io/crates/object_store) 0.14 (Apache Arrow) as the S3 client.
  It streams uploads and downloads, supports ranged reads, retries, and conditional writes, and
  can run against S3, MinIO, GCS, Azure, memory, or the local filesystem behind one trait.
- Do **not** make the first version a FUSE mount. Watch the existing storage directory with
  [`notify`](https://crates.io/crates/notify) and sync in the background. No file locks, no
  POSIX emulation, no Windows FUSE driver.
- The crate you remembered is [`store-stream`](https://crates.io/crates/store-stream) 0.1.0. It
  does chunked S3 writes with an offset table so readers can fetch bytes before the upload
  finishes. The design is right; the crate is too young to depend on (0.1.0, one maintainer,
  0 stars, old pinned AWS SDK versions, no conditional writes). Copy the layout, not the
  dependency. With fixed-size chunks we can drop its offset table and keep only a
  `confirmed_size` in the manifest, which makes each manifest update O(1).
- A normal S3 object cannot be partially visible. `PutObject` is atomic and multipart parts are
  invisible until `CompleteMultipartUpload`. To show bytes while they are still being written
  you need one of:
  1. **chunk objects + a manifest** (works on any S3 bucket, readers use our client);
  2. **S3 Express One Zone append** (`PutObject` with `x-amz-write-offset-bytes`, visible to any
     S3 reader, but directory buckets only, single writer, 10k appends);
   3. **a local file plus our app streaming it** (remote readers do not see it until upload ends).

## Current uploader

`infinitefs sync --watch <directory>` is a one-way, recursive uploader. It scans existing regular
files, then debounces filesystem events and rechecks the tree after an event overflow. It reads
every file in 1 MiB chunks and writes each chunk before it advances the manifest. The command uses
`INFINITEFS_S3_BUCKET` and standard AWS environment variables. It does not download or resolve
conflicts.

The current key layout is:

```
v1/files/<encoded-relative-file-path>/manifest
v1/files/<encoded-relative-file-path>/chunks/00000000
v1/files/<encoded-relative-file-path>/chunks/00000001
```

The manifest contains the number of confirmed bytes, the fixed chunk size, and `uploading` or
`complete`. A future reader can use it to fetch only chunks that have been published.

## The hard constraint

S3 is an object store, not a file store. Each key is replaced atomically. While a multipart
upload is in progress, the uploaded parts are not visible as an object. Mountpoint for Amazon S3
documents the same behavior: new files are visible to other S3 clients only after close or
`fsync`, unless you use Express One Zone with `--incremental-upload`.

That means "the user sees the file after a few bytes" has to be defined:

- **Inside InfiniteFS**: easy. Stream locally written bytes, or ranged-GET remote chunks.
- **From any S3 client / the AWS console**: only chunk objects with a reader that understands
  them, or S3 Express append. There is no way to make a single standard S3 object partially
  readable.

| Approach | Partial bytes visible to | Standard S3 bucket | Main cost |
| --- | --- | --- | --- |
| Chunk objects + manifest | our client (and any client we write) | yes | many objects, cleanup, manifest consistency |
| S3 Express One Zone append | any S3 reader | no, directory bucket only | single writer, 10k appends, same AZ, higher cost |
| Multipart upload (`object_store::BufWriter`) | nobody until complete | yes | no partial visibility |
| FUSE mount (Mountpoint, s3fs) | nobody until close; local readers blocked while writing | yes | POSIX limits, single writer, no locks |
| Local-first + app streaming | our app immediately | yes | remote readers wait for finalize |

## Library and tool survey

| Name | What it gives us | Verdict |
| --- | --- | --- |
| `object_store` 0.14.2 | `put_opts`, `put_multipart_opts`, `BufWriter`, streaming `GetResult`, `get_ranges`, conditional `PutMode`, S3/GCS/Azure/local/memory backends | **Use it.** Small, well tested, Apache Arrow. Feature `aws` for S3. |
| `aws-sdk-s3` | First-party S3 client, `ByteStream`, multipart, `put_object().write_offset_bytes(i64)` for Express append | Use only for the Express append path, or if `object_store` lacks an S3 feature. Much heavier. |
| `store-stream` 0.1.0 | Chunk objects `key/0000000000`, offset table `key.dat`, ranged reads before completion, resumable API | **Borrow the design.** Not production-ready as a dependency. |
| `opendal` 0.59 | One API for 50+ backends, streaming writer/reader, retry/cache/metric layers | Good alternative to `object_store`. S3 writes still use multipart, so no partial visibility. Bigger surface. |
| `notify` 8.x | Cross-platform filesystem events (inotify, FSEvents, ReadDirectoryChangesW) | **Use it** for the watcher. Add a periodic rescan as a safety net. |
| `fuser` 0.18 / WinFsp | FUSE / Windows filesystem mount | Later, only if a real mount is required. Not v1. |
| Mountpoint for Amazon S3 | AWS FUSE client, read-optimized, sequential writes, visible on close | Not embeddable, no Windows, no read-while-write. Useful as a reference, not as the product. |
| s3fs-fuse | POSIX-ish FUSE mount with local cache and multipart upload | Mature but config-heavy. Object still not partially visible. |
| rclone | `mount`, `sync`, `bisync` in Go | Good fallback/prototype. `mount --vfs-cache-mode writes` uploads on close, not live. `bisync` is not a live watcher. |
| JuiceFS / SeaweedFS | Full POSIX filesystem backed by S3 plus a metadata engine | Right answer if you want a mounted filesystem, wrong answer if you want plain S3 objects. Heavy. |
| Amazon S3 Files | Native NFS mount of an S3 bucket (launched April 2026) | Not embeddable, AWS compute only. Not a library. |
| s2.dev | Append-only streams with tails | Not S3. Only if we can choose a different storage API. |

## Recommended architecture

```
                 +-------------------+
  local folder ->| watcher (notify)  |
                 +---------+---------+
                           | changed path
                           v
                 +-------------------+
                 | uploader          |  chunk 1 MiB, PUT, update manifest
                 | (per-file queue)  |
                 +---------+---------+
                           | S3
                           v
                 +-------------------+
                 | chunk store       |  files/<path>/<version>/<n>
                 | + manifest.json   |  size, chunk size, hashes, state
                 +---------+---------+
                           ^
                           | ranged GET
                 +---------+---------+
                 | reader / cache    |  first chunk immediately, stream to UI
                 +-------------------+
```

### Data layout

```
v1/files/<url-encoded path>/<version>/manifest.json
v1/files/<url-encoded path>/<version>/00000000
v1/files/<url-encoded path>/<version>/00000001
```

Manifest example JSON:

```json
{
  "path": "notes/todo.md",
  "version": "01J...",
  "size": 12345,
  "chunk_size": 1048576,
  "sha256": "...",
  "state": "uploading",
  "updated_at": "2026-10-25T12:34:56Z"
}
```

`size` is the number of bytes confirmed so far. A reader may fetch any byte in `0..size`.
Chunk `n` is at `n * chunk_size`; the last chunk may be shorter. On completion, `state`
becomes `"complete"` and `size` is final.

### Writer path

1. `notify` reports a change. Debounce 200 ms. Do not open the file with an exclusive lock.
   Open read-only, read what is there, close. On Linux use `Access(Close(Write))` to know the
   writer finished; on other platforms use the closest event plus a periodic scan.
2. Start a new version for the path. Read the file in 1 MiB chunks from offset 0. After every
   chunk, `PUT` it and then update the manifest with `PutMode::Update` (ETag precondition). The
   manifest stores `confirmed_size` and `chunk_size`; chunk `n` starts at `n * chunk_size`, so
   readers can find it without an offset table. If the file changes underneath
   (mtime/inode/size), abort the version and restart.
3. On close, mark the manifest `complete` with final size and SHA-256. Optionally compose the
   chunks into one normal object with `UploadPartCopy` so non-InfiniteFS clients see a single
   `files/<path>` key. Do this only when someone needs it; it costs one server-side copy.
4. Rename = write a new manifest and tombstone the old one. Delete = tombstone. Chunks are
   immutable and garbage-collected by a background sweep. Never rewrite a chunk.
5. Prefix every object with a schema version, e.g. `v1/files/...`, so the layout can change.

### Reader path

1. Fetch `manifest.json` (small, cached). If the local file is complete and has the same hash,
   serve it. Otherwise use ranged GETs.
2. Stream the first chunk to the UI immediately, then continue in order. A text preview can
   stop after 64 KiB. Video can be seeked with ranges.
3. For remote -> local, download to `<name>.infinitefs-partial`, verify the hash, then rename
   into place. Never write a partial file under the real name.

### Conflict handling

- Use `PutMode::Create` for a brand-new manifest and `PutMode::Update(UpdateVersion { e_tag,
  version })` for every change. A precondition failure means someone else wrote; re-read and
  either merge or write a conflict copy (`name (conflict 2026-10-25 12-34-56).ext`).
- Keep the last synced manifest ETag per path in a local sidecar database. That is enough to
  distinguish local change, remote change, and both.
- Never let the sync agent upload its own downloads. Keep an in-memory skip set and a sidecar
  state file outside the watched tree (`~/.local/share/infinitefs/`).

### Scope limits for v1

- Regular files only. No symlinks (already rejected by `VPath`), sockets, or devices.
- No directory objects. Directories are implied by keys, like S3 does.
- No POSIX permissions, xattrs, or hard links.
- Ignore the app's own state/cache directory, editor temp files (`*.swp`, `*~`, `.#*`), and
  `.git` by default. Make the ignore list configurable.
- Single writer per path. Multiple machines writing the same file get a conflict copy, not a
  merge.

## Rust stack

```toml
[dependencies]
object_store = { version = "0.14", features = ["aws"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "fs", "sync", "time", "io-util"] }
notify = "8"
bytes = "1"
futures-util = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
thiserror = "2"
```

`object_store` already depends on `tokio`, `bytes`, and `futures-util`, but declare what we use
directly. Skip `aws-sdk-s3` until we decide to use Express append. Skip `anyhow` in library code;
use typed errors.

## API sketch

Upload one chunk and update the manifest conditionally:

```rust
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};

async fn put_chunk(
    store: &dyn ObjectStore,
    key: &Path,
    bytes: Bytes,
) -> object_store::Result<()> {
    store.put_opts(key, bytes.into(), PutOptions::default()).await?;
    Ok(())
}

async fn put_manifest(
    store: &dyn ObjectStore,
    key: &Path,
    json: Bytes,
    previous: Option<UpdateVersion>,
) -> object_store::Result<UpdateVersion> {
    let mode = match previous {
        Some(version) => PutMode::Update(version),
        None => PutMode::Create,
    };
    let result = store
        .put_opts(key, json.into(), PutOptions::from(mode))
        .await?;
    Ok(result.into())
}
```

Stream a remote file from the first byte:

```rust
use futures_util::StreamExt;
use object_store::GetOptions;

let result = store
    .get_opts(&key, GetOptions::new().with_range(0..64 * 1024))
    .await?;
let mut stream = result.into_stream();
while let Some(chunk) = stream.next().await {
    let bytes = chunk?;
    // Hand bytes to the TUI or write them to the local cache.
}
```

Buffer a whole upload when partial visibility is not needed:

```rust
use object_store::buffered::BufWriter;
use tokio::io::AsyncWriteExt;

let mut writer = BufWriter::with_capacity(store.clone(), key.clone(), 8 << 20);
writer.put(Bytes::from(chunk)).await?;
writer.shutdown().await?;
```

`BufWriter` keeps up to 8 MiB in memory, then switches to multipart. That is the right choice for
small files and for the final `files/<path>` object. It is not the right choice when the user
must see bytes early, because multipart parts are invisible.

## What I would build first

A vertical slice, not the whole sync engine:

1. Add the dependencies above.
2. `CloudStore` with four operations: `put_chunk`, `get_range`, `put_manifest`, `get_manifest`.
3. A watcher for one directory that uploads on close, 1 MiB chunks, manifest after each chunk.
4. A TUI action that opens a remote-only file by streaming the first 64 KiB, then continues in
   the background.
5. Tests against `object_store::memory::InMemory` and `object_store::local::LocalFileSystem`.
   Add a MinIO container only when the local tests pass.

That proves the risky part (partial visibility, resumability, conditional manifest) without
committing to FUSE, Windows, or a full bidirectional merge.

## Decisions needed

1. Is the bucket a standard general-purpose bucket or S3 Express One Zone? Express append is the
   only way to get one S3 object that any reader can see while it is being written.
2. Does "the user sees bytes" mean inside InfiniteFS, or from any S3 client? If it is any S3
   client on a standard bucket, the chunk layout needs a reader; the raw key will not be a file.
3. Is v1 a sync folder (watch + background upload/download) or a mounted drive? The sync folder
   is much smaller and matches the current TUI; a mount is a separate project.

## If not Rust

- Go: [`gocloud.dev/blob`](https://gocloud.dev/howto/blob/) is the closest equivalent to
  `object_store` (S3, GCS, Azure, local; `NewWriter` and `NewRangeReader` stream).
- Python: `smart_open` streams to S3; `boto3` with `s3transfer` does multipart.
- Node: `@aws-sdk/lib-storage` `Upload` pipes a stream into multipart.

None of them change the S3 visibility rule. Multipart is still invisible until complete.

## Sources

- `object_store` docs: <https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html>
- `object_store` `BufWriter`: <https://docs.rs/object_store/latest/object_store/buffered/struct.BufWriter.html>
- `store-stream`: <https://github.com/wavey-ai/store-stream>
- S3 multipart behavior: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html>
- S3 Express append: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html>
- Mountpoint semantics: <https://github.com/awslabs/mountpoint-s3/blob/main/doc/SEMANTICS.md>
- S3 conditional writes: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html>
- `notify`: <https://docs.rs/notify>
- Amazon S3 Files: <https://aws.amazon.com/blogs/aws/launching-s3-files-making-s3-buckets-accessible-as-file-systems/>
