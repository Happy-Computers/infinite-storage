# InfiniteFS

A cloud filesystem that's infinitely scalable for humans and agents alike.

## Current features

- CLI commands for version, ~~status, diagnostics~~, local storage, and the terminal UI.
- Local storage operations: write, read, list, and create directories, with path validation.
- Text preview for supported files up to 1 MB.
- Opens images and videos in the operating system's default application.
- Path validation rejects traversal, absolute paths, and symbolic links.
- Unit tests cover storage, path rules, storage location, CLI parsing, and the terminal UI.

## Planned roadmap

- [ ] Introduce a storage abstraction for local and cloud backends.
- [ ] Integrate Amazon S3 using the AWS SDK for Rust.
- [ ] Implement streaming and caching.
- [ ] Mount the storage as a Windows filesystem with WinFsp.
- [ ] Improve support for AI-agent file workflows.
- [ ] Benchmark performance, latency, bandwidth, and storage costs.
- [ ] Expand security testing and documentation.

## Requirements

The storage directory defaults to a per-user application data directory and can be overridden with the `INFINITE_STORAGE_DATA` environment variable.

### Local data directory

The storage directory is resolved when a command needs it:

1. `INFINITE_STORAGE_DATA`, if set to an absolute path.
2. `$XDG_DATA_HOME/infinite-storage` when `XDG_DATA_HOME` is an absolute path.
3. `$HOME/.local/share/infinite-storage` otherwise.
4. On Windows, `%LOCALAPPDATA%\infinite-storage`, falling back to
   `%USERPROFILE%\infinite-storage`.

Storage commands create the directory automatically. **The terminal UI requires the directory to exist**, so run a storage command before opening it for the first time. Relative values are rejected so the location never depends on the working directory. Override the location for development or tests:

```bash
INFINITE_STORAGE_DATA=/tmp/infinite-storage-data cargo run -- storage list
```

For a shared or persistent location outside `$HOME`, create the directory once and point the override at it:

```bash
sudo install -d -o "$USER" -g "$USER" /var/lib/infinite-storage
INFINITE_STORAGE_DATA=/var/lib/infinite-storage cargo run -- storage list
```

Keep test files and development data separate from important personal files.

The local browser is not a mounted cloud filesystem. S3 integration and cloud-backed access are planned.

