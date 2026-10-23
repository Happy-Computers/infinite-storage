# InfiniteFS

A cloud filesystem that's infinitely scalable for humans and agents alike. Early development.

InfiniteFS is an experimental filesystem interface for AI agents. Files stay in cloud storage, and programs read and write them with ordinary path operations.

The goal is to read and write cloud files without keeping a permanent local copy of every file.

> Project status: early development. The Rust CLI and local filesystem browser work. Cloud storage, streaming, caching, and filesystem mounting are not implemented.

## Current features

- CLI commands for version, status, diagnostics, local storage, and the terminal UI.
- Local storage operations: write, read, list, and create directories, with path validation.
- A terminal UI for browsing the configured storage directory.
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

None of these have a release date.

## Technology stack

| Component | Technology |
|---|---|
| Programming language | Rust (Edition 2024) |
| CLI argument parsing | Clap |
| Terminal interface | Ratatui |
| Terminal input and output | Crossterm |
| Opening files in default applications | `open` crate |
| Current storage | Local filesystem |
| Planned cloud storage | Amazon S3 |
| Planned Windows filesystem integration | WinFsp |

## Requirements

- Linux, macOS, or Windows.
- The Rust toolchain and Cargo.
- A terminal and Git for development.

The storage directory defaults to a per-user application data directory and can be overridden with the `INFINITE_STORAGE_DATA` environment variable.

## Getting started

### 1. Clone the repository

```bash
git clone https://github.com/Sahil4912/infinitefs.git
cd infinitefs
```

### 2. Build the project

```bash
cargo check
```

### 3. Run the tests

```bash
cargo test
```

### 4. Format the code

```bash
cargo fmt
```

### 5. View the available CLI commands

```bash
cargo run -- --help
```

### Local data directory

The storage directory is resolved when a command needs it:

1. `INFINITE_STORAGE_DATA`, if set to an absolute path.
2. `$XDG_DATA_HOME/infinite-storage` when `XDG_DATA_HOME` is an absolute path.
3. `$HOME/.local/share/infinite-storage` otherwise.
4. On Windows, `%LOCALAPPDATA%\infinite-storage`, falling back to
   `%USERPROFILE%\infinite-storage`.

The directory is created automatically. Relative values are rejected so the location never depends on the working directory. Override the location for development or tests:

```bash
INFINITE_STORAGE_DATA=/tmp/infinite-storage-data infinitefs storage list
```

For a shared or persistent location outside `$HOME`, create the directory once and point the override at it:

```bash
sudo install -d -o "$USER" -g "$USER" /var/lib/infinite-storage
INFINITE_STORAGE_DATA=/var/lib/infinite-storage infinitefs storage list
```

Keep test files and development data separate from important personal files.

The local browser is not a mounted cloud filesystem. S3 integration and cloud-backed access are planned.

## Security

The CLI validates every path before it touches the local filesystem and rejects symbolic links and traversal. These safeguards are still evolving and have not been audited.

- Never commit AWS credentials, passwords, access tokens, or private keys.
- Use secure AWS credential providers when cloud integration is introduced.
- Review changes that affect path validation, symbolic links, storage access, or permissions carefully.
- Do not use experimental builds with sensitive data without assessing their safety yourself.

For vulnerability reports, see [`SECURITY.md`](SECURITY.md).

## Contributing

Contributions, bug reports, and suggestions are welcome.

Before a substantial change, open an issue to discuss the approach. Contribute code through a fork and pull request so changes can be reviewed before merging.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup, hooks, and review expectations.

## Licence

InfiniteFS is licensed under the MIT Licence. See [`LICENSE`](LICENSE) for the complete licence text.

## Project goals

- Efficient access to large files.
- Low latency and less local storage.
- Safe filesystem operations.
- Usability for developers and AI agents.
- Clear performance and cost trade-offs.
