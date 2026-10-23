# InfiniteFS

**A cloud-backed filesystem for AI agents — under development.**

InfiniteFS is an experimental project exploring how AI agents and applications can work with files through a filesystem-like interface while keeping persistent storage in the cloud.

The goal is to make cloud-stored files easier to access and work with, without requiring every file to be permanently stored on the local machine.

> **Project status:** Early development. The Rust CLI foundation and local filesystem browser are implemented. Cloud-backed storage, streaming, caching, and filesystem mounting are planned and are not yet complete.

## Current Features

- **Rust CLI:** Command-line foundation with version, status, and diagnostic commands.
- **Local storage:** Basic file operations with path validation.
- **Terminal UI:** Browse files and directories within the configured local data directory.
- **Text preview:** Preview supported text files within a size limit.
- **Media opening:** Open supported image and video files using the operating system's default application.
- **Safety checks:** Validate file paths, reject path traversal, and check symbolic links in relevant local filesystem operations.
- **Automated tests:** Unit tests for core storage and terminal UI functionality.

## Planned Roadmap

- [ ] Introduce a storage abstraction for local and cloud backends.
- [ ] Integrate Amazon S3 using the AWS SDK for Rust.
- [ ] Implement efficient streaming and caching.
- [ ] Explore a mounted Windows filesystem using WinFsp.
- [ ] Improve support for AI-agent file workflows.
- [ ] Benchmark performance, latency, bandwidth, and storage costs.
- [ ] Expand security testing and documentation.

These are planned capabilities, not promises of a release date.

## Technology Stack

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

- Windows 11 or another supported Rust development environment.
- Rust toolchain and Cargo.
- A terminal and Git for development.

The current development configuration uses Windows and a local data directory at `F:\infiniteFS_Data`. This path is specific to the current development setup; it is not a universal installation path.

## Getting Started

### 1. Clone the repository

```powershell
git clone https://github.com/Sahil4912/infinitefs.git
cd infinitefs
```

### 2. Build the project

```powershell
cargo check
```

### 3. Run the tests

```powershell
cargo test
```

### 4. Format the code

```powershell
cargo fmt
```

### 5. View the available CLI commands

```powershell
cargo run -- --help
```

Consult the CLI help output for the commands supported by your current build.

### Local Data Directory

The current TUI development configuration uses:

```text
F:\infiniteFS_Data
```

Create this directory yourself before launching the TUI if it does not exist. Keep test files and development data separate from important personal files.

**Important:** Do not assume the current local browser is already a mounted cloud filesystem. S3 integration and cloud-backed file access remain planned work.

## Security

Security is an important design goal for InfiniteFS.

Current development includes local path validation and checks intended to restrict filesystem operations to the configured data directory. These safeguards are still evolving and should not be treated as a complete security audit.

- Never commit AWS credentials, passwords, access tokens, or private keys.
- Use secure AWS credential providers when cloud integration is introduced.
- Review changes that affect path validation, symbolic links, storage access, or permissions carefully.
- Do not use experimental builds with sensitive data without independently assessing their safety.

For vulnerability reports, follow the instructions in [`SECURITY.md`](SECURITY.md) once that file is available.

## Contributing

Contributions, bug reports, and suggestions are welcome.

Before making a substantial change, open an issue to discuss the proposed approach. For code contributions, use a fork and pull request so changes can be reviewed before merging.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for contribution guidelines once available.

## Licence

InfiniteFS is licensed under the MIT Licence. See [`LICENSE`](LICENSE) for the complete licence text.

## Project Goals

InfiniteFS aims to explore a practical filesystem-like interface for cloud-backed data access, with a focus on:

- Efficient access to large files.
- Low latency and reduced unnecessary local storage.
- Safe filesystem operations.
- Developer and AI-agent usability.
- Transparent performance and cost trade-offs.

The project is experimental, and its architecture and implementation may evolve as these goals are tested.
