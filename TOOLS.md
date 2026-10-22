
# InfiniteFS — Technology and Development Tracker

## Project

- Name: InfiniteFS
- Version: 0.1.0
- Goal: A cloud-backed filesystem for AI agents and applications.
- Current milestone: Local TUI file browsing.
- Primary language: Rust.

## Technologies Currently Used

| Technology | Purpose | Status |
|---|---|---|
| Rust | Application and filesystem logic | In use |
| Cargo | Build, dependency and test management | In use |
| Clap | CLI arguments and subcommands | In use |
| Ratatui | Terminal user interface | In use |
| Crossterm | Terminal input and events | In use |
| Rust standard library | Filesystem, paths, I/O and errors | In use |
| Git | Version control | In use |
| GitHub | Remote repository hosting | In use |
| PowerShell | Windows development and testing | In use |
| VS Code | Code editor | Development tool |

### Image Preview

- `image` — Decodes image files into pixel data.
- `ratatui-image` — Renders images inside the Ratatui TUI.
- Rendering strategy — Unicode half-block fallback for broad terminal compatibility.
- Image processing — Decode only when previewing; resize to fit the preview panel.
- Security — Restrict image dimensions and decoded memory usage for untrusted files.
- Current status — Dependencies being integrated; video previews are planned separately.


## Current Application Features

- Version, status and doctor CLI commands.
- Local storage core.
- File write and read operations through the CLI.
- File listing and directory creation through the CLI.
- Basic filename and path validation in the storage core.
- Interactive TUI navigation.
- TUI directory and file listing.
- Read-only text preview, limited to 1 MiB.
- TUI configured to browse F:\infiniteFS_Data.
- TUI rejects paths outside its configured root and skips symbolic links.

## Dependency Management

- Cargo.toml declares direct dependencies.
- Cargo.lock records resolved dependency versions.
- `cargo tree` displays direct and transitive dependencies.
- `cargo fmt --check` checks formatting.
- `cargo check` checks compilation.
- `cargo test` runs automated tests.

## Planned Technologies — Not Yet Implemented

| Technology | Intended use | Status |
|---|---|---|
| Tokio | Asynchronous I/O when needed | Planned |
| Serde | Serialization and deserialization | Planned |
| Tracing | Structured logging | Planned |
| AWS SDK for Rust | Access to Amazon S3 | Planned |
| Amazon S3 | Cloud object storage backend | Planned |
| Bounded local cache | Cache frequently accessed file data | Planned |
| FFmpeg | Extract video frames for previews | Under consideration |
| WinFsp | Windows filesystem mounting | Planned |
| FUSE-compatible tooling | Linux/macOS filesystem integration | Planned |
| API/authentication layer | Secure agent access | Planned |
| Image thumbnail renderer | Terminal-compatible image previews | Under research |

## Future Milestones

1. Verify the TUI against the configured local directory.
2. Add tests for root isolation and navigation.
3. Research and implement image previews.
4. Investigate video-frame previews.
5. Define a shared storage interface.
6. Implement the S3 backend.
7. Design streaming and bounded caching.
8. Evaluate filesystem mounting.
9. Benchmark performance and harden security.

## Security Principles

- Never commit passwords, API keys or cloud credentials.
- Do not automatically create the configured TUI root.
- Restrict TUI navigation to its configured root.
- Do not follow symbolic links in the TUI browser.
- Use isolated test directories for write-operation tests.
- Avoid destructive operations without explicit confirmation.
- Test changes before committing them.

## Useful Commands

```powershell
cargo fmt --check
cargo check
cargo test
cargo tree
cargo run -- --help
cargo run -- tui
```
