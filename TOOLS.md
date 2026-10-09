# InfiniteFS — Tools and Dependencies

## Project
A cloud-backed filesystem for AI agents.

Current stage: CLI foundation.

## Programming Language
- Rust — primary language for the CLI and planned filesystem engine.

## Direct Dependencies
- `clap` — parses command-line arguments and defines subcommands.
- `clap` derive feature — lets us define CLI commands using Rust derive macros.

## Dependency Management
- `Cargo.toml` — declares direct dependencies.
- `Cargo.lock` — records resolved dependency versions.
- `cargo tree` — displays direct and transitive dependencies.

## Development Tools
- Rust toolchain — compiles Rust code.
- Cargo — project, dependency, build, and test management.
- Visual Studio Code — code editor.
- Git — source control.
- GitHub — remote repository hosting.
- PowerShell — Windows terminal.

## Planned Technologies — Not Yet Implemented
- Tokio — asynchronous runtime.
- Serde — serialization and deserialization.
- Tracing — structured logging.
- AWS S3 — cloud object storage.
- WinFsp — Windows filesystem integration.
- FUSE-compatible tooling — Linux and potentially macOS filesystem integration.
- Backend API and database — authentication, metadata, and access control.

## Current Commands
- `cargo run -- --help`
- `cargo run -- version`
- `cargo run -- status`
- `cargo check`
- `cargo test`
- `cargo build`

## Security Principles
- Never commit passwords, API keys, or cloud credentials.
- Use synthetic test data during development.
- Keep generated build files out of Git.
- Use isolated test storage before connecting personal or production cloud data.
