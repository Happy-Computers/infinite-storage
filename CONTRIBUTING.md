# Contributing to InfiniteFS

Thank you for your interest in contributing to InfiniteFS!

InfiniteFS is an experimental Rust project exploring a filesystem-like interface for cloud-backed storage and AI-agent workflows. Contributions, bug reports, suggestions, and documentation improvements are welcome.

## Before You Start

- Check existing issues and pull requests to avoid duplicating work.
- For significant changes, open an issue first to discuss the proposed approach.
- Keep contributions focused and explain the problem they solve.
- Do not submit credentials, private keys, personal data, or other sensitive information.

## Development Setup

You will need Rust and Cargo installed.

Clone the repository:

```bash
git clone https://github.com/Sahil4912/infinitefs.git
cd infinitefs
```

Check the project:

```bash
cargo check
```

Run the tests:

```bash
cargo test
```

Check formatting:

```bash
cargo fmt --check
```

Check lints:

```bash
cargo clippy --locked --all-targets -- -D warnings
```

All checks should pass before submitting a pull request.

### Pre-commit Hooks

The repository uses [prek](https://prek.j178.dev/) to run formatting, linting, and tests before each commit. Install the hook once per clone:

```bash
prek install
```

Run the checks on demand with:

```bash
prek run --all-files
```

jj does not run Git commit hooks, so jj users should run that command before pushing.

## How to Contribute

### 1. Fork the Repository

Open the InfiniteFS GitHub repository and click **Fork**. This creates a copy under your GitHub account.

### 2. Clone Your Fork

Replace `YOUR_USERNAME` with your GitHub username:

```bash
git clone https://github.com/YOUR_USERNAME/infinitefs.git
cd infinitefs
```

### 3. Create a Branch

Use a descriptive branch name:

```bash
git switch -c fix/describe-the-bug
```

For a feature, use a name such as `feature/describe-the-feature`.

### 4. Make and Test Your Changes

- Keep changes focused.
- Add or update tests where appropriate.
- Run `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo check --locked`, and `cargo test --locked`.
- Review your changes before committing.

### 5. Commit and Push

```bash
git add .
git commit -m "Describe your change"
git push -u origin fix/describe-the-bug
```

If you chose a different branch name, use that name in the push command.

### 6. Open a Pull Request

On GitHub, open your fork and select **Compare & pull request** when offered. Otherwise, open the Pull requests tab and create a new pull request.

Set the base repository to `Sahil4912/infinitefs` and the base branch to `main`. Select your fork and feature branch as the source.

Describe the problem, explain your changes, and list the tests you ran.

The project maintainer will review your pull request and may ask for changes before merging it.

## Code and Security Guidelines

- Preserve existing path-validation and filesystem safety checks.
- Do not weaken security controls without a clear reason and appropriate tests.
- Do not introduce hardcoded credentials or secrets.
- Explain new dependencies and their purpose.
- Do not assume cloud operations are available unless they have been implemented.
- Keep documentation consistent with the current state of the project.

## Scope and Decisions

Not every proposed change will be accepted. Changes should align with the project's goals, security requirements, maintainability, and roadmap.

Please be respectful and constructive when discussing issues and pull requests.

## Licence

By submitting a contribution, you agree that your contribution will be distributed under the project's MIT Licence, as specified in the repository's `LICENSE` file.

Thank you for helping improve InfiniteFS!
