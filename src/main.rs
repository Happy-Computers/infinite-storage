#[allow(dead_code)]
mod core;
mod platform;
mod sync;
mod tui;
#[allow(dead_code)]
mod vpath;

use std::{
    env,
    ffi::OsString,
    fs,
    io::{self, BufRead, IsTerminal, Write},
    path::PathBuf,
    process::ExitCode,
};

fn main() -> ExitCode {
    match command() {
        Ok(Some(Command::Pull(command))) => return run_pull(command),
        Ok(Some(Command::Sync(command))) => return run_sync(command),
        Ok(None) => {}
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    }

    let storage_directory = match platform::storage_directory() {
        Ok(directory) => directory,
        Err(error) => {
            eprintln!("Could not resolve the storage directory: {error}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(error) = fs::create_dir_all(&storage_directory) {
        eprintln!("Could not create the storage directory: {error}");
        return ExitCode::FAILURE;
    }

    if let Err(error) = tui::run(&storage_directory) {
        eprintln!("Could not start the terminal UI: {error}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

enum Command {
    Pull(PullCommand),
    Sync(SyncCommand),
}

struct PullCommand {
    destination: PathBuf,
    root: String,
}

struct SyncCommand {
    directory: PathBuf,
    watch: bool,
}

fn command() -> Result<Option<Command>, &'static str> {
    let arguments: Vec<OsString> = env::args_os().skip(1).collect();
    parse_command(&arguments)
}

fn parse_command(arguments: &[OsString]) -> Result<Option<Command>, &'static str> {
    let Some(command) = arguments.first() else {
        return Ok(None);
    };

    match command.to_str() {
        Some("pull") => parse_pull(arguments).map(Some),
        Some("sync") => parse_sync(arguments).map(Some),
        _ => Err(
            "Usage: infinitefs sync [--watch] <directory>\n       infinitefs pull <s3-root> <directory>",
        ),
    }
}

fn parse_pull(arguments: &[OsString]) -> Result<Command, &'static str> {
    if arguments.len() != 3 {
        return Err("Usage: infinitefs pull <s3-root> <directory>");
    }
    let root = arguments[1]
        .to_str()
        .filter(|root| !root.is_empty())
        .ok_or("The S3 root must be UTF-8")?;
    Ok(Command::Pull(PullCommand {
        root: root.to_owned(),
        destination: PathBuf::from(&arguments[2]),
    }))
}

fn parse_sync(arguments: &[OsString]) -> Result<Command, &'static str> {
    let (watch, directory_index) = if arguments.get(1).is_some_and(|value| value == "--watch") {
        (true, 2)
    } else {
        (false, 1)
    };
    let directory = arguments
        .get(directory_index)
        .map(PathBuf::from)
        .ok_or("Usage: infinitefs sync [--watch] <directory>")?;
    if arguments.len() != directory_index + 1 {
        return Err("Usage: infinitefs sync [--watch] <directory>");
    }
    Ok(Command::Sync(SyncCommand { directory, watch }))
}

fn run_pull(command: PullCommand) -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Could not start the pull runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(sync::pull(&command.root, &command.destination)) {
        Ok(result) => {
            println!("Pulled {} files.", result.file_count);
            if result.newer_version_available
                && retry_newer_version(&mut io::stdin().lock(), &mut io::stderr())
            {
                match runtime.block_on(sync::pull(&command.root, &command.destination)) {
                    Ok(retry) => println!("Pulled {} files.", retry.file_count),
                    Err(error) => {
                        eprintln!("Could not pull {}: {error}", command.root);
                        return ExitCode::FAILURE;
                    }
                }
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Could not pull {}: {error}", command.root);
            ExitCode::FAILURE
        }
    }
}

fn retry_newer_version(input: &mut impl BufRead, output: &mut impl Write) -> bool {
    if !io::stdin().is_terminal() || !io::stderr().is_terminal() {
        eprintln!("A newer version is available. Run pull again to download it.");
        return false;
    }
    if write!(
        output,
        "A newer version is available. Download it now? [y/N] "
    )
    .is_err()
    {
        return false;
    }
    if output.flush().is_err() {
        return false;
    }
    wants_newer_version(input)
}

fn wants_newer_version(input: &mut impl BufRead) -> bool {
    let mut response = String::new();
    input
        .read_line(&mut response)
        .is_ok_and(|_| matches!(response.trim().to_ascii_lowercase().as_str(), "y" | "yes"))
}

fn run_sync(command: SyncCommand) -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Could not start the sync runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(sync::run(&command.directory, command.watch)) {
        Ok(file_count) => {
            println!("Synced {file_count} files.");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Could not sync {}: {error}", command.directory.display());
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pull_and_sync_commands() {
        assert!(parse_command(&[]).unwrap().is_none());
        let Command::Sync(command) = parse_command(&[
            OsString::from("sync"),
            OsString::from("--watch"),
            OsString::from("project"),
        ])
        .unwrap()
        .unwrap() else {
            panic!("expected sync command");
        };
        assert_eq!(command.directory, PathBuf::from("project"));
        assert!(command.watch);

        let Command::Pull(command) = parse_command(&[
            OsString::from("pull"),
            OsString::from("project"),
            OsString::from("output"),
        ])
        .unwrap()
        .unwrap() else {
            panic!("expected pull command");
        };
        assert_eq!(command.root, "project");
        assert_eq!(command.destination, PathBuf::from("output"));
    }

    #[test]
    fn accepts_only_explicit_pull_retry_answers() {
        for answer in ["y\n", "Y\n", "yes\n", "YES\n"] {
            assert!(wants_newer_version(&mut io::Cursor::new(answer)));
        }
        for answer in ["\n", "n\n", "no\n", "later\n", ""] {
            assert!(!wants_newer_version(&mut io::Cursor::new(answer)));
        }
    }
}
