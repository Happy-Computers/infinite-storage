#[allow(dead_code)]
mod core;
mod platform;
mod sync;
mod tui;
#[allow(dead_code)]
mod vpath;

use std::{env, ffi::OsString, fs, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    match sync_command() {
        Ok(Some(command)) => return run_sync(command),
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

struct SyncCommand {
    directory: PathBuf,
    watch: bool,
}

fn sync_command() -> Result<Option<SyncCommand>, &'static str> {
    let arguments: Vec<OsString> = env::args_os().skip(1).collect();
    parse_sync_command(&arguments)
}

fn parse_sync_command(arguments: &[OsString]) -> Result<Option<SyncCommand>, &'static str> {
    let Some(command) = arguments.first() else {
        return Ok(None);
    };
    if command != "sync" {
        return Err("Usage: infinitefs sync [--watch] <directory>");
    }

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
    Ok(Some(SyncCommand { directory, watch }))
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
    fn sync_command_requires_one_directory() {
        assert!(parse_sync_command(&[]).unwrap().is_none());
        let command = parse_sync_command(&[OsString::from("sync"), OsString::from("project")])
            .unwrap()
            .unwrap();
        assert_eq!(command.directory, PathBuf::from("project"));
        assert!(!command.watch);
        let command = parse_sync_command(&[
            OsString::from("sync"),
            OsString::from("--watch"),
            OsString::from("project"),
        ])
        .unwrap()
        .unwrap();
        assert!(command.watch);
        assert!(parse_sync_command(&[OsString::from("sync")]).is_err());
        assert!(parse_sync_command(&[OsString::from("other")]).is_err());
    }
}
