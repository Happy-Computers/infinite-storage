#[allow(dead_code)]
mod core;
mod platform;
mod sync;
mod tui;
#[allow(dead_code)]
mod vpath;

use std::{env, ffi::OsString, fs, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    match sync_directory() {
        Ok(Some(directory)) => return run_sync(directory),
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

fn sync_directory() -> Result<Option<PathBuf>, &'static str> {
    let arguments: Vec<OsString> = env::args_os().skip(1).collect();
    parse_sync_arguments(&arguments)
}

fn parse_sync_arguments(arguments: &[OsString]) -> Result<Option<PathBuf>, &'static str> {
    let Some(command) = arguments.first() else {
        return Ok(None);
    };
    if command != "sync" {
        return Err("Usage: infinitefs sync <directory>");
    }

    let directory = arguments
        .get(1)
        .map(PathBuf::from)
        .ok_or("Usage: infinitefs sync <directory>")?;
    if arguments.len() != 2 {
        return Err("Usage: infinitefs sync <directory>");
    }
    Ok(Some(directory))
}

fn run_sync(directory: PathBuf) -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Could not start the sync runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(sync::run(&directory)) {
        Ok(file_count) => {
            println!("Synced {file_count} files.");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Could not sync {}: {error}", directory.display());
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_command_requires_one_directory() {
        assert_eq!(parse_sync_arguments(&[]), Ok(None));
        assert_eq!(
            parse_sync_arguments(&[OsString::from("sync"), OsString::from("project")]),
            Ok(Some(PathBuf::from("project")))
        );
        assert!(parse_sync_arguments(&[OsString::from("sync")]).is_err());
        assert!(parse_sync_arguments(&[OsString::from("other")]).is_err());
    }
}
