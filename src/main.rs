// The CLI was the only consumer of the storage layer; keep it compiled for the
// upcoming cloud backend.
#[allow(dead_code)]
mod core;
mod platform;
mod tui;
#[allow(dead_code)]
mod vpath;

use std::{fs, process::ExitCode};

fn main() -> ExitCode {
    let storage_directory = match platform::storage_directory() {
        Ok(directory) => directory,
        Err(error) => {
            eprintln!("Could not resolve the storage directory: {error}");
            return ExitCode::FAILURE;
        }
    };

    // The terminal UI requires an existing storage root and no command creates
    // it anymore, so create it here.
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
