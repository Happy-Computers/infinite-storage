mod cli;
mod core;
mod tui;

use clap::Parser;
use cli::{Cli, Commands, StorageCommands};
use core::LocalStorage;
use std::io::{self, Write};

const STORAGE_DIRECTORY: &str = r"F:\infiniteFS_Data";

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Version => {
            println!("InfiniteFS CLI v{}", env!("CARGO_PKG_VERSION"));
        }

        Commands::Status => {
            println!("InfiniteFS development status");
            println!("CLI: Running");
            println!("Local storage: Configured");
            println!("Authentication: Not configured");
            println!("Cloud storage: Not connected");
            println!("Filesystem mount: Not implemented");
        }

        Commands::Doctor => {
            println!("InfiniteFS diagnostic check");
            println!("CLI: Available");
            println!("Local storage path: {STORAGE_DIRECTORY}");
            println!("Cloud storage: Not configured");
            println!("Filesystem mount: Not implemented");
        }

        Commands::Storage { command } => {
            run_storage_command(command);
        }

        Commands::Tui => {
            if let Err(error) = tui::run(STORAGE_DIRECTORY) {
                eprintln!("TUI error: {error}");
            }
        }
    }
}

fn run_storage_command(command: StorageCommands) {
    let storage = match LocalStorage::new(STORAGE_DIRECTORY) {
        Ok(storage) => storage,
        Err(error) => {
            eprintln!("Could not initialize storage: {error}");
            return;
        }
    };

    match command {
        StorageCommands::Write { path } => {
            print!("Enter one line of text to write to '{path}': ");

            if let Err(error) = io::stdout().flush() {
                eprintln!("Could not display input prompt: {error}");
                return;
            }

            let mut contents = String::new();

            match io::stdin().read_line(&mut contents) {
                Ok(0) => {
                    eprintln!("No input received; file was not written.");
                    return;
                }
                Ok(_) => {
                    while contents.ends_with('\n') || contents.ends_with('\r') {
                        contents.pop();
                    }
                }
                Err(error) => {
                    eprintln!("Could not read input: {error}");
                    return;
                }
            }

            match storage.write_file(&path, &contents) {
                Ok(()) => println!("File written: {path}"),
                Err(error) => {
                    eprintln!("Could not write file '{path}': {error}");
                }
            }
        }

        StorageCommands::Read { path } => match storage.read_file(&path) {
            Ok(contents) => {
                println!("Contents of '{path}':");
                println!("{contents}");
            }
            Err(error) => {
                eprintln!("Could not read file '{path}': {error}");
            }
        },

        StorageCommands::List { directory } => {
            let result = match directory.as_deref() {
                Some(path) => storage.list_files_in(path),
                None => storage.list_files(),
            };

            match result {
                Ok(files) => {
                    match directory.as_deref() {
                        Some(path) => println!("Files in '{path}':"),
                        None => println!("Files in InfiniteFS storage:"),
                    }

                    if files.is_empty() {
                        println!("(No files found)");
                    } else {
                        for file in files {
                            println!("{file}");
                        }
                    }
                }
                Err(error) => {
                    eprintln!("Could not list files: {error}");
                }
            }
        }

        StorageCommands::Mkdir { path } => match storage.create_dir(&path) {
            Ok(true) => println!("Directory created: {path}"),
            Ok(false) => println!("Directory already exists: {path}"),
            Err(error) => {
                eprintln!("Could not create directory '{path}': {error}");
            }
        },
    }
}
