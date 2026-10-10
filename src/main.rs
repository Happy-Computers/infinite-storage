mod cli;
mod core;

use clap::Parser;
use cli::{Cli, Commands};
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

        Commands::StorageWrite { path } => {
            let storage = match LocalStorage::new(STORAGE_DIRECTORY) {
                Ok(storage) => storage,
                Err(error) => {
                    eprintln!("Could not initialize storage: {error}");
                    return;
                }
            };

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
                Err(error) => eprintln!("Could not write file '{path}': {error}"),
            }
        }

        Commands::StorageRead { path } => match LocalStorage::new(STORAGE_DIRECTORY) {
            Ok(storage) => match storage.read_file(&path) {
                Ok(contents) => {
                    println!("Contents of '{path}':");
                    println!("{contents}");
                }
                Err(error) => eprintln!("Could not read file '{path}': {error}"),
            },
            Err(error) => eprintln!("Could not initialize storage: {error}"),
        },

        Commands::StorageList { directory } => match LocalStorage::new(STORAGE_DIRECTORY) {
            Ok(storage) => {
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
                    Err(error) => eprintln!("Could not list files: {error}"),
                }
            }
            Err(error) => eprintln!("Could not initialize storage: {error}"),
        },

        Commands::StorageMkdir { name } => match LocalStorage::new(STORAGE_DIRECTORY) {
            Ok(storage) => match storage.create_dir(&name) {
                Ok(true) => println!("Directory created: {name}"),
                Ok(false) => println!("Directory already exists: {name}"),
                Err(error) => {
                    eprintln!("Could not create directory '{name}': {error}");
                }
            },
            Err(error) => eprintln!("Could not initialize storage: {error}"),
        },
    }
}
