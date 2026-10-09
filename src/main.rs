mod cli;
mod core;

use clap::Parser;
use cli::{Cli, Commands};
use core::LocalStorage;

const STORAGE_DIRECTORY: &str = r"F:\infiniteFS_Data";
const TEST_FILE: &str = "hello.txt";

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Version => {
            println!("InfiniteFS CLI v{}", env!("CARGO_PKG_VERSION"));
        }
        Commands::Status => {
            println!("InfiniteFS development status");
            println!("CLI: Running");
            println!("Authentication: Not configured");
            println!("Cloud storage: Not connected");
            println!("Filesystem mount: Not implemented");
        }
        Commands::Doctor => {
            println!("InfiniteFS diagnostic check");
            println!("CLI: Available");
            println!("Cloud storage: Not configured");
            println!("Filesystem mount: Not implemented");
        }
        Commands::StorageWrite => match LocalStorage::new(STORAGE_DIRECTORY) {
            Ok(storage) => {
                let contents = "Hello from InfiniteFS!\nThis is a test file.";

                match storage.write_file(TEST_FILE, contents) {
                    Ok(()) => println!("Test file written to {}\\{}", STORAGE_DIRECTORY, TEST_FILE),
                    Err(error) => eprintln!("Could not write test file: {error}"),
                }
            }
            Err(error) => eprintln!("Could not initialize storage: {error}"),
        },
        Commands::StorageRead => match LocalStorage::new(STORAGE_DIRECTORY) {
            Ok(storage) => match storage.read_file(TEST_FILE) {
                Ok(contents) => {
                    println!("Contents of {TEST_FILE}:");
                    println!("{contents}");
                }
                Err(error) => eprintln!("Could not read test file: {error}"),
            },
            Err(error) => eprintln!("Could not initialize storage: {error}"),
        },
    }
}
