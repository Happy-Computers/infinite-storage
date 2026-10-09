
mod cli;

use clap::Parser;
use cli::{Cli, Commands};

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
    }
}
