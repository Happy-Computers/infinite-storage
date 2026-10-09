use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "infinitefs",
    version,
    about = "A cloud-backed filesystem for AI agents"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum Commands {
    /// Show the CLI version
    Version,

    /// Show the current filesystem status
    Status,

    /// Check the basic CLI environment
    Doctor,

    /// Write a test file to local storage
    StorageWrite,

    /// Read the test file from local storage
    StorageRead,

    /// List regular files in local test storage
    StorageList,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_version_command() {
        let cli = Cli::try_parse_from(["infinitefs", "version"])
            .expect("version command should be valid");

        assert_eq!(cli.command, Commands::Version);
    }

    #[test]
    fn recognizes_status_command() {
        let cli =
            Cli::try_parse_from(["infinitefs", "status"]).expect("status command should be valid");

        assert_eq!(cli.command, Commands::Status);
    }

    #[test]
    fn rejects_unknown_command() {
        let result = Cli::try_parse_from(["infinitefs", "unknown"]);

        assert!(result.is_err());
    }
}
