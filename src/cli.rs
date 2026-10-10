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

    /// Write one line of text to a relative file path
    StorageWrite {
        /// Relative file path, such as documents/reports/summary.txt
        path: String,
    },

    /// Read a text file from a relative file path
    StorageRead {
        /// Relative file path, such as documents/reports/summary.txt
        path: String,
    },

    /// List files in the storage root or a relative directory
    StorageList {
        /// Relative directory, such as documents/reports
        #[arg(value_name = "DIRECTORY")]
        directory: Option<String>,
    },

    /// Create a directory, including any missing parent directories
    StorageMkdir {
        /// Relative directory path, such as projects/infinitefs
        name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_version_command() {
        let cli = Cli::try_parse_from(["infinitefs", "version"]).unwrap();
        assert_eq!(cli.command, Commands::Version);
    }

    #[test]
    fn recognizes_status_command() {
        let cli = Cli::try_parse_from(["infinitefs", "status"]).unwrap();
        assert_eq!(cli.command, Commands::Status);
    }

    #[test]
    fn recognizes_storage_write_path() {
        let cli = Cli::try_parse_from([
            "infinitefs",
            "storage-write",
            "documents/reports/summary.txt",
        ])
        .unwrap();

        assert_eq!(
            cli.command,
            Commands::StorageWrite {
                path: "documents/reports/summary.txt".to_string()
            }
        );
    }

    #[test]
    fn recognizes_storage_read_path() {
        let cli = Cli::try_parse_from([
            "infinitefs",
            "storage-read",
            "documents/reports/summary.txt",
        ])
        .unwrap();

        assert_eq!(
            cli.command,
            Commands::StorageRead {
                path: "documents/reports/summary.txt".to_string()
            }
        );
    }

    #[test]
    fn recognizes_storage_list_without_directory() {
        let cli = Cli::try_parse_from(["infinitefs", "storage-list"]).unwrap();

        assert_eq!(cli.command, Commands::StorageList { directory: None });
    }

    #[test]
    fn recognizes_storage_list_with_directory() {
        let cli = Cli::try_parse_from(["infinitefs", "storage-list", "documents/reports"]).unwrap();

        assert_eq!(
            cli.command,
            Commands::StorageList {
                directory: Some("documents/reports".to_string())
            }
        );
    }

    #[test]
    fn recognizes_nested_directory_creation() {
        let cli =
            Cli::try_parse_from(["infinitefs", "storage-mkdir", "projects/infinitefs"]).unwrap();

        assert_eq!(
            cli.command,
            Commands::StorageMkdir {
                name: "projects/infinitefs".to_string()
            }
        );
    }

    #[test]
    fn rejects_missing_file_path() {
        assert!(Cli::try_parse_from(["infinitefs", "storage-read"]).is_err());
        assert!(Cli::try_parse_from(["infinitefs", "storage-write"]).is_err());
    }

    #[test]
    fn rejects_unknown_command() {
        assert!(Cli::try_parse_from(["infinitefs", "unknown"]).is_err());
    }
}
