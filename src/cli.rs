use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "infs",
    version,
    about = "infinite storage for humans and agents"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum Commands {
    Version,
    Status,
    Doctor,
    Tui,
    Storage {
        #[command(subcommand)]
        command: StorageCommands,
    },
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum StorageCommands {
    Write {
        path: String,
    },
    Read {
        path: String,
    },
    List {
        #[arg(value_name = "DIRECTORY")]
        directory: Option<String>,
    },
    Mkdir {
        path: String,
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
            "storage",
            "write",
            "documents/reports/summary.txt",
        ])
        .unwrap();

        assert_eq!(
            cli.command,
            Commands::Storage {
                command: StorageCommands::Write {
                    path: "documents/reports/summary.txt".to_string(),
                },
            }
        );
    }

    #[test]
    fn recognizes_storage_read_path() {
        let cli = Cli::try_parse_from([
            "infinitefs",
            "storage",
            "read",
            "documents/reports/summary.txt",
        ])
        .unwrap();

        assert_eq!(
            cli.command,
            Commands::Storage {
                command: StorageCommands::Read {
                    path: "documents/reports/summary.txt".to_string(),
                },
            }
        );
    }

    #[test]
    fn recognizes_storage_list_without_directory() {
        let cli = Cli::try_parse_from(["infinitefs", "storage", "list"]).unwrap();

        assert_eq!(
            cli.command,
            Commands::Storage {
                command: StorageCommands::List { directory: None },
            }
        );
    }

    #[test]
    fn recognizes_storage_list_with_directory() {
        let cli =
            Cli::try_parse_from(["infinitefs", "storage", "list", "documents/reports"]).unwrap();

        assert_eq!(
            cli.command,
            Commands::Storage {
                command: StorageCommands::List {
                    directory: Some("documents/reports".to_string()),
                },
            }
        );
    }

    #[test]
    fn recognizes_nested_directory_creation() {
        let cli =
            Cli::try_parse_from(["infinitefs", "storage", "mkdir", "projects/infinitefs"]).unwrap();

        assert_eq!(
            cli.command,
            Commands::Storage {
                command: StorageCommands::Mkdir {
                    path: "projects/infinitefs".to_string(),
                },
            }
        );
    }

    #[test]
    fn rejects_missing_file_path() {
        assert!(Cli::try_parse_from(["infinitefs", "storage", "read"]).is_err());
        assert!(Cli::try_parse_from(["infinitefs", "storage", "write"]).is_err());
    }

    #[test]
    fn rejects_unknown_command() {
        assert!(Cli::try_parse_from(["infinitefs", "unknown"]).is_err());
    }

    #[test]
    fn rejects_unknown_storage_command() {
        assert!(Cli::try_parse_from(["infinitefs", "storage", "unknown"]).is_err());
    }
}
