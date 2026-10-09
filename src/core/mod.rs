use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();

        fs::create_dir_all(&root)?;

        // Store an absolute, canonical path to the storage directory.
        let root = fs::canonicalize(root)?;

        Ok(Self { root })
    }

    fn file_path(&self, name: &str) -> io::Result<PathBuf> {
        validate_filename(name)?;

        Ok(self.root.join(name))
    }

    pub fn write_file(&self, name: &str, contents: &str) -> io::Result<()> {
        let path = self.file_path(name)?;
        fs::write(path, contents)
    }

    pub fn read_file(&self, name: &str) -> io::Result<String> {
        let path = self.file_path(name)?;
        fs::read_to_string(path)
    }
}

/// Accept only simple filenames, never paths to other directories.
fn validate_filename(name: &str) -> io::Result<()> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Invalid filename: use letters, numbers, dots, underscores, or hyphens only",
        )
    };

    if name.is_empty()
        || name == "."
        || name == ".."
        || name.starts_with('.')
        || name.ends_with('.')
        || name.len() > 255
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        return Err(invalid());
    }

    // Reject reserved Windows device names, including names with extensions.
    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();

    if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (["COM", "LPT"].iter().any(|prefix| {
            stem.strip_prefix(prefix).is_some_and(|suffix| {
                suffix.len() == 1 && suffix.as_bytes()[0].is_ascii_digit() && suffix != "0"
            })
        }))
    {
        return Err(invalid());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    // Each test gets its own temporary directory.
    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("infinitefs-test-{}-{id}", std::process::id()));

            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn writes_and_reads_file() {
        let directory = TestDirectory::new();
        let storage = LocalStorage::new(&directory.0).unwrap();

        storage
            .write_file("hello.txt", "Hello from InfiniteFS!")
            .unwrap();

        let contents = storage.read_file("hello.txt").unwrap();

        assert_eq!(contents, "Hello from InfiniteFS!");
    }

    #[test]
    fn missing_file_returns_not_found() {
        let directory = TestDirectory::new();
        let storage = LocalStorage::new(&directory.0).unwrap();

        let error = storage.read_file("missing.txt").unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn rejects_path_traversal_and_invalid_names() {
        let directory = TestDirectory::new();
        let storage = LocalStorage::new(&directory.0).unwrap();

        let invalid_names = [
            "",
            ".",
            "..",
            "../outside.txt",
            r"..\outside.txt",
            r"subfolder\file.txt",
            "/outside.txt",
            "hidden.txt?",
            "CON",
            "NUL.txt",
        ];

        for name in invalid_names {
            let error = storage.write_file(name, "should not be written");

            assert!(error.is_err(), "Expected filename {name:?} to be rejected");

            let error = storage.read_file(name).unwrap_err();

            assert_eq!(
                error.kind(),
                io::ErrorKind::InvalidInput,
                "Unexpected error for filename {name:?}"
            );
        }
    }
}
