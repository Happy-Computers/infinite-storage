use std::fs;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;

        Ok(Self { root })
    }

    /// Resolve a validated relative path and reject existing symbolic links.
    fn checked_path(&self, name: &str) -> io::Result<PathBuf> {
        let components = validate_relative_path(name)?;
        let mut path = self.root.clone();

        for (index, component) in components.iter().enumerate() {
            path.push(component);

            match fs::symlink_metadata(&path) {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() {
                        return Err(invalid_path("Symbolic links are not allowed"));
                    }

                    if index < components.len() - 1 && !metadata.is_dir() {
                        return Err(invalid_path("A parent component is not a directory"));
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }

        Ok(path)
    }

    /// Write a file. Its parent directory must already exist.
    pub fn write_file(&self, name: &str, contents: &str) -> io::Result<()> {
        let path = self.checked_path(name)?;
        fs::write(path, contents)
    }

    /// Read a UTF-8 text file.
    pub fn read_file(&self, name: &str) -> io::Result<String> {
        let path = self.checked_path(name)?;
        fs::read_to_string(path)
    }

    /// List regular files directly inside the storage root.
    pub fn list_files(&self) -> io::Result<Vec<String>> {
        self.list_files_in("")
    }

    /// List regular files directly inside a relative directory.
    /// An empty path means the storage root.
    pub fn list_files_in(&self, directory: &str) -> io::Result<Vec<String>> {
        let path = if directory.is_empty() {
            self.root.clone()
        } else {
            self.checked_path(directory)?
        };

        let metadata = fs::metadata(&path)?;
        if !metadata.is_dir() {
            return Err(invalid_path("The requested path is not a directory"));
        }

        let mut files = Vec::new();

        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;

            if file_type.is_file()
                && let Some(name) = entry.file_name().to_str()
            {
                files.push(name.to_owned());
            }
        }

        files.sort();
        Ok(files)
    }

    /// Create a directory and any missing parent directories.
    /// Returns true if the final directory was created, false if it existed.
    pub fn create_dir(&self, name: &str) -> io::Result<bool> {
        let path = self.checked_path(name)?;

        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(invalid_path("Symbolic links are not allowed"));
                }

                if metadata.is_dir() {
                    return Ok(false);
                }

                return Err(invalid_path(
                    "A file already exists at the requested directory path",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        fs::create_dir_all(&path)?;
        Ok(true)
    }
}

fn invalid_path(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Validate a relative path using either slash style.
/// Absolute paths, traversal components, empty components and invalid
/// filename characters are rejected.
fn validate_relative_path(name: &str) -> io::Result<Vec<&str>> {
    if name.is_empty() || name.starts_with('/') || name.starts_with('\\') || name.contains(':') {
        return Err(invalid_path("Expected a safe relative path"));
    }

    let components: Vec<&str> = name.split(['/', '\\']).collect();

    if components.is_empty() {
        return Err(invalid_path("Path cannot be empty"));
    }

    for component in &components {
        validate_component(component)?;
    }

    Ok(components)
}

fn validate_component(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.starts_with('.')
        || name.ends_with('.')
        || name.len() > 255
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(invalid_path("Invalid path component"));
    }

    // Windows reserves these device names, including when an extension
    // is present (for example, CON.txt).
    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();

    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'));

    if reserved {
        return Err(invalid_path("Reserved filename"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);

            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after the Unix epoch")
                .as_nanos();

            let path = std::env::temp_dir().join(format!(
                "infinitefs-test-{}-{timestamp}-{unique}",
                std::process::id()
            ));

            fs::create_dir_all(&path).expect("test directory should be created");
            Self(path)
        }

        fn storage(&self) -> LocalStorage {
            LocalStorage::new(&self.0).expect("storage should initialize")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn writes_and_reads_file() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        storage
            .write_file("hello.txt", "Hello InfiniteFS")
            .expect("write should succeed");

        assert_eq!(
            storage.read_file("hello.txt").expect("read should succeed"),
            "Hello InfiniteFS"
        );
    }

    #[test]
    fn missing_file_returns_not_found() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        assert_eq!(
            storage.read_file("missing.txt").unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn rejects_unsafe_paths() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        for path in [
            "../outside.txt",
            "..\\outside.txt",
            "folder/../outside.txt",
            "C:\\outside.txt",
            "\\\\server\\share.txt",
            "/outside.txt",
            "folder//file.txt",
            ".hidden",
            "CON.txt",
        ] {
            assert!(
                storage.read_file(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn lists_files_in_sorted_order() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        storage.write_file("zebra.txt", "z").unwrap();
        storage.write_file("apple.txt", "a").unwrap();

        assert_eq!(
            storage.list_files().unwrap(),
            vec!["apple.txt".to_string(), "zebra.txt".to_string()]
        );
    }

    #[test]
    fn list_files_ignores_directories() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        storage.write_file("hello.txt", "hello").unwrap();
        storage.create_dir("documents").unwrap();

        assert_eq!(storage.list_files().unwrap(), vec!["hello.txt"]);
    }

    #[test]
    fn creates_nested_directories() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        assert!(storage.create_dir("projects/infinitefs/notes").unwrap());
        assert!(!storage.create_dir("projects/infinitefs/notes").unwrap());

        assert!(temp.0.join("projects/infinitefs/notes").is_dir());
    }

    #[test]
    fn reads_and_writes_files_in_nested_directories() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        storage.create_dir("documents/reports").unwrap();
        storage
            .write_file("documents/reports/summary.txt", "Quarterly report")
            .unwrap();

        assert_eq!(
            storage.read_file("documents/reports/summary.txt").unwrap(),
            "Quarterly report"
        );
    }

    #[test]
    fn lists_files_inside_nested_directory() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        storage.create_dir("documents").unwrap();
        storage.write_file("documents/b.txt", "b").unwrap();
        storage.write_file("documents/a.txt", "a").unwrap();
        storage.write_file("root.txt", "root").unwrap();

        assert_eq!(
            storage.list_files_in("documents").unwrap(),
            vec!["a.txt".to_string(), "b.txt".to_string()]
        );
    }

    #[test]
    fn rejects_writing_when_parent_directory_is_missing() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        assert!(storage.write_file("missing/hello.txt", "hello").is_err());
    }

    #[test]
    fn rejects_directory_creation_where_file_exists() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        storage.write_file("notes.txt", "content").unwrap();

        assert!(storage.create_dir("notes.txt").is_err());
    }

    #[test]
    fn rejects_listing_a_file_as_a_directory() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        storage.write_file("notes.txt", "content").unwrap();

        assert!(storage.list_files_in("notes.txt").is_err());
    }

    #[test]
    fn rejects_nested_path_traversal() {
        let temp = TestDirectory::new();
        let storage = temp.storage();

        assert!(storage.create_dir("documents/../../outside").is_err());
    }
}
