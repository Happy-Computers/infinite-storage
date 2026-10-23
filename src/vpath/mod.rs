//! Virtual paths: the one path namespace shared by every storage backend.
//!
//! A virtual path is independent of any operating system. Backends translate
//! it into native paths (local) or keys (cloud); no other code interprets
//! path strings. The rules here are therefore stricter than POSIX: `/` is the
//! only separator, `\` and `:` are rejected instead of translated, and every
//! accepted path is portable to every platform we support. One unambiguous
//! meaning everywhere is worth rejecting a few unusual but legal filenames.
//!
//! Rules, and why they exist:
//!
//! - Absolute paths are rejected: a virtual path is always relative to the
//!   storage root, so a leading separator has no meaning.
//! - `\` is rejected rather than treated as a separator: POSIX treats it as
//!   an ordinary filename byte, so translating it would give one string two
//!   meanings depending on the platform.
//! - `:` is rejected: it introduces Windows drive and alternate-data-stream
//!   syntax, which must never leak into the virtual namespace.
//! - `.` and `..` are rejected as components: they are traversal, and the
//!   namespace does not need them.
//! - Components cannot start with `.`: leading dots hide files on POSIX and
//!   invite confusion with `.` and `..`.
//! - Components cannot end with `.`: Windows strips trailing dots, so such a
//!   name would not round-trip.
//! - Components are limited to 255 bytes: the common POSIX `NAME_MAX`.
//! - Components use ASCII letters, digits, `.`, `_` and `-` only: a narrow
//!   portable subset avoids Unicode normalization mismatches between
//!   filesystems and keeps cloud keys unambiguous.
//! - Windows device names (`CON`, `NUL`, `COM1`, ...) are rejected: a virtual
//!   path must be usable on every platform we support.

use std::io;

/// POSIX guarantees `NAME_MAX` of at least 255 bytes on the systems we target.
const MAX_COMPONENT_BYTES: usize = 255;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct VPath {
    components: Vec<String>,
}

impl VPath {
    pub(crate) fn parse(path: &str) -> io::Result<Self> {
        if path.is_empty() {
            return Err(invalid("Path cannot be empty"));
        }

        if path.starts_with('/') {
            return Err(invalid("Expected a relative path"));
        }

        if path.contains('\\') {
            return Err(invalid("Backslashes are not allowed in virtual paths"));
        }

        if path.contains(':') {
            return Err(invalid("Colons are not allowed in virtual paths"));
        }

        let mut components = Vec::new();

        for component in path.split('/') {
            validate_component(component)?;
            components.push(component.to_owned());
        }

        Ok(Self { components })
    }

    pub(crate) fn components(&self) -> &[String] {
        &self.components
    }
}

fn validate_component(component: &str) -> io::Result<()> {
    if component.is_empty() {
        return Err(invalid("Path components cannot be empty"));
    }

    if component == "." || component == ".." {
        return Err(invalid("Path components cannot be '.' or '..'"));
    }

    if component.starts_with('.') {
        return Err(invalid("Path components cannot start with '.'"));
    }

    if component.ends_with('.') {
        return Err(invalid("Path components cannot end with '.'"));
    }

    if component.len() > MAX_COMPONENT_BYTES {
        return Err(invalid("Path components cannot exceed 255 bytes"));
    }

    if !component.bytes().all(is_allowed_byte) {
        return Err(invalid(
            "Path components may only contain ASCII letters, digits, '.', '_' and '-'",
        ));
    }

    if is_reserved_device_name(component) {
        return Err(invalid("Path components cannot use reserved device names"));
    }

    Ok(())
}

fn is_allowed_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

fn is_reserved_device_name(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();

    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_components(path: &str) -> Vec<String> {
        VPath::parse(path)
            .expect("path should be valid")
            .components()
            .to_vec()
    }

    #[test]
    fn splits_nested_paths() {
        assert_eq!(
            parse_components("documents/reports/summary.txt"),
            ["documents", "reports", "summary.txt"]
        );
    }

    #[test]
    fn accepts_allowed_characters() {
        assert_eq!(
            parse_components("reports/2026-01_summary.v2.txt"),
            ["reports", "2026-01_summary.v2.txt"]
        );
    }

    #[test]
    fn rejects_absolute_paths() {
        for path in ["/etc/passwd", "/", "\\server\\share"] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_backslashes() {
        for path in ["a\\b.txt", "..\\outside.txt", "folder\\file.txt"] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_colons() {
        for path in ["C:\\outside.txt", "file:stream", "folder/a:b.txt"] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_traversal_components() {
        for path in [".", "..", "a/../b", "a/./b", "a/..", "../a"] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_empty_components() {
        for path in ["a//b", "a/", "a///b", "//a"] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_leading_dots() {
        for path in [".hidden", "a/.hidden", ".config/file.txt"] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_trailing_dots() {
        for path in ["file.", "dir./file", "a.b."] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_disallowed_characters() {
        for path in [
            "hello world.txt",
            "café.txt",
            "star*.txt",
            "question?.txt",
            "pipe|.txt",
            "tab\t.txt",
            "quote\".txt",
        ] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn bounds_component_length() {
        let longest = "a".repeat(MAX_COMPONENT_BYTES);
        assert!(VPath::parse(&longest).is_ok());

        let too_long = "a".repeat(MAX_COMPONENT_BYTES + 1);
        assert!(VPath::parse(&too_long).is_err());
    }

    #[test]
    fn rejects_reserved_device_names() {
        for path in [
            "CON",
            "con.txt",
            "NUL",
            "AUX",
            "PRN",
            "COM1",
            "LPT9",
            "folder/COM3.txt",
        ] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn accepts_names_that_resemble_reserved_device_names() {
        for path in ["COM0", "COM10", "LPT0", "CONSOLE.txt", "conifer.txt"] {
            assert!(
                VPath::parse(path).is_ok(),
                "path should be accepted: {path}"
            );
        }
    }
}
