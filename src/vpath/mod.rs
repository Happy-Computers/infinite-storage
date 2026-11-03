//! Virtual paths: the one path namespace shared by every storage backend.
//!
//! A virtual path is independent of any operating system. Backends translate
//! it into native paths (local) or keys (cloud); no other code interprets
//! path strings. The rules here are therefore stricter than POSIX: `/` is the
//! only separator, native path syntax is rejected, and every accepted
//! component has one portable meaning on Windows, Linux, and macOS. Names are
//! limited to ASCII so case collisions and Unicode normalization cannot vary
//! by filesystem.
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
//! - Dotfiles are allowed, while `.` and `..` are rejected as traversal.
//! - Components cannot begin or end with an ASCII space, or end with `.`:
//!   Windows strips leading and trailing spaces and trailing dots, so the name
//!   would not round-trip.
//! - Components are limited to 255 bytes by the InfiniteFS virtual namespace.
//!   This is a per-component policy, not a guarantee about every filesystem's
//!   component limit or the maximum length of a complete path.
//! - Components use printable ASCII except Windows path syntax. Percent signs
//!   round-trip through object-store path encoding, while rejecting Unicode
//!   avoids normalization differences between filesystems.
//! - Windows device names (`CON`, `NUL`, `COM1`, ...) are rejected: a virtual
//!   path must be usable on every platform we support.

use std::io;

/// InfiniteFS limits each component to 255 bytes; filesystems can impose
/// smaller component limits, and total path limits are separate.
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

    if component.starts_with(' ') {
        return Err(invalid("Path components cannot begin with a space"));
    }

    if component.ends_with(['.', ' ']) {
        return Err(invalid("Path components cannot end with a dot or space"));
    }

    if component.len() > MAX_COMPONENT_BYTES {
        return Err(invalid("Path components cannot exceed 255 bytes"));
    }

    if !component.bytes().all(is_allowed_byte) {
        return Err(invalid("Path component contains a non-portable character"));
    }

    if is_reserved_device_name(component) {
        return Err(invalid("Path components cannot use reserved device names"));
    }

    Ok(())
}

fn is_allowed_byte(byte: u8) -> bool {
    (0x20..=0x7e).contains(&byte)
        && !matches!(
            byte,
            b'<' | b'>' | b':' | b'"' | b'/' | b'\\' | b'|' | b'?' | b'*'
        )
}

fn is_reserved_device_name(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or("")
        .trim_end_matches([' ', '.']);
    let stem = stem.to_ascii_uppercase();

    matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || (stem.len() == 4
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
    fn accepts_portable_characters_and_dotfiles() {
        assert_eq!(
            parse_components("reports/.env/100%20 done.v2.txt"),
            ["reports", ".env", "100%20 done.v2.txt"]
        );
    }

    #[test]
    fn rejects_leading_space_components() {
        for path in [" name.txt", "nested/ name.txt"] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn accepts_interior_spaces() {
        assert_eq!(parse_components("a name.txt"), ["a name.txt"]);
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
    fn accepts_dotfiles_and_rejects_dot_components() {
        for path in [".env", ".gitignore", ".config/file"] {
            assert!(
                VPath::parse(path).is_ok(),
                "path should be accepted: {path}"
            );
        }
        for path in [".", "..", "a/../b"] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_trailing_dots() {
        for path in ["file.", "dir./file", "a.b.", "name "] {
            assert!(
                VPath::parse(path).is_err(),
                "path should be rejected: {path}"
            );
        }
    }

    #[test]
    fn rejects_disallowed_characters() {
        for path in [
            "café.txt",
            "star*.txt",
            "angle<.txt",
            "angle>.txt",
            "question?.txt",
            "pipe|.txt",
            "tab\t.txt",
            "quote\".txt",
            "non-ASCII-é",
            "control\u{1}.txt",
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
            "CON .txt",
            "CONIN$",
            "CONOUT$.log",
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
