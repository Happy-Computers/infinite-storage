//! Operating-system specifics live here so the rest of the program can work
//! with platform-neutral paths and settings. Code outside this module must not
//! branch on the operating system or hardcode an OS path.

use std::{env, ffi::OsString, io, path::PathBuf};

const STORAGE_DIRECTORY_ENV: &str = "INFINITE_STORAGE_DATA";

const STORAGE_DIRECTORY_NAME: &str = "infinite-storage";

/// `INFINITE_STORAGE_DATA` takes precedence on every platform; otherwise the
/// platform default applies.
pub fn storage_directory() -> io::Result<PathBuf> {
    storage_directory_from(|name| env::var_os(name))
}

/// The lookup is injected so tests can supply variables without mutating the
/// process environment, which is shared between concurrently running tests.
fn storage_directory_from<F>(lookup: F) -> io::Result<PathBuf>
where
    F: Fn(&str) -> Option<OsString>,
{
    if let Some(path) = environment_value(&lookup, STORAGE_DIRECTORY_ENV) {
        return Ok(PathBuf::from(path));
    }

    default_storage_directory(&lookup)
}

/// An empty value is treated as unset: an empty path would otherwise resolve
/// to the current working directory and silently change the storage location.
fn environment_value<F>(lookup: &F, name: &str) -> Option<OsString>
where
    F: Fn(&str) -> Option<OsString>,
{
    lookup(name).filter(|value| !value.is_empty())
}

/// POSIX defines `HOME` as the user's home directory. `XDG_DATA_HOME` is the
/// established convention for user application data on the Unix-like systems
/// we support, so it takes precedence when set.
#[cfg(not(windows))]
fn default_storage_directory<F>(lookup: &F) -> io::Result<PathBuf>
where
    F: Fn(&str) -> Option<OsString>,
{
    if let Some(data_home) = environment_value(lookup, "XDG_DATA_HOME") {
        return Ok(PathBuf::from(data_home).join(STORAGE_DIRECTORY_NAME));
    }

    match environment_value(lookup, "HOME") {
        Some(home) => Ok(PathBuf::from(home)
            .join(".local")
            .join("share")
            .join(STORAGE_DIRECTORY_NAME)),
        None => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "neither XDG_DATA_HOME nor HOME is set; set INFINITE_STORAGE_DATA to choose a \
             storage directory",
        )),
    }
}

#[cfg(windows)]
fn default_storage_directory<F>(lookup: &F) -> io::Result<PathBuf>
where
    F: Fn(&str) -> Option<OsString>,
{
    if let Some(local_app_data) = environment_value(lookup, "LOCALAPPDATA") {
        return Ok(PathBuf::from(local_app_data).join(STORAGE_DIRECTORY_NAME));
    }

    match environment_value(lookup, "USERPROFILE") {
        Some(user_profile) => Ok(PathBuf::from(user_profile).join(STORAGE_DIRECTORY_NAME)),
        None => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "neither LOCALAPPDATA nor USERPROFILE is set; set INFINITE_STORAGE_DATA to choose a \
             storage directory",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup_from<'a>(
        variables: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Option<OsString> + 'a {
        move |name| {
            variables
                .iter()
                .find(|(variable, _)| *variable == name)
                .map(|(_, value)| OsString::from(*value))
        }
    }

    #[test]
    fn explicit_override_is_used() {
        let lookup = lookup_from(&[
            (STORAGE_DIRECTORY_ENV, "/tmp/infinite-storage-test-data"),
            ("HOME", "/home/test"),
        ]);

        assert_eq!(
            storage_directory_from(lookup).unwrap(),
            PathBuf::from("/tmp/infinite-storage-test-data")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn empty_override_falls_back_to_the_platform_default() {
        let lookup = lookup_from(&[
            (STORAGE_DIRECTORY_ENV, ""),
            ("XDG_DATA_HOME", "/home/test/.local/share"),
        ]);

        assert_eq!(
            storage_directory_from(lookup).unwrap(),
            PathBuf::from("/home/test/.local/share/infinite-storage")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn xdg_data_home_precedes_home() {
        let lookup = lookup_from(&[("XDG_DATA_HOME", "/custom/data"), ("HOME", "/home/test")]);

        assert_eq!(
            storage_directory_from(lookup).unwrap(),
            PathBuf::from("/custom/data/infinite-storage")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn home_is_the_posix_fallback() {
        let lookup = lookup_from(&[("HOME", "/home/test")]);

        assert_eq!(
            storage_directory_from(lookup).unwrap(),
            PathBuf::from("/home/test/.local/share/infinite-storage")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn missing_home_is_an_error() {
        let error = storage_directory_from(lookup_from(&[])).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
}
