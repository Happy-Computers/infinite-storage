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
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{STORAGE_DIRECTORY_ENV} must be an absolute path"),
            ));
        }
        return Ok(path);
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

/// Return an environment variable's value as an absolute path.
///
/// The XDG base directory specification requires `XDG_DATA_HOME` to be
/// absolute. A relative value would make the storage location depend on the
/// working directory, which is what this module exists to prevent.
fn environment_path<F>(lookup: &F, name: &str) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<OsString>,
{
    let path = PathBuf::from(environment_value(lookup, name)?);
    if path.is_absolute() { Some(path) } else { None }
}

/// POSIX defines `HOME` as the user's home directory. `XDG_DATA_HOME` is the
/// established convention for user application data on the Unix-like systems
/// we support, so it takes precedence when set.
#[cfg(not(windows))]
fn default_storage_directory<F>(lookup: &F) -> io::Result<PathBuf>
where
    F: Fn(&str) -> Option<OsString>,
{
    if let Some(data_home) = environment_path(lookup, "XDG_DATA_HOME") {
        return Ok(data_home.join(STORAGE_DIRECTORY_NAME));
    }

    match environment_path(lookup, "HOME") {
        Some(home) => Ok(home
            .join(".local")
            .join("share")
            .join(STORAGE_DIRECTORY_NAME)),
        None => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "neither XDG_DATA_HOME nor HOME is set to an absolute path; set \
             INFINITE_STORAGE_DATA to choose a storage directory",
        )),
    }
}

#[cfg(windows)]
fn default_storage_directory<F>(lookup: &F) -> io::Result<PathBuf>
where
    F: Fn(&str) -> Option<OsString>,
{
    if let Some(local_app_data) = environment_path(lookup, "LOCALAPPDATA") {
        return Ok(local_app_data.join(STORAGE_DIRECTORY_NAME));
    }

    match environment_path(lookup, "USERPROFILE") {
        Some(user_profile) => Ok(user_profile.join(STORAGE_DIRECTORY_NAME)),
        None => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "neither LOCALAPPDATA nor USERPROFILE is set to an absolute path; set \
             INFINITE_STORAGE_DATA to choose a storage directory",
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

    #[test]
    fn relative_override_is_rejected() {
        let lookup = lookup_from(&[
            (STORAGE_DIRECTORY_ENV, "relative/data"),
            ("HOME", "/home/test"),
        ]);

        let error = storage_directory_from(lookup).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[cfg(not(windows))]
    #[test]
    fn relative_xdg_data_home_falls_back_to_home() {
        let lookup = lookup_from(&[("XDG_DATA_HOME", "relative/data"), ("HOME", "/home/test")]);

        assert_eq!(
            storage_directory_from(lookup).unwrap(),
            PathBuf::from("/home/test/.local/share/infinite-storage")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn relative_home_is_not_used() {
        let error = storage_directory_from(lookup_from(&[("HOME", "relative/home")])).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
}
