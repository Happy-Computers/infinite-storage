# InfiniteFS

A cloud filesystem that's infinitely scalable for humans and agents alike.

## Current features

- sync files to s3 (works locally as well) & pull from s3 using cli commands -> tui untested for this
- basic files like text, md, and other code files work and sync flawlessly.
- audio and videos sync but wait for uploading/downloading -> solving this is on the roadmap next


## Requirements

The storage directory defaults to a per-user application data directory and can be overridden with the `INFINITE_STORAGE_DATA` environment variable.

### Local data directory

The storage directory is resolved when a command needs it:

1. `INFINITE_STORAGE_DATA`, if set to an absolute path.
2. `$XDG_DATA_HOME/infinite-storage` when `XDG_DATA_HOME` is an absolute path.
3. `$HOME/.local/share/infinite-storage` otherwise.
4. On Windows, `%LOCALAPPDATA%\infinite-storage`, falling back to
   `%USERPROFILE%\infinite-storage`.

The storage directory is created automatically when the app starts. Relative values are rejected so the location never depends on the working directory. Override the location for development or tests:

```bash
INFINITE_STORAGE_DATA=/tmp/infinite-storage-data cargo run
```

For a shared or persistent location outside `$HOME`, create the directory once and point the override at it:

```bash
sudo install -d -o "$USER" -g "$USER" /var/lib/infinite-storage
INFINITE_STORAGE_DATA=/var/lib/infinite-storage cargo run
```

Keep test files and development data separate from important personal files.

The local browser is not a mounted cloud filesystem. S3 integration and cloud-backed access are planned.

