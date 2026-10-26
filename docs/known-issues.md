# Known issues

Status: pre-team-test. The uploader and `pull` work for a single machine testing against a
scratch bucket. The issues below are what stand between that and two people sharing a directory.

This file is a work queue, not a design document. Each entry says what goes wrong, why the code
does it, how to see it, and what "fixed" means. When an issue is fixed, delete the entry and land
the test named under **Exit**.

Severity means:

- **data-loss**: someone's file content can be overwritten or removed.
- **missing**: the workflow cannot be used as intended.
- **waste**: correct results, unnecessary cost or work.
- **sharp edge**: correct but surprising, and easy to trip over.

## Summary

| ID | Severity | Issue | Needs |
| --- | --- | --- | --- |
| KI-01 | data-loss | A watcher tombstones every remote file its directory does not contain | durable state or a guard |
| KI-02 | data-loss | `pull` overwrites local edits and applies remote deletes without checking | local state in `pull` |
| KI-03 | data-loss | Same-file edits are last-writer-wins; conflict copies only cover the upload window | durable state |
| KI-04 | missing | No durable per-path state outside the watched directory | state file |
| KI-05 | missing | No continuous pull; the reader runs `pull` by hand | a poller |
| KI-06 | data-loss | `.git` and other dot-directories are uploaded and restored | an ignore list |
| KI-07 | data-loss | A watcher uploads `pull`'s temporary files | an ignore rule |
| KI-08 | waste | Every `pull` re-downloads every file | local manifest cache |
| KI-09 | waste | A failed `pull` restarts from byte zero | resumable partials |
| KI-10 | sharp edge | Size is the only integrity check | content digests |
| KI-11 | waste | Chunks from old generations are never collected | manifest-driven cleanup |

## KI-01: A watcher tombstones every remote file its directory does not contain

**What goes wrong.** Start `sync --watch` in a directory that does not hold the full remote tree
and the next rescan publishes a `deleted` manifest for every remote path that is missing on
disk. The files disappear from S3 for every machine, on the rescan that runs at startup and on
each one after that. An empty directory with the right basename is enough to wipe the prefix.

**Why.** `SyncIndex::load` records every complete remote manifest in `remote_files`, whether or
not the corresponding local file exists. `SyncIndex::rescan` then treats "remote, not seen
locally" as a deletion. That is deliberate for the offline-delete case (a file deleted while the
watcher was stopped does propagate, see `rescan_tombstones_files_deleted_while_stopped`), but it
cannot tell "I deleted my file" apart from "I never had this file".

**Repro.** Upload `project/a.txt` and `project/b.txt`. Copy only `a.txt` into a new `project`
directory. Run `sync --watch project`. The startup rescan gives `project/b.txt` a `deleted`
manifest.

**Fix shape.** Tombstone only paths that were known to exist locally: either recorded in the
durable state file (KI-04) or uploaded by this process. Until that state exists, guard the
watcher: if the local tree is missing remote paths, refuse to start or refuse to delete and
report the paths it would have tombstoned. Deleting a file while the watcher is off stops
propagating; that is the correct trade until state exists.

**Exit.** `watch_does_not_tombstone_files_it_never_uploaded`, plus a test that an offline local
delete still propagates when the file was previously synced.

## KI-02: `pull` overwrites local edits and applies remote deletes without checking

**What goes wrong.** `pull` is a restore, not a merge. It writes every complete remote manifest
over the local file with that name, and removes local files that have a remote tombstone. Local
edits made since the last pull are lost with no warning, no backup, and no exit code. Pulling
into a directory being edited is the normal thing a person will try.

**Why.** `pull_file` renames the downloaded temporary file over the destination unconditionally.
`pull_deleted` removes the destination file unconditionally. Neither function consults local
state, so there is nothing to compare against.

**Repro.** Pull `project` twice with an edit to `notes.txt` between the pulls. The edit is gone.

**Fix shape.** Before replacing a local file, compare it to the snapshot recorded the last time
this machine applied that remote version. If the local file changed since then, write a conflict
copy locally and leave the local file in place. Refuse to apply a tombstone to a locally modified
file. This is the reader half of KI-03 and needs the same durable state (KI-04).

**Exit.** `pull_preserves_a_locally_modified_file` and
`pull_does_not_delete_a_locally_modified_file`.

## KI-03: Same-file edits are last-writer-wins; conflict copies only cover the upload window

**What goes wrong.** Two machines edit `notes.txt`. The later upload replaces the canonical
manifest, and the earlier version is gone from the canonical path. A conflict copy is created
only when the manifest changes between the head read at the start of an upload and the next
manifest write. For a small file that window is milliseconds, so in practice the common case
loses a version silently.

There is a second half. With no durable state, a restart makes the local file look unsynced, so
the local copy is uploaded again and takes back the canonical name. Which version is canonical
changes across restarts.

**Why.** `upload_file_at` reads the current manifest once through `manifest_put_mode` and then
writes. A sequential edit is a legal conditional update, not a conflict. And `SyncIndex::load`
keeps a path only when the local length and mtime match the remote manifest, which a pulled or
edited file never does.

**Repro.** Edit the same path on two machines ten seconds apart, with both watchers running.
Both uploads succeed. Only one survives under the canonical name.

**Fix shape.** Record the remote generation and local snapshot last applied per path (KI-04).
Compare both sides on every change. If local and remote both moved since the last applied state,
keep the local file and publish the competing version under the conflict name. The naming code
already exists in `conflict_relative`.

**Exit.** `two_machines_editing_one_path_keep_both_versions`, with the canonical name stable
across a restart of either watcher.

## KI-04: No durable per-path state outside the watched directory

**What goes wrong.** The process reconstructs what it knows on every start from the remote
manifests, and it cannot answer the questions the other issues need: did I ever sync this path,
which remote generation did I last apply, and did the local file change since then. Every
attempt to fix KI-01, KI-02, or KI-03 otherwise ends up encoding a guess.

**Why.** `SyncIndex` holds `files` and `remote_files` in memory only, and `files` is filled by
matching local length and mtime against the remote manifest.

**Fix shape.** One record per relative path in the app data directory, not inside the watched
tree: last remote generation and eTag, last applied remote snapshot, last applied local snapshot.
One owner of that state. This is the foundation item from the sync engine design.

**Exit.** `restart_keeps_conflict_state` and `state_lives_outside_the_watched_directory`.

## KI-05: No continuous pull; the reader runs `pull` by hand

**What goes wrong.** A teammate's change reaches the bucket and stops there. Nothing watches
remote manifests, so the other machine sees the change only when someone runs `pull`, which
downloads everything again (KI-08) and overwrites local work (KI-02).

**Why.** `pull` is a single pass that exits. There is no poller, and `sync --watch` only uploads.

**Fix shape.** A remote manifest poll on the same 5 second cadence as the rescan, feeding the same
reconciliation loop as local events. Needs KI-04 first, otherwise the puller's own writes look
like local edits and re-upload.

**Exit.** `two_directories_converge_without_manual_pull` from the roadmap.

## KI-06: `.git` and other dot-directories are uploaded and restored

**What goes wrong.** Syncing a repository uploads `.git` and everything else in the tree,
including any local secret file that is gitignored but inside the directory. `pull` restores all
of it. A full `.git` of a large repo is also a lot of objects nobody asked to copy.

**Why.** `temporary_path` filters only `.#*`, `*~`, and `*.swp`. Nothing filters dot-directories,
and the object key layer accepts a leading dot.

**Repro.** `sync --watch` any git repository and list the bucket.

**Fix shape.** A documented ignore list applied in `regular_files` and in the watcher event
filter, starting with `.git`. Configurable later; fixed for now.

**Exit.** `sync_skips_dot_directories`.

## KI-07: A watcher uploads `pull`'s temporary files

**What goes wrong.** Run `pull` into a directory that is also being watched and the watcher
uploads `.infinitefs-partial-<pid>-<n>` while it is still being written, then tombstones it when
the rename happens. Wasted transfer and junk keys in the bucket.

**Why.** `create_partial_file` names the file `.infinitefs-partial-...` and `temporary_path` does
not match that prefix.

**Fix shape.** Add the partial prefix to `temporary_path`. Prefer also keeping `pull`'s staging
directory outside the watched tree, since ignore rules are a weaker guarantee than not sharing a
directory.

**Exit.** `watcher_skips_pull_partial_files`.

## KI-08: Every `pull` re-downloads every file

**What goes wrong.** A pull of a 2 GB tree transfers 2 GB, even when nothing changed. For a
teammate polling a shared folder this is the whole cost of the feature, for no useful data.

**Why.** `pull_directory_with_concurrency` pushes every complete manifest into the download set.
There is no local record of the remote generation already applied, and no comparison before
fetching chunks.

**Fix shape.** Skip a file when its recorded remote generation already matches the manifest, and
its local file still matches the applied snapshot. Same state file as KI-04.

**Exit.** `pull_downloads_only_changed_files`.

## KI-09: A failed `pull` restarts from byte zero

**What goes wrong.** Interrupting a large pull loses the downloaded bytes. The next pull starts
over.

**Why.** `PartialFile` removes the temporary file on drop, on every exit path including failure.

**Fix shape.** Keep the partial and its manifest generation, resume at the first unwritten chunk,
and verify before renaming. Only discard when the generation no longer matches.

**Exit.** `pull_resumes_after_a_failure`.

## KI-10: Size is the only integrity check

**What goes wrong.** A chunk that comes back corrupted or truncated passes as long as the total
byte count matches the manifest. There is no way to tell a bad restore from a good one, and no
way to verify a bucket by hand.

**Why.** The manifest records `size`, `chunk_size`, `generation`, `modified_ns`, and `state`. No
digest, per chunk or per file.

**Fix shape.** A SHA-256 per file in the manifest, computed during upload, verified after
download and before the rename. Chunk digests only if the file digest proves too coarse for
resume.

**Exit.** `pull_rejects_a_corrupted_chunk`.

## KI-11: Chunks from old generations are never collected

**What goes wrong.** Every upload writes a new generation directory of chunks and nothing removes
the previous one. A 100 MB file saved ten times stores about 1 GB. The bill grows with edit
count, not with data size, and the bucket fills with objects no manifest points at.

**Why.** Chunks are immutable by design and there is no cleanup pass. The only `delete` call in
the code base is in a test.

**Fix shape.** After a successful publish, delete the chunks of the generation the new manifest
replaced, and only that generation, so a concurrent reader is never mid-read of a path that just
disappeared. A bucket lifecycle rule on the test bucket is the stopgap.

**Exit.** `publishing_a_generation_removes_the_previous_chunks`.

## Not issues

These are intentional limits. Do not "fix" them without a decision.

- Symbolic links are rejected rather than followed, on upload and on restore.
- Non-UTF-8 paths are rejected.
- Directories, permissions, xattrs, and hard links are not represented.
- Files are synced whole. Partial visibility is only for the planned media reader.
- One writer per path. Multiple machines writing the same path produce a second file, not a
  merge.
- Credential handling and encryption are out of scope for this list.

## Order

Each batch ends in a state that is safe to use, and the exit tests from the entries above land
with it.

1. **Stop the bleeding.** KI-06, KI-07, KI-01 guard. Small, local, no new architecture. After
   this a teammate can pull a shared directory without wiping it or uploading `.git`.
2. **Durable state.** KI-04, which closes KI-01 properly and removes the restart ambiguity in
   KI-03.
3. **Usable pull.** KI-08, KI-09, KI-02. A pull becomes incremental, resumable, and safe for a
   directory someone is editing.
4. **The missing half.** KI-05. Continuous pull on top of the state from batch 2.
5. **Correct conflicts.** KI-03, KI-10. Both versions survive, and a restore can be verified.
6. **Cost.** KI-11.

Batches 1 and 2 are what the two-machine test needs. Batches 3 and 4 are what makes it usable
daily. The roadmap's continuous pull item is batch 4 and should not start before batch 2.
