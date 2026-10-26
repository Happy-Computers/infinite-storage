# Roadmap

## Stream media before transfer completion

Videos, audio, and other large media must play while InfiniteFS uploads or downloads them.
The user must not wait for the whole file before opening it.

- Publish each confirmed chunk immediately and advance the manifest byte count only after its S3
  write succeeds.
- Give the media reader a blocking byte-range interface. If playback reaches unpublished bytes,
  wait for the next confirmed manifest update instead of treating the file as complete.
- Keep a bounded cache ahead of the current playback position. Evict chunks behind playback so a
  long video does not become an unbounded disk or memory download.
- Support seeking by fetching the chunk that covers the requested offset and then refilling the
  ahead-of-playback cache.
- Preserve the same behavior in both directions: local-to-S3 uploads and S3-to-local downloads.
- Prove the behavior with a long fixture whose chunks arrive gradually. Start playback before the
  final manifest, seek during transfer, and confirm that playback reaches the final byte.

This is a media-reader feature. Normal files already become usable as soon as their complete pull
finishes; media needs a reader that understands an incomplete manifest.
