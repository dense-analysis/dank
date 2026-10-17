class AssetSizeExceeded(Exception):
    """An intentional exclusion under the configured per-file size limit."""


class VideoSizeLimit:
    """Count actual bytes across separate video/audio downloads."""

    def __init__(self, maximum: int | None) -> None:
        self.maximum = maximum
        self.received: dict[str, int] = {}

    def __call__(self, progress: dict[str, object]) -> None:
        if not self.maximum or progress.get("status") not in {
            "downloading", "finished",
        }:
            return

        filename = progress.get("filename")
        downloaded = progress.get("downloaded_bytes")

        if not isinstance(filename, str) or not isinstance(downloaded, int):
            return

        self.received[filename] = max(
            self.received.get(filename, 0), downloaded,
        )

        if sum(self.received.values()) > self.maximum:
            raise AssetSizeExceeded(
                f"Downloaded streams exceed {self.maximum}-byte limit",
            )
