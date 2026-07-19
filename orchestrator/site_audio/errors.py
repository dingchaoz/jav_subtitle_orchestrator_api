class SiteAudioError(RuntimeError):
    """Base error with a stable command-line exit code."""

    exit_code = 5


class UnsupportedSiteURL(SiteAudioError):
    exit_code = 2


class SourceUnavailable(SiteAudioError):
    exit_code = 3


class BrowserResolutionTimeout(SiteAudioError):
    exit_code = 4


class DownloadFailure(SiteAudioError):
    exit_code = 5

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class ManifestValidationError(DownloadFailure):
    """The discovered HLS manifest is unsafe or unsuitable."""
