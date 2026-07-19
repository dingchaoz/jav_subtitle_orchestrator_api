import pytest

from orchestrator.site_audio.cli import PipelineOptions
from orchestrator.site_audio.errors import DownloadFailure, UnsupportedSiteURL
from orchestrator.site_audio.models import Provider, ResolvedStream
from orchestrator.site_audio.pipeline import SiteAudioPipeline


def _stream(token, *, refreshable=True):
    return ResolvedStream(
        provider=Provider.JABLE,
        page_url="https://jable.tv/videos/movie/",
        manifest_url=f"https://cdn.example/media.m3u8?token={token}",
        headers={},
        expected_duration=60,
        refreshable=refreshable,
    )


def test_pipeline_selects_provider_resolver_and_returns_writer_output(tmp_path):
    output = tmp_path / "movie.wav"
    resolved = _stream("first")
    calls = []

    class Resolver:
        def resolve(self, url):
            calls.append(("resolve", url))
            return resolved

    class Writer:
        def write(self, stream, output_path, *, overwrite):
            calls.append(("write", stream, output_path, overwrite))
            return output_path

    pipeline = SiteAudioPipeline(
        PipelineOptions(tmp_path / "profile", 10, False),
        resolvers={Provider.JABLE: Resolver()},
        writer=Writer(),
    )

    assert pipeline.download(resolved.page_url, output, overwrite=False) == output
    assert calls == [
        ("resolve", resolved.page_url),
        ("write", resolved, output, False),
    ]


def test_pipeline_reresolves_once_after_retryable_signed_stream_failure(tmp_path):
    streams = iter([_stream("expired"), _stream("fresh")])
    resolutions = []
    writes = []

    class Resolver:
        def resolve(self, url):
            stream = next(streams)
            resolutions.append(stream.manifest_url)
            return stream

    class Writer:
        def write(self, stream, output_path, *, overwrite):
            writes.append(stream.manifest_url)
            if "expired" in stream.manifest_url:
                raise DownloadFailure("manifest returned 403", retryable=True)
            return output_path

    pipeline = SiteAudioPipeline(
        PipelineOptions(tmp_path / "profile", 10, False),
        resolvers={Provider.JABLE: Resolver()},
        writer=Writer(),
    )

    output = tmp_path / "movie.wav"
    assert pipeline.download("https://jable.tv/videos/movie/", output, overwrite=False) == output
    assert len(resolutions) == 2
    assert len(writes) == 2


def test_pipeline_does_not_retry_nonretryable_failure(tmp_path):
    calls = 0

    class Resolver:
        def resolve(self, url):
            nonlocal calls
            calls += 1
            return _stream("bad")

    class Writer:
        def write(self, stream, output_path, *, overwrite):
            raise DownloadFailure("unsupported audio", retryable=False)

    pipeline = SiteAudioPipeline(
        PipelineOptions(tmp_path / "profile", 10, False),
        resolvers={Provider.JABLE: Resolver()},
        writer=Writer(),
    )

    with pytest.raises(DownloadFailure):
        pipeline.download("https://jable.tv/videos/movie/", tmp_path / "x.wav", overwrite=False)
    assert calls == 1


def test_pipeline_refuses_existing_output_without_overwrite(tmp_path):
    output = tmp_path / "movie.wav"
    output.write_bytes(b"existing")
    pipeline = SiteAudioPipeline(
        PipelineOptions(tmp_path / "profile", 10, False),
        resolvers={},
        writer=pytest.fail,
    )

    with pytest.raises(UnsupportedSiteURL, match="already exists"):
        pipeline.download("https://jable.tv/videos/movie/", output, overwrite=False)
