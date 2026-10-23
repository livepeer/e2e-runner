import asyncio
import contextlib
import json
import math
import sys
from array import array
from dataclasses import dataclass, field
from fractions import Fraction

import av
import numpy as np

from livepeer_gateway import (
    AudioDecodedMediaFrame,
    AudioOutputConfig,
    LivepeerGatewayError,
    MediaOutput,
    MediaPublish,
    MediaPublishConfig,
    VideoDecodedMediaFrame,
    VideoOutputConfig,
)
from livepeer_gateway.media_output import LagPolicy


DURATION_S = 3.0
FPS = 12
SAMPLE_RATE = 48_000
AUDIO_CHUNK_SAMPLES = 960
VIDEO_SPECS = (
    ("red", (176, 32, 32), (32, 48, 224)),
    ("green", (32, 160, 48), (224, 224, 32)),
)
AUDIO_FREQUENCIES = (440.0, 880.0)


@dataclass
class ObservedVideo:
    frame_count: int = 0
    rgb_sum: list[float] = field(default_factory=lambda: [0.0, 0.0, 0.0])

    def average_rgb(self) -> tuple[float, float, float]:
        if self.frame_count == 0:
            return (0.0, 0.0, 0.0)
        return tuple(value / self.frame_count for value in self.rgb_sum)


@dataclass
class ObservedAudio:
    sample_rate: int = SAMPLE_RATE
    frame_count: int = 0
    samples: array = field(default_factory=lambda: array("f"))


def render_video_frame(
    background_rgb: tuple[int, int, int],
    marker_rgb: tuple[int, int, int],
    frame_index: int,
) -> av.VideoFrame:
    rgb = np.empty((90, 160, 3), dtype=np.uint8)
    rgb[:, :] = background_rgb
    marker_x = 8 + ((frame_index * 5) % (160 - 16 - 16))
    rgb[37:53, marker_x : marker_x + 16] = marker_rgb
    frame = av.VideoFrame.from_ndarray(rgb, format="rgb24")
    frame.pts = frame_index
    frame.time_base = Fraction(1, FPS)
    return frame


def render_audio_frame(frequency_hz: float, start_sample: int) -> av.AudioFrame:
    payload = array("f")
    for offset in range(AUDIO_CHUNK_SAMPLES):
        sample_index = start_sample + offset
        t_s = sample_index / float(SAMPLE_RATE)
        payload.append(0.65 * math.sin(2.0 * math.pi * frequency_hz * t_s))
    frame = av.AudioFrame(
        format="fltp",
        layout="mono",
        samples=AUDIO_CHUNK_SAMPLES,
    )
    frame.sample_rate = SAMPLE_RATE
    frame.pts = start_sample
    frame.time_base = Fraction(1, SAMPLE_RATE)
    frame.planes[0].update(payload.tobytes())
    return frame


async def publish_video(
    track,
    spec: tuple[str, tuple[int, int, int], tuple[int, int, int]],
) -> None:
    _, background_rgb, marker_rgb = spec
    for frame_index in range(int(DURATION_S * FPS)):
        await track.write_frame(
            render_video_frame(background_rgb, marker_rgb, frame_index)
        )
        await asyncio.sleep(1.0 / FPS)


async def publish_audio(track, frequency_hz: float) -> None:
    total_samples = int(DURATION_S * SAMPLE_RATE)
    for start_sample in range(0, total_samples, AUDIO_CHUNK_SAMPLES):
        await track.write_frame(render_audio_frame(frequency_hz, start_sample))
        await asyncio.sleep(AUDIO_CHUNK_SAMPLES / float(SAMPLE_RATE))


def normalize_audio_samples(frame: av.AudioFrame) -> list[float]:
    values = frame.to_ndarray()
    if values.ndim == 2:
        values = (
            values.mean(axis=0)
            if values.shape[0] <= values.shape[1]
            else values.mean(axis=1)
        )
    if values.dtype.kind in {"i", "u"}:
        scale = float((2 ** ((values.dtype.itemsize * 8) - 1)) - 1)
        values = values.astype("float32") / max(scale, 1.0)
    else:
        values = values.astype("float32", copy=False)
    return [float(value) for value in values.tolist()]


async def collect_output(publish_url: str):
    observed_video: dict[int, ObservedVideo] = {}
    observed_audio: dict[int, ObservedAudio] = {}
    deadline = asyncio.get_running_loop().time() + 20.0

    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError("timed out waiting for decodable multi-track output")
        try:
            async with MediaOutput(
                publish_url,
                on_lag=LagPolicy.EARLIEST,
                max_segments=32,
            ) as output:
                async with asyncio.timeout(remaining):
                    async for decoded in output.frames():
                        if isinstance(decoded, VideoDecodedMediaFrame):
                            observed = observed_video.setdefault(
                                decoded.stream_index,
                                ObservedVideo(),
                            )
                            mean_rgb = decoded.frame.to_ndarray(
                                format="rgb24"
                            ).mean(axis=(0, 1))
                            observed.frame_count += 1
                            for index in range(3):
                                observed.rgb_sum[index] += float(mean_rgb[index])
                        elif isinstance(decoded, AudioDecodedMediaFrame):
                            observed = observed_audio.setdefault(
                                decoded.stream_index,
                                ObservedAudio(
                                    sample_rate=decoded.sample_rate or SAMPLE_RATE
                                ),
                            )
                            observed.frame_count += 1
                            observed.samples.extend(
                                normalize_audio_samples(decoded.frame)
                            )
                return observed_video, observed_audio, output.get_stats()
        except LivepeerGatewayError as exc:
            if observed_video or observed_audio or "EOFError" not in str(exc):
                raise
            await asyncio.sleep(0.25)


def goertzel_power(samples: array, sample_rate: int, frequency_hz: float) -> float:
    omega = (2.0 * math.pi * frequency_hz) / float(sample_rate)
    coeff = 2.0 * math.cos(omega)
    previous = 0.0
    previous2 = 0.0
    for sample in samples:
        current = float(sample) + (coeff * previous) - previous2
        previous2 = previous
        previous = current
    return (
        (previous2 * previous2)
        + (previous * previous)
        - (coeff * previous * previous2)
    )


def verify_video_signatures(observed: dict[int, ObservedVideo]) -> bool:
    if len(observed) != 2:
        return False
    unmatched = list(observed.values())
    matched: dict[str, ObservedVideo] = {}
    for name, expected_rgb, _marker_rgb in VIDEO_SPECS:
        candidate = min(
            unmatched,
            key=lambda item: sum(
                (item.average_rgb()[index] - expected_rgb[index]) ** 2
                for index in range(3)
            ),
        )
        matched[name] = candidate
        unmatched.remove(candidate)
    red = matched["red"].average_rgb()
    green = matched["green"].average_rgb()
    return (
        matched["red"].frame_count > 0
        and matched["green"].frame_count > 0
        and red[0] > red[1] * 2.0
        and red[0] > red[2] * 2.0
        and green[1] > green[0] * 2.0
        and green[1] > green[2] * 2.0
    )


def audio_frequency_ratios(observed: dict[int, ObservedAudio]) -> dict[str, float]:
    ratios: dict[str, float] = {}
    unmatched = list(observed.values())
    for frequency_hz in AUDIO_FREQUENCIES:
        if not unmatched:
            break
        other_frequency = next(
            candidate
            for candidate in AUDIO_FREQUENCIES
            if candidate != frequency_hz
        )
        selected = max(
            unmatched,
            key=lambda item: goertzel_power(
                item.samples, item.sample_rate, frequency_hz
            ),
        )
        target_power = goertzel_power(
            selected.samples, selected.sample_rate, frequency_hz
        )
        other_power = goertzel_power(
            selected.samples, selected.sample_rate, other_frequency
        )
        ratios[str(int(frequency_hz))] = target_power / max(other_power, 1e-12)
        unmatched.remove(selected)
    return ratios


async def run(publish_url: str) -> dict[str, object]:
    media = MediaPublish(
        publish_url,
        config=MediaPublishConfig(
            tracks=[
                VideoOutputConfig(fps=FPS, keyframe_interval_s=1.0),
                VideoOutputConfig(fps=FPS, keyframe_interval_s=1.0),
                AudioOutputConfig(
                    sample_rate=SAMPLE_RATE,
                    layout="mono",
                    format="fltp",
                ),
                AudioOutputConfig(
                    sample_rate=SAMPLE_RATE,
                    layout="mono",
                    format="fltp",
                ),
            ]
        ),
    )
    consumer_task = None
    try:
        video_tracks = media.get_tracks("video")
        audio_tracks = media.get_tracks("audio")
        publish_tasks = [
            *(
                asyncio.create_task(publish_video(track, spec))
                for track, spec in zip(video_tracks, VIDEO_SPECS, strict=True)
            ),
            *(
                asyncio.create_task(publish_audio(track, frequency_hz))
                for track, frequency_hz in zip(
                    audio_tracks,
                    AUDIO_FREQUENCIES,
                    strict=True,
                )
            ),
        ]
        await asyncio.sleep(0.5)
        consumer_task = asyncio.create_task(collect_output(publish_url))
        await asyncio.gather(*publish_tasks)
        await media.close()
        observed_video, observed_audio, output_stats = await consumer_task

        ratios = audio_frequency_ratios(observed_audio)
        video_ok = verify_video_signatures(observed_video)
        audio_ok = (
            len(observed_audio) == 2
            and set(ratios) == {"440", "880"}
            and all(ratio >= 2.5 for ratio in ratios.values())
        )
        assert len(observed_video) == 2, observed_video
        assert len(observed_audio) == 2, observed_audio
        assert all(track.frame_count > 0 for track in observed_video.values())
        assert all(len(track.samples) > 0 for track in observed_audio.values())
        assert video_ok, "decoded video color signatures did not match"
        assert audio_ok, f"decoded audio frequencies did not match: {ratios}"
        assert output_stats.segments_consumed > 0, "subscriber consumed no segments"
        assert media.get_stats().segments_started > 0, "publisher started no segments"
        return {"audio_frequency_ratios": ratios, "segments_consumed": output_stats.segments_consumed,
                "segments_started": media.get_stats().segments_started}

    finally:
        await media.close()
        if consumer_task is not None and not consumer_task.done():
            consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consumer_task


async def main() -> None:
    result = await run(sys.argv[1])
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
