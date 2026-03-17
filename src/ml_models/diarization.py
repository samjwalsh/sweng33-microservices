from __future__ import annotations

import os

from pyannote.audio import Pipeline

_pipeline = None


def merge_segments(segments: list[dict], gap: float = 0.35) -> list[dict]:
    if not segments:
        return []
    segments = sorted(segments, key=lambda s: (s["start"], s["end"]))
    merged = [segments[0].copy()]
    for seg in segments[1:]:
        prev = merged[-1]
        if seg["speaker"] == prev["speaker"] and seg["start"] - prev["end"] <= gap:
            prev["end"] = max(prev["end"], seg["end"])
        else:
            merged.append(seg.copy())
    return merged


def drop_short_segments(segments: list[dict], min_dur: float = 0.4) -> list[dict]:
    return [s for s in segments if (s["end"] - s["start"]) >= min_dur]


def _get_pipeline() -> Pipeline:
    global _pipeline
    if _pipeline is None:
        token = os.environ.get("MY_TOKEN")
        if not token:
            raise RuntimeError("MY_TOKEN not set. Run: export MY_TOKEN='hf_...'")

        _pipeline = Pipeline.from_pretrained(
            "pyannote/speaker-diarization-3.1",
            token=token,
        )
    return _pipeline


def diarize(audio_path: str) -> list[dict]:
    """
    Run speaker diarization and return merged segments.

    Returns:
      [{"speaker": "SPEAKER_00", "start": 0.0, "end": 1.23}, ...]
    """
    pipeline = _get_pipeline()

    out = pipeline(audio_path)

    if hasattr(out, "speaker_diarization"):
        annotation = out.speaker_diarization
    elif hasattr(out, "diarization"):
        annotation = out.diarization
    elif isinstance(out, dict) and "diarization" in out:
        annotation = out["diarization"]
    else:
        annotation = out

    segments: list[dict] = []
    for turn, _, speaker in annotation.itertracks(yield_label=True):
        segments.append(
            {
                "speaker": speaker,
                "start": round(turn.start, 2),
                "end": round(turn.end, 2),
            }
        )

    segments = drop_short_segments(segments, min_dur=0.4)
    segments = merge_segments(segments, gap=0.35)
    return segments