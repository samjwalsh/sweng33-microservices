from __future__ import annotations

import logging
import os

from pyannote.audio import Pipeline

try:
    import torch
except Exception:  # pragma: no cover - torch import depends on runtime environment
    torch = None

logger = logging.getLogger(__name__)

_pipeline = None


def _get_hf_token() -> str | None:
    for env_name in ("MY_TOKEN", "HF_TOKEN", "HUGGINGFACE_HUB_TOKEN"):
        token = os.environ.get(env_name)
        if token and token.strip():
            return token.strip()
    return None


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


def _configure_runtime() -> str:
    """
    Configure torch runtime to reduce RAM pressure on CPU-heavy hosts.

    Environment variables:
      DIARIZATION_DEVICE: cpu|cuda (default: cpu)
      DIARIZATION_NUM_THREADS: positive integer torch thread cap (default: 4)
    """
    device_name = os.environ.get("DIARIZATION_DEVICE", "cpu").strip().lower()
    if device_name not in {"cpu", "cuda"}:
        device_name = "cpu"

    if torch is not None:
        raw_threads = os.environ.get("DIARIZATION_NUM_THREADS", "4").strip()
        try:
            threads = max(1, int(raw_threads))
        except ValueError:
            threads = 4

        torch.set_num_threads(threads)
        try:
            torch.set_num_interop_threads(1)
        except RuntimeError:
            # Can fail if interop threads are already initialized.
            pass

        if device_name == "cuda" and not torch.cuda.is_available():
            logger.warning("DIARIZATION_DEVICE=cuda requested but CUDA is unavailable; falling back to CPU")
            device_name = "cpu"

    return device_name


def _get_pipeline() -> Pipeline:
    global _pipeline
    if _pipeline is None:
        device_name = _configure_runtime()
        token = _get_hf_token()
        if not token:
            raise RuntimeError(
                "No Hugging Face token found. Set one of: MY_TOKEN, HF_TOKEN, HUGGINGFACE_HUB_TOKEN."
            )

        model_id = os.environ.get("DIARIZATION_MODEL", "pyannote/speaker-diarization-3.1")

        try:
            try:
                _pipeline = Pipeline.from_pretrained(
                    model_id,
                    token=token,
                )
            except TypeError:
                _pipeline = Pipeline.from_pretrained(
                    model_id,
                    use_auth_token=token,
                )
        except Exception as error:
            raise RuntimeError(
                "Failed to load diarization pipeline. Ensure your token is valid and has accepted model terms on Hugging Face for "
                f"{model_id}. Also verify DIARIZATION_MODEL is correct. Original error: {error}"
            ) from error

        if torch is not None:
            _pipeline.to(torch.device(device_name))
    return _pipeline


def diarize(audio_path: str) -> list[dict]:
    """
    Run speaker diarization and return merged segments.

    Returns:
      [{"speaker": "SPEAKER_00", "start": 0.0, "end": 1.23}, ...]
    """
    pipeline = _get_pipeline()

    try:
        if torch is not None:
            with torch.inference_mode():
                out = pipeline(audio_path)
        else:
            out = pipeline(audio_path)
    except (MemoryError, RuntimeError) as error:
        message = str(error).lower()
        looks_like_oom = (
            "out of memory" in message
            or "std::bad_alloc" in message
            or "cannot allocate memory" in message
            or "can't allocate memory" in message
        )
        if looks_like_oom:
            raise RuntimeError(
                "Diarization ran out of memory. Try setting "
                "DIARIZATION_NUM_THREADS=2 (or 1), DIARIZATION_DEVICE=cpu, "
                "and if needed DIARIZATION_MODEL=pyannote/speaker-diarization-3.0."
            ) from error
        raise

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