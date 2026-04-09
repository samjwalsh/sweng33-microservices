from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import soundfile as sf


def extract_wav_snippet(
    wav_path: Union[str, Path],
    start_sec: float,
    end_sec: float,
    *,
    out_path: Optional[Union[str, Path]] = None,
    dtype: str = "float32",
):
    wav_path = Path(wav_path)

    if not wav_path.exists():
        raise FileNotFoundError(f"File not found: {wav_path}")
    if wav_path.suffix.lower() != ".wav":
        raise ValueError("Input must be a .wav file")
    if start_sec < 0 or end_sec <= start_sec:
        raise ValueError("Invalid timestamps")

    info = sf.info(str(wav_path))
    sr = info.samplerate
    total_frames = info.frames

    start_frame = int(round(start_sec * sr))
    end_frame = int(round(end_sec * sr))

    # clamp so it's not past file length
    start_frame = max(0, min(start_frame, total_frames))
    end_frame = max(0, min(end_frame, total_frames))

    if end_frame <= start_frame:
        raise ValueError("Snippet is empty after clamping")

    with sf.SoundFile(str(wav_path), "r") as f:
        f.seek(start_frame)
        audio = f.read(end_frame - start_frame, dtype=dtype, always_2d=False)

    if out_path is not None:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        sf.write(str(out_path), audio, sr)

    return audio, sr

