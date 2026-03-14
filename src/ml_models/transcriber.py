from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional


@dataclass
class WordTimestamp:
    word: str
    start: float
    end: float


@dataclass
class TranscriptionResult:
    text: str
    words: list[WordTimestamp]
    model_name: str

_ASR_PIPELINE = None
_ASR_MODEL_NAME = None
_ASR_DEVICE = None


def format_timestamp(seconds: float) -> str: #Convert seconds to mm:ss.sss 
    minutes = int(seconds // 60)
    remaining_seconds = seconds - minutes * 60
    return f"{minutes:02d}:{remaining_seconds:06.3f}"


def write_transcript_txt(result: TranscriptionResult, output_path: str | Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    for segment in result.words:
        lines.append(
            f"[{format_timestamp(segment.start)} - {format_timestamp(segment.end)}] {segment.word}"
        )


    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path

def _get_asr_pipeline(model_name: str, device: str, torch_dtype):
    global _ASR_PIPELINE, _ASR_MODEL_NAME, _ASR_DEVICE

    if (
        _ASR_PIPELINE is None
        or _ASR_MODEL_NAME != model_name
        or _ASR_DEVICE != device
    ):
        from transformers import pipeline

        _ASR_PIPELINE = pipeline(
            task="automatic-speech-recognition",
            model=model_name,
            device=0 if device == "cuda" else -1,
            torch_dtype=torch_dtype,
        )
        _ASR_MODEL_NAME = model_name
        _ASR_DEVICE = device

    return _ASR_PIPELINE


def transcribe_with_timestamps(
    wav_path: str | Path,
    model_name: str = "openai/whisper-large-v3",
    timestamp_level: Literal["word", "chunk"] = "word",
    device: Optional[str] = None,
    chunk_length_s: int = 30,  # chunk long audio to improve stability
    stride_length_s: int = 5, #overlap between chunks to avoid loss of recording 
) -> TranscriptionResult:

   
    wav_path = Path(wav_path)

    if not wav_path.exists():
        raise FileNotFoundError(f"File not found: {wav_path}")

    if wav_path.suffix.lower() != ".wav":
        raise ValueError("Input file must be a .wav file")

    import torch

    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"

    torch_dtype = torch.float16 if device == "cuda" else torch.float32

    speech_to_text_pipeline = _get_asr_pipeline(model_name, device, torch_dtype)

    pipeline_output: dict[str, Any] = speech_to_text_pipeline(
        str(wav_path),
        return_timestamps=timestamp_level,
        chunk_length_s=chunk_length_s,
        stride_length_s=stride_length_s,
    )

    transcript_text = (pipeline_output.get("text") or "").strip()
    chunks = pipeline_output.get("chunks", [])

    word_timestamps: list[WordTimestamp] = []
    for chunk in chunks:
        timestamp = chunk.get("timestamp")
        text = chunk.get("text", "").strip()

        if not timestamp or timestamp[0] is None or timestamp[1] is None:
            continue
        if not text:
            continue

        word_timestamps.append(
            WordTimestamp(
                word=text,
                start=float(timestamp[0]),
                end=float(timestamp[1]),
            )
        )

    return TranscriptionResult(
        text=transcript_text,
        words=word_timestamps,
        model_name=model_name,
    )