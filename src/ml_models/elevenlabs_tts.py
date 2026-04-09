from __future__ import annotations

import os
import subprocess
from io import BytesIO
from pathlib import Path
from typing import Any

from elevenlabs.client import ElevenLabs


_eleven_client: ElevenLabs | None = None


def get_eleven_client() -> ElevenLabs:
    global _eleven_client

    if _eleven_client is None:
        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            raise RuntimeError("ELEVENLABS_API_KEY is not set")
        _eleven_client = ElevenLabs(api_key=api_key)

    return _eleven_client


def clone_voice_from_refs(
    *,
    voice_name: str,
    training_audio_refs: list[str],
    load_audio_bytes,
) -> str:
    if not training_audio_refs:
        raise ValueError("No training audio refs provided")

    client = get_eleven_client()

    files: list[BytesIO] = []
    for ref in training_audio_refs:
        audio_bytes = load_audio_bytes(ref)
        files.append(BytesIO(audio_bytes))

    voice = client.voices.ivc.create(
        name=voice_name,
        files=files,
    )

    return voice.voice_id


def delete_voice(
    *,
    voice_id: str,
) -> None:
    client = get_eleven_client()
    client.voices.delete(voice_id=voice_id)


def generate_tts_audio(
    *,
    voice_id: str,
    text: str,
    language_code: str | None = None,
    model_id: str = "eleven_multilingual_v2",
    output_format: str = "mp3_44100_128",
):
    if not text.strip():
        raise ValueError("Cannot generate audio for empty text")

    client = get_eleven_client()

    kwargs: dict[str, Any] = {
        "voice_id": voice_id,
        "text": text,
        "model_id": model_id,
        "output_format": output_format,
    }
    if language_code:
        kwargs["language_code"] = language_code

    return client.text_to_speech.convert(**kwargs)


def save_audio_stream(audio, output_path: str | Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("wb") as handle:
        if isinstance(audio, (bytes, bytearray)):
            handle.write(audio)
        else:
            for chunk in audio:
                if chunk:
                    handle.write(chunk)

    return output_path


def convert_mp3_to_wav(
    *,
    mp3_path: str | Path,
    wav_path: str | Path,
    sample_rate: int = 44100,
    channels: int = 1,
) -> Path:
    mp3_path = Path(mp3_path)
    wav_path = Path(wav_path)
    wav_path.parent.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(mp3_path),
            "-acodec",
            "pcm_s16le",
            "-ar",
            str(sample_rate),
            "-ac",
            str(channels),
            str(wav_path),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return wav_path