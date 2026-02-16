from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import soundfile as sf
import torch

@dataclass(frozen=True)
class TTSSegment:
    speaker: str
    text: str


class QwenTTSService:
    """
    wrapper around Qwen3-TTS CustomVoice.
    Input:
      - language of speech - needds to be one of the ten QWEN languages
      - segments: list of (speaker, text) coming from translation output
    Output:
      - path to a .wav file
    """

    def __init__(
        self,
        model_id: str = "Qwen/Qwen3-TTS-12Hz-0.6B-CustomVoice",
        # STRETCH GOAL - VOICE CLONE - take input instruct the tone of the person speaking
        default_instruct: str = "Clear, natural, professional tone. Medium pace.",
        speaker_pool: Optional[list[str]] = None,
        device_map: Optional[str] = None,
        dtype: Optional[torch.dtype] = None,
    ):
        self.model_id = model_id
        self.default_instruct = default_instruct
        self.speaker_pool = speaker_pool or ["Ryan", "Aiden"]  # extend later

        # Allow explicit override for tests / deployment
        self.device_map, self.dtype = self._pick_device_and_dtype(device_map, dtype)

        self._model: Any = None  # lazy load

    def _pick_device_and_dtype(self, device_map: Optional[str], dtype: Optional[torch.dtype]):
        if device_map is not None and dtype is not None:
            return device_map, dtype

        # Default: GPU if available, else CPU
        if torch.cuda.is_available():
            return "cuda:0", torch.bfloat16
        return "cpu", torch.float32

    def _load_model(self):
        if self._model is None:
            # Import here so unit tests can collect without loading qwen runtime deps.
            from qwen_tts import Qwen3TTSModel

            self._model = Qwen3TTSModel.from_pretrained(
                self.model_id,
                device_map=self.device_map,
                dtype=self.dtype,
            )
        return self._model

    def _map_speaker(self, speaker_label: str) -> str:
        """
        Deterministic mapping: any speaker label maps to a stable voice in speaker_pool.
        """
        # SHOULD CHANGE HERE IF CLONING
        idx = abs(hash(speaker_label)) % len(self.speaker_pool)
        return self.speaker_pool[idx]

    def synthesize(
        self,
        language: str,
        segments: Iterable[TTSSegment],
        out_wav_path: str | Path,
        instruct: Optional[str] = None,
    ) -> Path:
        segments = list(segments)
        if not segments:
            raise ValueError("No segments provided")

        model = self._load_model()
        instruct = instruct or self.default_instruct

        out_wav_path = Path(out_wav_path)
        out_wav_path.parent.mkdir(parents=True, exist_ok=True)

        # Generate each segment separately, then concatenate in Python
        all_audio = []
        sample_rate = None

        for seg in segments:
            if not seg.text.strip():
                continue

            voice = self._map_speaker(seg.speaker)

            wavs, sr = model.generate_custom_voice(
                text=seg.text,
                language=language,
                speaker=voice,
                instruct=instruct,
            )

            if sample_rate is None:
                sample_rate = sr
            elif sr != sample_rate:
                raise ValueError(f"Sample rate mismatch: {sr} vs {sample_rate}")

            all_audio.append(wavs[0])

        if not all_audio:
            raise ValueError("All segments were empty")

        # Concatenate into one waveform
        import numpy as np
        full = np.concatenate(all_audio, axis=0)

        sf.write(out_wav_path.as_posix(), full, sample_rate)
        return out_wav_path
