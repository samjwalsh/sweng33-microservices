import numpy as np
import pytest

from src.ml_models.tts import QwenTTSService, TTSSegment


class DummyModel:
    def generate_custom_voice(self, text, language, speaker, instruct):
        # should return 0.1 seconds of a fake audio with 16000Hz
        sr = 16000
        wav = np.zeros(int(0.1 * sr), dtype=np.float32)
        return [wav], sr


def test_empty_segments_raises(tmp_path):
    svc = QwenTTSService()
    svc._model = DummyModel()  # bypass load
    with pytest.raises(ValueError):
        svc.synthesize("English", [], tmp_path / "out.wav")


def test_speaker_mapping_is_deterministic():
    svc = QwenTTSService(speaker_pool=["Ryan", "Aiden"])
    a1 = svc._map_speaker("A")
    a2 = svc._map_speaker("A")
    assert a1 == a2


def test_writes_wav_file(tmp_path):
    svc = QwenTTSService()
    svc._model = DummyModel()  # bypass load

    out = tmp_path / "out.wav"
    segments = [
        TTSSegment(speaker="A", text="Hello"),
        TTSSegment(speaker="B", text="World"),
    ]
    result = svc.synthesize("English", segments, out)

    assert result.exists()
    assert result.stat().st_size > 0

