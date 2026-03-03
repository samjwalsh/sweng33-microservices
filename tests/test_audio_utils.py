import pytest
import numpy as np
import soundfile as sf

from src.audio_utils import extract_wav_snippet


def test_extract_wav_snippet(tmp_path):
    sr = 16000
    samples = np.zeros(sr * 3)
    test_wav = tmp_path / "test.wav"
    sf.write(test_wav, samples, sr)

    audio, out_sr = extract_wav_snippet(test_wav, 1, 2)

    assert out_sr == sr
    assert len(audio) == sr


def test_extract_wav_snippet_invalid_range(tmp_path):
    sr = 16000
    samples = np.zeros(sr * 2)
    test_wav = tmp_path / "test.wav"
    sf.write(test_wav, samples, sr)

    with pytest.raises(ValueError):
        extract_wav_snippet(test_wav, 2, 1)  

    with pytest.raises(ValueError):
        extract_wav_snippet(test_wav, -1, 1)  


def test_extract_wav_snippet_clamps_end(tmp_path):
    sr = 16000
    samples = np.zeros(sr * 2)  
    test_wav = tmp_path / "test.wav"
    sf.write(test_wav, samples, sr)

    audio, out_sr = extract_wav_snippet(test_wav, 1, 10)

    assert out_sr == sr
    assert len(audio) == sr  