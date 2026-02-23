from pathlib import Path
from src.audio_processing.audio_stretch import stretch_audio_to_time, wav_duration_seconds  
import wave

inp = "data/harvard.wav"
target = 8 


def wav_info(path: str):
    with wave.open(path, "rb") as wf:
        return wf.getframerate(), wf.getnframes(), wf.getnchannels()



orig = wav_duration_seconds(inp)
out = stretch_audio_to_time(inp, target)
new = wav_duration_seconds(out)
print("original", orig, "new", new, "target", target)
assert abs(new - target) < 0.25


