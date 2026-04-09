from src.audio_processing.audio_stretch import AudioStretch

inp = "data/spanish_audio.wav"
target = 20

orig = AudioStretch.ffprobe_duration(inp)
out = AudioStretch.stretch_audio_to_time(inp, target)
new = AudioStretch.ffprobe_duration(out)

print("original", orig, "new", new, "target", target)
assert abs(new - target) < 0.25
