from ml_models.transcriber_sdk import transcribe_audio
from ml_models.translator_sdk import translate_text
from utils import extract_audio
import os

print("Extracting audio")
extract_audio("data/english_video.mp4","data/english_audio.wav")
print("Transcribing audio")
transcription = transcribe_audio("data/english_audio.wav", locale="en-US")
file_path = "data/english_audio.wav"

if os.path.exists(file_path):
    os.remove(file_path)

print("translating audio")
translated = translate_text("en", "es", transcription.full_text)

print("generating audio")

