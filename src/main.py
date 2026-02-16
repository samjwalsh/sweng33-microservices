from ml_models.transcriber_sdk import transcribe_audio
from ml_models.translator_sdk import translate_text
from utils import extract_audio

transcription = transcribe_audio("data/spanish_audio.wav", locale="en-US")
translated = translate_text("es", "en", transcription.full_text)
print(translated)
