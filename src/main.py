from ml_models.transcriber_sdk import transcribe_audio
from ml_models.translator_sdk import translate_text
from utils import extract_audio, merge_audio
from ml_models.tts import TTS, TTSSegment 

print("Extracting audio")
extract_audio("data/english_video_tiny.mp4","data/english_audio_tiny.wav")
print("Transcribing audio")
transcription = transcribe_audio("data/english_audio_tiny.wav", locale="en-US")

print("translating audio")
translated = translate_text("en", "es", transcription.full_text)

print("generating audio")
tts_service = TTS()

segments = [
    TTSSegment(speaker="Speaker1", text=translated)
]

output_path = tts_service.synthesize(
    language="spanish",  
    segments=segments,
    out_wav_path="data/spanish_audio_tiny.wav"
)

print(f"Audio generated at: {output_path}")

merge_audio("data/english_video_tiny.mp4", "data/spanish_audio_tiny.wav", "data/spanish_video_tiny.mp4")