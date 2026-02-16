import os
from pathlib import Path
import azure.cognitiveservices.speech as speechsdk
from dotenv import load_dotenv

load_dotenv()


class TranscriptionPhrase:
    def __init__(self, text: str, start_ms: int, end_ms: int):
        self.text = text
        self.start_ms = start_ms
        self.end_ms = end_ms

    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms


class Transcription:
    def __init__(self, full_text: str, phrases: list[TranscriptionPhrase], locale: str, audio_file_path: str):
        self.full_text = full_text
        self.phrases = phrases
        self.locale = locale
        self.audio_file_path = audio_file_path

    def get_phrases_in_range(self, start_ms: int, end_ms: int) -> list[TranscriptionPhrase]:
        return [
            phrase for phrase in self.phrases
            if phrase.start_ms >= start_ms and phrase.end_ms <= end_ms
        ]

    def get_text_in_range(self, start_ms: int, end_ms: int) -> str:
        phrases = self.get_phrases_in_range(start_ms, end_ms)
        return " ".join(phrase.text for phrase in phrases)

    def to_srt(self) -> str:
        srt_lines = []
        for i, phrase in enumerate(self.phrases, 1):
            start = self._ms_to_srt_time(phrase.start_ms)
            end = self._ms_to_srt_time(phrase.end_ms)
            srt_lines.append(f"{i}\n{start} --> {end}\n{phrase.text}\n")
        return "\n".join(srt_lines)

    @staticmethod
    def _ms_to_srt_time(ms: int) -> str:
        seconds, ms = divmod(ms, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d},{ms:03d}"

    def save_srt(self, output_path: str) -> None:
        Path(output_path).write_text(self.to_srt(), encoding="utf-8")


def transcribe_audio(audio_file_path: str, locale: str = "en-US") -> Transcription:
    if not Path(audio_file_path).is_file():
        raise FileNotFoundError(f"Audio file not found: {audio_file_path}")

    speech_config = speechsdk.SpeechConfig(
        subscription=os.environ["TRANSCRIBER_KEY"],
        endpoint=os.environ["TRANSCRIBER_ENDPOINT"],
    )

    speech_config.speech_recognition_language = locale

    audio_config = speechsdk.audio.AudioConfig(filename=audio_file_path)
    speech_recognizer = speechsdk.SpeechRecognizer(
        speech_config=speech_config,
        audio_config=audio_config
    )

    phrases = []
    full_text_parts = []

    done = False

    def recognized(evt):
        if evt.result.reason == speechsdk.ResultReason.RecognizedSpeech:
            text = evt.result.text
            offset_ms = evt.result.offset / 10000  
            duration_ms = evt.result.duration / 10000

            phrases.append(
                TranscriptionPhrase(
                    text=text,
                    start_ms=int(offset_ms),
                    end_ms=int(offset_ms + duration_ms),
                )
            )
            full_text_parts.append(text)

    def stop_cb(evt):
        nonlocal done
        done = True

    speech_recognizer.recognized.connect(recognized)
    speech_recognizer.session_stopped.connect(stop_cb)
    speech_recognizer.canceled.connect(stop_cb)

    speech_recognizer.start_continuous_recognition()

    while not done:
        pass

    speech_recognizer.stop_continuous_recognition()

    return Transcription(
        full_text=" ".join(full_text_parts),
        phrases=phrases,
        locale=locale,
        audio_file_path=audio_file_path
    )

"""
transcription = transcribe_audio("data/spanish_audio.wav", locale="en-US")
print(f"Full transcription:\n{transcription.full_text}\n")

transcription.save_srt("data/spanish_audio.srt")
print("SRT file saved to data/spanish_audio.srt")
"""