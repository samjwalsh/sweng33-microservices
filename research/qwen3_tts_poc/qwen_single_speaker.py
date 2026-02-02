from pathlib import Path

import torch
import soundfile as sf
from qwen_tts import Qwen3TTSModel

MODEL_ID = "Qwen/Qwen3-TTS-12Hz-0.6B-CustomVoice"


def pick_device_and_dtype():
    if torch.cuda.is_available():
        return "cuda:0", torch.bfloat16
    return "cpu", torch.float32


def main():
    device_map, dtype = pick_device_and_dtype()

    model = Qwen3TTSModel.from_pretrained(
        MODEL_ID,
        device_map=device_map,
        dtype=dtype,
    )

    text = (
        "Hi, this is a text to speech demo using Qwen3 Text To Speech. "
        "In our pipeline, Azure handles translation and Qwen generates the dubbed audio."
	"Chickens and Potatoes"
    )

    wavs, sr = model.generate_custom_voice(
        text=text,
        language="English",
        speaker="Ryan",
        instruct="Calm, clear, professional tone. Medium pace.",
    )

    out = Path("research/qwen3_tts_poc/outputs/single_speaker.wav")
    out.parent.mkdir(parents=True, exist_ok=True)
    sf.write(out.as_posix(), wavs[0], sr)
    print("Saved:", out.resolve())


if __name__ == "__main__":
    main()

