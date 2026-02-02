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

    speaker_A = "Ryan"
    speaker_B = "Aiden"

    script = [
        ("A", "Welcome. This is our two speaker dubbing demo."),
        ("B", "I am the second speaker. Notice the different voice."),
        ("A", "In production, we generate each speaker turn separately and then align it back to the video."),
    ]

    out_dir = Path("research/qwen3_tts_poc/outputs")
    out_dir.mkdir(parents=True, exist_ok=True)

    for i, (spk, line) in enumerate(script, start=1):
        speaker = speaker_A if spk == "A" else speaker_B

        wavs, sr = model.generate_custom_voice(
            text=line,
            language="English",
            speaker=speaker,
            instruct="Natural conversational tone. Clear pronunciation.",
        )

        out = out_dir / f"line_{i:02d}_{spk}_{speaker}.wav"
        sf.write(out.as_posix(), wavs[0], sr)
        print("Saved:", out)

if __name__ == "__main__":
    main()

