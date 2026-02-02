import argparse
from pathlib import Path

import torch
import soundfile as sf
from qwen_tts import Qwen3TTSModel


def pick_device_and_dtype(force_cpu: bool):
    if (not force_cpu) and torch.cuda.is_available():
        return "cuda:0", torch.bfloat16
    return "cpu", torch.float32


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="Qwen/Qwen3-TTS-12Hz-0.6B-Base",
                    help="HF model id (0.6B is lighter; use 1.7B for higher quality if you have GPU)")
    ap.add_argument("--text", required=True, help="Target text to synthesize")
    ap.add_argument("--language", default="English", help="English, German, etc.")
    ap.add_argument("--ref_audio", required=True, help="Reference audio path (wav) or URL")
    ap.add_argument("--ref_text", required=True, help="Transcript of the reference audio")
    ap.add_argument("--out", default="research/qwen3_tts_poc/outputs/out.wav")
    ap.add_argument("--cpu", action="store_true", help="Force CPU")
    ap.add_argument("--flash_attn", action="store_true",
                    help="Use FlashAttention2 (GPU + bf16/fp16 only, optional)")
    args = ap.parse_args()

    device_map, dtype = pick_device_and_dtype(args.cpu)

    kwargs = {}
    if args.flash_attn:
        kwargs["attn_implementation"] = "flash_attention_2"

    model = Qwen3TTSModel.from_pretrained(
        args.model,
        device_map=device_map,
        dtype=dtype,
        **kwargs,
    )

    wavs, sr = model.generate_voice_clone(
        text=args.text,
        language=args.language,
        ref_audio=args.ref_audio,
        ref_text=args.ref_text,
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sf.write(out_path.as_posix(), wavs[0], sr)
    print(f"Saved: {out_path.resolve()} (sr={sr})")


if __name__ == "__main__":
    main()
