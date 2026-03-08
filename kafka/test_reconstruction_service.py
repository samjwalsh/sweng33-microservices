
from dataclasses import dataclass
from pathlib import Path
import wave

from microservices.reconstruction_service import fit_segment_audio_to_timing


@dataclass
class TTSSegment:
    start: float
    end: float


def get_wav_duration(path: str | Path) -> float:
    path = Path(path)
    with wave.open(str(path), "rb") as wf:
        frames = wf.getnframes()
        rate = wf.getframerate()
        if rate <= 0:
            raise ValueError(f"Invalid sample rate in {path}")
        return frames / rate


def main():
    input_audio_path = "data/spanish_audio.wav"
    output_audio_path = "data/spanish_audio_stretched_12.00s.wav"

    if not Path(input_audio_path).exists():
        raise FileNotFoundError(f"Input file not found: {input_audio_path}")

    segment = TTSSegment(
        start=0.0,
        end=12.0,
    )

    target_duration = segment.end - segment.start
    input_duration = get_wav_duration(input_audio_path)

    print(f"Input file: {input_audio_path}")
    print(f"Input duration: {input_duration:.3f}s")
    print(f"Target duration: {target_duration:.3f}s")

    returned_path = fit_segment_audio_to_timing(
        segment=segment,
        input_audio_path=input_audio_path,
        output_audio_path=output_audio_path,
    )

    print(f"Returned path: {returned_path}")

    if not Path(returned_path).exists():
        raise FileNotFoundError(f"Output file was not created: {returned_path}")

    output_duration = get_wav_duration(returned_path)
    diff = abs(output_duration - target_duration)

    print(f"Output duration: {output_duration:.3f}s")
    print(f"Difference from target: {diff:.3f}s")

    tolerance = 0.10
    if diff <= tolerance:
        print(f"PASS: output duration is within {tolerance:.2f}s tolerance")
    else:
        print(f"FAIL: output duration is outside {tolerance:.2f}s tolerance")


if __name__ == "__main__":
    main()