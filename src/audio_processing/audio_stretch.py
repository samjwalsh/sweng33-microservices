from audiostretchy.stretch import stretch_audio
from pathlib import Path
import wave


def stretch_audio_to_time(input_file: str, target_duration: float, output_file: str | None = None) -> str:
    if target_duration <= 0:
        raise ValueError("target_duration must be > 0 seconds")
    
    original_duration = wav_duration_seconds(input_file)
    if original_duration <= 0:
        raise ValueError(f"Could not determine duration for {input_file}")


    input_path = Path(input_file)

    if output_file is None:
        output_path = input_path.with_name(f"{input_path.stem}_stretched_{target_duration:.2f}s{input_path.suffix}")
    else:
        output_path = Path(output_file)

    stretch_factor =  target_duration / original_duration
    stretch_audio(str(input_path), str(output_path), ratio=stretch_factor)

    return str(output_path)



def wav_duration_seconds(input_file: str) -> float:
    with wave.open(input_file, "rb") as wf:
        return wf.getnframes() / float(wf.getframerate())