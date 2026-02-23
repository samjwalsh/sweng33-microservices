import json
import subprocess
from pathlib import Path
from typing import Optional, Union


class AudioStretch:
    @staticmethod
    def stretch_audio_to_time(
        input: Union[str, Path],
        target_seconds: float,
        output: Optional[Union[str, Path]] = None,
    ) -> str:
        
        input_path = Path(input)

        if target_seconds <= 0:
            raise ValueError("target_seconds must be > 0")

        if output is None:
            output_path = input_path.with_name(
                f"{input_path.stem}_stretched_{target_seconds:.2f}s{input_path.suffix}"
            )
        else:
            output_path = Path(output)

        orig = AudioStretch.ffprobe_duration(input_path)
        if orig <= 0:
            raise ValueError(f"Could not determine duration for: {input_path}")

        time_ratio = target_seconds / orig

        try:
            subprocess.run(
                [
                    "rubberband",
                    "-t",
                    f"{time_ratio:.8f}",
                    str(input_path),
                    str(output_path),
                ],
                check=True,
            )
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "Could not find `rubberband` on PATH"
            ) from e

        return str(output_path)

    @staticmethod
    def ffprobe_duration(path: Union[str, Path]) -> float:
        
        try:
            p = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "json",
                    str(path),
                ],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            )
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "Could not find `ffprobe` on PATH"
            ) from e

        return float(json.loads(p.stdout)["format"]["duration"])
