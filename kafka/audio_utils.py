import subprocess
from pathlib import Path
from pydub import AudioSegment
import json
from typing import Optional, Union


def merge_audio(input_mp4: str | Path, input_wav: str | Path, output_mp4: str | Path) -> None:
    input_mp4 = Path(input_mp4)
    input_wav = Path(input_wav)
    output_mp4 = Path(output_mp4)

    if not input_mp4.exists():
        raise FileNotFoundError(f"Input video not found: {input_mp4}")
    if not input_wav.exists():
        raise FileNotFoundError(f"Input audio not found: {input_wav}")

    output_mp4.parent.mkdir(parents=True, exist_ok=True)

    merge_audio_command = [
        "ffmpeg",
        "-y",
        "-i", str(input_mp4),
        "-i", str(input_wav),
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "copy",   
        "-c:a", "aac",
        "-shortest",
        "-map_metadata", "0",
        str(output_mp4),
    ]

    subprocess.run(
        merge_audio_command,
        check=True,
        text=False,
    )


def extract_audio(input_mp4: str | Path, output_wav: str | Path) -> None:
    input_mp4 = Path(input_mp4)
    output_wav = Path(output_wav)

    if not input_mp4.exists():
        raise FileNotFoundError(f"Input video not found: {input_mp4}")

    output_wav.parent.mkdir(parents=True, exist_ok=True)

    extract_audio_command = [
        "ffmpeg",
        "-y",   #overwrite output if it exists
        "-i", str(input_mp4),
        "-vn",   #remove video 
        "-acodec", "pcm_s16le",   #uncompressed wav
        str(output_wav),
    ]

    subprocess.run(
        extract_audio_command,
        check=True,
        text=False,
    )

def stitch_audio_with_timestamps(segments_info: list[dict], output_path: str | Path):
    """
    segments_info: list of {'path': Path, 'start': float (seconds)}
    Handles gaps by inserting silence based on start timestamps.
    """
    segments_info.sort(key=lambda x: x['start'])
    
    combined = AudioSegment.silent(duration=0)
    current_ms = 0

    for info in segments_info:
        clip = AudioSegment.from_file(info['path'])
        target_start_ms = int(info['start'] * 1000)
        
        if target_start_ms > current_ms:
            gap_duration = target_start_ms - current_ms
            combined += AudioSegment.silent(duration=gap_duration)
        
        combined += clip
        current_ms = len(combined)

    combined.export(output_path, format="wav")
    return output_path

def get_audio_snippet(input_wav: str | Path, start_sec: float, end_sec: float, output_path: str | Path):
    audio = AudioSegment.from_file(input_wav)
    snippet = audio[start_sec * 1000 : end_sec * 1000]
    snippet.export(output_path, format="wav")
    return output_path


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

    orig = ffprobe_duration(input_path)
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

def prepare_segment(input_wav: str | Path, start_sec: float, end_sec: float, output_path: str | Path) -> Path:
    target_duration = end_sec - start_sec
    snippet_path = get_audio_snippet(input_wav, start_sec, end_sec, output_path)
    stretch_path = stretch_audio_to_time(snippet_path,target_duration,output_path)
    return Path(stretch_path)
