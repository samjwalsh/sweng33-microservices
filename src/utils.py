import subprocess
from pathlib import Path


def extract_audio(input_mp4: str | Path, output_wav: str | Path) -> None:
  
    ## Extract audio from MP4 video and save as WAV
   
    input_mp4 = Path(input_mp4)
    output_wav = Path(output_wav)

    if not input_mp4.exists():
        raise FileNotFoundError(f"Input video not found: {input_mp4}")

    output_wav.parent.mkdir(parents=True, exist_ok=True)

    extract_audio_command = [
        "ffmpeg",
        "-y",                     #overwrite output if it exists
        "-i", str(input_mp4),
        "-vn",                    #remove video 
        "-acodec", "pcm_s16le",   #uncompressed wav
        str(output_wav),
    ]

    subprocess.run(
        extract_audio_command,
        check=True,
        text=False,
    )

def merge_audio(input_mp4: str | Path, input_wav: str | Path, output_mp4: str | Path) -> None:
    ##Merge WAV audio back into MP4 video
  
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
