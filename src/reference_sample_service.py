
from pathlib import Path
from pydub import AudioSegment
from audio_utils import extract_wav_snippet


def group_segments_by_speaker(diarisation_output: list[dict]) -> dict[int, list[dict]]:
    "Based on the diarisation output, groups segments together by speaker, returns a dict mapping speaker_id to list of segments"
    
    grouped_segments = {}

    for segment in diarisation_output:
        raw_speaker_id = segment["speaker"]
        speaker_id = int(raw_speaker_id.split("_")[-1])

        if speaker_id not in grouped_segments:
            grouped_segments[speaker_id] = []
        
        grouped_segments[speaker_id].append({
            "start": segment["start"],
            "end": segment["end"]
        })
    
    return grouped_segments


def cut_segments_from_audio(segments: list[dict], audio_path: str, temp_dir: str | Path, speaker_id: int) -> list[Path]:
    "Based on the grouped segments, cuts those segments from the original audio file"
    cut_audio_paths = []

    for i, segment in enumerate(segments):
        start = segment["start"]
        end = segment["end"]

        out_path = Path(temp_dir) / f"speaker_{speaker_id}_segment_{i}.wav"
        extract_wav_snippet(audio_path, start, end, out_path=out_path)
        cut_audio_paths.append(out_path)
    
    return cut_audio_paths


def combine_segments_for_speaker(segments: list[str | Path], output_wav: str | Path) -> None:

    output_wav = Path(output_wav)
    output_wav.parent.mkdir(parents=True, exist_ok=True)

    combined_audio = AudioSegment.empty()

    for segment in segments:
        audio_segment = AudioSegment.from_wav(segment)
        combined_audio += audio_segment

    combined_audio.export(output_wav, format="wav")
    
       
 
def compile_voice_cloning_samples(diarisation_output: list[dict], audio_path: str | Path, output_dir: str | Path) -> list[Path]:

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    temp_dir = output_dir / "temp_snippets"
    temp_dir.mkdir(parents=True, exist_ok=True)

    final_output_paths = []
    
    #1. group segments together by speaker
    grouped_segments = group_segments_by_speaker(diarisation_output)

    #2. cut those segments from the original audio file, combine each speakers clips into a single file, and save one output file per speaker
    for speaker_id, segments in grouped_segments.items():
        snippet_paths = cut_segments_from_audio(segments, audio_path, temp_dir=temp_dir, speaker_id=speaker_id)
        
        output_wav = output_dir / f"speaker_{speaker_id}_reference.wav"

        combine_segments_for_speaker(snippet_paths, output_wav = output_wav)
        final_output_paths.append(output_wav)
    
    return final_output_paths

