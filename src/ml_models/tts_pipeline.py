from __future__ import annotations

from pathlib import Path
from src.ml_models.qwen_tts_service import QwenTTSService, TTSSegment
from src.storage.blob_upload import AzureBlobStorage


def generate_wav_and_upload(
    job_id: str,
    language: str,
    segments: list[TTSSegment],
    *,
    container: str = "dubbed-audio",
) -> dict:
    out_dir = Path("outputs")
    out_dir.mkdir(parents=True, exist_ok=True)

    wav_path = out_dir / f"{job_id}_{language}.wav"

    tts = QwenTTSService()
    wav_path = tts.synthesize(language=language, segments=segments, out_wav_path=wav_path)

    storage = AzureBlobStorage(container=container)
    blob_name = f"{job_id}/{wav_path.name}"
    blob = storage.upload_file(wav_path, blob_name)

    return {
        "job_id": job_id,
        "language": language,
        "local_wav_path": str(wav_path),
        "azure_container": blob.container,
        "azure_blob_name": blob.blob_name,
        "azure_url": blob.url,
    }

