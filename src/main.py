from fastapi import FastAPI
from pydantic import BaseModel

from src.tts.qwen_tts_service import TTSSegment
from src.tts.tts_pipeline import generate_wav_and_upload

app = FastAPI()


class SegmentIn(BaseModel):
    speaker: str
    text: str


class TTSRequest(BaseModel):
    job_id: str
    language: str
    segments: list[SegmentIn]


@app.post("/tts")
def tts(req: TTSRequest):
    segments = [TTSSegment(speaker=s.speaker, text=s.text) for s in req.segments]
    return generate_wav_and_upload(
        job_id=req.job_id,
        language=req.language,
        segments=segments,
    )

