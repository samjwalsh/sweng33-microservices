TOPIC_INGEST = "ingest"
TOPIC_TRANSLATE_SEGMENTS = "translate_segments"
TOPIC_TEXT_TO_SPEECH = "text_to_speech"
TOPIC_RECONSTRUCT_VIDEO = "reconstruct_video"


def key_by_src_blob(src_blob: str) -> str:
    return src_blob


def key_by_src_blob_and_speaker(src_blob: str, speaker_id: str) -> str:
    return f"{src_blob}:{speaker_id}"
