from kafka_pipeline import topics as t


# -------------------------
# constants
# -------------------------

def test_topic_constants():
    assert t.TOPIC_INGEST == "ingest"
    assert t.TOPIC_TRANSLATE_SEGMENTS == "translate_segments"
    assert t.TOPIC_TEXT_TO_SPEECH == "text_to_speech"
    assert t.TOPIC_RECONSTRUCT_VIDEO == "reconstruct_video"


# -------------------------
# key functions
# -------------------------

def test_key_by_src_blob_returns_same_value():
    assert t.key_by_src_blob("video123") == "video123"


def test_key_by_src_blob_and_speaker_formats_correctly():
    result = t.key_by_src_blob_and_speaker("video123", "speaker_1")
    assert result == "video123:speaker_1"


def test_key_by_src_blob_and_speaker_handles_empty_values():
    assert t.key_by_src_blob_and_speaker("", "") == ":"