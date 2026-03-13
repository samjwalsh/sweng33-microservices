import sys
import types
import importlib


def load_translation_module():
    fake_db_helper = types.ModuleType("db_helper")
    fake_db_helper.upsert_tts_placeholder = lambda *args, **kwargs: None
    sys.modules["db_helper"] = fake_db_helper

    fake_microservice_template = types.ModuleType("microservice_template")
    fake_microservice_template.KafkaMicroservice = object
    fake_microservice_template.MessageContext = object
    sys.modules["microservice_template"] = fake_microservice_template

    fake_payload_validation = types.ModuleType("payload_validation")
    fake_payload_validation.PayloadValidationError = Exception
    fake_payload_validation.validate_translate_payload = lambda payload: None
    sys.modules["payload_validation"] = fake_payload_validation

    fake_topics = types.ModuleType("topics")
    fake_topics.TOPIC_TEXT_TO_SPEECH = "tts"
    fake_topics.TOPIC_TRANSLATE_SEGMENTS = "translate"
    fake_topics.key_by_src_blob_and_speaker = lambda src_blob, speaker_id: f"{src_blob}:{speaker_id}"
    sys.modules["topics"] = fake_topics

    if "kafka.microservices.translation_service" in sys.modules:
        del sys.modules["kafka.microservices.translation_service"]

    return importlib.import_module("kafka.microservices.translation_service")


def test_empty_text_returns_empty_string():
    module = load_translation_module()
    assert module.translate_segment_text("", "en", "fr") == ""


def test_none_text_returns_empty_string():
    module = load_translation_module()
    assert module.translate_segment_text(None, "en", "fr") == ""


def test_same_language_returns_original():
    module = load_translation_module()
    assert module.translate_segment_text("Hello world", "en", "en") == "Hello world"


def test_returns_original_text_if_translator_unavailable():
    module = load_translation_module()
    module.translator_client = None
    assert module.translate_segment_text("Hello", "en", "fr") == "Hello"


def test_returns_translated_text_when_client_works():
    module = load_translation_module()

    class FakeTranslation:
        def __init__(self, text):
            self.text = text

    class FakeItem:
        def __init__(self, text):
            self.translations = [FakeTranslation(text)]

    class FakeClient:
        def translate(self, **kwargs):
            return [FakeItem("Bonjour")]

    module.translator_client = FakeClient()

    assert module.translate_segment_text("Hello", "en", "fr") == "Bonjour"