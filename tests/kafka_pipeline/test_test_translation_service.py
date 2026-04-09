import types

from kafka_pipeline import test_translation_service as manual_test


def test_load_translation_module_stubs_dependencies(monkeypatch):
    fake_imported_module = types.SimpleNamespace(name="loaded-module")
    captured = {}

    def fake_import_module(name):
        captured["name"] = name
        return fake_imported_module

    monkeypatch.setattr(manual_test.importlib, "import_module", fake_import_module)
    monkeypatch.setattr(manual_test.sys, "modules", dict(manual_test.sys.modules))

    module = manual_test.load_translation_module()

    assert module is fake_imported_module
    assert captured["name"] == "kafka.microservices.translation_service"

    assert "db_helper" in manual_test.sys.modules
    assert "microservice_template" in manual_test.sys.modules
    assert "payload_validation" in manual_test.sys.modules
    assert "topics" in manual_test.sys.modules


def test_load_translation_module_removes_existing_target(monkeypatch):
    fake_modules = dict(manual_test.sys.modules)
    fake_modules["kafka.microservices.translation_service"] = object()

    removed = {"imported": False}

    def fake_import_module(name):
        removed["imported"] = True
        return types.SimpleNamespace()

    monkeypatch.setattr(manual_test.sys, "modules", fake_modules)
    monkeypatch.setattr(manual_test.importlib, "import_module", fake_import_module)

    manual_test.load_translation_module()

    assert removed["imported"] is True
    assert "kafka.microservices.translation_service" not in fake_modules


def test_test_empty_text_returns_empty_string(monkeypatch):
    fake_module = types.SimpleNamespace(
        translate_segment_text=lambda text, src, dest: ""
    )
    monkeypatch.setattr(manual_test, "load_translation_module", lambda: fake_module)

    manual_test.test_empty_text_returns_empty_string()


def test_test_none_text_returns_empty_string(monkeypatch):
    fake_module = types.SimpleNamespace(
        translate_segment_text=lambda text, src, dest: ""
    )
    monkeypatch.setattr(manual_test, "load_translation_module", lambda: fake_module)

    manual_test.test_none_text_returns_empty_string()


def test_test_same_language_returns_original(monkeypatch):
    fake_module = types.SimpleNamespace(
        translate_segment_text=lambda text, src, dest: text
    )
    monkeypatch.setattr(manual_test, "load_translation_module", lambda: fake_module)

    manual_test.test_same_language_returns_original()


def test_test_returns_original_text_if_translator_unavailable(monkeypatch):
    class FakeModule:
        translator_client = object()

        @staticmethod
        def translate_segment_text(text, src, dest):
            return "Hello"

    fake_module = FakeModule()
    monkeypatch.setattr(manual_test, "load_translation_module", lambda: fake_module)

    manual_test.test_returns_original_text_if_translator_unavailable()
    assert fake_module.translator_client is None


def test_test_returns_translated_text_when_client_works(monkeypatch):
    class FakeModule:
        translator_client = None

        @staticmethod
        def translate_segment_text(text, src, dest):
            return "Bonjour"

    monkeypatch.setattr(manual_test, "load_translation_module", lambda: FakeModule())

    manual_test.test_returns_translated_text_when_client_works()