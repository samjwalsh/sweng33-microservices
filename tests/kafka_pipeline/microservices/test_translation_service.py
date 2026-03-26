from types import SimpleNamespace


from kafka_pipeline.microservices import translation_service as ts


def valid_segment(**overrides):
    segment = {
        "segment_id": 1,
        "speaker_id": "speaker_0",
        "start": 0.0,
        "end": 1.0,
        "text": "hello",
    }
    segment.update(overrides)
    return segment


def valid_payload(**overrides):
    payload = {
        "src_blob": "blob-1",
        "src_lang": "en",
        "dest_lang": "fr",
        "segments": [valid_segment()],
    }
    payload.update(overrides)
    return payload


class FakeTranslation:
    def __init__(self, text):
        self.text = text


class FakeResponseItem:
    def __init__(self, text):
        self.translations = [FakeTranslation(text)]


def test_translate_segment_text_returns_empty_for_none(monkeypatch):
    monkeypatch.setattr(ts, "translator_client", object())
    assert ts.translate_segment_text(None, "en", "fr") == ""


def test_translate_segment_text_returns_empty_for_blank(monkeypatch):
    monkeypatch.setattr(ts, "translator_client", object())
    assert ts.translate_segment_text("   ", "en", "fr") == ""


def test_translate_segment_text_returns_original_when_same_language(monkeypatch):
    monkeypatch.setattr(ts, "translator_client", object())
    assert ts.translate_segment_text(" hello ", "en", "en") == "hello"


def test_translate_segment_text_returns_original_when_no_client(monkeypatch):
    monkeypatch.setattr(ts, "translator_client", None)
    assert ts.translate_segment_text(" hello ", "en", "fr") == "hello"


def test_translate_segment_text_uses_translator(monkeypatch):
    calls = []

    class FakeClient:
        def translate(self, content, to, from_parameter):
            calls.append(
                {
                    "content": content,
                    "to": to,
                    "from_parameter": from_parameter,
                }
            )
            return [FakeResponseItem("bonjour")]

    monkeypatch.setattr(ts, "translator_client", FakeClient())
    monkeypatch.setattr(ts, "InputTextItem", lambda text: {"text": text})

    result = ts.translate_segment_text(" hello ", "en", "fr")

    assert result == "bonjour"
    assert calls == [
        {
            "content": [{"text": "hello"}],
            "to": ["fr"],
            "from_parameter": "en",
        }
    ]


def test_translate_segment_text_returns_original_when_translation_missing(monkeypatch):
    class FakeClient:
        def translate(self, content, to, from_parameter):
            return []

    monkeypatch.setattr(ts, "translator_client", FakeClient())
    monkeypatch.setattr(ts, "InputTextItem", lambda text: {"text": text})

    assert ts.translate_segment_text("hello", "en", "fr") == "hello"


def test_translate_segment_text_returns_original_when_translator_raises(monkeypatch):
    class FakeClient:
        def translate(self, content, to, from_parameter):
            raise RuntimeError("boom")

    monkeypatch.setattr(ts, "translator_client", FakeClient())
    monkeypatch.setattr(ts, "InputTextItem", lambda text: {"text": text})

    assert ts.translate_segment_text("hello", "en", "fr") == "hello"


def test_handler_skips_when_no_non_empty_segments(capsys):
    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: None)
    context = SimpleNamespace(offset=7)

    ts.handler(
        {
            "src_blob": "blob-1",
            "src_lang": "en",
            "dest_lang": "fr",
            "segments": [
                {"text": ""},
                {"text": "   "},
                {"text": None},
                {"not_text": "x"},
            ],
        },
        context,
        service,
    )

    out = capsys.readouterr().out
    assert "No non-empty segments to process at offset=7; skipping" in out


def test_handler_invalid_payload(capsys, monkeypatch):
    def fake_validate(payload):
        raise ts.PayloadValidationError("bad payload")

    monkeypatch.setattr(ts, "validate_translate_payload", fake_validate)

    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: None)
    context = SimpleNamespace(offset=8)

    ts.handler(valid_payload(), context, service)

    out = capsys.readouterr().out
    assert "Invalid payload at offset=8: bad payload" in out


def test_handler_filters_empty_segments_before_validation(monkeypatch, capsys):
    validated = []

    def fake_validate(payload):
        validated.append(payload)

    monkeypatch.setattr(ts, "validate_translate_payload", fake_validate)
    monkeypatch.setattr(ts, "translate_segment_text", lambda text, src_lang, dest_lang: f"X-{text}")
    monkeypatch.setattr(ts, "upsert_tts_placeholder", lambda **kwargs: None)
    monkeypatch.setattr(ts, "increment_tts_total_tasks", lambda **kwargs: None)
    monkeypatch.setattr(ts, "increment_translation_completed_tasks", lambda **kwargs: None)
    monkeypatch.setattr(ts, "key_by_src_blob_and_speaker", lambda src_blob, speaker_id: f"{src_blob}:{speaker_id}")
    monkeypatch.setattr(ts, "TOPIC_TEXT_TO_SPEECH", "tts")

    published = []
    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: published.append(kwargs))
    context = SimpleNamespace(offset=9)

    payload = {
        "src_blob": "blob-1",
        "src_lang": "en",
        "dest_lang": "fr",
        "segments": [
            valid_segment(segment_id=1, speaker_id="speaker_0", text="hello"),
            valid_segment(segment_id=2, speaker_id="speaker_0", text="   "),
            valid_segment(segment_id=3, speaker_id="speaker_1", text="world"),
        ],
    }

    ts.handler(payload, context, service)

    assert len(validated) == 1
    assert [s["segment_id"] for s in validated[0]["segments"]] == [1, 3]
    assert len(published) == 2
    out = capsys.readouterr().out
    assert "Processed 2 segments and published 2 speaker batch(es) for src_blob=blob-1" in out


def test_handler_groups_by_speaker_and_publishes(monkeypatch, capsys):
    monkeypatch.setattr(ts, "validate_translate_payload", lambda payload: None)
    monkeypatch.setattr(ts, "translate_segment_text", lambda text, src_lang, dest_lang: f"{text}-fr")

    upsert_calls = []
    monkeypatch.setattr(
        ts,
        "upsert_tts_placeholder",
        lambda **kwargs: upsert_calls.append(kwargs),
    )

    tts_total_calls = []
    translation_completed_calls = []

    monkeypatch.setattr(
        ts,
        "increment_tts_total_tasks",
        lambda **kwargs: tts_total_calls.append(kwargs),
    )
    monkeypatch.setattr(
        ts,
        "increment_translation_completed_tasks",
        lambda **kwargs: translation_completed_calls.append(kwargs),
    )

    monkeypatch.setattr(ts, "key_by_src_blob_and_speaker", lambda src_blob, speaker_id: f"{src_blob}:{speaker_id}")
    monkeypatch.setattr(ts, "TOPIC_TEXT_TO_SPEECH", "text_to_speech")

    published = []
    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: published.append(kwargs))
    context = SimpleNamespace(offset=10)

    payload = {
        "src_blob": "blob-1",
        "src_lang": "en",
        "dest_lang": "fr",
        "segments": [
            valid_segment(segment_id=1, speaker_id="speaker_0", start=0.0, end=1.0, text="hello"),
            valid_segment(segment_id=2, speaker_id="speaker_1", start=1.0, end=2.0, text="world"),
            valid_segment(segment_id=3, speaker_id="speaker_0", start=2.0, end=3.0, text="again"),
        ],
    }

    ts.handler(payload, context, service)

    assert upsert_calls == [
        {
            "src_blob": "blob-1",
            "segment_id": 1,
            "speaker_id": "speaker_0",
            "start": 0.0,
            "end": 1.0,
        },
        {
            "src_blob": "blob-1",
            "segment_id": 2,
            "speaker_id": "speaker_1",
            "start": 1.0,
            "end": 2.0,
        },
        {
            "src_blob": "blob-1",
            "segment_id": 3,
            "speaker_id": "speaker_0",
            "start": 2.0,
            "end": 3.0,
        },
    ]

    assert published == [
        {
            "topic": "text_to_speech",
            "key": "blob-1:speaker_0",
            "value": {
                "src_blob": "blob-1",
                "src_lang": "en",
                "dest_lang": "fr",
                "speaker_id": "speaker_0",
                "segments": [
                    {
                        "segment_id": 1,
                        "speaker_id": "speaker_0",
                        "start": 0.0,
                        "end": 1.0,
                        "text": "hello-fr",
                    },
                    {
                        "segment_id": 3,
                        "speaker_id": "speaker_0",
                        "start": 2.0,
                        "end": 3.0,
                        "text": "again-fr",
                    },
                ],
            },
        },
        {
            "topic": "text_to_speech",
            "key": "blob-1:speaker_1",
            "value": {
                "src_blob": "blob-1",
                "src_lang": "en",
                "dest_lang": "fr",
                "speaker_id": "speaker_1",
                "segments": [
                    {
                        "segment_id": 2,
                        "speaker_id": "speaker_1",
                        "start": 1.0,
                        "end": 2.0,
                        "text": "world-fr",
                    },
                ],
            },
        },
    ]

    assert tts_total_calls == [
        {"src_blob": "blob-1"},
        {"src_blob": "blob-1"},
    ]
    assert translation_completed_calls == [{"src_blob": "blob-1"}]

    out = capsys.readouterr().out
    assert "Processed 3 segments and published 2 speaker batch(es) for src_blob=blob-1" in out


def test_handler_preserves_existing_order_within_speaker(monkeypatch):
    monkeypatch.setattr(ts, "validate_translate_payload", lambda payload: None)
    monkeypatch.setattr(ts, "translate_segment_text", lambda text, src_lang, dest_lang: text.upper())
    monkeypatch.setattr(ts, "upsert_tts_placeholder", lambda **kwargs: None)
    monkeypatch.setattr(ts, "increment_tts_total_tasks", lambda **kwargs: None)
    monkeypatch.setattr(ts, "increment_translation_completed_tasks", lambda **kwargs: None)
    monkeypatch.setattr(ts, "key_by_src_blob_and_speaker", lambda src_blob, speaker_id: f"{src_blob}:{speaker_id}")
    monkeypatch.setattr(ts, "TOPIC_TEXT_TO_SPEECH", "tts")

    published = []
    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: published.append(kwargs))
    context = SimpleNamespace(offset=11)

    payload = {
        "src_blob": "blob-2",
        "src_lang": "en",
        "dest_lang": "fr",
        "segments": [
            valid_segment(segment_id=10, speaker_id="speaker_0", text="first"),
            valid_segment(segment_id=11, speaker_id="speaker_0", text="second"),
        ],
    }

    ts.handler(payload, context, service)

    assert published[0]["value"]["segments"][0]["segment_id"] == 10
    assert published[0]["value"]["segments"][1]["segment_id"] == 11
    assert published[0]["value"]["segments"][0]["text"] == "FIRST"
    assert published[0]["value"]["segments"][1]["text"] == "SECOND"


def test_build_parser_defaults(monkeypatch):
    monkeypatch.setattr(ts.time, "time", lambda: 1234567890)

    parser = ts.build_parser()
    args = parser.parse_args([])

    assert args.group_id == "translation-1234567890"
    assert args.bootstrap_server is None
    assert args.from_beginning is False


def test_build_parser_from_beginning_flag():
    parser = ts.build_parser()
    args = parser.parse_args(["--from-beginning"])

    assert args.from_beginning is True


def test_main_builds_service_and_runs(monkeypatch):
    class Args:
        group_id = "group-1"
        bootstrap_server = "broker:9092"
        from_beginning = True

    run_calls = []

    class FakeService:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def run(self, handler):
            run_calls.append((self.kwargs, handler))

    monkeypatch.setattr(ts, "build_parser", lambda: SimpleNamespace(parse_args=lambda: Args))
    monkeypatch.setattr(ts, "KafkaMicroservice", FakeService)
    monkeypatch.setattr(ts, "TOPIC_TRANSLATE_SEGMENTS", "translate_segments")

    ts.main()

    assert len(run_calls) == 1
    kwargs, handler = run_calls[0]
    assert kwargs == {
        "service_name": "translation-service",
        "input_topic": "translate_segments",
        "group_id": "group-1",
        "bootstrap_server": "broker:9092",
        "from_beginning": True,
    }
    assert handler is ts.handler