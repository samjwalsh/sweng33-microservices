from app.language_identifier import detect_language


class FakeClientOK:
    def detect_language(self, documents):
        class FakeDoc:
            is_error = False
            primary_language = type(
                "Lang", (),
                {"iso6391_name": "en", "confidence_score": 0.99}
            )
        return [FakeDoc()]


class FakeClientError:
    def detect_language(self, documents):
        class FakeErr:
            message = "Bad request"
        class FakeDoc:
            is_error = True
            error = FakeErr()
        return [FakeDoc()]


def test_detect_language_ok():
    result = detect_language("Hello there", client=FakeClientOK())
    assert result.language == "en"
    assert result.probability_pct == 99.0


def test_detect_language_empty_text():
    try:
        detect_language("   ", client=FakeClientOK())
        assert False, "Expected ValueError"
    except ValueError:
        assert True


def test_detect_language_azure_error():
    try:
        detect_language("Hello", client=FakeClientError())
        assert False, "Expected RuntimeError"
    except RuntimeError as e:
        assert "Bad request" in str(e)