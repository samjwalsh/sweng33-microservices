from unittest.mock import patch, MagicMock
from src.translator_sdk import translate_text
from azure.core.exceptions import HttpResponseError

def test_translate_text_success():
    mock_translation = MagicMock()
    mock_translation.text = "Hola"
    mock_item = MagicMock()
    mock_item.translations = [mock_translation]
    mock_response = [mock_item]

    with patch("app.translator_sdk.TextTranslationClient") as MockClient:
        instance = MockClient.return_value
        instance.translate.return_value = mock_response

        result = translate_text("en", "es", "Hello")
        
        assert result == "Hola"
        instance.translate.assert_called_once()

def test_translate_text_empty_response():
    with patch("app.translator_sdk.TextTranslationClient") as MockClient:
        instance = MockClient.return_value
        instance.translate.return_value = None

        result = translate_text("en", "es", "Hello")
        assert result is None

def test_translate_text_handles_http_error():
    with patch("app.translator_sdk.TextTranslationClient") as MockClient:
        instance = MockClient.return_value
        
        mock_response = MagicMock()
        mock_response.status_code = 400
        
        error = HttpResponseError(response=mock_response)
        error.error = MagicMock()
        error.error.code = "InvalidRequest"
        error.error.message = "Invalid translation request"
        
        instance.translate.side_effect = error

        result = translate_text("en", "es", "Hello")
        assert result is None