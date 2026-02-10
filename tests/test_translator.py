import pytest
import torch
from unittest.mock import Mock

from app.translator import translate_text
from config.model_config import LANGUAGE_CODES


@pytest.fixture
def mock_model():
    model = Mock()
    model.device = "cuda"
    model.generate = Mock(return_value=torch.tensor([[1, 2, 3, 4, 5]]))
    return model


@pytest.fixture
def mock_tokenizer():
    tokenizer = Mock()
    mock_inputs = Mock()
    mock_inputs.to = Mock(return_value=mock_inputs)
    tokenizer.return_value = mock_inputs
    tokenizer.convert_tokens_to_ids = Mock(return_value=250004)
    tokenizer.batch_decode = Mock(return_value=["Bonjour"])
    return tokenizer

class TestTranslateText:
    def test_basic_translation(self, mock_model, mock_tokenizer):
        result = translate_text(
            mock_model,
            mock_tokenizer,
            "Hello",
            "English",
            "French"
        )
        
        assert isinstance(result, list)
        assert result == ["Bonjour"]
        
        assert mock_tokenizer.src_lang == LANGUAGE_CODES["English"]
        
        mock_tokenizer.assert_called_once_with("Hello", return_tensors="pt")
        
        call_kwargs = mock_model.generate.call_args.kwargs
        assert call_kwargs["forced_bos_token_id"] == 250004
        assert call_kwargs["max_length"] == 512
    
    def test_validates_inputs(self, mock_model, mock_tokenizer):
        with pytest.raises(ValueError, match="Model and tokenizer must be provided"):
            translate_text(None, mock_tokenizer, "Hello", "English", "French")
        
        with pytest.raises(ValueError, match="Model and tokenizer must be provided"):
            translate_text(mock_model, None, "Hello", "English", "French")
        
        with pytest.raises(KeyError):
            translate_text(mock_model, mock_tokenizer, "Hello", "InvalidLang", "French")