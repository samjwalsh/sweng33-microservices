import pytest
from unittest.mock import patch, MagicMock
from app.model_manager import ModelManager

TEST_CONFIG_PATH = "tests/test_config.yaml"
DUMMY_MODEL_NAME = "translation"

@pytest.fixture
def model_manager(monkeypatch):
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING", # fake azure connection string for tests
        "DefaultEndpointsProtocol=https;AccountName=dummy;AccountKey=dummy;EndpointSuffix=core.windows.net"
    )

    with patch("app.model_manager.BlobServiceClient") as mock_blob:
        mock_container_client = MagicMock()
        mock_blob.from_connection_string.return_value.get_container_client.return_value = mock_container_client

        dummy_blob = MagicMock()
        dummy_blob.name = "dummy_file.bin"
        dummy_blob.size = 10
        mock_container_client.list_blobs.return_value = [dummy_blob]

        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = b"dummy"
        mock_container_client.get_blob_client.return_value = mock_blob_client

        with patch("torch.cuda.is_available", return_value=True):
            yield ModelManager(config_path=TEST_CONFIG_PATH)


@patch("app.model_manager.AutoTokenizer.from_pretrained")
@patch("app.model_manager.AutoModelForSeq2SeqLM.from_pretrained")
def test_load_model_seq2seq(mock_model, mock_tokenizer, model_manager):
    mock_tokenizer.return_value = "tokenizer_obj"
    mock_model.return_value = "model_obj"
    
    result = model_manager.load_model(DUMMY_MODEL_NAME)

    assert model_manager.get_model(DUMMY_MODEL_NAME) == "model_obj"
    assert model_manager.get_tokenizer(DUMMY_MODEL_NAME) == "tokenizer_obj"
    assert result is True

def test_get_device_info(model_manager):
    info = model_manager.get_device_info()
    assert info["device"] in ["cuda", "cpu"]
    assert "gpu_available" in info

def test_missing_connection_string(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(ValueError):
        mm = ModelManager(config_path=TEST_CONFIG_PATH)
        mm._download_model_from_blob(DUMMY_MODEL_NAME)

def test_invalid_model_name(model_manager):
    with pytest.raises(KeyError):
        model_manager.load_model("nonexistent_model")

@pytest.mark.skip(reason="requires real small model in Azure for end-to-end test")
def test_load_real_model():
    from app.model_manager import ModelManager
    mm = ModelManager(config_path="tests/test_real_model_config.yaml")
    mm.load_all_models()
    assert "translation" in mm.models
