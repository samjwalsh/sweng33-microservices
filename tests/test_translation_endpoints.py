import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
from fastapi import FastAPI

from app.routes import translation

@pytest.fixture
def mock_model():
    model = Mock()
    model.device = "cuda"
    model.generate = Mock(return_value=Mock())
    return model

@pytest.fixture
def mock_tokenizer():
    tokenizer = Mock()
    tokenizer.src_lang = None
    tokenizer.return_value = Mock(to=Mock(return_value=Mock()))
    tokenizer.convert_tokens_to_ids = Mock(return_value=12345)
    tokenizer.batch_decode = Mock(return_value=["Bonjour, comment allez-vous?"])
    return tokenizer

@pytest.fixture
def mock_model_manager(mock_model, mock_tokenizer):
    manager = Mock()
    manager.device = "cuda"
    manager.get_model = Mock(return_value=mock_model)
    manager.get_tokenizer = Mock(return_value=mock_tokenizer)
    return manager

@pytest.fixture
def app(mock_model_manager):
    test_app = FastAPI()
    test_app.state.model_manager = mock_model_manager
    test_app.include_router(translation.router, prefix="/translate")
    return test_app

@pytest.fixture
def client(app):
    return TestClient(app)

class TestTranslateEndpoint:
    def test_translate_success(self, client):
        response = client.post(
            "/translate/",
            json={
                "text": "Hello, how are you?",
                "language_from": "English",
                "language_to": "French"
            }
        )
        data = response.json()
        
        assert response.status_code == 200
        assert data["translation"] == "Bonjour, comment allez-vous?"
        assert data["language_from"] == "English"
        assert data["language_to"] == "French"
        assert data["original_text"] == "Hello, how are you?"
        assert data["device"] == "cuda"
        assert isinstance(data["inference_time_ms"], float)
    
    def test_translate_uses_defaults(self, client):
        response = client.post("/translate/", json={"text": "Hello"})
        data = response.json()
        
        assert response.status_code == 200
        assert data["language_from"] == "English"
        assert data["language_to"] == "French"
    
    def test_translate_invalid_language(self, client):
        response = client.post(
            "/translate/",
            json={
                "text": "Hello",
                "language_from": "Irish", # Irish isn't on the list
                "language_to": "French"
            }
        )
        
        assert response.status_code == 400
        assert "Irish" in response.json()["detail"]["error"]
    
    def test_translate_model_not_loaded(self, app):
        app.state.model_manager.get_model = Mock(return_value=None)
        client = TestClient(app)
        
        response = client.post(
            "/translate/",
            json={"text": "Hello", "language_from": "English", "language_to": "French"}
        )
        
        assert response.status_code == 500
        assert "Translation model not loaded" in response.json()["detail"]["error"]

class TestBatchTranslateEndpoint:
    def test_batch_translate_success(self, client):
        response = client.post(
            "/translate/batch",
            json={
                "texts": ["Hello", "Goodbye", "Thank you"],
                "language_from": "English",
                "language_to": "French"
            }
        )
        data = response.json()
        
        assert response.status_code == 200
        assert data["count"] == 3
        assert len(data["translations"]) == 3
        assert len(data["originals"]) == 3
        assert data["originals"] == ["Hello", "Goodbye", "Thank you"]
        assert isinstance(data["total_time_ms"], float)
        assert isinstance(data["avg_time_per_text_ms"], float)
    
    def test_batch_translate_uses_defaults(self, client):
        response = client.post("/translate/batch", json={"texts": ["Hello"]})
        data = response.json()
        
        assert response.status_code == 200
        assert data["language_from"] == "English"
        assert data["language_to"] == "English"
    
    def test_batch_translate_empty_list(self, client):
        response = client.post(
            "/translate/batch",
            json={"texts": [], "language_from": "English", "language_to": "French"}
        )
        assert response.status_code == 422  
    
    def test_batch_translate_invalid_language(self, client):
        response = client.post(
            "/translate/batch",
            json={"texts": ["Hello"], "language_from": "Irish", "language_to": "English"}
        )
        
        assert response.status_code == 400
        assert "Unsupported language" in response.json()["detail"]["error"]
    
    @patch('app.routes.translation.translate_text')
    def test_batch_translate_handles_error(self, mock_translate, client):
        mock_translate.side_effect = Exception("Translation failed")
        
        response = client.post(
            "/translate/batch",
            json={"texts": ["Hello"], "language_from": "English", "language_to": "French"}
        )
        
        assert response.status_code == 500
        assert "Batch translation failed" in response.json()["detail"]["error"]