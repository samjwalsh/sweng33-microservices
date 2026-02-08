import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock
from fastapi import FastAPI

from app.routes import translation

@pytest.fixture
def mock_model_manager():
    manager = Mock()
    manager.device = "cuda"
    
    model = Mock()
    model.device = "cuda"
    model.generate = Mock(return_value=Mock())
    
    tokenizer = Mock()
    tokenizer.src_lang = None
    tokenizer.return_value = Mock(to=Mock(return_value=Mock()))
    tokenizer.convert_tokens_to_ids = Mock(return_value=12345)
    tokenizer.batch_decode = Mock(return_value=["Bonjour"])
    
    manager.get_model = Mock(return_value=model)
    manager.get_tokenizer = Mock(return_value=tokenizer)
    return manager

@pytest.fixture
def client(mock_model_manager):
    app = FastAPI()
    app.state.model_manager = mock_model_manager
    app.include_router(translation.router, prefix="/translate")
    return TestClient(app)

class TestTranslationEndpoints:
    def test_single_translation(self, client):
        response = client.post(
            "/translate/",
            json={
                "text": "Hello",
                "language_from": "English",
                "language_to": "French"
            }
        )
        data = response.json()
        
        assert response.status_code == 200
        assert data["translation"] == "Bonjour"
        assert data["language_from"] == "English"
        assert data["language_to"] == "French"
        assert isinstance(data["inference_time_ms"], float)
    
    def test_single_translation_errors(self, client):
        response = client.post(
            "/translate/",
            json={"text": "Hello", "language_from": "InvalidLang", "language_to": "French"}
        )
        assert response.status_code == 400
        
        client.app.state.model_manager.get_model = Mock(return_value=None)
        response = client.post(
            "/translate/",
            json={"text": "Hello", "language_from": "English", "language_to": "French"}
        )
        assert response.status_code == 500
    
    def test_batch_translation(self, client):
        response = client.post(
            "/translate/batch",
            json={
                "texts": ["Hello", "Goodbye"],
                "language_from": "English",
                "language_to": "French"
            }
        )
        data = response.json()
        
        assert response.status_code == 200
        assert data["count"] == 2
        assert len(data["translations"]) == 2
        assert isinstance(data["total_time_ms"], float)
    
    def test_batch_translation_errors(self, client):
        response = client.post("/translate/batch", json={"texts": []})
        assert response.status_code == 422
        
        response = client.post(
            "/translate/batch",
            json={"texts": ["Hello"], "language_from": "InvalidLang", "language_to": "French"}
        )
        assert response.status_code == 400