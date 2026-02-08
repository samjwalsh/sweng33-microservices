import pytest
from fastapi.testclient import TestClient
from fastapi import FastAPI

from app.routes import health
from config.model_config import LANGUAGE_CODES

@pytest.fixture
def mock_model_manager():
    from unittest.mock import Mock
    manager = Mock()
    manager.models = {"translation": Mock(), "asr": Mock()}
    manager.config = {
        'models': {
            'translation': {'model_type': 'seq2seq'},
            'asr': {'model_type': 'asr'},
            'tts': {'model_type': 'text-to-speech'}
        }
    }
    manager.get_device_info.return_value = {
        "device": "cuda",
        "gpu_available": True,
        "gpu_name": "NVIDIA A100"
    }
    return manager

@pytest.fixture
def client(mock_model_manager):
    app = FastAPI()
    app.state.model_manager = mock_model_manager
    app.include_router(health.router)
    return TestClient(app)

class TestHealthEndpoints:
    def test_health_endpoint(self, client):
        response = client.get("/health")
        data = response.json()
        
        assert response.status_code == 200
        assert data["status"] == "healthy"
        assert data["total_models"] == 2
        assert "translation" in data["loaded_models"]
        assert "asr" in data["loaded_models"]
        assert "device" in data
    
    def test_device_endpoint(self, client):
        response = client.get("/device")
        data = response.json()
        
        assert response.status_code == 200
        assert data["device"] == "cuda"
        assert data["gpu_available"] is True
    
    def test_models_endpoint(self, client):
        response = client.get("/models")
        data = response.json()
        
        assert response.status_code == 200
        assert data["translation"]["loaded"] is True
        assert data["asr"]["loaded"] is True
        assert data["tts"]["loaded"] is False  # Not in manager.models
    
    def test_languages_endpoint(self, client):
        response = client.get("/languages")
        data = response.json()
        
        assert response.status_code == 200
        assert data["total"] == len(LANGUAGE_CODES)
        assert len(data["supported_languages"]) == data["total"]