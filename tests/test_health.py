import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock
from fastapi import FastAPI

from app.routes import health
from config.model_config import LANGUAGE_CODES


@pytest.fixture
def mock_model_manager():
    manager = Mock()
    manager.models = {
        "translation": Mock(),
        "asr": Mock()
    }
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
        "gpu_name": "NVIDIA A100",
        "gpu_memory_total_gb": 40.0,
        "gpu_memory_allocated_gb": 8.5,
        "gpu_memory_reserved_gb": 10.0
    }
    return manager


@pytest.fixture
def app(mock_model_manager):
    test_app = FastAPI()
    test_app.state.model_manager = mock_model_manager
    test_app.include_router(health.router)
    return test_app


@pytest.fixture
def client(app):
    return TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200
    
    def test_health_returns_correct_structure(self, client):
        response = client.get("/health")
        data = response.json()
        
        assert "status" in data
        assert "loaded_models" in data
        assert "total_models" in data
        assert "device" in data
    
    def test_health_status_is_healthy(self, client):
        response = client.get("/health")
        data = response.json()
        
        assert data["status"] == "healthy"
    
    def test_health_loaded_models_count(self, client):
        response = client.get("/health")
        data = response.json()
        
        assert len(data["loaded_models"]) == data["total_models"]
        assert data["total_models"] == 2
    
    def test_health_loaded_models_list(self, client):
        response = client.get("/health")
        data = response.json()
        
        assert "translation" in data["loaded_models"]
        assert "asr" in data["loaded_models"]


class TestDeviceEndpoint:
    def test_device_returns_200(self, client):
        response = client.get("/device")
        assert response.status_code == 200
    
    def test_device_returns_gpu_info(self, client):
        response = client.get("/device")
        data = response.json()
        
        assert data["device"] == "cuda"
        assert data["gpu_available"] is True


class TestModelsEndpoint:
    def test_models_returns_200(self, client):
        response = client.get("/models")
        assert response.status_code == 200
    
    def test_models_lists_all_configured_models(self, client):
        response = client.get("/models")
        data = response.json()
        
        assert "translation" in data
        assert "asr" in data
        assert "tts" in data
    
    def test_models_shows_loaded_status(self, client):
        response = client.get("/models")
        data = response.json()
        
        assert data["translation"]["loaded"] is True
        assert data["asr"]["loaded"] is True
        assert data["tts"]["loaded"] is False


class TestLanguagesEndpoint:
    def test_languages_returns_200(self, client):
        response = client.get("/languages")
        assert response.status_code == 200
    
    def test_languages_returns_correct_structure(self, client):
        response = client.get("/languages")
        data = response.json()
        
        assert "supported_languages" in data
        assert "total" in data
    
    def test_languages_total_matches_list_length(self, client):
        response = client.get("/languages")
        data = response.json()
        
        assert data["total"] == len(data["supported_languages"])
        assert data["total"] == len(LANGUAGE_CODES)