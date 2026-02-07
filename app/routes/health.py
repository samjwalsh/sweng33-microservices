from fastapi import APIRouter, Request
from app.schemas import HealthResponse, LanguagesResponse
from config.model_config import LANGUAGE_CODES

router = APIRouter()

@router.get("/health", response_model=HealthResponse)
def health(request: Request):
    model_manager = request.app.state.model_manager
    device_info = model_manager.get_device_info()
    loaded_models = list(model_manager.models.keys())
    
    return {
        "status": "healthy",
        "loaded_models": loaded_models,
        "total_models": len(loaded_models),
        "device": device_info
    }

@router.get("/device")
def device_info(request: Request):
    model_manager = request.app.state.model_manager
    return model_manager.get_device_info()

@router.get("/models")
def list_models(request: Request):
    model_manager = request.app.state.model_manager
    models_info = {}
    for model_name in model_manager.config['models'].keys():
        models_info[model_name] = {
            'loaded': model_name in model_manager.models,
            'type': model_manager.config['models'][model_name]['model_type']
        }
    return models_info

@router.get("/languages", response_model=LanguagesResponse)
def list_languages():
    return {
        "supported_languages": list(LANGUAGE_CODES.keys()),
        "total": len(LANGUAGE_CODES)
    }