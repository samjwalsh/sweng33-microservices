from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from app.model_manager import ModelManager
from app.routes import translation, health

app = FastAPI(
    title="Translation API",
    description="Machine translation service with multi-language support",
    version="1.0.0"
)

try:
    model_manager = ModelManager()
    model_manager.load_all_models()
except Exception as e:
    raise RuntimeError(f"ERROR: Could not initialize server: {str(e)}") from e

app.state.model_manager = model_manager

app.include_router(health.router, tags=["Health & Info"])
app.include_router(translation.router, prefix="/translate", tags=["Translation"])

"""
Run with:
uvicorn app.main:app --host 0.0.0.0 --port 5000 --reload
"""