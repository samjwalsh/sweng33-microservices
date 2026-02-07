from pydantic import BaseModel, Field
from typing import List

class TranslationRequest(BaseModel):
    text: str = Field(...)
    language_from: str = Field(default="English") # We will have to change one at runtime but this is fine if we get the language identifier
    language_to: str = Field(default="English")

class BatchTranslationRequest(BaseModel):
    texts: List[str] = Field(..., min_items=1)
    language_from: str = Field(default="English")
    language_to: str = Field(default="English")

class TranslationResponse(BaseModel):
    translation: str
    language_from: str
    language_to: str
    original_text: str
    inference_time_ms: float
    device: str

class BatchTranslationResponse(BaseModel):
    translations: List[str]
    originals: List[str]
    count: int
    language_from: str
    language_to: str
    total_time_ms: float
    avg_time_per_text_ms: float
    device: str

class HealthResponse(BaseModel):
    status: str
    loaded_models: List[str]
    total_models: int
    device: dict

class ModelInfo(BaseModel):
    loaded: bool
    type: str

class LanguagesResponse(BaseModel):
    supported_languages: List[str]
    total: int