from fastapi import APIRouter, HTTPException, Request
import time
import traceback

from app.schemas import (
    TranslationRequest,
    TranslationResponse,
    BatchTranslationRequest,
    BatchTranslationResponse
)
from app.translator import translate_text
from config.model_config import LANGUAGE_CODES

router = APIRouter()

@router.post("/", response_model=TranslationResponse)
def translate(request: Request, translation_request: TranslationRequest):
    model_manager = request.app.state.model_manager
    
    if translation_request.language_from not in LANGUAGE_CODES:
        raise HTTPException(
            status_code=400,
            detail={
                'error': f'Unsupported source language: {translation_request.language_from}',
                'supported_languages': list(LANGUAGE_CODES.keys())
            }
        )
    
    if translation_request.language_to not in LANGUAGE_CODES:
        raise HTTPException(
            status_code=400,
            detail={
                'error': f'Unsupported target language: {translation_request.language_to}',
                'supported_languages': list(LANGUAGE_CODES.keys())
            }
        )
    
    if translation_request.language_from == translation_request.language_to:
        raise HTTPException(
            status_code=400,
            detail={
                'error': f'Source and target languages cannot be the same: {translation_request.language_from}'
            }
        )
    
    
    model = model_manager.get_model("translation")
    tokenizer = model_manager.get_tokenizer("translation")

    if not model or not tokenizer:
        raise HTTPException(
            status_code=500,
            detail={'error': 'Translation model not loaded'}
        )
    
    try:
        start_time = time.time()
        
        result = translate_text(
            model, 
            tokenizer, 
            translation_request.text, 
            translation_request.language_from, 
            translation_request.language_to
        )
        
        inference_time = time.time() - start_time

        return {
            'translation': result[0] if isinstance(result, list) else result, # return first from list of translations if list
            'language_from': translation_request.language_from,
            'language_to': translation_request.language_to,
            'original_text': translation_request.text,
            'inference_time_ms': round(inference_time * 1000, 2),
            'device': model_manager.device
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail={'error': f'Translation failed: {str(e)}'}
        )

@router.post("/batch", response_model=BatchTranslationResponse)
def translate_batch(request: Request, batch_request: BatchTranslationRequest):
    model_manager = request.app.state.model_manager
    
    if batch_request.language_from not in LANGUAGE_CODES or batch_request.language_to not in LANGUAGE_CODES:
        raise HTTPException(
            status_code=400,
            detail={'error': 'Unsupported language'}
        )
    
    model = model_manager.get_model("translation")
    tokenizer = model_manager.get_tokenizer("translation")
    
    if not model or not tokenizer:
        raise HTTPException(
            status_code=500,
            detail={'error': 'Translation model not loaded'}
        )
    
    try:
        start_time = time.time()
        
        translations = []
        for text in batch_request.texts:
            result = translate_text(
                model, 
                tokenizer, 
                text, 
                batch_request.language_from, 
                batch_request.language_to
            )
            translations.append(result[0] if isinstance(result, list) else result)
        
        total_time = time.time() - start_time
        avg_time = total_time / len(batch_request.texts)
        
        return {
            'translations': translations,
            'originals': batch_request.texts,
            'count': len(translations),
            'language_from': batch_request.language_from,
            'language_to': batch_request.language_to,
            'total_time_ms': round(total_time * 1000, 2),
            'avg_time_per_text_ms': round(avg_time * 1000, 2),
            'device': model_manager.device
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail={'error': f'Batch translation failed: {str(e)}'}
        )