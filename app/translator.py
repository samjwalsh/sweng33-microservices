import torch
from config.model_config import LANGUAGE_CODES

def translate_text(model, tokenizer, text, language_from, language_to):
    """
    Translate text using the provided model and tokenizer
    
    Args:
        model: Pre-loaded translation model from ModelManager
        tokenizer: Pre-loaded tokenizer from ModelManager
        text: Text to translate
        language_from: Source language name (e.g., "English")
        language_to: Target language name (e.g., "French")
    
    Returns:
        List of translated strings
    """
    if model is None or tokenizer is None:
        raise ValueError("Model and tokenizer must be provided")
    
    # Set source language
    tokenizer.src_lang = LANGUAGE_CODES[language_from]
    
    # Tokenize input
    inputs = tokenizer(text, return_tensors="pt").to(model.device)
    
    # Get target language token ID
    forced_bos_token_id = tokenizer.convert_tokens_to_ids(LANGUAGE_CODES[language_to])

    # Generate translation
    with torch.no_grad():
        gen_tokens = model.generate(
            **inputs, 
            forced_bos_token_id=forced_bos_token_id,
            max_length=512
        )

    # Decode and return
    return tokenizer.batch_decode(gen_tokens, skip_special_tokens=True)