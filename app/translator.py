import torch
from config.model_config import LANGUAGE_CODES

def translate_text(model, tokenizer, text, language_from, language_to):
    if model is None or tokenizer is None:
        raise ValueError("Model and tokenizer must be provided")
    
    tokenizer.src_lang = LANGUAGE_CODES[language_from]
    
    inputs = tokenizer(text, return_tensors="pt").to(model.device)
    
    forced_bos_token_id = tokenizer.convert_tokens_to_ids(LANGUAGE_CODES[language_to])

    with torch.no_grad():
        gen_tokens = model.generate(
            **inputs, 
            forced_bos_token_id=forced_bos_token_id,
            max_length=512
        )

    return tokenizer.batch_decode(gen_tokens, skip_special_tokens=True)