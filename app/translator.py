import os
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
import torch
from config.model_config import TRANSLATION_MODELS, LANGUAGE_CODES

def load_translator(model_name):
    hf_token = os.getenv("HF_TOKEN")
    
    if not hf_token:
        raise ValueError("Hugging Face token not found! Set HF_TOKEN as an environment variable.")
    
    model = AutoModelForSeq2SeqLM.from_pretrained(TRANSLATION_MODELS[model_name], torch_dtype="auto", device_map="auto")
    tokenizer = AutoTokenizer.from_pretrained(TRANSLATION_MODELS[model_name])
    return model, tokenizer

def translate_text(model, tokenizer, text, language_from, language_to):
    tokenizer.src_lang = LANGUAGE_CODES[language_from]
    inputs = tokenizer(text, return_tensors="pt").to(model.device)
    forced_bos_token_id = tokenizer.convert_tokens_to_ids(LANGUAGE_CODES[language_to])

    with torch.no_grad():
        gen_tokens = model.generate(**inputs, forced_bos_token_id=forced_bos_token_id)

    return tokenizer.batch_decode(gen_tokens, skip_special_tokens=True)

# model, tokenizer = load_translator("tiny")
# print(translate_text(model, tokenizer, "good morning", "English", "French"))