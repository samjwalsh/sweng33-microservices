import os
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
import torch

MODEL_ORIGINAL = "facebook/nllb-200-3.3B"
MODEL_GREEN = "facebook/nllb-200-distilled-1.3B" # distilled means compressed - less processing power = green computing
MODEL_TINY = "facebook/nllb-200-distilled-600M" # using this smaller model for testing to save compute

def load_translator(model_name):
    hf_token = os.getenv("HF_TOKEN")
    
    if not hf_token:
        raise ValueError("Hugging Face token not found! Set HF_TOKEN as an environment variable.")
    
    model = AutoModelForSeq2SeqLM.from_pretrained(model_name, torch_dtype="auto", device_map="auto")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    return model, tokenizer

def translate_text(model, tokenizer, text, language_from, language_to):
    tokenizer.src_lang = language_from  
    inputs = tokenizer(text, return_tensors="pt").to(model.device)
    forced_bos_token_id = tokenizer.convert_tokens_to_ids(language_to)

    with torch.no_grad():
        gen_tokens = model.generate(**inputs, forced_bos_token_id=forced_bos_token_id)

    return tokenizer.batch_decode(gen_tokens, skip_special_tokens=True)

# model, tokenizer = load_translator(MODEL_TINY)
# print(translate_text(model, tokenizer, "good morning", "eng_Latn", "fra_Latn"))