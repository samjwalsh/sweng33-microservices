"""
This translation model is for use in the MVP only.
After the MVP presentation, we can host the models on GPU and use a hugging face model
"""

import os
from dotenv import load_dotenv

from azure.ai.translation.text import TextTranslationClient, TranslatorCredential
from azure.ai.translation.text.models import InputTextItem
from azure.core.exceptions import HttpResponseError

load_dotenv()
key = os.getenv("AZURE_FOUNDRY_KEY")
endpoint = os.getenv("AZURE_FOUNDRY_ENDPOINT")
region = os.getenv("AZURE_FOUNDRY_REGION")

def translate_text(source_language, target_language, text):
    credential = TranslatorCredential(key, region)
    text_translator = TextTranslationClient(endpoint=endpoint, credential=credential)
    input_text_element = [InputTextItem(text=text)]
    target_language = [target_language] # azure uses a list of translated languages but this is overcomplicated so I just changed the input to a list

    try:
        response = text_translator.translate(
            content = input_text_element, 
            to = target_language, 
            from_parameter = source_language
        )
        
        translation = response if response else None

        if translation:
            for item in response:
                for translated_text in item.translations:
                    return translated_text.text

    except HttpResponseError as exception:
        print(f"Error Code: {exception.error.code}")
        print(f"Message: {exception.error.message}")

translated = translate_text("en", "es", "Hello")
print(translated)
