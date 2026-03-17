#!/usr/bin/env python3
from dotenv import load_dotenv
import os
import sys

try:
    from azure.ai.translation.text import TextTranslationClient, TranslatorCredential
except Exception as e:
    print("Missing azure translation package. Install with: pip install azure-ai-translation-text")
    raise

load_dotenv()
endpoint = os.getenv("TRANSLATOR_ENDPOINT")
key = os.getenv("TRANSLATOR_KEY")
region = os.getenv("TRANSLATOR_REGION")

if not (endpoint and key and region):
    print("TRANSLATOR_ENDPOINT, TRANSLATOR_KEY, and TRANSLATOR_REGION must be set in the environment or .env")
    sys.exit(2)

cred = TranslatorCredential(key, region)
client = TextTranslationClient(endpoint=endpoint, credential=cred)

print("Fetching supported languages from Azure Translator...")
langs = client.get_languages()
translation = langs.translation or {}
print(f"Found {len(translation)} translation languages:\n")

for code, info in sorted(translation.items()):
    name = getattr(info, "name", "")
    native = getattr(info, "native_name", "")
    script = getattr(info, "script", None)
    direction = getattr(info, "dir", None)
    extras = []
    if script:
        extras.append(f"script={script}")
    if direction:
        extras.append(f"dir={direction}")
    extras_str = (" (" + ", ".join(extras) + ")") if extras else ""
    print(f"{code}\t{name}\t{native}{extras_str}")

print("\nDone.")
