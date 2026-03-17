#!/usr/bin/env python3
from dotenv import load_dotenv
import os
import sys
import json
import argparse

try:
    from azure.ai.translation.text import TextTranslationClient, TranslatorCredential
except Exception:
    print("Missing azure translation package. Install with: pip install azure-ai-translation-text")
    raise


def main() -> None:
    parser = argparse.ArgumentParser(description="List supported Azure Translator languages")
    parser.add_argument("--out", help="Write JSON output to this file (optional)")
    args = parser.parse_args()

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

    out_map = {}
    for code, info in sorted(translation.items()):
        name = getattr(info, "name", "")
        native = getattr(info, "native_name", "")
        script = getattr(info, "script", None)
        direction = getattr(info, "dir", None)
        out_map[code] = {
            "name": name,
            "native_name": native,
            "script": script,
            "dir": direction,
        }
        extras = []
        if script:
            extras.append(f"script={script}")
        if direction:
            extras.append(f"dir={direction}")
        extras_str = (" (" + ", ".join(extras) + ")") if extras else ""
        print(f"{code}\t{name}\t{native}{extras_str}")

    if args.out:
        out_dir = os.path.dirname(args.out)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(out_map, fh, ensure_ascii=False, indent=2)
        print(f"\nWrote {len(out_map)} languages to {args.out}")

    print("\nDone.")


if __name__ == "__main__":
    main()
