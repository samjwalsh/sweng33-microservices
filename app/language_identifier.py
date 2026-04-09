import os
from dataclasses import dataclass
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient


@dataclass
class LanguagePrediction:
    language: str
    confidence: float
    probability_pct: float


def _get_client() -> TextAnalyticsClient:
    endpoint = os.getenv("AZURE_LANGUAGE_ENDPOINT")
    key = os.getenv("AZURE_LANGUAGE_KEY")

    if not endpoint or not key:
        raise RuntimeError(
            "Azure Language credentials not configured. "
            "Set AZURE_LANGUAGE_ENDPOINT and AZURE_LANGUAGE_KEY."
        )

    return TextAnalyticsClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(key),
    )


def detect_language(text: str, *, client: TextAnalyticsClient | None = None) -> LanguagePrediction:
    if not text or not text.strip():
        raise ValueError("Text must not be empty.")

    client = client or _get_client()

    response = client.detect_language(documents=[text])
    doc = response[0]

    if getattr(doc, "is_error", False):
        raise RuntimeError(doc.error.message)

    lang = doc.primary_language.iso6391_name
    confidence = doc.primary_language.confidence_score

    return LanguagePrediction(
        language=lang,
        confidence=confidence,
        probability_pct=round(confidence * 100, 2),
    )