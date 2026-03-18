FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# Runtime binaries needed by the pipeline workers.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    rubberband-cli \
    git \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

# Override command per Dokploy app.
CMD ["python", "-m", "kafka_pipeline.microservices.translation_service", "--group-id", "translation-v1"]
