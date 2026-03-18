You should see messages printed by the consumer.

## Reusable scaffolds for MVP services

### 1) Send a message to a queue/topic

Use `kafka_pipeline/pipeline_producer.py` and call `send_kafka_message`:

```python
from pipeline_producer import send_kafka_message

metadata = send_kafka_message(
	topic="translate_segments",
	key="my-src-blob",
	value={
		"src_blob": "my-src-blob",
		"src_lang": "en",
		"dest_lang": "fr",
		"segments": [],
	},
)
print(metadata)
```

### 2) Create a microservice consumer quickly

Use `kafka_pipeline/microservice_template.py` as a wrapper/template. It gives you:

- a configured `KafkaConsumer`
- a configured `KafkaProducer`
- graceful shutdown handling
- a `run(handler)` loop with parsed payload and message context

Run the template service:

```bash
python kafka_pipeline/microservice_template.py --input-topic text_to_speech --group-id tts-worker
```

Replace `example_handler` with your real business logic and publish downstream events via `service.publish(...)`.

### Other useful MVP scaffolds

- A `topics.py` constants file for topic names and message key rules.
- A tiny payload validator (required fields + type checks) per topic.
- A DB helper that upserts segment rows by `src_blob + segment_id`.

## Added helper modules

### `kafka_pipeline/topics.py`

- Topic constants:
  - `TOPIC_INGEST`
  - `TOPIC_TRANSLATE_SEGMENTS`
  - `TOPIC_TEXT_TO_SPEECH`
  - `TOPIC_RECONSTRUCT_VIDEO`
- Key helpers:
  - `key_by_src_blob(src_blob)`
  - `key_by_src_blob_and_speaker(src_blob, speaker_id)`

### `kafka_pipeline/db_helper.py`

Postgres helper for the `tts` table using `DATABASE_URL` from `.env`.

Functions:

- `upsert_tts_placeholder(...)`
- `set_tts_generated_blob(...)`
- `are_all_segments_generated(src_blob)`
- `get_segments_for_src_blob(src_blob)`

## Pipeline workers (initial implementation)

Implemented workers:

- `kafka_pipeline/microservices/diarization_service.py`
- `kafka_pipeline/microservices/translation_service.py`
- `kafka_pipeline/microservices/tts_service.py`
- `kafka_pipeline/microservices/reconstruction_service.py`

Shared support:

- `kafka_pipeline/payload_validation.py`
- `kafka_pipeline/topics.py`

### Run order (separate terminals)

```bash
python -m kafka_pipeline.microservices.diarization_service --group-id diarization-v1
python -m kafka_pipeline.microservices.translation_service --group-id translation-v1
python -m kafka_pipeline.microservices.tts_service --group-id tts-v1
python -m kafka_pipeline.microservices.reconstruction_service --group-id reconstruct-v1
```

### Send an ingest event

You can use your web server, or publish manually:

```python
from pipeline_producer import send_kafka_message
from topics import TOPIC_INGEST, key_by_src_blob

payload = {
	"src_blob": "video-001",
	"src_lang": None,
	"dest_lang": "fr"
}

send_kafka_message(
	topic=TOPIC_INGEST,
	key=key_by_src_blob(payload["src_blob"]),
	value=payload,
)
```

### Create test jobs for all microservice queues

Use `kafka_pipeline/microservices_job_creator.py` to publish valid test jobs to one queue or all queues.

Publish to all queues:

```bash
python kafka_pipeline/microservices_job_creator.py --target all
```

Publish only to one queue (examples):

```bash
python kafka_pipeline/microservices_job_creator.py --target ingest
python kafka_pipeline/microservices_job_creator.py --target translate
python kafka_pipeline/microservices_job_creator.py --target tts --tts-speaker-id speaker_0
python kafka_pipeline/microservices_job_creator.py --target reconstruct
```

Useful options:

- `--src-blob video-001`
- `--src-lang en --dest-lang fr`
- `--speakers 2 --segments-per-speaker 3`
- `--bootstrap-server kafka.samjw.xyz:9092`

## Notes on current MVP implementation

- Diarization service has explicit integration hooks for language detection and diarization/transcription.
- Translation tries Azure translator first and falls back to passthrough text if unavailable.
- TTS currently writes generated segment placeholders under `data/generated_segments/` and updates DB.
- Reconstruction currently validates readiness and logs a TODO for time-stretch/compress + mux.

## Dokploy deployment plan (4 workers)

Use one shared Docker image and create 4 Dokploy services/apps with different start commands.

### Service-specific Dockerfiles (alternative)

If you prefer picking a Dockerfile per app in Dokploy, use:

- `Dockerfile.diarization` -> diarization worker
- `Dockerfile.translation` -> translation worker
- `Dockerfile.tts` -> TTS worker
- `Dockerfile.reconstruction` -> reconstruction worker

Each file already has the correct default `CMD` for its microservice.

### Step 1: Build from repo root

- Dockerfile: `Dockerfile`
- Env template: `.env.example`
- Runtime deps included: ffmpeg, ffprobe (via ffmpeg package), rubberband

### Step 2: Create shared environment in Dokploy

Set these variables in Dokploy (copy from `.env.example`):

- `KAFKA_BOOTSTRAP_SERVER`
- `DATABASE_URL`
- `AZURE_STORAGE_CONTAINER`
- `AZURE_STORAGE_CONNECTION_STRING` (or `AZURE_STORAGE_ACCOUNT` + `AZURE_STORAGE_KEY`)
- `TRANSLATOR_KEY`, `TRANSLATOR_ENDPOINT`, `TRANSLATOR_REGION`
- `ELEVENLABS_API_KEY`
- one of `MY_TOKEN` / `HF_TOKEN` / `HUGGINGFACE_HUB_TOKEN`

Optional tuning vars:

- `LANGID_WHISPER_MODEL`
- `TRANSCRIBER_MODEL`
- `TRANSCRIBER_CHUNK_LENGTH_S`
- `TRANSCRIBER_STRIDE_LENGTH_S`
- `DIARIZATION_DEVICE`
- `DIARIZATION_NUM_THREADS`
- `DIARIZATION_MODEL`

### Step 3: Create 4 Dokploy worker apps

Each app uses the same image/build context, with one command:

```bash
python -m kafka_pipeline.microservices.diarization_service --group-id diarization-v1
python -m kafka_pipeline.microservices.translation_service --group-id translation-v1
python -m kafka_pipeline.microservices.tts_service --group-id tts-v1
python -m kafka_pipeline.microservices.reconstruction_service --group-id reconstruct-v1
```

### Step 4: Resource sizing (starting point)

- diarization-service: highest RAM/CPU of the four (model load + inference)
- translation-service: low-medium
- tts-service: medium
- reconstruction-service: medium-high (audio/video processing)

### Step 5: Validate end-to-end

1. Deploy all 4 apps.
2. Publish one ingest event.
3. Confirm logs show consume -> publish progression across all workers.
4. Verify DB status counters and generated blob paths update as expected.
