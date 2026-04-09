# Directory Structure

All of the final microservices are located within `kafka_pipeline`, the diarization microservice has dependencies in the `src` dir.
Rest of the files are unused when the project is running in production, but were used by us during development and testing.

# Running

Each of the microservices has its own dockerfile.
I used Dokploy because I've used it before in the past.

# .env

```env
# Schema info located in webapp repo in schema.ts
DATABASE_URL=""

AZURE_STORAGE_ACCOUNT=""
AZURE_STORAGE_KEY=""
AZURE_STORAGE_CONTAINER=""

KAFKA_BOOTSTRAP_SERVER=""

# Microsoft Translator
TRANSLATOR_KEY = ""
TRANSLATOR_ENDPOINT = ""
TRANSLATOR_REGION = "ukwest"

# Microsoft Transcriber
TRANSCRIBER_KEY=""
TRANSCRIBER_ENDPOINT=""
TRANSLATOR_REGION="ukwest"

# Microsoft Foundry
AZURE_FOUNDRY_KEY = ""
AZURE_FOUNDRY_ENDPOINT = ""
AZURE_FOUNDRY_REGION = "ukwest"

# Eleven Labs
ELEVENLABS_API_KEY=""

# Huggingface
MY_TOKEN=""

# Various settings to get project to run on my PC, you can change these
TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD="1"
DIARIZATION_DEVICE=cpu
DIARIZATION_NUM_THREADS=16
DIARIZATION_MODEL=pyannote/speaker-diarization-3.1
TRANSCRIBER_MODEL=openai/whisper-large-v3-turbo
TRANSCRIBER_CHUNK_LENGTH_S=15
TRANSCRIBER_STRIDE_LENGTH_S=3
LANGID_WHISPER_MODEL=turbo
```
