Use 4 topics only:

1. `ingest`
2. `translate_segments`
3. `text_to_speech`
4. `reconstruct_video`

Keep one payload shape across stages where possible:

```json
{
  "src_blob": "string",
  "src_lang": "null | string",
  "dest_lang": "string",
  "segments": [
    {
      "segment_id": "int",
      "speaker_id": "string",
      "start": "float",
      "end": "float",
      "text": "string"
    }
  ]
}
```

`segments` is absent until diarization completes.

### 1) Web Server (different repository)

Uploads source video to blob storage and sends message to `ingest`.

Input message to `ingest`:

```json
{
  "src_blob": "string",
  "src_lang": "null | string",
  "dest_lang": "string"
}
```

### 2) Diarization (+ optional language detection)

Consumes from `ingest`.

If `src_lang` is `null`, detect language in this step.
Then diarize/transcribe into timestamped segments.

Output message to `translate_segments`:

```json
{
  "src_blob": "string",
  "src_lang": "string",
  "dest_lang": "string",
  "segments": [
    {
      "segment_id": "int",
      "speaker_id": "string",
      "start": "float",
      "end": "float",
      "text": "string"
    }
  ]
}
```

### 3) Translation

Consumes from `translate_segments` and translates each segment text.

Writes placeholder rows to DB (one row per segment):

- `src_blob`: string
- `gen_blob`: null
- `segment_id`: int
- `speaker_id`: string
- `start`: float
- `end`: float

Publishes translated segments to `text_to_speech` grouped by speaker (**one message per speaker** per `src_blob`).

Kafka message key for `text_to_speech`:

- `src_blob:speaker_id`

Output message to `text_to_speech`:

```json
{
  "src_blob": "string",
  "src_lang": "string",
  "dest_lang": "string",
  "speaker_id": "string",
  "segments": [
    {
      "segment_id": "int",
      "speaker_id": "string",
      "start": "float",
      "end": "float",
      "text": "string"
    }
  ]
}
```

### 4) Text-to-Speech

Consumes from `text_to_speech`.

Clones voice once for the message speaker, then generates audio blobs for all segments in that message.

Updates DB rows:

- `src_blob`: string
- `gen_blob`: string
- `segment_id`: int
- `speaker_id`: string
- `start`: float
- `end`: float

After all segments across all speakers for `src_blob` are generated, publish one message to `reconstruct_video`:

```json
{
  "src_blob": "string"
}
```

### 5) Reconstruction

Consumes from `reconstruct_video`.

Looks up all DB rows for `src_blob`.
If every row has `gen_blob` populated, reconstruct final dubbed video.
Will need to stretch/compress the gen_blob audio tracks to fit into the specified start and end times.
