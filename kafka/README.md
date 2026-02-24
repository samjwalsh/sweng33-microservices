# Kafka test scripts

These scripts let you quickly verify publish/consume against:

- broker: `kafka.samjw.xyz:9092`
- topic: `test`

## Install dependency

From the repo root:

```bash
pip install -r requirements.txt
```

## 1) Start consumer (terminal A)

```bash
python kafka/consumer_test.py --max-messages 5
```

Wait until you see `Consumer ready. Assigned partitions: ...` before publishing.

Optional: read old messages too

```bash
python kafka/consumer_test.py --from-beginning --max-messages 5
```

If assignment is slow, increase startup wait:

```bash
python kafka/consumer_test.py --max-messages 5 --startup-timeout-seconds 30
```

## 2) Publish test events (terminal B)

```bash
python kafka/producer_test.py --count 5
```

You should see messages printed by the consumer.
