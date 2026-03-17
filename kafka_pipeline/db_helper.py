import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class TTSSegment:
    src_blob: str
    gen_blob: str | None
    segment_id: int
    speaker_id: str
    start: float
    end: float


def _get_db_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return db_url


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_db_url())


def _to_segment(row: Sequence[Any]) -> TTSSegment:
    return TTSSegment(
        src_blob=row[0],
        gen_blob=row[1],
        segment_id=row[2],
        speaker_id=row[3],
        start=float(row[4]),
        end=float(row[5]),
    )


def upsert_tts_placeholder(
    *,
    src_blob: str,
    segment_id: int,
    speaker_id: str,
    start: float,
    end: float,
) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                                UPDATE "pg-drizzle_tts"
                SET speaker_id = %s,
                    start = %s,
                    "end" = %s
                WHERE src_blob = %s
                  AND segment_id = %s;
                """,
                (speaker_id, start, end, src_blob, segment_id),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO "pg-drizzle_tts" (src_blob, gen_blob, segment_id, speaker_id, start, "end")
                    VALUES (%s, NULL, %s, %s, %s, %s);
                    """,
                    (src_blob, segment_id, speaker_id, start, end),
                )
        conn.commit()


def set_tts_generated_blob(*, src_blob: str, segment_id: int, gen_blob: str) -> bool:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                                UPDATE "pg-drizzle_tts"
                SET gen_blob = %s
                WHERE src_blob = %s
                  AND segment_id = %s;
                """,
                (gen_blob, src_blob, segment_id),
            )
            updated = cur.rowcount > 0
        conn.commit()
    return updated


def are_all_segments_generated(src_blob: str) -> bool:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM "pg-drizzle_tts"
                WHERE src_blob = %s;
                """,
                (src_blob,),
            )
            total_rows = cur.fetchone()[0]

            if total_rows == 0:
                return False

            cur.execute(
                """
                SELECT COUNT(*)
                                FROM "pg-drizzle_tts"
                WHERE src_blob = %s
                  AND gen_blob IS NULL;
                """,
                (src_blob,),
            )
            null_blob_rows = cur.fetchone()[0]

    return null_blob_rows == 0


def get_segments_for_src_blob(src_blob: str) -> list[TTSSegment]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT src_blob, gen_blob, segment_id, speaker_id, start, "end"
                FROM "pg-drizzle_tts"
                WHERE src_blob = %s
                ORDER BY start ASC, segment_id ASC;
                """,
                (src_blob,),
            )
            rows = cur.fetchall()

    return [_to_segment(row) for row in rows]


def set_video_completed_blob(*, src_blob: str, completed_blob: str) -> bool:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE "pg-drizzle_videos"
                SET completed_blob = %s,
                    status = 'done'
                WHERE blob = %s;
                """,
                (completed_blob, src_blob),
            )
            updated = cur.rowcount > 0
        conn.commit()
    return updated
