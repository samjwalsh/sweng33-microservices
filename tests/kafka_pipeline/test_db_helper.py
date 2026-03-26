import pytest

from kafka_pipeline import db_helper as db


class FakeCursor:
    def __init__(self, execute_effects=None, fetchone_results=None, fetchall_result=None):
        self.execute_effects = execute_effects or []
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_result = fetchall_result if fetchall_result is not None else []
        self.execute_calls = []
        self.rowcount = 0

    def execute(self, query, params=None):
        self.execute_calls.append((query, params))
        effect = self.execute_effects.pop(0) if self.execute_effects else {}
        self.rowcount = effect.get("rowcount", 0)

    def fetchone(self):
        return self.fetchone_results.pop(0)

    def fetchall(self):
        return self.fetchall_result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit_calls = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_calls += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_get_db_url_returns_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    assert db._get_db_url() == "postgres://example"


def test_get_db_url_raises_when_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
        db._get_db_url()


def test_connect_calls_psycopg_connect(monkeypatch):
    calls = []
    monkeypatch.setattr(db, "_get_db_url", lambda: "postgres://db-url")
    monkeypatch.setattr(db.psycopg, "connect", lambda url: calls.append(url) or "conn")

    result = db._connect()

    assert result == "conn"
    assert calls == ["postgres://db-url"]


def test_to_segment_converts_row():
    row = ("blob1", "gen1", 3, "speaker_0", "1.5", 2)
    result = db._to_segment(row)

    assert result == db.TTSSegment(
        src_blob="blob1",
        gen_blob="gen1",
        segment_id=3,
        speaker_id="speaker_0",
        start=1.5,
        end=2.0,
    )


def test_upsert_tts_placeholder_updates_existing(monkeypatch):
    cursor = FakeCursor(execute_effects=[{"rowcount": 1}])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    db.upsert_tts_placeholder(
        src_blob="blob1",
        segment_id=5,
        speaker_id="speaker_1",
        start=1.2,
        end=2.3,
    )

    assert len(cursor.execute_calls) == 1
    _, params = cursor.execute_calls[0]
    assert params == ("speaker_1", 1.2, 2.3, "blob1", 5)
    assert conn.commit_calls == 1


def test_upsert_tts_placeholder_inserts_when_missing(monkeypatch):
    cursor = FakeCursor(execute_effects=[{"rowcount": 0}, {"rowcount": 1}])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    db.upsert_tts_placeholder(
        src_blob="blob1",
        segment_id=5,
        speaker_id="speaker_1",
        start=1.2,
        end=2.3,
    )

    assert len(cursor.execute_calls) == 2
    _, update_params = cursor.execute_calls[0]
    _, insert_params = cursor.execute_calls[1]
    assert update_params == ("speaker_1", 1.2, 2.3, "blob1", 5)
    assert insert_params == ("blob1", 5, "speaker_1", 1.2, 2.3)
    assert conn.commit_calls == 1


def test_set_tts_generated_blob_returns_true_when_updated(monkeypatch):
    cursor = FakeCursor(execute_effects=[{"rowcount": 1}])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db.set_tts_generated_blob(src_blob="blob1", segment_id=2, gen_blob="gen.wav")

    assert result is True
    assert cursor.execute_calls[0][1] == ("gen.wav", "blob1", 2)
    assert conn.commit_calls == 1


def test_set_tts_generated_blob_returns_false_when_not_updated(monkeypatch):
    cursor = FakeCursor(execute_effects=[{"rowcount": 0}])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db.set_tts_generated_blob(src_blob="blob1", segment_id=2, gen_blob="gen.wav")

    assert result is False
    assert conn.commit_calls == 1


def test_are_all_segments_generated_false_when_no_rows(monkeypatch):
    cursor = FakeCursor(fetchone_results=[(0,)])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db.are_all_segments_generated("blob1")

    assert result is False
    assert len(cursor.execute_calls) == 1


def test_are_all_segments_generated_true_when_none_missing(monkeypatch):
    cursor = FakeCursor(fetchone_results=[(3,), (0,)])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db.are_all_segments_generated("blob1")

    assert result is True
    assert len(cursor.execute_calls) == 2


def test_are_all_segments_generated_false_when_some_missing(monkeypatch):
    cursor = FakeCursor(fetchone_results=[(3,), (1,)])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db.are_all_segments_generated("blob1")

    assert result is False


def test_get_segments_for_src_blob_returns_segments(monkeypatch):
    rows = [
        ("blob1", "gen1", 1, "speaker_0", 0, 1.5),
        ("blob1", "gen2", 2, "speaker_1", 2, 3),
    ]
    cursor = FakeCursor(fetchall_result=rows)
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db.get_segments_for_src_blob("blob1")

    assert result == [
        db.TTSSegment("blob1", "gen1", 1, "speaker_0", 0.0, 1.5),
        db.TTSSegment("blob1", "gen2", 2, "speaker_1", 2.0, 3.0),
    ]
    assert cursor.execute_calls[0][1] == ("blob1",)


def test_set_video_completed_blob_true(monkeypatch):
    cursor = FakeCursor(execute_effects=[{"rowcount": 1}])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db.set_video_completed_blob(src_blob="blob1", completed_blob="done.mp4")

    assert result is True
    assert cursor.execute_calls[0][1] == ("done.mp4", "blob1")
    assert conn.commit_calls == 1


def test_set_video_completed_blob_false(monkeypatch):
    cursor = FakeCursor(execute_effects=[{"rowcount": 0}])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db.set_video_completed_blob(src_blob="blob1", completed_blob="done.mp4")

    assert result is False
    assert conn.commit_calls == 1


def test_increment_video_task_counter_true(monkeypatch):
    cursor = FakeCursor(execute_effects=[{"rowcount": 1}])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db._increment_video_task_counter(
        src_blob="blob1",
        column_name="tts_completed_tasks",
    )

    assert result is True
    query, params = cursor.execute_calls[0]
    assert params == ("blob1",)
    assert conn.commit_calls == 1
    assert query is not None


def test_increment_video_task_counter_false(monkeypatch):
    cursor = FakeCursor(execute_effects=[{"rowcount": 0}])
    conn = FakeConnection(cursor)
    monkeypatch.setattr(db, "_connect", lambda: conn)

    result = db._increment_video_task_counter(
        src_blob="blob1",
        column_name="tts_completed_tasks",
    )

    assert result is False
    assert conn.commit_calls == 1


def test_increment_diarization_total_tasks_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db,
        "_increment_video_task_counter",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    result = db.increment_diarization_total_tasks(src_blob="blob1")

    assert result is True
    assert calls == [{"src_blob": "blob1", "column_name": "diarization_total_tasks"}]


def test_increment_diarization_completed_tasks_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db,
        "_increment_video_task_counter",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    result = db.increment_diarization_completed_tasks(src_blob="blob1")

    assert result is True
    assert calls == [{"src_blob": "blob1", "column_name": "diarization_completed_tasks"}]


def test_increment_translation_total_tasks_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db,
        "_increment_video_task_counter",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    result = db.increment_translation_total_tasks(src_blob="blob1")

    assert result is True
    assert calls == [{"src_blob": "blob1", "column_name": "translation_total_tasks"}]


def test_increment_translation_completed_tasks_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db,
        "_increment_video_task_counter",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    result = db.increment_translation_completed_tasks(src_blob="blob1")

    assert result is True
    assert calls == [{"src_blob": "blob1", "column_name": "translation_completed_tasks"}]


def test_increment_tts_total_tasks_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db,
        "_increment_video_task_counter",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    result = db.increment_tts_total_tasks(src_blob="blob1")

    assert result is True
    assert calls == [{"src_blob": "blob1", "column_name": "tts_total_tasks"}]


def test_increment_tts_completed_tasks_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db,
        "_increment_video_task_counter",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    result = db.increment_tts_completed_tasks(src_blob="blob1")

    assert result is True
    assert calls == [{"src_blob": "blob1", "column_name": "tts_completed_tasks"}]


def test_increment_reconstruction_total_tasks_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db,
        "_increment_video_task_counter",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    result = db.increment_reconstruction_total_tasks(src_blob="blob1")

    assert result is True
    assert calls == [{"src_blob": "blob1", "column_name": "reconstruction_total_tasks"}]


def test_increment_reconstruction_completed_tasks_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db,
        "_increment_video_task_counter",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    result = db.increment_reconstruction_completed_tasks(src_blob="blob1")

    assert result is True
    assert calls == [{"src_blob": "blob1", "column_name": "reconstruction_completed_tasks"}]