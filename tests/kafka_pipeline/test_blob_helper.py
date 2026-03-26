
import pytest

from kafka_pipeline import blob_helper as bh


class DummyDownload:
    def __init__(self, content: bytes):
        self._content = content

    def readall(self):
        return self._content


class DummyBlobClient:
    def __init__(self, content: bytes = b""):
        self.content = content
        self.upload_calls = []

    def download_blob(self):
        return DummyDownload(self.content)

    def upload_blob(self, data, overwrite=False):
        self.upload_calls.append({"data": data, "overwrite": overwrite})


class DummyContainerClient:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.create_calls = 0

    def create_container(self):
        self.create_calls += 1
        if self.should_fail:
            raise Exception("already exists")


class DummyServiceClient:
    def __init__(self, blob_client=None, container_client=None):
        self._blob_client = blob_client or DummyBlobClient()
        self._container_client = container_client or DummyContainerClient()
        self.blob_requests = []
        self.container_requests = []

    def get_blob_client(self, container, blob):
        self.blob_requests.append({"container": container, "blob": blob})
        return self._blob_client

    def get_container_client(self, container):
        self.container_requests.append(container)
        return self._container_client


def test_container_name_from_env(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", "my-container")
    assert bh._container_name() == "my-container"


def test_container_name_default(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_CONTAINER", raising=False)
    assert bh._container_name() == "healthcheck"


def test_blob_service_client_from_connection_string(monkeypatch):
    calls = []

    class FakeBlobServiceClient:
        @staticmethod
        def from_connection_string(value):
            calls.append(value)
            return "client-from-conn"

    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_KEY", raising=False)

    import sys
    from types import SimpleNamespace

    monkeypatch.setitem(
        sys.modules,
        "azure.storage.blob",
        SimpleNamespace(BlobServiceClient=FakeBlobServiceClient),
    )

    assert bh._blob_service_client() == "client-from-conn"
    assert calls == ["UseDevelopmentStorage=true"]


def test_blob_service_client_from_account_and_key(monkeypatch):
    init_calls = []

    class FakeBlobServiceClient:
        def __init__(self, account_url, credential):
            init_calls.append({"account_url": account_url, "credential": credential})

    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct123")
    monkeypatch.setenv("AZURE_STORAGE_KEY", "key123")

    import sys
    from types import SimpleNamespace

    monkeypatch.setitem(
        sys.modules,
        "azure.storage.blob",
        SimpleNamespace(BlobServiceClient=FakeBlobServiceClient),
    )

    client = bh._blob_service_client()

    assert isinstance(client, FakeBlobServiceClient)
    assert init_calls == [
        {
            "account_url": "https://acct123.blob.core.windows.net",
            "credential": "key123",
        }
    ]


def test_blob_service_client_raises_when_missing_credentials(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Azure Blob credentials are not configured"):
        bh._blob_service_client()


def test_ensure_container_exists_calls_create(monkeypatch):
    container_client = DummyContainerClient()
    service_client = DummyServiceClient(container_client=container_client)

    monkeypatch.setattr(bh, "_blob_service_client", lambda: service_client)
    monkeypatch.setattr(bh, "_container_name", lambda: "test-container")

    bh.ensure_container_exists()

    assert service_client.container_requests == ["test-container"]
    assert container_client.create_calls == 1


def test_ensure_container_exists_swallows_errors(monkeypatch):
    container_client = DummyContainerClient(should_fail=True)
    service_client = DummyServiceClient(container_client=container_client)

    monkeypatch.setattr(bh, "_blob_service_client", lambda: service_client)
    monkeypatch.setattr(bh, "_container_name", lambda: "test-container")

    bh.ensure_container_exists()

    assert container_client.create_calls == 1


def test_download_blob(monkeypatch):
    blob_client = DummyBlobClient(content=b"hello")
    service_client = DummyServiceClient(blob_client=blob_client)

    monkeypatch.setattr(bh, "_blob_service_client", lambda: service_client)
    monkeypatch.setattr(bh, "_container_name", lambda: "test-container")

    result = bh.download_blob("folder/file.txt")

    assert result == b"hello"
    assert service_client.blob_requests == [
        {"container": "test-container", "blob": "folder/file.txt"}
    ]


def test_download_blob_to_file(tmp_path, monkeypatch):
    monkeypatch.setattr(bh, "download_blob", lambda blob_location: b"abc123")

    output_path = tmp_path / "nested" / "file.bin"
    result = bh.download_blob_to_file(blob_location="blob-name", output_path=output_path)

    assert result == output_path
    assert output_path.read_bytes() == b"abc123"


def test_upload_blob_bytes(monkeypatch):
    blob_client = DummyBlobClient()
    service_client = DummyServiceClient(blob_client=blob_client)

    monkeypatch.setattr(bh, "ensure_container_exists", lambda: None)
    monkeypatch.setattr(bh, "_blob_service_client", lambda: service_client)
    monkeypatch.setattr(bh, "_container_name", lambda: "test-container")
    monkeypatch.setattr(bh.uuid, "uuid4", lambda: type("U", (), {"hex": "abc123"})())

    result = bh.upload_blob_bytes(
        blob_bytes=b"payload",
        original_filename="audio.wav",
        folder="uploads",
        overwrite=True,
    )

    assert result == "uploads/abc123.wav"
    assert service_client.blob_requests == [
        {"container": "test-container", "blob": "uploads/abc123.wav"}
    ]
    assert blob_client.upload_calls == [{"data": b"payload", "overwrite": True}]


def test_upload_blob_bytes_defaults_bin_extension(monkeypatch):
    blob_client = DummyBlobClient()
    service_client = DummyServiceClient(blob_client=blob_client)

    monkeypatch.setattr(bh, "ensure_container_exists", lambda: None)
    monkeypatch.setattr(bh, "_blob_service_client", lambda: service_client)
    monkeypatch.setattr(bh, "_container_name", lambda: "test-container")
    monkeypatch.setattr(bh.uuid, "uuid4", lambda: type("U", (), {"hex": "xyz789"})())

    result = bh.upload_blob_bytes(
        blob_bytes=b"payload",
        original_filename="no_extension",
    )

    assert result == "xyz789.bin"


def test_upload_file_with_explicit_blob_name(tmp_path, monkeypatch):
    local_file = tmp_path / "sample.txt"
    local_file.write_bytes(b"hello file")

    blob_client = DummyBlobClient()
    service_client = DummyServiceClient(blob_client=blob_client)

    monkeypatch.setattr(bh, "ensure_container_exists", lambda: None)
    monkeypatch.setattr(bh, "_blob_service_client", lambda: service_client)
    monkeypatch.setattr(bh, "_container_name", lambda: "test-container")

    result = bh.upload_file(
        local_path=local_file,
        blob_name="custom/file.txt",
        overwrite=False,
    )

    assert result == "custom/file.txt"
    assert service_client.blob_requests == [
        {"container": "test-container", "blob": "custom/file.txt"}
    ]
    assert len(blob_client.upload_calls) == 1
    assert blob_client.upload_calls[0]["overwrite"] is False
    uploaded_data = blob_client.upload_calls[0]["data"].read()
    assert uploaded_data == b"hello file"


def test_upload_file_generates_name_when_missing(tmp_path, monkeypatch):
    local_file = tmp_path / "sample.wav"
    local_file.write_bytes(b"audio")

    blob_client = DummyBlobClient()
    service_client = DummyServiceClient(blob_client=blob_client)

    monkeypatch.setattr(bh, "ensure_container_exists", lambda: None)
    monkeypatch.setattr(bh, "_blob_service_client", lambda: service_client)
    monkeypatch.setattr(bh, "_container_name", lambda: "test-container")
    monkeypatch.setattr(bh.uuid, "uuid4", lambda: type("U", (), {"hex": "gen123"})())

    result = bh.upload_file(
        local_path=local_file,
        folder="folder1",
    )

    assert result == "folder1/gen123.wav"


def test_build_tts_segment_blob_name():
    result = bh.build_tts_segment_blob_name(
        src_blob="videos/test\\clip.mp4",
        speaker_id="speaker_1",
        segment_id=7,
        ext=".wav",
    )

    assert result == "generated/segments/videos_test_clip.mp4/speaker_1/segment_7.wav"


def test_build_tts_segment_blob_name_normalizes_extension():
    result = bh.build_tts_segment_blob_name(
        src_blob="video.mp4",
        speaker_id="speaker_0",
        segment_id=1,
        ext="mp3",
    )

    assert result.endswith("/segment_1.mp3")


def test_build_reconstruction_blob_name(monkeypatch):
    monkeypatch.setattr(bh.uuid, "uuid4", lambda: type("U", (), {"hex": "final123"})())

    result = bh.build_reconstruction_blob_name(
        src_blob="videos/test\\clip.mp4",
        ext=".mp4",
    )

    assert result == "generated/reconstruction/videos_test_clip.mp4/final_final123.mp4"


def test_build_reconstruction_blob_name_normalizes_extension(monkeypatch):
    monkeypatch.setattr(bh.uuid, "uuid4", lambda: type("U", (), {"hex": "abc999"})())

    result = bh.build_reconstruction_blob_name(
        src_blob="video.mp4",
        ext="mov",
    )

    assert result == "generated/reconstruction/video.mp4/final_abc999.mov"