"""
Unit tests for streaming upload tools (`create_upload_slot`,
`commit_upload`) and the `_SlotStore` registry.

The HTTP `PUT /upload/{token}` route is exercised separately via direct
calls to `upload_blob` with a fake Starlette Request, since pulling the
whole FastMCP app up in unit tests is overkill for what is essentially a
streaming-write handler.
"""

import asyncio
import os
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)


def _unwrap(tool):
    """Strip FastMCP / decorator wrapping to reach the inner async fn."""
    fn = tool.fn if hasattr(tool, "fn") else tool
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


# ---------------------------------------------------------------------------
# _SlotStore
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_slot_store_create_and_get(tmp_path, monkeypatch):
    """create() returns a usable slot; get() retrieves it by token."""
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)

    store = upload_slots._SlotStore()
    slot = await store.create("user@example.com")

    assert slot.token
    assert slot.user_email == "user@example.com"
    assert slot.status == "pending"
    assert slot.size_bytes == 0
    assert slot.expires_at > time.time()

    got = await store.get(slot.token)
    assert got is slot


@pytest.mark.asyncio
async def test_slot_store_get_unknown_returns_none(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)

    store = upload_slots._SlotStore()
    assert await store.get("nonexistent-token") is None


@pytest.mark.asyncio
async def test_slot_store_expired_slot_is_evicted(tmp_path, monkeypatch):
    """Lazy expiry: get() drops slots past their TTL and deletes the file."""
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)

    store = upload_slots._SlotStore()
    slot = await store.create("user@example.com")
    # Stage a file to verify cleanup
    staged = tmp_path / slot.token
    staged.write_bytes(b"stale")
    # Force expiry
    slot.expires_at = time.time() - 1

    assert await store.get(slot.token) is None
    assert slot.token not in store._slots
    assert not staged.exists()


@pytest.mark.asyncio
async def test_slot_store_mark_uploaded(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)

    store = upload_slots._SlotStore()
    slot = await store.create("user@example.com")

    ok = await store.mark_uploaded(slot.token, 12345)
    assert ok is True
    assert slot.status == "uploaded"
    assert slot.size_bytes == 12345

    # Unknown token returns False, doesn't crash
    assert await store.mark_uploaded("nope", 1) is False


@pytest.mark.asyncio
async def test_slot_store_discard_removes_file(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)

    store = upload_slots._SlotStore()
    slot = await store.create("user@example.com")
    staged = tmp_path / slot.token
    staged.write_bytes(b"data")

    await store.discard(slot.token)
    assert slot.token not in store._slots
    assert not staged.exists()

    # Idempotent
    await store.discard(slot.token)


# ---------------------------------------------------------------------------
# PUT /upload/{token} HTTP route
# ---------------------------------------------------------------------------


class _FakeRequest:
    """Minimal Request stand-in for the Starlette handler.

    Implements only what `upload_blob` touches: `path_params`, `headers`,
    and `stream()` as an async iterator.
    """

    def __init__(self, token, chunks, content_length=None):
        self.path_params = {"token": token}
        self.headers = (
            {"content-length": str(content_length)}
            if content_length is not None
            else {}
        )
        self._chunks = list(chunks)

    async def stream(self):
        for c in self._chunks:
            yield c


@pytest.mark.asyncio
async def test_upload_blob_happy_path(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    slot = await fresh_store.create("user@example.com")
    body = b"PK\x03\x04hello-world-bytes"
    req = _FakeRequest(slot.token, [body], content_length=len(body))

    resp = await _unwrap(upload_slots.upload_blob)(req)
    assert resp.status_code == 200
    assert (tmp_path / slot.token).read_bytes() == body
    assert slot.status == "uploaded"
    assert slot.size_bytes == len(body)


@pytest.mark.asyncio
async def test_upload_blob_unknown_token(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    monkeypatch.setattr(upload_slots, "_store", upload_slots._SlotStore())

    req = _FakeRequest("does-not-exist", [b"x"])
    resp = await _unwrap(upload_slots.upload_blob)(req)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_upload_blob_rejects_oversized_content_length(
    tmp_path, monkeypatch
):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_MAX_BYTES", 16)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    slot = await fresh_store.create("user@example.com")
    req = _FakeRequest(slot.token, [b"x" * 100], content_length=100)

    resp = await _unwrap(upload_slots.upload_blob)(req)
    assert resp.status_code == 413


@pytest.mark.asyncio
async def test_upload_blob_streaming_limit_kicks_in_without_cl(
    tmp_path, monkeypatch
):
    """Without Content-Length we must enforce the limit during streaming."""
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_MAX_BYTES", 8)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    slot = await fresh_store.create("user@example.com")
    # No content-length set — limit must be enforced mid-stream
    req = _FakeRequest(slot.token, [b"AAAA", b"BBBB", b"CCCC"])

    resp = await _unwrap(upload_slots.upload_blob)(req)
    assert resp.status_code == 413
    assert not (tmp_path / slot.token).exists()


@pytest.mark.asyncio
async def test_upload_blob_rejects_second_put_after_upload(
    tmp_path, monkeypatch
):
    """After successful PUT the slot is in `uploaded` state; reuse → 409."""
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    slot = await fresh_store.create("user@example.com")
    req1 = _FakeRequest(slot.token, [b"first"], content_length=5)
    resp1 = await _unwrap(upload_slots.upload_blob)(req1)
    assert resp1.status_code == 200

    req2 = _FakeRequest(slot.token, [b"second"], content_length=6)
    resp2 = await _unwrap(upload_slots.upload_blob)(req2)
    assert resp2.status_code == 409


@pytest.mark.asyncio
async def test_upload_blob_empty_body_rejected(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    slot = await fresh_store.create("user@example.com")
    req = _FakeRequest(slot.token, [], content_length=0)

    resp = await _unwrap(upload_slots.upload_blob)(req)
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# create_upload_slot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_upload_slot_returns_token_and_url(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    monkeypatch.setattr(upload_slots, "_store", upload_slots._SlotStore())
    monkeypatch.setenv(
        "WORKSPACE_EXTERNAL_URL", "https://gws.percust.com"
    )

    fn = _unwrap(upload_slots.create_upload_slot)
    result = await fn(
        service=Mock(), user_google_email="user@percust.com"
    )

    assert "token=" in result
    assert "upload_url=https://gws.percust.com/upload/" in result
    assert "max_bytes=" in result


@pytest.mark.asyncio
async def test_create_upload_slot_relative_url_when_no_base(
    tmp_path, monkeypatch
):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    monkeypatch.setattr(upload_slots, "_store", upload_slots._SlotStore())
    monkeypatch.delenv("WORKSPACE_EXTERNAL_URL", raising=False)
    monkeypatch.delenv("WORKSPACE_MCP_BASE_URI", raising=False)

    fn = _unwrap(upload_slots.create_upload_slot)
    result = await fn(
        service=Mock(), user_google_email="user@percust.com"
    )

    assert "upload_url=/upload/" in result
    assert "WORKSPACE_EXTERNAL_URL is not configured" in result


# ---------------------------------------------------------------------------
# commit_upload
# ---------------------------------------------------------------------------


def _make_drive_create_mock(response: dict) -> Mock:
    mock_service = Mock()
    mock_request = Mock()
    mock_request.execute.return_value = response
    mock_service.files.return_value.create.return_value = mock_request
    return mock_service


@pytest.mark.asyncio
async def test_commit_upload_happy_path(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    # Stage a slot manually
    slot = await fresh_store.create("user@percust.com")
    staged = tmp_path / slot.token
    staged.write_bytes(b"PK\x03\x04 fake-xlsx")
    await fresh_store.mark_uploaded(slot.token, staged.stat().st_size)

    mock_service = _make_drive_create_mock(
        {
            "id": "drive_file_1",
            "name": "report.xlsx",
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "webViewLink": "https://drive.google.com/file/d/drive_file_1/view",
        }
    )

    fn = _unwrap(upload_slots.commit_upload)
    with patch(
        "gdrive.drive_helpers.resolve_folder_id",
        new_callable=AsyncMock,
        return_value="parent_folder_id",
    ):
        result = await fn(
            service=mock_service,
            user_google_email="user@percust.com",
            token=slot.token,
            file_name="report.xlsx",
            folder_id="parent_folder_id",
            mime_type=None,
            convert_to_google_format=False,
        )

    create_call = mock_service.files.return_value.create.call_args
    body = create_call.kwargs["body"]
    assert body["name"] == "report.xlsx"
    assert body["parents"] == ["parent_folder_id"]
    assert create_call.kwargs["supportsAllDrives"] is True
    assert "Successfully uploaded" in result
    assert "drive_file_1" in result
    # Slot is consumed and the staged file is gone
    assert slot.token not in fresh_store._slots
    assert not staged.exists()


@pytest.mark.asyncio
async def test_commit_upload_wrong_user_rejected(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    slot = await fresh_store.create("alice@percust.com")
    staged = tmp_path / slot.token
    staged.write_bytes(b"data")
    await fresh_store.mark_uploaded(slot.token, 4)

    fn = _unwrap(upload_slots.commit_upload)
    with pytest.raises(Exception, match="different user"):
        await fn(
            service=Mock(),
            user_google_email="bob@percust.com",
            token=slot.token,
            file_name="x.bin",
        )
    # Slot is preserved (Alice can still legitimately commit)
    assert slot.token in fresh_store._slots


@pytest.mark.asyncio
async def test_commit_upload_status_pending_rejected(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    slot = await fresh_store.create("user@percust.com")
    # Note: no PUT, status stays "pending"

    fn = _unwrap(upload_slots.commit_upload)
    with pytest.raises(Exception, match="pending"):
        await fn(
            service=Mock(),
            user_google_email="user@percust.com",
            token=slot.token,
            file_name="x.bin",
        )


@pytest.mark.asyncio
async def test_commit_upload_unknown_token_rejected(tmp_path, monkeypatch):
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    monkeypatch.setattr(upload_slots, "_store", upload_slots._SlotStore())

    fn = _unwrap(upload_slots.commit_upload)
    with pytest.raises(Exception, match="Unknown or expired"):
        await fn(
            service=Mock(),
            user_google_email="user@percust.com",
            token="ghost-token",
            file_name="x.bin",
        )


@pytest.mark.asyncio
async def test_commit_upload_missing_file_discards_slot(tmp_path, monkeypatch):
    """If the staged file vanished between PUT and commit, slot is discarded."""
    from gdrive import upload_slots

    monkeypatch.setattr(upload_slots, "UPLOAD_SLOT_DIR", tmp_path)
    fresh_store = upload_slots._SlotStore()
    monkeypatch.setattr(upload_slots, "_store", fresh_store)

    slot = await fresh_store.create("user@percust.com")
    # Mark uploaded but never write the file
    await fresh_store.mark_uploaded(slot.token, 10)

    fn = _unwrap(upload_slots.commit_upload)
    with pytest.raises(Exception, match="missing on the server"):
        await fn(
            service=Mock(),
            user_google_email="user@percust.com",
            token=slot.token,
            file_name="x.bin",
        )
    assert slot.token not in fresh_store._slots
