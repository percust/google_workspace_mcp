"""
Streaming uploads to Google Drive via temporary upload slots.

Two-step protocol that bypasses MCP message-size limits (no base64 in the
JSON-RPC stream):

  1. `create_upload_slot` returns a one-shot URL bound to the calling user.
  2. The MCP client PUTs raw file bytes to that URL — handled by the
     `PUT /upload/{token}` custom route below.
  3. `commit_upload` streams the staged bytes into Drive and discards them.

Slots are scoped per-user (via Google OAuth email), single-use, and expire
after `SLOT_TTL_SECONDS`. Stale slots are garbage-collected by a background
task started lazily on first slot creation.
"""

import asyncio
import logging
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from googleapiclient.http import MediaFileUpload
from starlette.requests import Request
from starlette.responses import JSONResponse

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

UPLOAD_SLOT_DIR = Path(
    os.environ.get("UPLOAD_SLOT_DIR", "/tmp/workspace-mcp-uploads")
)
# 100 MiB. Matches the project's "any office doc / image / small archive"
# coverage; larger payloads should use Drive resumable uploads via a
# different transport.
UPLOAD_SLOT_MAX_BYTES = 100 * 1024 * 1024
# 15 minutes is enough headroom for slow links while keeping /tmp tidy.
SLOT_TTL_SECONDS = 15 * 60
GC_INTERVAL_SECONDS = 60


# ---------------------------------------------------------------------------
# In-memory slot store
# ---------------------------------------------------------------------------


@dataclass
class _Slot:
    token: str
    user_email: str
    expires_at: float
    status: str = "pending"  # pending -> uploaded -> (committed/discarded)
    size_bytes: int = 0


class _SlotStore:
    """In-memory slot registry with a lazy background GC task.

    State is intentionally process-local: TTL is short (15 min), and a
    container restart should drop everything. Persistence would only delay
    cleanup and complicate the design.
    """

    def __init__(self) -> None:
        self._slots: dict[str, _Slot] = {}
        self._lock = asyncio.Lock()
        self._gc_task: Optional[asyncio.Task] = None

    # -- background GC -----------------------------------------------------

    async def _ensure_gc(self) -> None:
        if self._gc_task is None or self._gc_task.done():
            try:
                self._gc_task = asyncio.create_task(self._gc_loop())
            except RuntimeError:
                # No running loop (e.g. unit tests calling sync helpers);
                # rely on lazy expiry inside `get` instead.
                self._gc_task = None

    async def _gc_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(GC_INTERVAL_SECONDS)
                await self._sweep()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover — diagnostic safety net
                logger.exception("[upload_slots] GC sweep failed")

    async def _sweep(self) -> None:
        now = time.time()
        async with self._lock:
            stale = [t for t, s in self._slots.items() if s.expires_at < now]
            for t in stale:
                self._slots.pop(t, None)
        for t in stale:
            self._delete_file(t)
        if stale:
            logger.info(
                f"[upload_slots] GC removed {len(stale)} expired slot(s)"
            )

    # -- file helpers ------------------------------------------------------

    def _path(self, token: str) -> Path:
        return UPLOAD_SLOT_DIR / token

    def _delete_file(self, token: str) -> None:
        p = self._path(token)
        try:
            if p.exists():
                p.unlink()
        except Exception:  # pragma: no cover
            logger.exception(f"[upload_slots] failed to remove {p}")

    # -- public API --------------------------------------------------------

    async def create(self, user_email: str) -> _Slot:
        UPLOAD_SLOT_DIR.mkdir(parents=True, exist_ok=True)
        token = secrets.token_urlsafe(32)
        slot = _Slot(
            token=token,
            user_email=user_email,
            expires_at=time.time() + SLOT_TTL_SECONDS,
        )
        async with self._lock:
            self._slots[token] = slot
        await self._ensure_gc()
        return slot

    async def get(self, token: str) -> Optional[_Slot]:
        async with self._lock:
            slot = self._slots.get(token)
            if slot is None:
                return None
            if slot.expires_at < time.time():
                self._slots.pop(token, None)
                expired_path = self._path(token)
            else:
                expired_path = None
        if expired_path is not None:
            try:
                if expired_path.exists():
                    expired_path.unlink()
            except Exception:  # pragma: no cover
                logger.exception(
                    f"[upload_slots] lazy-cleanup failed for {expired_path}"
                )
            return None
        return slot

    async def mark_uploaded(self, token: str, size: int) -> bool:
        async with self._lock:
            slot = self._slots.get(token)
            if slot is None:
                return False
            slot.status = "uploaded"
            slot.size_bytes = size
            return True

    async def discard(self, token: str) -> None:
        async with self._lock:
            self._slots.pop(token, None)
        self._delete_file(token)


_store = _SlotStore()


def _public_base_url() -> str:
    """Resolve the externally reachable URL for this MCP server.

    Order of precedence matches the rest of the project:
      1. `WORKSPACE_EXTERNAL_URL` (set in production deployment)
      2. `WORKSPACE_MCP_BASE_URI` (legacy/fallback)
    Empty string if neither is configured — caller will fall back to a
    relative path and tell the user to prepend their server URL.
    """
    base = os.environ.get("WORKSPACE_EXTERNAL_URL") or os.environ.get(
        "WORKSPACE_MCP_BASE_URI", ""
    )
    return base.rstrip("/")


# ---------------------------------------------------------------------------
# HTTP route: PUT /upload/{token}
# ---------------------------------------------------------------------------


@server.custom_route("/upload/{token}", methods=["PUT"])
async def upload_blob(request: Request) -> JSONResponse:
    """Stream raw request body into the staging file for a pending slot.

    Authorization is by knowledge of the 32-byte random token. The slot
    itself is bound to a user email; only that user can later commit the
    upload via `commit_upload`, so a leaked token at most lets an attacker
    overwrite their own staged bytes (slot is single-use).
    """
    token = request.path_params["token"]
    slot = await _store.get(token)
    if slot is None:
        return JSONResponse(
            {"error": "Unknown or expired upload token"}, status_code=404
        )
    if slot.status != "pending":
        return JSONResponse(
            {"error": f"Slot is not accepting uploads (status={slot.status})"},
            status_code=409,
        )

    # Pre-flight check on Content-Length when present.
    cl_header = request.headers.get("content-length")
    if cl_header:
        try:
            if int(cl_header) > UPLOAD_SLOT_MAX_BYTES:
                return JSONResponse(
                    {
                        "error": (
                            f"Payload Content-Length {cl_header} exceeds "
                            f"{UPLOAD_SLOT_MAX_BYTES} bytes"
                        )
                    },
                    status_code=413,
                )
        except ValueError:
            pass

    UPLOAD_SLOT_DIR.mkdir(parents=True, exist_ok=True)
    path = _store._path(token)
    written = 0
    try:
        with open(path, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                written += len(chunk)
                if written > UPLOAD_SLOT_MAX_BYTES:
                    f.close()
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
                    return JSONResponse(
                        {
                            "error": (
                                f"Payload exceeds {UPLOAD_SLOT_MAX_BYTES} "
                                f"bytes during streaming"
                            )
                        },
                        status_code=413,
                    )
                f.write(chunk)
    except Exception as exc:
        logger.exception(
            f"[upload_slots] write failed for token {token[:8]}…"
        )
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return JSONResponse(
            {"error": f"Upload failed: {exc}"}, status_code=500
        )

    if written == 0:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return JSONResponse({"error": "Empty body"}, status_code=400)

    await _store.mark_uploaded(token, written)
    logger.info(
        f"[upload_slots] staged {written} bytes for slot "
        f"{token[:8]}… ({slot.user_email})"
    )
    return JSONResponse({"status": "uploaded", "size_bytes": written})


# ---------------------------------------------------------------------------
# MCP tool: create_upload_slot
# ---------------------------------------------------------------------------


@server.tool()
@handle_http_errors("create_upload_slot", service_type="drive")
@require_google_service("drive", "drive_file")
async def create_upload_slot(service, user_google_email: str) -> str:
    """
    Allocate a one-shot URL for streaming a file into Google Drive without
    embedding its bytes in the MCP message stream.

    Use this for any binary file (xlsx, pdf, png, zip, etc.) where you have
    direct filesystem access on the calling client and want to avoid the
    base64 path of `upload_drive_file`. Three-step workflow:

      1. Call `create_upload_slot` — receive `token` and `upload_url`.
      2. PUT the raw file bytes to `upload_url` (e.g. `curl -T file URL`
         or any HTTP client supporting streaming PUT).
      3. Call `commit_upload(token, file_name, folder_id, ...)` to move the
         bytes into Drive. The slot is then invalidated.

    The slot is bound to the calling Google account: only the same
    `user_google_email` may later call `commit_upload` for this token.
    Slots expire after 15 minutes.

    Args:
        user_google_email (str): The user's Google email address. Required.

    Returns:
        str: A line-delimited record with `token`, `upload_url`,
        `expires_in_seconds`, and `max_bytes`. Parse it for the token and
        URL; the rest is informational.
    """
    slot = await _store.create(user_google_email)
    base = _public_base_url()
    if not base:
        upload_url = f"/upload/{slot.token}"
        url_note = (
            " Note: WORKSPACE_EXTERNAL_URL is not configured; prepend your "
            "server's public URL to upload_url before using it."
        )
    else:
        upload_url = f"{base}/upload/{slot.token}"
        url_note = ""

    ttl = max(0, int(slot.expires_at - time.time()))
    return (
        f"Upload slot created for {user_google_email}.\n"
        f"token={slot.token}\n"
        f"upload_url={upload_url}\n"
        f"expires_in_seconds={ttl}\n"
        f"max_bytes={UPLOAD_SLOT_MAX_BYTES}\n"
        f"Next: PUT raw file bytes to upload_url, then call "
        f"commit_upload(token, file_name, folder_id).{url_note}"
    )


# ---------------------------------------------------------------------------
# MCP tool: commit_upload
# ---------------------------------------------------------------------------


@server.tool()
@handle_http_errors("commit_upload", service_type="drive")
@require_google_service("drive", "drive_file")
async def commit_upload(
    service,
    user_google_email: str,
    token: str,
    file_name: str,
    folder_id: str = "root",
    mime_type: Optional[str] = None,
    convert_to_google_format: bool = False,
) -> str:
    """
    Finalize a streaming upload created by `create_upload_slot` by moving
    the staged bytes into Google Drive. The slot is consumed regardless of
    success, so on failure call `create_upload_slot` again to retry.

    Args:
        user_google_email (str): Must match the user that created the slot.
        token (str): The slot token returned by `create_upload_slot`.
        file_name (str): Target file name in Drive (with extension).
        folder_id (str): Parent folder ID. Defaults to 'root'. For shared
            drives, pass a folder ID inside the shared drive.
        mime_type (Optional[str]): Source MIME type. If omitted, inferred
            from `file_name` extension; falls back to
            application/octet-stream.
        convert_to_google_format (bool): When True, convert to native Google
            Sheets / Docs / Slides on upload (only when the source MIME has
            a native equivalent — otherwise uploaded as-is).

    Returns:
        str: Confirmation including the new Drive file ID, MIME type, byte
        size, and webViewLink.
    """
    # Local imports avoid an import cycle (drive_tools imports this module
    # at the bottom to ensure registration).
    from gdrive.drive_tools import (
        GOOGLE_NATIVE_CONVERT_MAP,
        UPLOAD_CHUNK_SIZE_BYTES,
        _resolve_source_mime_type,
    )
    from gdrive.drive_helpers import resolve_folder_id

    slot = await _store.get(token)
    if slot is None:
        raise Exception(
            "Unknown or expired upload token. Call create_upload_slot again."
        )
    if slot.user_email != user_google_email:
        raise Exception(
            "This upload slot was created by a different user; refusing to "
            "commit."
        )
    if slot.status != "uploaded":
        raise Exception(
            f"Slot is in status '{slot.status}', expected 'uploaded'. "
            f"Did you PUT the file bytes to the upload_url first?"
        )

    path = _store._path(token)
    if not path.exists():
        await _store.discard(token)
        raise Exception(
            "Staged upload file is missing on the server; slot discarded."
        )

    source_mime = _resolve_source_mime_type(file_name, mime_type)
    target_mime = source_mime
    conversion_note = ""
    if convert_to_google_format:
        mapped = GOOGLE_NATIVE_CONVERT_MAP.get(source_mime)
        if mapped:
            target_mime = mapped
        else:
            conversion_note = (
                f" Note: convert_to_google_format=True ignored — source "
                f"MIME '{source_mime}' has no native Google equivalent."
            )

    resolved_folder_id = await resolve_folder_id(service, folder_id)
    file_metadata = {
        "name": file_name,
        "parents": [resolved_folder_id],
        "mimeType": target_mime,
    }

    media = MediaFileUpload(
        str(path),
        mimetype=source_mime,
        resumable=True,
        chunksize=UPLOAD_CHUNK_SIZE_BYTES,
    )

    staged_size = slot.size_bytes
    try:
        created = await asyncio.to_thread(
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, name, mimeType, webViewLink",
                supportsAllDrives=True,
            )
            .execute
        )
    finally:
        # Whatever happened, drop the staged bytes and the slot so neither
        # the disk nor the registry leaks.
        await _store.discard(token)

    link = created.get("webViewLink", "No link available")
    final_mime = created.get("mimeType", target_mime)
    return (
        f"Successfully uploaded file '{created.get('name', file_name)}' "
        f"(ID: {created.get('id', 'N/A')}, MIME: {final_mime}, "
        f"size: {staged_size} bytes) to folder '{folder_id}' for "
        f"{user_google_email}.{conversion_note} Link: {link}"
    )
