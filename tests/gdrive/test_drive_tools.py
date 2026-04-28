"""
Unit tests for Google Drive MCP tools.

Tests create_drive_folder with mocked API responses, plus coverage for
`search_drive_files` and `list_drive_items` pagination, `detailed` output,
and `file_type` filtering behaviors.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from gdrive.drive_helpers import build_drive_list_params
from gdrive.drive_tools import list_drive_items, search_drive_files


def _unwrap(tool):
    """Unwrap a FunctionTool + decorator chain to the original async function.

    Handles both older FastMCP (FunctionTool with .fn) and newer FastMCP
    (server.tool() returns the function directly).
    """
    fn = tool.fn if hasattr(tool, "fn") else tool
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


# ---------------------------------------------------------------------------
# search_drive_files — page_token
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_drive_files_page_token_passed_to_api():
    """page_token is forwarded to the Drive API as pageToken."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "f1",
                "name": "Report.pdf",
                "mimeType": "application/pdf",
                "webViewLink": "https://drive.google.com/file/f1",
                "modifiedTime": "2024-01-01T00:00:00Z",
            }
        ]
    }

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="budget",
        page_token="tok_abc123",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert call_kwargs.get("pageToken") == "tok_abc123"


@pytest.mark.asyncio
async def test_search_drive_files_next_page_token_in_output():
    """nextPageToken from the API response is appended at the end of the output."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "f2",
                "name": "Notes.docx",
                "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "webViewLink": "https://drive.google.com/file/f2",
                "modifiedTime": "2024-02-01T00:00:00Z",
            }
        ],
        "nextPageToken": "next_tok_xyz",
    }

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="notes",
    )

    assert result.endswith("nextPageToken: next_tok_xyz")


@pytest.mark.asyncio
async def test_search_drive_files_no_next_page_token_when_absent():
    """nextPageToken does not appear in output when the API has no more pages."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "f3",
                "name": "Summary.txt",
                "mimeType": "text/plain",
                "webViewLink": "https://drive.google.com/file/f3",
                "modifiedTime": "2024-03-01T00:00:00Z",
            }
        ]
        # no nextPageToken key
    }

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="summary",
    )

    assert "nextPageToken" not in result


# ---------------------------------------------------------------------------
# list_drive_items — page_token
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_drive_items_page_token_passed_to_api(mock_resolve_folder):
    """page_token is forwarded to the Drive API as pageToken."""
    mock_resolve_folder.return_value = "root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "folder1",
                "name": "Archive",
                "mimeType": "application/vnd.google-apps.folder",
                "webViewLink": "https://drive.google.com/drive/folders/folder1",
                "modifiedTime": "2024-01-15T00:00:00Z",
            }
        ]
    }

    await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        page_token="tok_page2",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert call_kwargs.get("pageToken") == "tok_page2"


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_drive_items_next_page_token_in_output(mock_resolve_folder):
    """nextPageToken from the API response is appended at the end of the output."""
    mock_resolve_folder.return_value = "root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "file99",
                "name": "data.csv",
                "mimeType": "text/csv",
                "webViewLink": "https://drive.google.com/file/file99",
                "modifiedTime": "2024-04-01T00:00:00Z",
            }
        ],
        "nextPageToken": "next_list_tok",
    }

    result = await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
    )

    assert result.endswith("nextPageToken: next_list_tok")


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_drive_items_no_next_page_token_when_absent(mock_resolve_folder):
    """nextPageToken does not appear in output when the API has no more pages."""
    mock_resolve_folder.return_value = "root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "file100",
                "name": "readme.txt",
                "mimeType": "text/plain",
                "webViewLink": "https://drive.google.com/file/file100",
                "modifiedTime": "2024-05-01T00:00:00Z",
            }
        ]
        # no nextPageToken key
    }

    result = await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
    )

    assert "nextPageToken" not in result


# ---------------------------------------------------------------------------
# search_drive_files — order_by
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_drive_files_order_by_passed_to_api():
    """order_by is forwarded to the Drive API as orderBy."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "f1",
                "name": "Recent.pdf",
                "mimeType": "application/pdf",
                "webViewLink": "https://drive.google.com/file/f1",
                "modifiedTime": "2024-06-01T00:00:00Z",
            }
        ]
    }

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="test",
        order_by="modifiedTime desc",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert call_kwargs.get("orderBy") == "modifiedTime desc"


@pytest.mark.asyncio
async def test_search_drive_files_order_by_not_set_when_none():
    """orderBy is not included in API call when order_by is None."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "f2",
                "name": "File.txt",
                "mimeType": "text/plain",
                "webViewLink": "https://drive.google.com/file/f2",
                "modifiedTime": "2024-06-02T00:00:00Z",
            }
        ]
    }

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="test",
        # order_by not specified (defaults to None)
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "orderBy" not in call_kwargs


# ---------------------------------------------------------------------------
# list_drive_items — order_by
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_drive_items_order_by_passed_to_api(mock_resolve_folder):
    """order_by is forwarded to the Drive API as orderBy."""
    mock_resolve_folder.return_value = "root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "folder1",
                "name": "Archive",
                "mimeType": "application/vnd.google-apps.folder",
                "webViewLink": "https://drive.google.com/drive/folders/folder1",
                "modifiedTime": "2024-06-01T00:00:00Z",
            }
        ]
    }

    await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        order_by="folder,modifiedTime desc",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert call_kwargs.get("orderBy") == "folder,modifiedTime desc"


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_drive_items_order_by_not_set_when_none(mock_resolve_folder):
    """orderBy is not included in API call when order_by is None."""
    mock_resolve_folder.return_value = "root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            {
                "id": "file1",
                "name": "Document.docx",
                "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "webViewLink": "https://drive.google.com/file/file1",
                "modifiedTime": "2024-06-02T00:00:00Z",
            }
        ]
    }

    await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        # order_by not specified (defaults to None)
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "orderBy" not in call_kwargs


# Helpers
# ---------------------------------------------------------------------------


def _make_file(
    file_id: str,
    name: str,
    mime_type: str,
    link: str = "http://link",
    modified: str = "2024-01-01T00:00:00Z",
    size: str | None = None,
) -> dict:
    item = {
        "id": file_id,
        "name": name,
        "mimeType": mime_type,
        "webViewLink": link,
        "modifiedTime": modified,
    }
    if size is not None:
        item["size"] = size
    return item


# ---------------------------------------------------------------------------
# create_drive_folder
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_drive_folder():
    """Test create_drive_folder returns success message with folder id, name, and link."""
    from gdrive.drive_tools import _create_drive_folder_impl

    mock_service = Mock()
    mock_response = {
        "id": "folder123",
        "name": "My Folder",
        "webViewLink": "https://drive.google.com/drive/folders/folder123",
    }
    mock_request = Mock()
    mock_request.execute.return_value = mock_response
    mock_service.files.return_value.create.return_value = mock_request

    with patch(
        "gdrive.drive_tools.resolve_folder_id",
        new_callable=AsyncMock,
        return_value="root",
    ):
        result = await _create_drive_folder_impl(
            service=mock_service,
            user_google_email="user@example.com",
            folder_name="My Folder",
            parent_folder_id="root",
        )

    assert "Successfully created folder" in result
    assert "My Folder" in result
    assert "folder123" in result
    assert "user@example.com" in result
    assert "https://drive.google.com/drive/folders/folder123" in result


# ---------------------------------------------------------------------------
# build_drive_list_params — detailed flag (pure unit tests, no I/O)
# ---------------------------------------------------------------------------


def test_build_params_detailed_true_includes_extra_fields():
    """detailed=True requests modifiedTime, webViewLink, and size from the API."""
    params = build_drive_list_params(query="name='x'", page_size=10, detailed=True)
    assert "modifiedTime" in params["fields"]
    assert "webViewLink" in params["fields"]
    assert "size" in params["fields"]


def test_build_params_detailed_false_omits_extra_fields():
    """detailed=False omits modifiedTime, webViewLink, and size from the API request."""
    params = build_drive_list_params(query="name='x'", page_size=10, detailed=False)
    assert "modifiedTime" not in params["fields"]
    assert "webViewLink" not in params["fields"]
    assert "size" not in params["fields"]


def test_build_params_detailed_false_keeps_core_fields():
    """detailed=False still requests id, name, and mimeType."""
    params = build_drive_list_params(query="name='x'", page_size=10, detailed=False)
    assert "id" in params["fields"]
    assert "name" in params["fields"]
    assert "mimeType" in params["fields"]


def test_build_params_default_is_detailed():
    """Omitting detailed behaves identically to detailed=True."""
    params_default = build_drive_list_params(query="q", page_size=5)
    params_true = build_drive_list_params(query="q", page_size=5, detailed=True)
    assert params_default["fields"] == params_true["fields"]


def test_build_params_order_by_trims_surrounding_whitespace():
    """order_by is normalized before being sent to the Drive API."""
    params = build_drive_list_params(
        query="q", page_size=5, order_by="  modifiedTime desc  "
    )
    assert params["orderBy"] == "modifiedTime desc"


def test_build_params_order_by_omits_whitespace_only_values():
    """Whitespace-only order_by values are omitted to avoid invalid API requests."""
    params = build_drive_list_params(query="q", page_size=5, order_by="   ")
    assert "orderBy" not in params


# ---------------------------------------------------------------------------
# search_drive_files — detailed flag
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_detailed_true_output_includes_metadata():
    """detailed=True (default) includes modified time and link in output."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file(
                "f1",
                "My Doc",
                "application/vnd.google-apps.document",
                modified="2024-06-01T12:00:00Z",
                link="http://link/f1",
            )
        ]
    }

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="my doc",
        detailed=True,
    )

    assert "My Doc" in result
    assert "2024-06-01T12:00:00Z" in result
    assert "http://link/f1" in result


@pytest.mark.asyncio
async def test_search_detailed_false_output_excludes_metadata():
    """detailed=False omits modified time and link from output."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file(
                "f1",
                "My Doc",
                "application/vnd.google-apps.document",
                modified="2024-06-01T12:00:00Z",
                link="http://link/f1",
            )
        ]
    }

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="my doc",
        detailed=False,
    )

    assert "My Doc" in result
    assert "f1" in result
    assert "2024-06-01T12:00:00Z" not in result
    assert "http://link/f1" not in result


@pytest.mark.asyncio
async def test_search_detailed_true_with_size():
    """When the item has a size field, detailed=True includes it in output."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file("f2", "Big File", "application/pdf", size="102400"),
        ]
    }

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="big",
        detailed=True,
    )

    assert "102400" in result


@pytest.mark.asyncio
async def test_search_detailed_true_requests_extra_api_fields():
    """detailed=True passes full fields string to the Drive API."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="anything",
        detailed=True,
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "modifiedTime" in call_kwargs["fields"]
    assert "webViewLink" in call_kwargs["fields"]
    assert "size" in call_kwargs["fields"]


@pytest.mark.asyncio
async def test_search_detailed_false_requests_compact_api_fields():
    """detailed=False passes compact fields string to the Drive API."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="anything",
        detailed=False,
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "modifiedTime" not in call_kwargs["fields"]
    assert "webViewLink" not in call_kwargs["fields"]
    assert "size" not in call_kwargs["fields"]


@pytest.mark.asyncio
async def test_search_default_detailed_matches_detailed_true():
    """Omitting detailed produces the same output as detailed=True."""
    file = _make_file(
        "f1",
        "Doc",
        "application/vnd.google-apps.document",
        modified="2024-01-01T00:00:00Z",
        link="http://l",
    )

    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": [file]}
    result_default = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="doc",
    )

    mock_service.files().list().execute.return_value = {"files": [file]}
    result_true = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="doc",
        detailed=True,
    )

    assert result_default == result_true


# ---------------------------------------------------------------------------
# list_drive_items — detailed flag
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_detailed_true_output_includes_metadata(mock_resolve_folder):
    """detailed=True (default) includes modified time and link in output."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file(
                "id1",
                "Report",
                "application/vnd.google-apps.document",
                modified="2024-03-15T08:00:00Z",
                link="http://link/id1",
            )
        ]
    }

    result = await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        detailed=True,
    )

    assert "Report" in result
    assert "2024-03-15T08:00:00Z" in result
    assert "http://link/id1" in result


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_detailed_false_output_excludes_metadata(mock_resolve_folder):
    """detailed=False omits modified time and link from output."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file(
                "id1",
                "Report",
                "application/vnd.google-apps.document",
                modified="2024-03-15T08:00:00Z",
                link="http://link/id1",
            )
        ]
    }

    result = await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        detailed=False,
    )

    assert "Report" in result
    assert "id1" in result
    assert "2024-03-15T08:00:00Z" not in result
    assert "http://link/id1" not in result


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_detailed_true_with_size(mock_resolve_folder):
    """When item has a size field, detailed=True includes it in output."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file("id2", "Big File", "application/pdf", size="204800"),
        ]
    }

    result = await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        detailed=True,
    )

    assert "204800" in result


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_detailed_true_requests_extra_api_fields(mock_resolve_folder):
    """detailed=True passes full fields string to the Drive API."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        detailed=True,
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "modifiedTime" in call_kwargs["fields"]
    assert "webViewLink" in call_kwargs["fields"]
    assert "size" in call_kwargs["fields"]


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_detailed_false_requests_compact_api_fields(mock_resolve_folder):
    """detailed=False passes compact fields string to the Drive API."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        detailed=False,
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "modifiedTime" not in call_kwargs["fields"]
    assert "webViewLink" not in call_kwargs["fields"]
    assert "size" not in call_kwargs["fields"]


# ---------------------------------------------------------------------------
# Existing behavior coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_free_text_returns_results():
    """Free-text query is wrapped in fullText contains and results are formatted."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file("f1", "My Doc", "application/vnd.google-apps.document"),
        ]
    }

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="my doc",
    )

    assert "Found 1 files" in result
    assert "My Doc" in result
    assert "f1" in result


@pytest.mark.asyncio
async def test_search_no_results():
    """No results returns a clear message."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="nothing here",
    )

    assert "No files found" in result


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_items_basic(mock_resolve_folder):
    """Basic listing without filters returns all items."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file("id1", "Folder A", "application/vnd.google-apps.folder"),
            _make_file("id2", "Doc B", "application/vnd.google-apps.document"),
        ]
    }

    result = await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
    )

    assert "Found 2 items" in result
    assert "Folder A" in result
    assert "Doc B" in result


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_items_no_results(mock_resolve_folder):
    """Empty folder returns a clear message."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    result = await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
    )

    assert "No items found" in result


# ---------------------------------------------------------------------------
# file_type filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_file_type_folder_adds_mime_filter():
    """file_type='folder' appends the folder MIME type to the query."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [
            _make_file("fold1", "My Folder", "application/vnd.google-apps.folder")
        ]
    }

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="my",
        file_type="folder",
    )

    assert "Found 1 files" in result
    assert "My Folder" in result

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType = 'application/vnd.google-apps.folder'" in call_kwargs["q"]


@pytest.mark.asyncio
async def test_search_file_type_document_alias():
    """Alias 'doc' resolves to the Google Docs MIME type."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="report",
        file_type="doc",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType = 'application/vnd.google-apps.document'" in call_kwargs["q"]


@pytest.mark.asyncio
async def test_search_file_type_plural_alias():
    """Plural aliases are resolved for friendlier natural-language usage."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="project",
        file_type="folders",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType = 'application/vnd.google-apps.folder'" in call_kwargs["q"]


@pytest.mark.asyncio
async def test_search_file_type_sheet_alias():
    """Alias 'sheet' resolves to the Google Sheets MIME type."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="budget",
        file_type="sheet",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType = 'application/vnd.google-apps.spreadsheet'" in call_kwargs["q"]


@pytest.mark.asyncio
async def test_search_file_type_raw_mime():
    """A raw MIME type string is passed through unchanged."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [_make_file("p1", "Report.pdf", "application/pdf")]
    }

    result = await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="report",
        file_type="application/pdf",
    )

    assert "Report.pdf" in result
    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType = 'application/pdf'" in call_kwargs["q"]


@pytest.mark.asyncio
async def test_search_file_type_none_no_mime_filter():
    """When file_type is None no mimeType clause is added to the query."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="anything",
        file_type=None,
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType" not in call_kwargs["q"]


@pytest.mark.asyncio
async def test_search_file_type_structured_query_combined():
    """file_type filter is appended even when the query is already structured."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="name contains 'budget'",
        file_type="spreadsheet",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    q = call_kwargs["q"]
    assert "name contains 'budget'" in q
    assert "mimeType = 'application/vnd.google-apps.spreadsheet'" in q


@pytest.mark.asyncio
async def test_search_file_type_unknown_raises_value_error():
    """An unrecognised friendly type name raises ValueError immediately."""
    mock_service = Mock()

    with pytest.raises(ValueError, match="Unknown file_type"):
        await _unwrap(search_drive_files)(
            service=mock_service,
            user_google_email="user@example.com",
            query="something",
            file_type="notatype",
        )


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_items_file_type_folder_adds_mime_filter(mock_resolve_folder):
    """file_type='folder' appends the folder MIME clause to the query."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {
        "files": [_make_file("sub1", "SubFolder", "application/vnd.google-apps.folder")]
    }

    result = await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        file_type="folder",
    )

    assert "Found 1 items" in result
    assert "SubFolder" in result

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    q = call_kwargs["q"]
    assert "'resolved_root' in parents" in q
    assert "trashed=false" in q
    assert "mimeType = 'application/vnd.google-apps.folder'" in q


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_items_file_type_spreadsheet(mock_resolve_folder):
    """file_type='spreadsheet' appends the Sheets MIME clause."""
    mock_resolve_folder.return_value = "folder_xyz"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="folder_xyz",
        file_type="spreadsheet",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType = 'application/vnd.google-apps.spreadsheet'" in call_kwargs["q"]


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_items_file_type_raw_mime(mock_resolve_folder):
    """A raw MIME type string is passed through unchanged."""
    mock_resolve_folder.return_value = "folder_abc"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="folder_abc",
        file_type="application/pdf",
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType = 'application/pdf'" in call_kwargs["q"]


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_items_file_type_none_no_mime_filter(mock_resolve_folder):
    """When file_type is None no mimeType clause is added."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(list_drive_items)(
        service=mock_service,
        user_google_email="user@example.com",
        folder_id="root",
        file_type=None,
    )

    call_kwargs = mock_service.files.return_value.list.call_args.kwargs
    assert "mimeType" not in call_kwargs["q"]


@pytest.mark.asyncio
@patch("gdrive.drive_tools.resolve_folder_id", new_callable=AsyncMock)
async def test_list_items_file_type_unknown_raises(mock_resolve_folder):
    """An unrecognised friendly type name raises ValueError."""
    mock_resolve_folder.return_value = "resolved_root"
    mock_service = Mock()

    with pytest.raises(ValueError, match="Unknown file_type"):
        await _unwrap(list_drive_items)(
            service=mock_service,
            user_google_email="user@example.com",
            folder_id="root",
            file_type="unknowntype",
        )


# ---------------------------------------------------------------------------
# OR-precedence grouping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_or_query_is_grouped_before_mime_filter():
    """An OR structured query is wrapped in parentheses so MIME filter precedence is correct."""
    mock_service = Mock()
    mock_service.files().list().execute.return_value = {"files": []}

    await _unwrap(search_drive_files)(
        service=mock_service,
        user_google_email="user@example.com",
        query="name contains 'a' or name contains 'b'",
        file_type="document",
    )

    q = mock_service.files.return_value.list.call_args.kwargs["q"]
    assert q.startswith("(")
    assert "name contains 'a' or name contains 'b'" in q
    assert ") and mimeType = 'application/vnd.google-apps.document'" in q


# ---------------------------------------------------------------------------
# MIME type validation
# ---------------------------------------------------------------------------


def test_resolve_file_type_mime_invalid_mime_raises():
    """A raw string with '/' but containing quotes raises ValueError."""
    from gdrive.drive_helpers import resolve_file_type_mime

    with pytest.raises(ValueError, match="Invalid MIME type"):
        resolve_file_type_mime("application/pdf' or '1'='1")


def test_resolve_file_type_mime_strips_whitespace():
    """Leading/trailing whitespace is stripped from raw MIME strings."""
    from gdrive.drive_helpers import resolve_file_type_mime

    assert resolve_file_type_mime("  application/pdf  ") == "application/pdf"


def test_resolve_file_type_mime_normalizes_case():
    """Raw MIME types are normalized to lowercase for Drive query consistency."""
    from gdrive.drive_helpers import resolve_file_type_mime

    assert resolve_file_type_mime("Application/PDF") == "application/pdf"


def test_resolve_file_type_mime_empty_raises():
    """Blank values are rejected with a clear validation error."""
    from gdrive.drive_helpers import resolve_file_type_mime

    with pytest.raises(ValueError, match="cannot be empty"):
        resolve_file_type_mime("   ")


# ---------------------------------------------------------------------------
# upload_drive_file
# ---------------------------------------------------------------------------


def _make_drive_create_mock(response: dict) -> Mock:
    """Build a mock service whose files().create().execute() returns response."""
    mock_service = Mock()
    mock_request = Mock()
    mock_request.execute.return_value = response
    mock_service.files.return_value.create.return_value = mock_request
    return mock_service


@pytest.mark.asyncio
async def test_upload_drive_file_basic_xlsx():
    """Standard base64 xlsx upload preserves source MIME and reports size."""
    import base64
    from gdrive.drive_tools import upload_drive_file

    payload = b"PK\x03\x04 fake-xlsx-bytes"
    b64 = base64.b64encode(payload).decode("ascii")

    mock_service = _make_drive_create_mock({
        "id": "file_xlsx_1",
        "name": "report.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "webViewLink": "https://drive.google.com/file/d/file_xlsx_1/view",
    })

    fn = _unwrap(upload_drive_file)
    with patch(
        "gdrive.drive_tools.resolve_folder_id",
        new_callable=AsyncMock,
        return_value="root",
    ):
        result = await fn(
            service=mock_service,
            user_google_email="user@example.com",
            file_name="report.xlsx",
            content_base64=b64,
            folder_id="root",
            mime_type=None,
            convert_to_google_format=False,
        )

    # Inspect what was passed to the API
    create_call = mock_service.files.return_value.create.call_args
    body = create_call.kwargs["body"]
    assert body["name"] == "report.xlsx"
    assert body["parents"] == ["root"]
    # MIME inferred from .xlsx extension, no conversion
    assert body["mimeType"] == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert create_call.kwargs["supportsAllDrives"] is True

    assert "Successfully uploaded" in result
    assert "file_xlsx_1" in result
    assert f"{len(payload)} bytes" in result


@pytest.mark.asyncio
async def test_upload_drive_file_converts_xlsx_to_sheets():
    """convert_to_google_format=True remaps xlsx MIME to Google Sheets in metadata."""
    import base64
    from gdrive.drive_tools import upload_drive_file

    payload = b"PK\x03\x04 fake-xlsx"
    b64 = base64.b64encode(payload).decode("ascii")

    mock_service = _make_drive_create_mock({
        "id": "sheet_1",
        "name": "report",
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "webViewLink": "https://docs.google.com/spreadsheets/d/sheet_1/edit",
    })

    fn = _unwrap(upload_drive_file)
    with patch(
        "gdrive.drive_tools.resolve_folder_id",
        new_callable=AsyncMock,
        return_value="0AJdQD--8bjkkUk9PVA",
    ):
        result = await fn(
            service=mock_service,
            user_google_email="user@example.com",
            file_name="report.xlsx",
            content_base64=b64,
            folder_id="0AJdQD--8bjkkUk9PVA",
            mime_type=None,
            convert_to_google_format=True,
        )

    body = mock_service.files.return_value.create.call_args.kwargs["body"]
    # Target MIME swapped to native Google Sheets
    assert body["mimeType"] == "application/vnd.google-apps.spreadsheet"
    # Parent is the resolved shared drive folder
    assert body["parents"] == ["0AJdQD--8bjkkUk9PVA"]
    assert "sheet_1" in result


@pytest.mark.asyncio
async def test_upload_drive_file_conversion_unsupported_falls_through():
    """Unsupported source MIME with convert flag set: upload as-is + note in result."""
    import base64
    from gdrive.drive_tools import upload_drive_file

    payload = b"\x89PNG\r\n\x1a\n fake-png"
    b64 = base64.b64encode(payload).decode("ascii")

    mock_service = _make_drive_create_mock({
        "id": "img_1",
        "name": "screenshot.png",
        "mimeType": "image/png",
        "webViewLink": "https://drive.google.com/file/d/img_1/view",
    })

    fn = _unwrap(upload_drive_file)
    with patch(
        "gdrive.drive_tools.resolve_folder_id",
        new_callable=AsyncMock,
        return_value="root",
    ):
        result = await fn(
            service=mock_service,
            user_google_email="user@example.com",
            file_name="screenshot.png",
            content_base64=b64,
            folder_id="root",
            mime_type=None,
            convert_to_google_format=True,
        )

    body = mock_service.files.return_value.create.call_args.kwargs["body"]
    # PNG has no native Google equivalent — stays image/png
    assert body["mimeType"] == "image/png"
    assert "no native Google equivalent" in result


@pytest.mark.asyncio
async def test_upload_drive_file_explicit_mime_overrides_extension():
    """Explicit mime_type wins over extension-based inference."""
    import base64
    from gdrive.drive_tools import upload_drive_file

    payload = b"hello"
    b64 = base64.b64encode(payload).decode("ascii")

    mock_service = _make_drive_create_mock({
        "id": "f1",
        "name": "weird_name",
        "mimeType": "application/pdf",
        "webViewLink": "https://drive.google.com/file/d/f1/view",
    })

    fn = _unwrap(upload_drive_file)
    with patch(
        "gdrive.drive_tools.resolve_folder_id",
        new_callable=AsyncMock,
        return_value="root",
    ):
        await fn(
            service=mock_service,
            user_google_email="user@example.com",
            file_name="weird_name",  # no extension
            content_base64=b64,
            folder_id="root",
            mime_type="application/pdf",
            convert_to_google_format=False,
        )

    body = mock_service.files.return_value.create.call_args.kwargs["body"]
    assert body["mimeType"] == "application/pdf"


@pytest.mark.asyncio
async def test_upload_drive_file_unknown_extension_falls_back_to_octet_stream():
    """Unknown extension and no explicit MIME → application/octet-stream."""
    import base64
    from gdrive.drive_tools import upload_drive_file

    payload = b"raw-bytes"
    b64 = base64.b64encode(payload).decode("ascii")

    mock_service = _make_drive_create_mock({
        "id": "f1",
        "name": "blob.xyz",
        "mimeType": "application/octet-stream",
        "webViewLink": "https://drive.google.com/file/d/f1/view",
    })

    fn = _unwrap(upload_drive_file)
    with patch(
        "gdrive.drive_tools.resolve_folder_id",
        new_callable=AsyncMock,
        return_value="root",
    ):
        await fn(
            service=mock_service,
            user_google_email="user@example.com",
            file_name="blob.xyz",
            content_base64=b64,
            folder_id="root",
            mime_type=None,
            convert_to_google_format=False,
        )

    body = mock_service.files.return_value.create.call_args.kwargs["body"]
    assert body["mimeType"] == "application/octet-stream"


@pytest.mark.asyncio
async def test_upload_drive_file_rejects_empty_base64():
    """Empty content_base64 raises with a clear message."""
    from gdrive.drive_tools import upload_drive_file

    fn = _unwrap(upload_drive_file)
    with pytest.raises(Exception, match="must not be empty"):
        await fn(
            service=Mock(),
            user_google_email="user@example.com",
            file_name="x.txt",
            content_base64="",
        )


@pytest.mark.asyncio
async def test_upload_drive_file_rejects_invalid_base64():
    """Non-base64 garbage raises a decoding error."""
    from gdrive.drive_tools import upload_drive_file

    fn = _unwrap(upload_drive_file)
    with pytest.raises(Exception, match="Failed to decode"):
        await fn(
            service=Mock(),
            user_google_email="user@example.com",
            file_name="x.txt",
            content_base64="!!!not base64@@@",
        )


@pytest.mark.asyncio
async def test_upload_drive_file_tolerates_missing_padding_and_whitespace():
    """Whitespace/newlines and missing '=' padding are tolerated."""
    import base64
    from gdrive.drive_tools import upload_drive_file

    payload = b"abc"  # encodes to 'YWJj' (no padding) → after wrap, normalize
    b64 = base64.b64encode(payload).decode("ascii").rstrip("=")
    # Inject whitespace and a newline
    b64_messy = b64[:2] + " \n" + b64[2:]

    mock_service = _make_drive_create_mock({
        "id": "f1",
        "name": "x.txt",
        "mimeType": "text/plain",
        "webViewLink": "https://drive.google.com/file/d/f1/view",
    })

    fn = _unwrap(upload_drive_file)
    with patch(
        "gdrive.drive_tools.resolve_folder_id",
        new_callable=AsyncMock,
        return_value="root",
    ):
        result = await fn(
            service=mock_service,
            user_google_email="user@example.com",
            file_name="x.txt",
            content_base64=b64_messy,
        )

    assert "Successfully uploaded" in result
    assert f"{len(payload)} bytes" in result


@pytest.mark.asyncio
async def test_upload_drive_file_rejects_oversize_payload():
    """Payload above the configured limit is rejected before upload."""
    import base64
    from gdrive import drive_tools as dt

    # Tiny limit so the test is fast. Patch the module constant.
    payload = b"x" * 1024
    b64 = base64.b64encode(payload).decode("ascii")

    fn = _unwrap(dt.upload_drive_file)
    with patch.object(dt, "UPLOAD_DRIVE_FILE_MAX_BYTES", 100):
        with pytest.raises(Exception, match="exceeds"):
            await fn(
                service=Mock(),
                user_google_email="user@example.com",
                file_name="big.bin",
                content_base64=b64,
            )
