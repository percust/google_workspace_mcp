"""
Google Sheets MCP Tools - Extended (Phase A+B+C additions, May 2026)

Adds advanced spreadsheet operations:
  Phase A (must-have):
    - merge_sheet_range / unmerge_sheet_range
    - set_range_borders
    - set_data_validation
    - find_replace_sheet
    - sort_range
  Phase B (often needed):
    - manage_named_range
    - manage_protected_range
    - manage_dimension_group
    - manage_filter_view
    - clear_range_formatting
  Phase C (occasional):
    - manage_chart
    - copy_paste_range
    - insert_cells_with_shift

Note: format_sheet_range extensions (font_family, strikethrough, underline,
text_rotation) live in sheets_tools.py — kept there to extend the existing tool
rather than create a parallel one.
"""

import logging
import asyncio
import json
import time
import uuid
from typing import Any, List, Optional, Union

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors, UserInputError
from gsheets.sheets_helpers import (
    _column_to_index,
    _parse_a1_range,
    _parse_hex_color,
    _select_sheet,
)

logger = logging.getLogger(__name__)


# ---------- Phase 7.4: soft-confirm tokens for destructive ops ----------
#
# In-memory only; the workspace-mcp container is a single process so this
# survives without external state. TTL is short (5 min) and the dict is
# bounded by lazy cleanup on every access. If we ever scale to multiple
# replicas, swap this for Redis or an explicit DB-backed store — but the
# tool's interface (two-call dance, no client-side state) stays the same.

_DESTRUCTIVE_CONFIRM_TTL_SECONDS = 300
_pending_destructive_confirms: dict = {}


def _cleanup_pending_confirms() -> None:
    now = time.time()
    expired = [
        tok
        for tok, rec in _pending_destructive_confirms.items()
        if now - rec.get("created_at", 0) > _DESTRUCTIVE_CONFIRM_TTL_SECONDS
    ]
    for tok in expired:
        _pending_destructive_confirms.pop(tok, None)


def _issue_destructive_confirm(op: str, payload: dict) -> str:
    """Generate a one-shot confirm token for an irreversible op. Returns the token."""
    _cleanup_pending_confirms()
    token = uuid.uuid4().hex
    _pending_destructive_confirms[token] = {
        "op": op,
        "payload": payload,
        "created_at": time.time(),
    }
    return token


def _consume_destructive_confirm(op: str, token: str, payload: dict) -> None:
    """Validate and pop a confirm token. Raises UserInputError on mismatch/expiry."""
    _cleanup_pending_confirms()
    rec = _pending_destructive_confirms.pop(token, None)
    if rec is None:
        raise UserInputError(
            f"confirm_token '{token[:8]}...' is unknown or expired. "
            f"Call the tool again without confirm_token to obtain a fresh one."
        )
    if rec.get("op") != op:
        raise UserInputError(
            f"confirm_token was issued for op '{rec.get('op')}', not '{op}'."
        )
    saved_payload = rec.get("payload", {})
    for key, expected in saved_payload.items():
        if payload.get(key) != expected:
            raise UserInputError(
                f"confirm_token does not match current arguments "
                f"(field '{key}' differs). Re-request a fresh token."
            )


# ---------- Shared utilities ----------


def _parse_json(value, name):
    """Parse a JSON string parameter, or return as-is if already parsed."""
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise UserInputError(f"Invalid JSON for {name}: {e}")


async def _fetch_sheets_metadata(service, spreadsheet_id: str) -> list:
    """Fetch lightweight sheets metadata (sheetId, title)."""
    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute
    )
    sheets = metadata.get("sheets", [])
    if not sheets:
        raise UserInputError("No sheets found in spreadsheet.")
    return sheets


async def _fetch_sheets_with(service, spreadsheet_id: str, extra: str) -> list:
    """Fetch sheets with an extended fields mask, e.g. 'merges',
    'conditionalFormats', 'basicFilter', 'protectedRanges'.

    Kept separate from _fetch_sheets_metadata which is intentionally cheap
    and used for sheetId/title lookups across many tools.
    """
    fields = f"sheets(properties(sheetId,title),{extra})"
    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields=fields)
        .execute
    )
    sheets = metadata.get("sheets", [])
    if not sheets:
        raise UserInputError("No sheets found in spreadsheet.")
    return sheets


def _build_border(border_spec: dict) -> dict:
    """Build a Sheets API border object from {style, color, width}."""
    if not isinstance(border_spec, dict):
        raise UserInputError("Each border spec must be a dict.")
    style = border_spec.get("style", "SOLID")
    allowed_styles = {
        "DOTTED", "DASHED", "SOLID", "SOLID_MEDIUM", "SOLID_THICK",
        "NONE", "DOUBLE",
    }
    if style not in allowed_styles:
        raise UserInputError(
            f"Border style '{style}' invalid. Use one of {sorted(allowed_styles)}."
        )
    border: dict = {"style": style}
    color = border_spec.get("color")
    if color:
        parsed = _parse_hex_color(color)
        if parsed:
            border["color"] = parsed
    width = border_spec.get("width")
    if width is not None:
        if not isinstance(width, (int, float)) or width <= 0:
            raise UserInputError("Border width must be a positive number.")
        border["width"] = int(width)
    return border


# ============================================================================
# Phase A — must-have
# ============================================================================


@server.tool()
@handle_http_errors("merge_sheet_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def merge_sheet_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    merge_type: str = "MERGE_ALL",
) -> str:
    """
    Merges cells within a range.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        range_name (str): A1-style range, e.g. "Sheet1!E1:F1". Required.
        merge_type (str): MERGE_ALL (default), MERGE_COLUMNS, or MERGE_ROWS.

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[merge_sheet_range] %s, %s, range=%s, type=%s",
        user_google_email, spreadsheet_id, range_name, merge_type,
    )
    allowed = {"MERGE_ALL", "MERGE_COLUMNS", "MERGE_ROWS"}
    if merge_type not in allowed:
        raise UserInputError(
            f"merge_type must be one of {sorted(allowed)}, got '{merge_type}'."
        )
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    grid_range = _parse_a1_range(range_name, sheets)
    # Phase 7.2: unmerge the same range first inside the same batchUpdate, so the
    # operation is idempotent and survives both pre-existing overlapping merges
    # and freshly-created sheets (Sheets API otherwise yells "You must select
    # all cells in a merged range"). Unmerge on a range with no merges is a no-op.
    body = {
        "requests": [
            {"unmergeCells": {"range": grid_range}},
            {"mergeCells": {"range": grid_range, "mergeType": merge_type}},
        ]
    }
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    return (
        f"Merged range '{range_name}' (type={merge_type}) in spreadsheet "
        f"{spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("unmerge_sheet_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def unmerge_sheet_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
) -> str:
    """
    Unmerges any merged cells overlapping the range.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        range_name (str): A1-style range. Required.

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[unmerge_sheet_range] %s, %s, range=%s",
        user_google_email, spreadsheet_id, range_name,
    )
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    grid_range = _parse_a1_range(range_name, sheets)
    body = {"requests": [{"unmergeCells": {"range": grid_range}}]}
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    return (
        f"Unmerged range '{range_name}' in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("set_range_borders", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def set_range_borders(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    top: Optional[Union[str, dict]] = None,
    bottom: Optional[Union[str, dict]] = None,
    left: Optional[Union[str, dict]] = None,
    right: Optional[Union[str, dict]] = None,
    inner_horizontal: Optional[Union[str, dict]] = None,
    inner_vertical: Optional[Union[str, dict]] = None,
    all_borders: Optional[Union[str, dict]] = None,
) -> str:
    """
    Sets borders on a range. Each border accepts a dict {style, color, width}
    or a JSON string of the same. all_borders shortcut applies the same border
    spec to top/bottom/left/right/inner_horizontal/inner_vertical at once.

    Border styles: DOTTED, DASHED, SOLID (default), SOLID_MEDIUM, SOLID_THICK,
    DOUBLE, NONE (use NONE to clear).

    Example border spec: {"style": "SOLID", "color": "#000000"}

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        range_name (str): A1-style range. Required.
        top, bottom, left, right (dict|str): Border specs per side.
        inner_horizontal, inner_vertical (dict|str): Inner grid borders.
        all_borders (dict|str): Shortcut for all six sides.

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[set_range_borders] %s, %s, range=%s",
        user_google_email, spreadsheet_id, range_name,
    )
    sides = {
        "top": _parse_json(top, "top"),
        "bottom": _parse_json(bottom, "bottom"),
        "left": _parse_json(left, "left"),
        "right": _parse_json(right, "right"),
        "innerHorizontal": _parse_json(inner_horizontal, "inner_horizontal"),
        "innerVertical": _parse_json(inner_vertical, "inner_vertical"),
    }
    all_b = _parse_json(all_borders, "all_borders")
    if all_b is not None:
        for k in sides:
            if sides[k] is None:
                sides[k] = all_b
    if not any(v is not None for v in sides.values()):
        raise UserInputError(
            "Provide at least one of: top, bottom, left, right, "
            "inner_horizontal, inner_vertical, or all_borders."
        )
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    grid_range = _parse_a1_range(range_name, sheets)
    update_borders: dict = {"range": grid_range}
    for k, v in sides.items():
        if v is not None:
            update_borders[k] = _build_border(v)
    body = {"requests": [{"updateBorders": update_borders}]}
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    applied = [k for k, v in sides.items() if v is not None]
    return (
        f"Set borders ({', '.join(applied)}) on '{range_name}' in spreadsheet "
        f"{spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("set_data_validation", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def set_data_validation(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    condition_type: str,
    values: Optional[Union[str, List]] = None,
    source_range: Optional[str] = None,
    strict: bool = True,
    show_dropdown: bool = True,
    input_message: Optional[str] = None,
    clear: bool = False,
) -> str:
    """
    Sets data validation (dropdowns, number/date constraints) on a range.

    Condition types (Sheets API ConditionType):
      ONE_OF_LIST           — values: list of allowed strings/numbers
      ONE_OF_RANGE          — source_range: A1 range with allowed values
      NUMBER_GREATER, NUMBER_GREATER_THAN_EQ, NUMBER_LESS,
      NUMBER_LESS_THAN_EQ, NUMBER_EQ, NUMBER_NOT_EQ — values: [number]
      NUMBER_BETWEEN, NUMBER_NOT_BETWEEN           — values: [low, high]
      TEXT_CONTAINS, TEXT_NOT_CONTAINS, TEXT_STARTS_WITH,
      TEXT_ENDS_WITH, TEXT_EQ, TEXT_IS_EMAIL, TEXT_IS_URL — values: [str]
      DATE_BEFORE, DATE_AFTER, DATE_ON_OR_BEFORE, DATE_ON_OR_AFTER,
      DATE_EQ, DATE_BETWEEN, DATE_NOT_BETWEEN, DATE_IS_VALID
      BLANK, NOT_BLANK
      BOOLEAN — values: [] for plain checkbox, or [true_val, false_val]
      CUSTOM_FORMULA — values: ["=YOUR_FORMULA"]

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        range_name (str): A1-style target range. Required.
        condition_type (str): Type of validation rule. Required.
        values (list|str): Values for the condition (see above).
        source_range (str): A1 range for ONE_OF_RANGE.
        strict (bool): Reject invalid input (True) or just warn (False).
        show_dropdown (bool): Show dropdown UI for ONE_OF_* types.
        input_message (str): Tooltip text shown when the cell is selected.
        clear (bool): If True, remove validation from the range (ignore other args).

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[set_data_validation] %s, %s, range=%s, type=%s",
        user_google_email, spreadsheet_id, range_name, condition_type,
    )
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    grid_range = _parse_a1_range(range_name, sheets)

    if clear:
        body = {
            "requests": [
                {"setDataValidation": {"range": grid_range}}  # no rule = clear
            ]
        }
        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute
        )
        return (
            f"Cleared data validation on '{range_name}' in spreadsheet "
            f"{spreadsheet_id} for {user_google_email}."
        )

    condition: dict = {"type": condition_type.upper()}

    if condition_type.upper() == "ONE_OF_RANGE":
        if not source_range:
            raise UserInputError("ONE_OF_RANGE requires source_range.")
        condition["values"] = [{"userEnteredValue": "=" + source_range}]
    else:
        parsed_values = _parse_json(values, "values")
        if parsed_values is not None:
            if not isinstance(parsed_values, list):
                parsed_values = [parsed_values]
            condition["values"] = [
                {"userEnteredValue": str(v)} for v in parsed_values
            ]

    rule: dict = {"condition": condition, "strict": strict, "showCustomUi": show_dropdown}
    if input_message:
        rule["inputMessage"] = input_message

    body = {
        "requests": [
            {"setDataValidation": {"range": grid_range, "rule": rule}}
        ]
    }
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    return (
        f"Set data validation ({condition_type}) on '{range_name}' in spreadsheet "
        f"{spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("find_replace_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def find_replace_sheet(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    find: str,
    replacement: str,
    range_name: Optional[str] = None,
    sheet_name: Optional[str] = None,
    all_sheets: bool = False,
    match_case: bool = False,
    match_entire_cell: bool = False,
    search_by_regex: bool = False,
    include_formulas: bool = False,
) -> str:
    """
    Finds and replaces text in a range, a whole sheet, or all sheets.

    Specify exactly one scope: range_name (A1 range), sheet_name (whole sheet),
    or all_sheets=True (entire spreadsheet).

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        find (str): String/pattern to find. Required.
        replacement (str): Replacement string. Required.
        range_name (str): Optional A1-style range to limit scope.
        sheet_name (str): Optional sheet name (whole sheet scope).
        all_sheets (bool): Scope = entire spreadsheet.
        match_case (bool): Case-sensitive search.
        match_entire_cell (bool): Whole-cell match only.
        search_by_regex (bool): Interpret find as a regex.
        include_formulas (bool): Also search inside cell formulas.

    Returns:
        str: Confirmation including occurrencesChanged/valuesChanged stats.
    """
    logger.info(
        "[find_replace_sheet] %s, %s, find=%r",
        user_google_email, spreadsheet_id, find,
    )
    scope_count = sum(bool(x) for x in (range_name, sheet_name, all_sheets))
    if scope_count != 1:
        raise UserInputError(
            "Specify exactly one scope: range_name, sheet_name, or all_sheets=True."
        )
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    fr: dict = {
        "find": find,
        "replacement": replacement,
        "matchCase": match_case,
        "matchEntireCell": match_entire_cell,
        "searchByRegex": search_by_regex,
        "includeFormulas": include_formulas,
    }
    if range_name:
        fr["range"] = _parse_a1_range(range_name, sheets)
    elif sheet_name:
        sheet = _select_sheet(sheets, sheet_name)
        fr["sheetId"] = sheet["properties"]["sheetId"]
    else:
        fr["allSheets"] = True

    body = {"requests": [{"findReplace": fr}]}
    resp = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    replies = resp.get("replies", [])
    stats = replies[0].get("findReplace", {}) if replies else {}
    return (
        f"find_replace: occurrencesChanged={stats.get('occurrencesChanged', 0)}, "
        f"valuesChanged={stats.get('valuesChanged', 0)}, "
        f"rowsChanged={stats.get('rowsChanged', 0)}, "
        f"sheetsChanged={stats.get('sheetsChanged', 0)} "
        f"in {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("sort_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def sort_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    sort_specs: Union[str, List[dict]],
) -> str:
    """
    Sorts a range by one or more columns.

    sort_specs is a list of dicts: [{"column": "A", "order": "ASCENDING"}, ...]
    column can be a letter ("A") or 0-based index relative to the sheet.
    order: ASCENDING or DESCENDING (default ASCENDING).

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        range_name (str): A1-style range to sort. Required.
        sort_specs (list|str): Sort specifications. Required.

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[sort_range] %s, %s, range=%s",
        user_google_email, spreadsheet_id, range_name,
    )
    specs = _parse_json(sort_specs, "sort_specs")
    if not isinstance(specs, list) or not specs:
        raise UserInputError("sort_specs must be a non-empty list.")
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    grid_range = _parse_a1_range(range_name, sheets)
    api_specs = []
    for spec in specs:
        if not isinstance(spec, dict):
            raise UserInputError("Each sort spec must be a dict.")
        col = spec.get("column")
        if col is None:
            raise UserInputError("sort spec missing 'column'.")
        if isinstance(col, str):
            col_idx = _column_to_index(col.upper())
            if col_idx is None:
                raise UserInputError(f"Invalid column letter: '{col}'.")
        else:
            col_idx = int(col)
        order = spec.get("order", "ASCENDING").upper()
        if order not in {"ASCENDING", "DESCENDING"}:
            raise UserInputError("order must be ASCENDING or DESCENDING.")
        api_specs.append({"dimensionIndex": col_idx, "sortOrder": order})
    body = {
        "requests": [
            {"sortRange": {"range": grid_range, "sortSpecs": api_specs}}
        ]
    }
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    return (
        f"Sorted '{range_name}' by {len(api_specs)} key(s) in spreadsheet "
        f"{spreadsheet_id} for {user_google_email}."
    )


# ============================================================================
# Phase B — often needed
# ============================================================================


@server.tool()
@handle_http_errors("manage_named_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_named_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    name: Optional[str] = None,
    range_name: Optional[str] = None,
    named_range_id: Optional[str] = None,
) -> str:
    """
    Manages named ranges. Action: create, update, delete, list.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        action (str): create | update | delete | list. Required.
        name (str): Named range name (create/update).
        range_name (str): A1-style range (create/update).
        named_range_id (str): ID (update/delete).

    Returns:
        str: Confirmation or list of named ranges.
    """
    logger.info(
        "[manage_named_range] %s, %s, action=%s",
        user_google_email, spreadsheet_id, action,
    )
    action = action.lower()
    if action == "list":
        full = await asyncio.to_thread(
            service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="namedRanges")
            .execute
        )
        named = full.get("namedRanges", [])
        if not named:
            return f"No named ranges in {spreadsheet_id}."
        lines = [
            f"- {nr.get('name')} (id={nr.get('namedRangeId')})"
            for nr in named
        ]
        return f"Named ranges in {spreadsheet_id}:\n" + "\n".join(lines)

    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    requests: list = []

    if action == "create":
        if not name or not range_name:
            raise UserInputError("create requires name and range_name.")
        grid = _parse_a1_range(range_name, sheets)
        requests.append(
            {"addNamedRange": {"namedRange": {"name": name, "range": grid}}}
        )
    elif action == "update":
        if not named_range_id:
            raise UserInputError("update requires named_range_id.")
        nr: dict = {"namedRangeId": named_range_id}
        fields = []
        if name:
            nr["name"] = name
            fields.append("name")
        if range_name:
            nr["range"] = _parse_a1_range(range_name, sheets)
            fields.append("range")
        if not fields:
            raise UserInputError("update requires name and/or range_name.")
        requests.append(
            {"updateNamedRange": {"namedRange": nr, "fields": ",".join(fields)}}
        )
    elif action == "delete":
        if not named_range_id:
            raise UserInputError("delete requires named_range_id.")
        requests.append({"deleteNamedRange": {"namedRangeId": named_range_id}})
    else:
        raise UserInputError(f"Unknown action '{action}'.")

    resp = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
        .execute
    )
    replies = resp.get("replies", [])
    extra = ""
    if action == "create" and replies:
        new_id = replies[0].get("addNamedRange", {}).get("namedRange", {}).get("namedRangeId")
        extra = f" (id={new_id})"
    return (
        f"Named range {action}{extra} in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("manage_protected_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_protected_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    range_name: Optional[str] = None,
    sheet_name: Optional[str] = None,
    protected_range_id: Optional[int] = None,
    description: Optional[str] = None,
    warning_only: bool = False,
    editors_emails: Optional[Union[str, List[str]]] = None,
    domain_users_can_edit: Optional[bool] = None,
) -> str:
    """
    Manages protected ranges. Action: create, update, delete, list.

    Protect a range (A1) or a whole sheet (sheet_name).

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        action (str): create | update | delete | list. Required.
        range_name (str): A1-style range (create).
        sheet_name (str): Whole-sheet protection (create).
        protected_range_id (int): ID (update/delete).
        description (str): Optional description.
        warning_only (bool): True = show warning on edit, allow anyway.
        editors_emails (list|str): Explicit list of editor emails.
        domain_users_can_edit (bool): Allow any domain user (overrides editors).

    Returns:
        str: Confirmation or list.
    """
    logger.info(
        "[manage_protected_range] %s, %s, action=%s",
        user_google_email, spreadsheet_id, action,
    )
    action = action.lower()
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)

    if action == "list":
        full = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(sheetId,title),protectedRanges)",
            )
            .execute
        )
        lines = []
        for sh in full.get("sheets", []):
            title = sh.get("properties", {}).get("title")
            for pr in sh.get("protectedRanges", []):
                lines.append(
                    f"- sheet='{title}' id={pr.get('protectedRangeId')} "
                    f"desc='{pr.get('description', '')}' "
                    f"warning_only={pr.get('warningOnly', False)}"
                )
        if not lines:
            return f"No protected ranges in {spreadsheet_id}."
        return f"Protected ranges in {spreadsheet_id}:\n" + "\n".join(lines)

    requests: list = []
    if action == "create":
        protected: dict = {"warningOnly": warning_only}
        if range_name:
            protected["range"] = _parse_a1_range(range_name, sheets)
        elif sheet_name:
            sheet = _select_sheet(sheets, sheet_name)
            protected["range"] = {"sheetId": sheet["properties"]["sheetId"]}
        else:
            raise UserInputError("create requires range_name or sheet_name.")
        if description:
            protected["description"] = description
        if not warning_only:
            editors = _parse_json(editors_emails, "editors_emails")
            editors_obj: dict = {}
            if editors:
                if isinstance(editors, str):
                    editors = [editors]
                editors_obj["users"] = editors
            if domain_users_can_edit is not None:
                editors_obj["domainUsersCanEdit"] = domain_users_can_edit
            if editors_obj:
                protected["editors"] = editors_obj
        requests.append({"addProtectedRange": {"protectedRange": protected}})
    elif action == "update":
        if protected_range_id is None:
            raise UserInputError("update requires protected_range_id.")
        protected = {"protectedRangeId": protected_range_id}
        fields = []
        if description is not None:
            protected["description"] = description
            fields.append("description")
        if warning_only is not None:
            protected["warningOnly"] = warning_only
            fields.append("warningOnly")
        if range_name:
            protected["range"] = _parse_a1_range(range_name, sheets)
            fields.append("range")
        if not fields:
            raise UserInputError("update requires at least one field to change.")
        requests.append(
            {"updateProtectedRange": {
                "protectedRange": protected, "fields": ",".join(fields)
            }}
        )
    elif action == "delete":
        if protected_range_id is None:
            raise UserInputError("delete requires protected_range_id.")
        requests.append({"deleteProtectedRange": {"protectedRangeId": protected_range_id}})
    else:
        raise UserInputError(f"Unknown action '{action}'.")

    resp = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
        .execute
    )
    extra = ""
    if action == "create":
        replies = resp.get("replies", [])
        if replies:
            new_id = replies[0].get("addProtectedRange", {}).get("protectedRange", {}).get("protectedRangeId")
            extra = f" (id={new_id})"
    return (
        f"Protected range {action}{extra} in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("manage_dimension_group", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_dimension_group(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    sheet_name: Optional[str] = None,
    dimension: str = "ROWS",
    start_index: Optional[int] = None,
    end_index: Optional[int] = None,
    collapsed: Optional[bool] = None,
) -> str:
    """
    Manages collapsible dimension groups (row/column groups with toggle).

    Action: create, delete, set_state (just collapse/expand existing group).

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        action (str): create | delete | set_state. Required.
        sheet_name (str): Target sheet (defaults to first).
        dimension (str): ROWS or COLUMNS. Default ROWS.
        start_index (int): 0-based start index. Required for create/delete.
        end_index (int): 0-based exclusive end index. Required for create/delete.
        collapsed (bool): For set_state — collapsed or expanded.

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[manage_dimension_group] %s, %s, action=%s, dim=%s, %s-%s",
        user_google_email, spreadsheet_id, action, dimension, start_index, end_index,
    )
    action = action.lower()
    dim = dimension.upper()
    if dim not in {"ROWS", "COLUMNS"}:
        raise UserInputError("dimension must be ROWS or COLUMNS.")
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    sheet = _select_sheet(sheets, sheet_name)
    sheet_id = sheet["properties"]["sheetId"]

    if action == "set_state":
        if collapsed is None:
            raise UserInputError("set_state requires collapsed=True/False.")
        # Use updateDimensionGroup with collapsed flag.
        # We need the group's depth — fetch sheet's groups first.
        full = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(sheetId),rowGroups,columnGroups)",
            )
            .execute
        )
        target = None
        for sh in full.get("sheets", []):
            if sh.get("properties", {}).get("sheetId") == sheet_id:
                groups_key = "rowGroups" if dim == "ROWS" else "columnGroups"
                for g in sh.get(groups_key, []):
                    r = g.get("range", {})
                    if (start_index is None or r.get("startIndex") == start_index) and \
                       (end_index is None or r.get("endIndex") == end_index):
                        target = g
                        break
                break
        if target is None:
            raise UserInputError("Could not find matching group on sheet.")
        body = {
            "requests": [
                {"updateDimensionGroup": {
                    "dimensionGroup": {
                        "range": target.get("range"),
                        "depth": target.get("depth", 1),
                        "collapsed": collapsed,
                    },
                    "fields": "collapsed",
                }}
            ]
        }
    elif action in ("create", "delete"):
        if start_index is None or end_index is None:
            raise UserInputError("create/delete require start_index and end_index.")
        req_type = "addDimensionGroup" if action == "create" else "deleteDimensionGroup"
        body = {
            "requests": [
                {req_type: {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": dim,
                        "startIndex": start_index,
                        "endIndex": end_index,
                    }
                }}
            ]
        }
    else:
        raise UserInputError(f"Unknown action '{action}'.")

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    return (
        f"Dimension group {action} ({dim} {start_index}-{end_index}) on sheet "
        f"'{sheet['properties']['title']}' in {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("manage_filter_view", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_filter_view(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    filter_view_id: Optional[int] = None,
    title: Optional[str] = None,
    range_name: Optional[str] = None,
    criteria: Optional[Union[str, dict]] = None,
    sort_specs: Optional[Union[str, List[dict]]] = None,
) -> str:
    """
    Manages filter views (named saved filters). Action: create, update, delete, list.

    criteria: dict mapping 0-based column index (as string) to a FilterCriteria
    dict {"hiddenValues": [...]} or {"condition": {"type": "...", "values": [...]}}.
    See Sheets API FilterCriteria.

    sort_specs: same format as sort_range tool.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        action (str): create | update | delete | list. Required.
        filter_view_id (int): ID (update/delete).
        title (str): Filter view name.
        range_name (str): A1-style range covered.
        criteria (dict|str): Filter criteria.
        sort_specs (list|str): Sort specifications.

    Returns:
        str: Confirmation or list.
    """
    logger.info(
        "[manage_filter_view] %s, %s, action=%s",
        user_google_email, spreadsheet_id, action,
    )
    action = action.lower()

    if action == "list":
        full = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(title),filterViews)",
            )
            .execute
        )
        lines = []
        for sh in full.get("sheets", []):
            stitle = sh.get("properties", {}).get("title")
            for fv in sh.get("filterViews", []):
                lines.append(
                    f"- sheet='{stitle}' id={fv.get('filterViewId')} "
                    f"title='{fv.get('title', '')}'"
                )
        if not lines:
            return f"No filter views in {spreadsheet_id}."
        return f"Filter views in {spreadsheet_id}:\n" + "\n".join(lines)

    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)

    def _build_filter_body() -> dict:
        fv: dict = {}
        if title:
            fv["title"] = title
        if range_name:
            fv["range"] = _parse_a1_range(range_name, sheets)
        crit = _parse_json(criteria, "criteria")
        if crit:
            if not isinstance(crit, dict):
                raise UserInputError("criteria must be a dict.")
            fv["criteria"] = crit
        specs = _parse_json(sort_specs, "sort_specs")
        if specs:
            if not isinstance(specs, list):
                raise UserInputError("sort_specs must be a list.")
            api_specs = []
            for sp in specs:
                col = sp.get("column")
                if isinstance(col, str):
                    col_idx = _column_to_index(col.upper())
                else:
                    col_idx = int(col)
                order = sp.get("order", "ASCENDING").upper()
                api_specs.append({"dimensionIndex": col_idx, "sortOrder": order})
            fv["sortSpecs"] = api_specs
        return fv

    if action == "create":
        if not title or not range_name:
            raise UserInputError("create requires title and range_name.")
        body = {"requests": [{"addFilterView": {"filter": _build_filter_body()}}]}
    elif action == "update":
        if filter_view_id is None:
            raise UserInputError("update requires filter_view_id.")
        fv = _build_filter_body()
        fv["filterViewId"] = filter_view_id
        fields = []
        if title is not None:
            fields.append("title")
        if range_name is not None:
            fields.append("range")
        if criteria is not None:
            fields.append("criteria")
        if sort_specs is not None:
            fields.append("sortSpecs")
        if not fields:
            raise UserInputError("update requires at least one field.")
        body = {
            "requests": [
                {"updateFilterView": {"filter": fv, "fields": ",".join(fields)}}
            ]
        }
    elif action == "delete":
        if filter_view_id is None:
            raise UserInputError("delete requires filter_view_id.")
        body = {"requests": [{"deleteFilterView": {"filterId": filter_view_id}}]}
    else:
        raise UserInputError(f"Unknown action '{action}'.")

    resp = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    extra = ""
    if action == "create":
        replies = resp.get("replies", [])
        if replies:
            new_id = replies[0].get("addFilterView", {}).get("filter", {}).get("filterViewId")
            extra = f" (id={new_id})"
    return (
        f"Filter view {action}{extra} in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("clear_range_formatting", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def clear_range_formatting(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
) -> str:
    """
    Clears all userEnteredFormat from a range (resets to default).

    This wipes background color, text color, alignment, bold/italic, number
    format, borders, wrap strategy, etc. — but does NOT delete cell values,
    formulas, data validation, or conditional formatting rules.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        range_name (str): A1-style range. Required.

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[clear_range_formatting] %s, %s, range=%s",
        user_google_email, spreadsheet_id, range_name,
    )
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    grid_range = _parse_a1_range(range_name, sheets)
    body = {
        "requests": [
            {"repeatCell": {
                "range": grid_range,
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat",
            }}
        ]
    }
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    return (
        f"Cleared formatting on '{range_name}' in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


# ============================================================================
# Phase C — occasional
# ============================================================================


@server.tool()
@handle_http_errors("manage_chart", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_chart(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    chart_id: Optional[int] = None,
    sheet_name: Optional[str] = None,
    chart_type: str = "COLUMN",
    title: Optional[str] = None,
    source_range: Optional[str] = None,
    domain_axis_range: Optional[str] = None,
    series_ranges: Optional[Union[str, List[str]]] = None,
    anchor_sheet: Optional[str] = None,
    anchor_row: Optional[int] = None,
    anchor_col: Optional[int] = None,
    width_pixels: int = 600,
    height_pixels: int = 371,
    legend_position: str = "BOTTOM_LEGEND",
    stacked_type: Optional[str] = None,
) -> str:
    """
    Manages basic charts. Action: create, delete, list.

    For create, two modes:
      (1) Single source_range: domain is column A of the range, series are
          remaining columns. Simplest case.
      (2) Explicit domain_axis_range + series_ranges (list of A1 ranges).

    Chart types: COLUMN, BAR, LINE, AREA, SCATTER, COMBO. Use stacked_type
    STACKED or PERCENT_STACKED for stacked variants.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        action (str): create | delete | list. Required.
        chart_id (int): ID (delete).
        sheet_name (str): Source sheet for list scope.
        chart_type (str): COLUMN, BAR, LINE, AREA, SCATTER, COMBO.
        title (str): Chart title.
        source_range (str): A1 range covering domain + series (mode 1).
        domain_axis_range (str): A1 range for domain axis (mode 2).
        series_ranges (list|str): List of A1 ranges, one per series (mode 2).
        anchor_sheet (str): Sheet where the chart is placed (defaults to source).
        anchor_row, anchor_col (int): 0-based anchor cell.
        width_pixels, height_pixels (int): Chart size.
        legend_position (str): BOTTOM_LEGEND, TOP_LEGEND, LEFT_LEGEND,
            RIGHT_LEGEND, NO_LEGEND.
        stacked_type (str): STACKED or PERCENT_STACKED for stacking.

    Returns:
        str: Confirmation or list.
    """
    logger.info(
        "[manage_chart] %s, %s, action=%s, type=%s",
        user_google_email, spreadsheet_id, action, chart_type,
    )
    action = action.lower()

    if action == "list":
        full = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(title),charts(chartId,spec(title)))",
            )
            .execute
        )
        lines = []
        for sh in full.get("sheets", []):
            stitle = sh.get("properties", {}).get("title")
            for c in sh.get("charts", []):
                lines.append(
                    f"- sheet='{stitle}' id={c.get('chartId')} "
                    f"title='{c.get('spec', {}).get('title', '')}'"
                )
        if not lines:
            return f"No charts in {spreadsheet_id}."
        return f"Charts in {spreadsheet_id}:\n" + "\n".join(lines)

    if action == "delete":
        if chart_id is None:
            raise UserInputError("delete requires chart_id.")
        body = {"requests": [{"deleteEmbeddedObject": {"objectId": chart_id}}]}
        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute
        )
        return (
            f"Deleted chart id={chart_id} in {spreadsheet_id} for {user_google_email}."
        )

    if action != "create":
        raise UserInputError(f"Unknown action '{action}'.")

    # Create
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    domains: list = []
    series: list = []

    if source_range:
        rng = _parse_a1_range(source_range, sheets)
        # domain = first column
        domain_range = dict(rng)
        domain_range["endColumnIndex"] = rng["startColumnIndex"] + 1
        domains.append({"domain": {"sourceRange": {"sources": [domain_range]}}})
        # series = each remaining column
        for col in range(rng["startColumnIndex"] + 1, rng["endColumnIndex"]):
            s_range = dict(rng)
            s_range["startColumnIndex"] = col
            s_range["endColumnIndex"] = col + 1
            series.append({"series": {"sourceRange": {"sources": [s_range]}}})
    elif domain_axis_range and series_ranges:
        domain_grid = _parse_a1_range(domain_axis_range, sheets)
        domains.append({"domain": {"sourceRange": {"sources": [domain_grid]}}})
        sr = _parse_json(series_ranges, "series_ranges")
        if isinstance(sr, str):
            sr = [sr]
        for r in sr:
            s_grid = _parse_a1_range(r, sheets)
            series.append({"series": {"sourceRange": {"sources": [s_grid]}}})
    else:
        raise UserInputError(
            "Provide source_range, or domain_axis_range + series_ranges."
        )

    basic_chart: dict = {
        "chartType": chart_type.upper(),
        "legendPosition": legend_position.upper(),
        "domains": domains,
        "series": series,
        "headerCount": 1,
    }
    if stacked_type:
        basic_chart["stackedType"] = stacked_type.upper()

    spec: dict = {"basicChart": basic_chart}
    if title:
        spec["title"] = title

    # Anchor: pick source sheet if not specified
    anchor_sheet_name = anchor_sheet or (sheet_name if sheet_name else None)
    if anchor_sheet_name:
        anchor_sh = _select_sheet(sheets, anchor_sheet_name)
        anchor_sheet_id = anchor_sh["properties"]["sheetId"]
    else:
        # Take sheetId from first range
        anchor_sheet_id = (
            domains[0]["domain"]["sourceRange"]["sources"][0]["sheetId"]
        )
    position = {
        "overlayPosition": {
            "anchorCell": {
                "sheetId": anchor_sheet_id,
                "rowIndex": anchor_row if anchor_row is not None else 0,
                "columnIndex": anchor_col if anchor_col is not None else 0,
            },
            "widthPixels": width_pixels,
            "heightPixels": height_pixels,
        }
    }

    body = {
        "requests": [
            {"addChart": {"chart": {"spec": spec, "position": position}}}
        ]
    }
    resp = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    new_id = None
    replies = resp.get("replies", [])
    if replies:
        new_id = replies[0].get("addChart", {}).get("chart", {}).get("chartId")
    return (
        f"Created {chart_type} chart (id={new_id}) in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("copy_paste_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def copy_paste_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    source_range: str,
    destination_range: str,
    paste_type: str = "PASTE_NORMAL",
    paste_orientation: str = "NORMAL",
) -> str:
    """
    Copies content/format from source_range to destination_range.

    paste_type:
      PASTE_NORMAL (default) — values, formulas, formatting, data validation
      PASTE_VALUES           — values only
      PASTE_FORMAT           — formatting and data validation only
      PASTE_NO_BORDERS       — like NORMAL but no borders
      PASTE_FORMULA          — formulas only
      PASTE_DATA_VALIDATION  — validation only
      PASTE_CONDITIONAL_FORMATTING — CF only

    paste_orientation: NORMAL or TRANSPOSE.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        source_range (str): A1-style source range. Required.
        destination_range (str): A1-style destination range. Required.
        paste_type (str): See above. Default PASTE_NORMAL.
        paste_orientation (str): NORMAL or TRANSPOSE.

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[copy_paste_range] %s, %s, %s -> %s",
        user_google_email, spreadsheet_id, source_range, destination_range,
    )
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    src = _parse_a1_range(source_range, sheets)
    dst = _parse_a1_range(destination_range, sheets)
    body = {
        "requests": [
            {"copyPaste": {
                "source": src,
                "destination": dst,
                "pasteType": paste_type.upper(),
                "pasteOrientation": paste_orientation.upper(),
            }}
        ]
    }
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    return (
        f"Copied '{source_range}' -> '{destination_range}' ({paste_type}) "
        f"in {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("insert_cells_with_shift", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def insert_cells_with_shift(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    shift_dimension: str = "ROWS",
) -> str:
    """
    Inserts empty cells at the given range, shifting existing cells away.

    shift_dimension:
      ROWS    — push cells in the range down (insert rows in that block only).
      COLUMNS — push cells in the range right.

    Note: this is partial insertion — unlike resize_sheet_dimensions which
    inserts whole rows/columns across the entire sheet.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        range_name (str): A1-style range to insert. Required.
        shift_dimension (str): ROWS (default) or COLUMNS.

    Returns:
        str: Confirmation message.
    """
    logger.info(
        "[insert_cells_with_shift] %s, %s, range=%s, dim=%s",
        user_google_email, spreadsheet_id, range_name, shift_dimension,
    )
    dim = shift_dimension.upper()
    if dim not in {"ROWS", "COLUMNS"}:
        raise UserInputError("shift_dimension must be ROWS or COLUMNS.")
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    grid_range = _parse_a1_range(range_name, sheets)
    body = {
        "requests": [
            {"insertRange": {"range": grid_range, "shiftDimension": dim}}
        ]
    }
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )
    return (
        f"Inserted cells at '{range_name}' (shift={dim}) in {spreadsheet_id} "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("manage_sheet_tab", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_sheet_tab(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    sheet_name: Optional[str] = None,
    new_name: Optional[str] = None,
    new_index: Optional[int] = None,
    insert_sheet_index: Optional[int] = None,
    confirm_token: Optional[str] = None,
) -> str:
    """
    Manages sheet tabs in a spreadsheet (full clone, rename, delete, reorder).

    Actions:
        - duplicate: clones a sheet with ALL properties (merges, frozen rows,
          column widths, formatting, conditional rules, charts).
          Requires sheet_name. Optional: new_name, insert_sheet_index.
        - rename: changes a sheet's title. Requires sheet_name and new_name.
        - delete: removes a sheet. Requires sheet_name. TWO-STEP:
            1) Call without confirm_token -> returns a JSON envelope with
               requires_confirmation=true and a token (TTL 5 min).
            2) Call again with confirm_token=<token> and the SAME
               spreadsheet_id + sheet_name -> performs the delete.
            Token is consumed on use and cannot be replayed.
        - reorder: moves a sheet to a new 0-based position. Requires
          sheet_name and new_index.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        action (str): duplicate | rename | delete | reorder. Required.
        sheet_name (str): Source/target sheet name. Required for all actions.
        new_name (str): New sheet name (rename / optional for duplicate).
        new_index (int): 0-based target position (reorder).
        insert_sheet_index (int): 0-based insertion position (duplicate).
        confirm_token (str): Two-step confirmation token for action=delete.
            Ignored by other actions.

    Returns:
        str: Confirmation; for duplicate includes new sheet id and title.
            For an unconfirmed delete returns a JSON envelope with the token.
    """
    logger.info(
        "[manage_sheet_tab] %s, %s, action=%s, sheet=%s, has_confirm=%s",
        user_google_email, spreadsheet_id, action, sheet_name, bool(confirm_token),
    )
    action = action.lower()
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    target = _select_sheet(sheets, sheet_name)
    sheet_id = target["properties"]["sheetId"]
    resolved_sheet_name = target["properties"].get("title", sheet_name)

    if action == "duplicate":
        dup: dict = {"sourceSheetId": sheet_id}
        if new_name:
            dup["newSheetName"] = new_name
        if insert_sheet_index is not None:
            dup["insertSheetIndex"] = insert_sheet_index
        requests = [{"duplicateSheet": dup}]
    elif action == "rename":
        if not new_name:
            raise UserInputError("rename requires new_name.")
        requests = [{
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "title": new_name},
                "fields": "title",
            }
        }]
    elif action == "delete":
        # Phase 7.4: two-step confirmation. First call returns a token; second
        # call with the token (and matching spreadsheet_id + sheet_name) deletes.
        confirm_payload = {
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": resolved_sheet_name,
            "sheet_id": sheet_id,
        }
        if not confirm_token:
            token = _issue_destructive_confirm("manage_sheet_tab:delete", confirm_payload)
            envelope = {
                "requires_confirmation": True,
                "op": "manage_sheet_tab:delete",
                "token": token,
                "ttl_seconds": _DESTRUCTIVE_CONFIRM_TTL_SECONDS,
                "spreadsheet_id": spreadsheet_id,
                "sheet_name": resolved_sheet_name,
                "sheet_id": sheet_id,
                "instructions": (
                    "Sheet deletion is irreversible. Show the user the sheet name "
                    "above, get explicit approval, then call manage_sheet_tab again "
                    "with action='delete', the same spreadsheet_id and sheet_name, "
                    "and confirm_token set to the value above."
                ),
            }
            return json.dumps(envelope, ensure_ascii=False)
        _consume_destructive_confirm(
            "manage_sheet_tab:delete", confirm_token, confirm_payload
        )
        requests = [{"deleteSheet": {"sheetId": sheet_id}}]
    elif action == "reorder":
        if new_index is None:
            raise UserInputError("reorder requires new_index.")
        requests = [{
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "index": new_index},
                "fields": "index",
            }
        }]
    else:
        raise UserInputError(
            f"Unknown action '{action}'. Use: duplicate, rename, delete, reorder."
        )

    resp = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
        .execute
    )

    extra = ""
    if action == "duplicate":
        replies = resp.get("replies", [])
        if replies:
            new_props = replies[0].get("duplicateSheet", {}).get("properties", {})
            new_id = new_props.get("sheetId")
            new_title = new_props.get("title")
            extra = f" \u2192 new sheet '{new_title}' (id={new_id})"

    return (
        f"Sheet tab {action} on '{sheet_name}' in spreadsheet "
        f"{spreadsheet_id} for {user_google_email}{extra}."
    )


# ============================================================================
# Phase 7.6: set_sparse_cells — write a scattered map of {A1: value} in one call
# ============================================================================


@server.tool()
@handle_http_errors("set_sparse_cells", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def set_sparse_cells(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    cells: Union[str, dict],
    sheet_name: Optional[str] = None,
    value_input_option: str = "USER_ENTERED",
) -> str:
    """
    Writes a scattered set of cells in ONE call. The opposite of
    modify_sheet_values which requires a dense matrix.

    Use this when you need to set a handful of cells in disparate locations,
    e.g. {"A1": "Title", "B5": 42, "AT15": "=SUM(A1:A10)"}. Compared to
    modify_sheet_values on a wide range, this avoids transmitting empty
    cells in between (x10+ saving on sparse layouts).

    Under the hood: one spreadsheets.values.batchUpdate request with one
    ValueRange per cell. Sheets de-duplicates against existing cells, so
    formulas referencing other cells still resolve.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        cells (Union[str, dict]): Map from A1 reference to value. Each key
            may include a sheet prefix ("Sheet1!A1") or omit it ("A1") in
            which case sheet_name applies. Values can be any JSON-serialisable
            scalar (str / int / float / bool / None). Accepts a JSON string.
            Example: {"A1": "Hello", "B5": 42, "Sheet2!AT15": "=SUM(A1:A10)"}
        sheet_name (Optional[str]): Default sheet for keys that lack a prefix.
            If omitted, the spreadsheet's first sheet is used.
        value_input_option (str): "USER_ENTERED" (default, Sheets parses
            numbers, dates, formulas) or "RAW".

    Returns:
        str: Confirmation with updated cell count and range summary.
    """
    logger.info(
        "[set_sparse_cells] %s, %s, sheet=%s",
        user_google_email, spreadsheet_id, sheet_name,
    )

    if isinstance(cells, str):
        try:
            cells = json.loads(cells)
        except json.JSONDecodeError as e:
            raise UserInputError(f"Invalid JSON for cells: {e}")
    if not isinstance(cells, dict):
        raise UserInputError("cells must be a dict of {A1: value}.")
    if not cells:
        raise UserInputError("cells is empty — nothing to write.")
    if value_input_option not in ("USER_ENTERED", "RAW"):
        raise UserInputError(
            f"value_input_option must be USER_ENTERED or RAW, got '{value_input_option}'."
        )

    # Resolve default sheet name when keys omit the prefix.
    default_sheet = sheet_name
    if default_sheet is None:
        sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
        default_sheet = sheets[0]["properties"]["title"]

    def _resolve_range(a1_key: str) -> str:
        a1_key = a1_key.strip()
        if not a1_key:
            raise UserInputError("Empty cell key.")
        if "!" in a1_key:
            return a1_key
        # Quote sheet name if it contains spaces or special chars.
        if any(c in default_sheet for c in " !'\""):
            safe = default_sheet.replace("'", "''")
            return f"'{safe}'!{a1_key}"
        return f"{default_sheet}!{a1_key}"

    data = [
        {"range": _resolve_range(key), "values": [[value]]}
        for key, value in cells.items()
    ]

    body = {"valueInputOption": value_input_option, "data": data}
    result = await asyncio.to_thread(
        service.spreadsheets()
        .values()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )

    updated_cells = result.get("totalUpdatedCells", len(data))
    updated_ranges = result.get("totalUpdatedRanges", len(data))
    return (
        f"set_sparse_cells: wrote {updated_cells} cell(s) across "
        f"{updated_ranges} range(s) in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


# ============================================================================
# Phase 7.7: read_sheet_summary — structure-only peek, no full grid dump
# ============================================================================


@server.tool()
@handle_http_errors("read_sheet_summary", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def read_sheet_summary(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: Optional[str] = None,
    header_rows: int = 3,
    sample_columns: int = 26,
) -> str:
    """
    Returns the SHAPE of a sheet without dumping every cell. Use this as a
    cheap first look at a large sheet — far smaller than read_sheet_values.

    Payload contains:
        - dimensions (rowCount, columnCount, frozenRows, frozenColumns)
        - merged ranges (count + a sample)
        - the first `header_rows` rows of values (default 3)
        - the LAST non-empty row of values (when the sheet is taller than
          header_rows)
        - a sample of column letters

    Costs two Sheets API calls (metadata + values.get for the header band)
    plus one optional call for the trailing row. Compare to read_sheet_values
    on A1:Z1000 which can blow 50-100 KB of tokens for nothing.

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        sheet_name (Optional[str]): Sheet to inspect. Defaults to the first
            sheet of the spreadsheet.
        header_rows (int): How many top rows to sample (1-10). Default 3.
        sample_columns (int): How many leftmost columns to include in the
            header sample (1-50). Default 26 (A..Z).

    Returns:
        str: JSON envelope with the summary.
    """
    logger.info(
        "[read_sheet_summary] %s, %s, sheet=%s",
        user_google_email, spreadsheet_id, sheet_name,
    )
    if not 1 <= header_rows <= 10:
        raise UserInputError("header_rows must be between 1 and 10.")
    if not 1 <= sample_columns <= 50:
        raise UserInputError("sample_columns must be between 1 and 50.")

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields=(
                "properties.title,"
                "sheets(properties(sheetId,title,index,gridProperties),merges)"
            ),
        )
        .execute
    )
    sheets = metadata.get("sheets", [])
    if not sheets:
        raise UserInputError("No sheets in spreadsheet.")

    target = _select_sheet(sheets, sheet_name)
    props = target.get("properties", {})
    grid = props.get("gridProperties", {})
    resolved_name = props.get("title")
    row_count = grid.get("rowCount", 0)
    col_count = grid.get("columnCount", 0)

    # Convert sample_columns to an end-column letter (1 -> "A", 26 -> "Z", 27 -> "AA").
    def _col_letter(n: int) -> str:
        result = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            result = chr(ord("A") + rem) + result
        return result

    end_col = _col_letter(min(sample_columns, col_count or sample_columns))
    header_end = min(header_rows, row_count) if row_count else header_rows
    header_range = f"'{resolved_name}'!A1:{end_col}{header_end}"

    header_resp = await asyncio.to_thread(
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=header_range)
        .execute
    )
    header_values = header_resp.get("values", [])

    last_row_values = None
    if row_count > header_rows and row_count > 0:
        last_range = f"'{resolved_name}'!A{row_count}:{end_col}{row_count}"
        last_resp = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=last_range)
            .execute
        )
        last_row_values = last_resp.get("values", [[]])
        last_row_values = last_row_values[0] if last_row_values else []

    merges = target.get("merges", []) or []
    merge_sample = []
    for m in merges[:5]:
        merge_sample.append({
            "start_row": m.get("startRowIndex", 0) + 1,
            "end_row": m.get("endRowIndex", 0),
            "start_col": _col_letter(m.get("startColumnIndex", 0) + 1),
            "end_col": _col_letter(m.get("endColumnIndex", 0)),
        })

    envelope = {
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_title": metadata.get("properties", {}).get("title"),
        "sheet": {
            "name": resolved_name,
            "sheet_id": props.get("sheetId"),
            "index": props.get("index"),
            "row_count": row_count,
            "column_count": col_count,
            "frozen_rows": grid.get("frozenRowCount", 0),
            "frozen_columns": grid.get("frozenColumnCount", 0),
        },
        "merges": {
            "count": len(merges),
            "sample": merge_sample,
        },
        "header_sample": {
            "range": header_range,
            "values": header_values,
        },
        "last_row": {
            "row_number": row_count if row_count > header_rows else None,
            "values": last_row_values,
        },
        "note": (
            "This is a structural summary, not a full dump. "
            "Use read_sheet_values with a narrow range to fetch actual data."
        ),
    }
    return json.dumps(envelope, ensure_ascii=False)


# ============================================================================
# Phase 7.8: batch_update_spreadsheet — raw Sheets batchUpdate, one round-trip
# ============================================================================


# Whitelist of request types the generic batcher accepts. Adding a new type:
# (a) audit the request shape in the Sheets API ref, (b) make sure existing
# specialised tools (merge_sheet_range etc.) still cover the common path, so
# Claude doesn't *have* to learn the raw shape unless it wants a multi-op
# round-trip.
_BATCH_UPDATE_ALLOWED_REQUEST_TYPES = {
    # Cell content & format
    "updateCells",
    "repeatCell",
    "appendCells",
    # Merges
    "mergeCells",
    "unmergeCells",
    # Borders
    "updateBorders",
    # Sheet & dimension properties
    "updateSheetProperties",
    "updateDimensionProperties",
    "insertDimension",
    "deleteDimension",
    "autoResizeDimensions",
    "appendDimension",
    # Charts (embedded objects)
    "addChart",
    "updateChartSpec",
    "deleteEmbeddedObject",
    "updateEmbeddedObjectPosition",
    # Protected / named ranges
    "addProtectedRange",
    "updateProtectedRange",
    "deleteProtectedRange",
    "addNamedRange",
    "updateNamedRange",
    "deleteNamedRange",
    # Banding & conditional formatting
    "addBanding",
    "updateBanding",
    "deleteBanding",
    "addConditionalFormatRule",
    "updateConditionalFormatRule",
    "deleteConditionalFormatRule",
    # Filter views & basic filters
    "addFilterView",
    "updateFilterView",
    "deleteFilterView",
    "setBasicFilter",
    "clearBasicFilter",
    # Data validation
    "setDataValidation",
    # Sort
    "sortRange",
    # Find & replace
    "findReplace",
    # Group / ungroup dimensions
    "addDimensionGroup",
    "deleteDimensionGroup",
    "updateDimensionGroup",
    # Copy / paste / cut-paste / clear formatting
    "copyPaste",
    "cutPaste",
    "updateCells",  # already above; harmless dup
    # Phase 9: extra requests
    "moveDimension",
    "autoFill",
    "deleteDuplicates",
    "trimWhitespace",
    "insertRange",
    "deleteRange",
    # Note: deleteSheet is intentionally NOT here — channel it through
    # manage_sheet_tab so the soft-confirm gate stays in place.
}


@server.tool()
@handle_http_errors("batch_update_spreadsheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def batch_update_spreadsheet(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    requests: Union[str, List[dict]],
    include_responses: bool = False,
    dry_run: bool = False,
) -> str:
    """
    Sends a list of raw Sheets API requests as ONE batchUpdate call.

    When to reach for this: when you need to combine several operations
    (merge + format + border + freeze + chart) on the same spreadsheet
    and would otherwise issue 5-10 separate calls. One batchUpdate =
    one round-trip + one quota slot.

    When NOT to: for a single operation, prefer the specialised tool
    (merge_sheet_range, set_range_borders, format_sheet_range,
    manage_sheet_tab, etc.). The specialised tools have ergonomic
    A1-range parsing and cleaner argument shapes — this tool is the
    bare Sheets API.

    Each request in the array is a dict with EXACTLY ONE top-level key
    naming the request type. Request bodies follow the Sheets API
    Request schema verbatim — see https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request

    Allowed request types (subset of Sheets API; deleteSheet is gated
    through manage_sheet_tab for soft-confirm):
      cells: updateCells, repeatCell, appendCells
      merges: mergeCells, unmergeCells
      borders: updateBorders
      sheet/dim props: updateSheetProperties, updateDimensionProperties,
        insertDimension, deleteDimension, autoResizeDimensions, appendDimension
      charts: addChart, updateChartSpec, deleteEmbeddedObject, updateEmbeddedObjectPosition
      ranges: addProtectedRange/update/delete, addNamedRange/update/delete
      banding/CF: addBanding/update/delete, addConditionalFormatRule/update/delete
      filters: addFilterView/update/delete, setBasicFilter, clearBasicFilter
      misc: setDataValidation, sortRange, findReplace, copyPaste, cutPaste,
        addDimensionGroup/delete/update

    Example — merge B2:D2 and bold-format the result in one call:
      [
        {"mergeCells": {
            "range": {"sheetId": 0, "startRowIndex": 1, "endRowIndex": 2,
                      "startColumnIndex": 1, "endColumnIndex": 4},
            "mergeType": "MERGE_ALL"}},
        {"repeatCell": {
            "range": {"sheetId": 0, "startRowIndex": 1, "endRowIndex": 2,
                      "startColumnIndex": 1, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": true}}},
            "fields": "userEnteredFormat.textFormat.bold"}}
      ]

    Args:
        user_google_email (str): The user's Google email. Required.
        spreadsheet_id (str): Spreadsheet ID. Required.
        requests (Union[str, List[dict]]): Array of Sheets API Request objects
            (or a JSON string). Each request must have exactly one allowed
            top-level key. Required, must be non-empty.
        include_responses (bool): If True, the JSON envelope includes the
            replies array from batchUpdate (useful for duplicateSheet,
            addChart, addFilterView, etc.). Default False — fewer tokens.
        dry_run (bool): If True, validates request shapes and returns a
            preview envelope WITHOUT calling the API. Default False.

    Returns:
        str: JSON envelope with applied request count and (optionally) replies.
    """
    logger.info(
        "[batch_update_spreadsheet] %s, %s, dry_run=%s, include_responses=%s",
        user_google_email, spreadsheet_id, dry_run, include_responses,
    )

    if isinstance(requests, str):
        try:
            requests = json.loads(requests)
        except json.JSONDecodeError as e:
            raise UserInputError(f"Invalid JSON for requests: {e}")
    if not isinstance(requests, list):
        raise UserInputError("requests must be a list of Sheets API Request objects.")
    if not requests:
        raise UserInputError("requests is empty.")

    seen_types: dict = {}
    for i, req in enumerate(requests):
        if not isinstance(req, dict):
            raise UserInputError(f"requests[{i}] must be a dict, got {type(req).__name__}.")
        if len(req) != 1:
            raise UserInputError(
                f"requests[{i}] must have exactly one top-level key (the request type), "
                f"got keys: {sorted(req.keys())}"
            )
        req_type = next(iter(req))
        if req_type not in _BATCH_UPDATE_ALLOWED_REQUEST_TYPES:
            raise UserInputError(
                f"requests[{i}] uses disallowed type '{req_type}'. "
                f"Allowed types: {sorted(_BATCH_UPDATE_ALLOWED_REQUEST_TYPES)}. "
                f"For deleteSheet, use manage_sheet_tab(action='delete')."
            )
        seen_types[req_type] = seen_types.get(req_type, 0) + 1

    if dry_run:
        return json.dumps(
            {
                "dry_run": True,
                "spreadsheet_id": spreadsheet_id,
                "request_count": len(requests),
                "request_type_counts": seen_types,
                "note": "Shapes validated, no API call made. Drop dry_run=True to apply.",
            },
            ensure_ascii=False,
        )

    body = {"requests": requests, "includeSpreadsheetInResponse": False}
    resp = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute
    )

    envelope: dict = {
        "spreadsheet_id": spreadsheet_id,
        "applied_count": len(requests),
        "request_type_counts": seen_types,
    }
    if include_responses:
        envelope["replies"] = resp.get("replies", [])
    return json.dumps(envelope, ensure_ascii=False)


# ============================================================================
# Phase 8: copy_sheets_to_spreadsheet — copy/move sheets between spreadsheets
# ============================================================================


@server.tool()
@handle_http_errors("copy_sheets_to_spreadsheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def copy_sheets_to_spreadsheet(
    service,
    user_google_email: str,
    source_spreadsheet_id: str,
    sheet_names: List[str],
    destination_spreadsheet_id: str,
    new_names: Optional[dict] = None,
    delete_from_source: bool = False,
    confirm_token: Optional[str] = None,
) -> str:
    """
    Copy (or move) one or more sheet tabs from a source spreadsheet to a destination
    spreadsheet. Each sheet is copied via spreadsheets.sheets.copyTo, optionally
    renamed in the destination, and optionally deleted from the source.

    Per-sheet semantics:
        - Each sheet name in ``sheet_names`` is processed independently. A failure
          on one sheet (lookup, copy, or rename) does NOT abort the rest; instead
          the per-sheet status records the error and processing continues.
        - On a successful copy, if ``new_names[<sheet_name>]`` is provided, the
          freshly created sheet in the destination is renamed from its default
          ("Copy of <name>") to the requested name. Rename failures (e.g. name
          collision in destination) mark the per-sheet status as error but the
          copy itself stays in place — the caller can rename manually afterward.
        - When ``delete_from_source=True``, only sheets that copied successfully
          are deleted from the source. Sheets whose copy failed are left intact.

    Soft-confirm (Phase 7.4 pattern) applies to ``delete_from_source=True``:
        1) First call with ``delete_from_source=True`` and no ``confirm_token``
           returns a JSON envelope with ``requires_confirmation=true`` and a
           one-shot token (TTL 5 min).
        2) Second call with ``confirm_token`` set and the same
           ``source_spreadsheet_id`` / ``destination_spreadsheet_id`` /
           ``sheet_names`` performs the move.
        Pure copies (``delete_from_source=False``) execute immediately.

    Args:
        user_google_email (str): The user's Google email. Required.
        source_spreadsheet_id (str): Spreadsheet ID to copy FROM. Required.
        sheet_names (List[str]): Sheet tab titles to copy. Required, non-empty.
        destination_spreadsheet_id (str): Spreadsheet ID to copy TO. Required.
            May be the same as source for in-spreadsheet duplication, but
            ``manage_sheet_tab(action='duplicate')`` is cheaper for that case.
        new_names (dict): Optional map ``{old_name: new_name}`` to rename
            copied sheets in the destination. Keys must appear in ``sheet_names``.
        delete_from_source (bool): If True, delete each successfully copied
            sheet from the source after copying. Requires two-step confirm.
        confirm_token (str): Confirm token from the first call. Required iff
            ``delete_from_source=True``.

    Returns:
        str: JSON envelope. On unconfirmed move: requires_confirmation block.
            On execution: per-sheet results (status, source_sheet_id, new_sheet_id,
            final_title, error if any) + counters (copied, renamed, deleted, failed).
    """
    logger.info(
        "[copy_sheets_to_spreadsheet] %s, src=%s, dst=%s, names=%s, delete=%s, has_confirm=%s",
        user_google_email,
        source_spreadsheet_id,
        destination_spreadsheet_id,
        sheet_names,
        delete_from_source,
        bool(confirm_token),
    )

    # ---- validation ----
    if not isinstance(sheet_names, list) or not sheet_names:
        raise UserInputError("sheet_names must be a non-empty list of sheet titles.")
    if any(not isinstance(n, str) or not n.strip() for n in sheet_names):
        raise UserInputError("sheet_names entries must be non-empty strings.")
    if len(set(sheet_names)) != len(sheet_names):
        raise UserInputError("sheet_names contains duplicates.")
    if new_names is not None:
        if not isinstance(new_names, dict):
            raise UserInputError("new_names must be a dict {old_name: new_name}.")
        stray = [k for k in new_names if k not in sheet_names]
        if stray:
            raise UserInputError(
                f"new_names contains keys not in sheet_names: {stray}"
            )
        if any(not isinstance(v, str) or not v.strip() for v in new_names.values()):
            raise UserInputError("new_names values must be non-empty strings.")
    if not destination_spreadsheet_id:
        raise UserInputError("destination_spreadsheet_id is required.")

    # ---- soft-confirm gate for delete_from_source ----
    if delete_from_source:
        confirm_payload = {
            "source_spreadsheet_id": source_spreadsheet_id,
            "destination_spreadsheet_id": destination_spreadsheet_id,
            "sheet_names": sorted(sheet_names),
        }
        if not confirm_token:
            token = _issue_destructive_confirm(
                "copy_sheets_to_spreadsheet:move", confirm_payload
            )
            envelope = {
                "requires_confirmation": True,
                "op": "copy_sheets_to_spreadsheet:move",
                "token": token,
                "ttl_seconds": _DESTRUCTIVE_CONFIRM_TTL_SECONDS,
                "source_spreadsheet_id": source_spreadsheet_id,
                "destination_spreadsheet_id": destination_spreadsheet_id,
                "sheet_names": sheet_names,
                "instructions": (
                    "delete_from_source=True will permanently delete the listed sheets "
                    "from the source after a successful copy. Show the user the source "
                    "spreadsheet and sheet names above, get explicit approval, then call "
                    "copy_sheets_to_spreadsheet again with the SAME arguments and "
                    "confirm_token set to the value above."
                ),
            }
            return json.dumps(envelope, ensure_ascii=False)
        _consume_destructive_confirm(
            "copy_sheets_to_spreadsheet:move", confirm_token, confirm_payload
        )
    elif confirm_token:
        raise UserInputError(
            "confirm_token was supplied but delete_from_source is False — "
            "tokens only apply to move operations."
        )

    new_names_map: dict = new_names or {}

    # ---- source metadata + sheet_id resolution ----
    source_sheets = await _fetch_sheets_metadata(service, source_spreadsheet_id)
    by_title: dict = {
        s["properties"].get("title"): s["properties"].get("sheetId")
        for s in source_sheets
    }

    results: list = []
    deletable_sheet_ids: list = []
    rename_requests: list = []

    # ---- per-sheet copy + queued rename ----
    for name in sheet_names:
        result: dict = {
            "sheet_name": name,
            "status": "ok",
            "source_sheet_id": None,
            "new_sheet_id": None,
            "final_title": None,
            "error": None,
        }
        src_id = by_title.get(name)
        if src_id is None:
            result["status"] = "error"
            result["error"] = "sheet not found in source spreadsheet"
            results.append(result)
            continue
        result["source_sheet_id"] = src_id

        try:
            new_props = await asyncio.to_thread(
                service.spreadsheets()
                .sheets()
                .copyTo(
                    spreadsheetId=source_spreadsheet_id,
                    sheetId=src_id,
                    body={"destinationSpreadsheetId": destination_spreadsheet_id},
                )
                .execute
            )
        except Exception as exc:  # noqa: BLE001 — surface message, keep loop alive
            result["status"] = "error"
            result["error"] = f"copyTo failed: {exc}"
            results.append(result)
            continue

        new_sheet_id = new_props.get("sheetId")
        new_title = new_props.get("title")
        result["new_sheet_id"] = new_sheet_id
        result["final_title"] = new_title

        # queue rename. If new_names doesn't override, target = original name
        # (Sheets API defaults to "Copy of <name>"; the plan says "= original").
        requested_new_name = new_names_map.get(name, name)
        if requested_new_name and requested_new_name != new_title:
            rename_requests.append(
                (
                    name,
                    new_sheet_id,
                    requested_new_name,
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": new_sheet_id,
                                "title": requested_new_name,
                            },
                            "fields": "title",
                        }
                    },
                )
            )

        deletable_sheet_ids.append((name, src_id))
        results.append(result)

    # ---- batch rename in destination (one batchUpdate, but track failures per-sheet) ----
    if rename_requests:
        # Try the batch first. If it fails (e.g. one collision aborts all),
        # fall back to per-request attempts so the rest of the renames go through.
        body = {"requests": [r[3] for r in rename_requests]}
        try:
            await asyncio.to_thread(
                service.spreadsheets()
                .batchUpdate(spreadsheetId=destination_spreadsheet_id, body=body)
                .execute
            )
            for name, _new_id, requested_new_name, _req in rename_requests:
                for r in results:
                    if r["sheet_name"] == name:
                        r["final_title"] = requested_new_name
                        break
        except Exception:
            # Per-request retry to isolate the offender
            for name, new_sheet_id, requested_new_name, req in rename_requests:
                try:
                    await asyncio.to_thread(
                        service.spreadsheets()
                        .batchUpdate(
                            spreadsheetId=destination_spreadsheet_id,
                            body={"requests": [req]},
                        )
                        .execute
                    )
                    for r in results:
                        if r["sheet_name"] == name:
                            r["final_title"] = requested_new_name
                            break
                except Exception as exc:  # noqa: BLE001
                    for r in results:
                        if r["sheet_name"] == name:
                            r["status"] = "error"
                            r["error"] = (
                                f"copy ok, rename to '{requested_new_name}' failed: {exc}"
                            )
                            break

    # ---- delete from source for successful copies ----
    deleted_count = 0
    if delete_from_source and deletable_sheet_ids:
        delete_requests = [
            {"deleteSheet": {"sheetId": src_id}} for _name, src_id in deletable_sheet_ids
        ]
        try:
            await asyncio.to_thread(
                service.spreadsheets()
                .batchUpdate(
                    spreadsheetId=source_spreadsheet_id,
                    body={"requests": delete_requests},
                )
                .execute
            )
            deleted_count = len(delete_requests)
            for name, _src_id in deletable_sheet_ids:
                for r in results:
                    if r["sheet_name"] == name:
                        r["deleted_from_source"] = True
                        break
        except Exception as exc:  # noqa: BLE001
            # If the whole batch delete failed, retry one at a time
            for name, src_id in deletable_sheet_ids:
                try:
                    await asyncio.to_thread(
                        service.spreadsheets()
                        .batchUpdate(
                            spreadsheetId=source_spreadsheet_id,
                            body={"requests": [{"deleteSheet": {"sheetId": src_id}}]},
                        )
                        .execute
                    )
                    deleted_count += 1
                    for r in results:
                        if r["sheet_name"] == name:
                            r["deleted_from_source"] = True
                            break
                except Exception as inner_exc:  # noqa: BLE001
                    for r in results:
                        if r["sheet_name"] == name:
                            r["deleted_from_source"] = False
                            existing = r.get("error")
                            note = f"delete from source failed: {inner_exc}"
                            r["error"] = f"{existing}; {note}" if existing else note
                            r["status"] = "error" if r["status"] == "ok" else r["status"]
                            break

    copied = sum(1 for r in results if r["new_sheet_id"] is not None)
    failed = sum(1 for r in results if r["status"] == "error")
    renamed = sum(
        1
        for r in results
        if r["new_sheet_id"] is not None
        and r["final_title"] == new_names_map.get(r["sheet_name"], r["sheet_name"])
    )

    envelope = {
        "source_spreadsheet_id": source_spreadsheet_id,
        "destination_spreadsheet_id": destination_spreadsheet_id,
        "requested": len(sheet_names),
        "copied": copied,
        "renamed": renamed,
        "deleted_from_source": deleted_count,
        "failed": failed,
        "results": results,
    }
    return json.dumps(envelope, ensure_ascii=False)


# ============================================================================
# Phase 9: structure-only reads + renumber_column (21 мая 2026)
# ============================================================================


def _gridrange_to_a1(gr: dict, sheet_title: str) -> str:
    """Convert a GridRange dict (zero-based half-open) to an A1 string.

    The Sheets API returns ranges as half-open intervals: startRowIndex is
    inclusive, endRowIndex is exclusive. We map back to A1 with safe
    handling of "whole row" or "whole column" ranges that omit indices.
    """

    def _col_letter(n: int) -> str:
        result = ""
        n += 1
        while n > 0:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

    sr = gr.get("startRowIndex")
    er = gr.get("endRowIndex")
    sc = gr.get("startColumnIndex")
    ec = gr.get("endColumnIndex")
    parts = ["'%s'" % sheet_title if " " in sheet_title or "." in sheet_title else sheet_title]
    if sc is not None and ec is not None and sr is not None and er is not None:
        start = f"{_col_letter(sc)}{sr + 1}"
        end = f"{_col_letter(ec - 1)}{er}"
        return f"{parts[0]}!{start}:{end}"
    if sc is not None and ec is not None:
        return f"{parts[0]}!{_col_letter(sc)}:{_col_letter(ec - 1)}"
    if sr is not None and er is not None:
        return f"{parts[0]}!{sr + 1}:{er}"
    return parts[0]


@server.tool()
@handle_http_errors("get_merged_ranges", service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_merged_ranges(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: Optional[str] = None,
) -> str:
    """
    List merged ranges in a spreadsheet.

    Compact alternative to read_sheet_values with includeGridData — returns
    only the structural metadata, no cell values. One API call regardless of
    sheet size.

    Args:
        user_google_email: The user's Google email.
        spreadsheet_id: Spreadsheet ID.
        sheet_name: Limit to a specific sheet. If None, returns merges for all
            sheets in the spreadsheet.

    Returns:
        str: JSON envelope with per-sheet merge lists (A1 references).
    """
    sheets = await _fetch_sheets_with(service, spreadsheet_id, "merges")
    if sheet_name:
        sheets = [_select_sheet(sheets, sheet_name)]
    out = []
    for s in sheets:
        title = s["properties"].get("title", "")
        merges = s.get("merges", []) or []
        out.append({
            "sheet_name": title,
            "sheet_id": s["properties"].get("sheetId"),
            "merge_count": len(merges),
            "merges": [_gridrange_to_a1(m, title) for m in merges],
        })
    return json.dumps({
        "spreadsheet_id": spreadsheet_id,
        "sheets": out,
    }, ensure_ascii=False)


@server.tool()
@handle_http_errors("get_named_ranges", service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_named_ranges(
    service,
    user_google_email: str,
    spreadsheet_id: str,
) -> str:
    """
    List named ranges in a spreadsheet.

    Compact one-call read of spreadsheet-level named-range definitions. Used
    when wiring up cross-sheet formulas or auditing range references without
    pulling cell data.

    Returns:
        str: JSON envelope with named ranges (name, id, range as A1).
    """
    meta = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="namedRanges,sheets.properties(sheetId,title)",
        )
        .execute
    )
    sheet_title_by_id = {
        s["properties"]["sheetId"]: s["properties"].get("title", "")
        for s in meta.get("sheets", [])
    }
    out = []
    for nr in meta.get("namedRanges", []):
        rng = nr.get("range", {})
        sid = rng.get("sheetId")
        title = sheet_title_by_id.get(sid, "")
        out.append({
            "name": nr.get("name"),
            "named_range_id": nr.get("namedRangeId"),
            "sheet_name": title,
            "range": _gridrange_to_a1(rng, title),
        })
    return json.dumps({
        "spreadsheet_id": spreadsheet_id,
        "named_ranges": out,
    }, ensure_ascii=False)


@server.tool()
@handle_http_errors("get_conditional_formats", service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_conditional_formats(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: Optional[str] = None,
) -> str:
    """
    List conditional format rules in a spreadsheet.

    Returns compact rule descriptors (rule index, ranges, type, condition
    values, format hint) without dumping the underlying cell data.

    Args:
        sheet_name: Restrict to a single sheet. If None, scans all sheets.

    Returns:
        str: JSON envelope with one entry per CF rule.
    """
    sheets = await _fetch_sheets_with(service, spreadsheet_id, "conditionalFormats")
    if sheet_name:
        sheets = [_select_sheet(sheets, sheet_name)]
    out = []
    for s in sheets:
        title = s["properties"].get("title", "")
        rules = s.get("conditionalFormats", []) or []
        for idx, rule in enumerate(rules):
            ranges_a1 = [_gridrange_to_a1(r, title) for r in rule.get("ranges", [])]
            entry: dict = {
                "sheet_name": title,
                "rule_index": idx,
                "ranges": ranges_a1,
            }
            if "booleanRule" in rule:
                cond = rule["booleanRule"].get("condition", {})
                entry["kind"] = "boolean"
                entry["condition_type"] = cond.get("type")
                entry["condition_values"] = [
                    v.get("userEnteredValue") for v in cond.get("values", [])
                ]
            elif "gradientRule" in rule:
                entry["kind"] = "gradient"
            out.append(entry)
    return json.dumps({
        "spreadsheet_id": spreadsheet_id,
        "rules": out,
    }, ensure_ascii=False)


@server.tool()
@handle_http_errors("get_data_validation_rules", service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_data_validation_rules(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: Optional[str] = None,
) -> str:
    """
    List data validation rules in a spreadsheet (or one sheet).

    Reads the grid metadata with a narrow field mask and collects only cells
    that have dataValidation set. Aggregates by exact rule signature so a
    300-cell dropdown range comes back as one entry, not 300.

    Returns:
        str: JSON envelope with rules per sheet (type, criteria values,
            strict flag, A1 ranges).
    """
    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    if sheet_name:
        sheets = [_select_sheet(sheets, sheet_name)]
    sheet_ids = [s["properties"]["sheetId"] for s in sheets]
    ranges = [s["properties"]["title"] for s in sheets]
    full = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            fields=(
                "sheets.properties(sheetId,title),"
                "sheets.data.rowData.values.dataValidation,"
                "sheets.data.startRow,"
                "sheets.data.startColumn"
            ),
        )
        .execute
    )
    out_sheets = []
    for s in full.get("sheets", []):
        sid = s["properties"]["sheetId"]
        if sid not in sheet_ids:
            continue
        title = s["properties"].get("title", "")
        sheet_rules: dict = {}
        for data_block in s.get("data", []):
            start_row = data_block.get("startRow", 0)
            start_col = data_block.get("startColumn", 0)
            for ri, row in enumerate(data_block.get("rowData", [])):
                for ci, cell in enumerate(row.get("values", [])):
                    dv = cell.get("dataValidation")
                    if not dv:
                        continue
                    sig = json.dumps(dv, sort_keys=True)
                    key = sheet_rules.setdefault(
                        sig,
                        {"rule": dv, "cells": []},
                    )
                    key["cells"].append((start_row + ri, start_col + ci))
        for sig, group in sheet_rules.items():
            cells = group["cells"]
            out_sheets.append({
                "sheet_name": title,
                "condition_type": group["rule"].get("condition", {}).get("type"),
                "condition_values": [
                    v.get("userEnteredValue")
                    for v in group["rule"].get("condition", {}).get("values", [])
                ],
                "strict": group["rule"].get("strict"),
                "show_custom_ui": group["rule"].get("showCustomUi"),
                "cell_count": len(cells),
                "first_cell": _gridrange_to_a1(
                    {
                        "startRowIndex": cells[0][0],
                        "endRowIndex": cells[0][0] + 1,
                        "startColumnIndex": cells[0][1],
                        "endColumnIndex": cells[0][1] + 1,
                    },
                    title,
                ),
            })
    return json.dumps({
        "spreadsheet_id": spreadsheet_id,
        "rules": out_sheets,
    }, ensure_ascii=False)


@server.tool()
@handle_http_errors("get_basic_filter_range", service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_basic_filter_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: Optional[str] = None,
) -> str:
    """
    Read the basic filter range currently set on each sheet.

    Useful before vertical merges (filter headers block merges) — gives the
    A1 of the filter so the caller can clearBasicFilter, do its work, and
    restore the same filter.

    Returns:
        str: JSON envelope; per sheet either basic_filter range or null.
    """
    sheets = await _fetch_sheets_with(service, spreadsheet_id, "basicFilter")
    if sheet_name:
        sheets = [_select_sheet(sheets, sheet_name)]
    out = []
    for s in sheets:
        title = s["properties"].get("title", "")
        bf = s.get("basicFilter")
        entry: dict = {
            "sheet_name": title,
            "sheet_id": s["properties"].get("sheetId"),
            "basic_filter": None,
        }
        if bf and "range" in bf:
            entry["basic_filter"] = _gridrange_to_a1(bf["range"], title)
        out.append(entry)
    return json.dumps({
        "spreadsheet_id": spreadsheet_id,
        "sheets": out,
    }, ensure_ascii=False)


@server.tool()
@handle_http_errors("get_protected_ranges", service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_protected_ranges(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: Optional[str] = None,
) -> str:
    """
    List protected ranges per sheet.

    Returns the range, protection id, description, warning-only flag and
    editor list (users / domain / sheet-wide flag) without dumping any cell
    contents.
    """
    sheets = await _fetch_sheets_with(service, spreadsheet_id, "protectedRanges")
    if sheet_name:
        sheets = [_select_sheet(sheets, sheet_name)]
    out = []
    for s in sheets:
        title = s["properties"].get("title", "")
        for p in s.get("protectedRanges", []) or []:
            rng = p.get("range", {})
            editors = p.get("editors", {})
            out.append({
                "sheet_name": title,
                "protected_range_id": p.get("protectedRangeId"),
                "description": p.get("description"),
                "warning_only": p.get("warningOnly", False),
                "whole_sheet": "range" not in p or not rng,
                "range": _gridrange_to_a1(rng, title) if rng else None,
                "editors_users": editors.get("users", []),
                "editors_domain_users_can_edit": editors.get("domainUsersCanEdit", False),
            })
    return json.dumps({
        "spreadsheet_id": spreadsheet_id,
        "protected_ranges": out,
    }, ensure_ascii=False)


@server.tool()
@handle_http_errors("renumber_column", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def renumber_column(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
    column: str,
    start_row: int,
    end_row: int,
    start_value: int = 1,
    step: int = 1,
    skip_blank_in_column: Optional[str] = None,
    dry_run: bool = False,
) -> str:
    """
    Rewrite a column as a contiguous integer sequence.

    Common case: after reordering rows (move/insert/delete), the "№" column
    is no longer monotonic. This tool writes start_value, start_value+step,
    start_value+2*step, ... into column[start_row..end_row] in one updateCells
    call.

    Args:
        sheet_name: Sheet to operate on.
        column: Target column in A1 (e.g. "A").
        start_row, end_row: Inclusive 1-based row range to renumber.
        start_value: First number (default 1).
        step: Increment (default 1).
        skip_blank_in_column: Optional column letter to inspect. Rows where
            this column is blank get an empty value (no number). Lets you
            renumber only the rows that have content somewhere else.
        dry_run: If True, return the planned values without writing.

    Returns:
        str: JSON envelope with planned/applied count and a preview of the
            first/last assignments.
    """
    if start_row < 1 or end_row < start_row:
        raise UserInputError("Invalid start_row/end_row (1-based, end >= start).")

    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    target = _select_sheet(sheets, sheet_name)
    sheet_id = target["properties"]["sheetId"]

    col_idx = _column_to_index(column)

    blank_mask: Optional[List[bool]] = None
    if skip_blank_in_column:
        check_col = _column_to_index(skip_blank_in_column)
        check_col_letter = skip_blank_in_column.upper()
        rng = f"'{sheet_name}'!{check_col_letter}{start_row}:{check_col_letter}{end_row}"
        resp = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=rng)
            .execute
        )
        rows = resp.get("values", [])
        blank_mask = []
        for i in range(end_row - start_row + 1):
            row = rows[i] if i < len(rows) else []
            val = row[0] if row else ""
            blank_mask.append(not (isinstance(val, str) and val.strip()) and not val)
        _ = check_col  # silence linters

    plan: list = []
    current = start_value
    for offset in range(end_row - start_row + 1):
        if blank_mask is not None and blank_mask[offset]:
            plan.append(None)
        else:
            plan.append(current)
            current += step

    preview = {
        "first": plan[:3],
        "last": plan[-3:],
        "total_rows": len(plan),
        "non_blank_count": sum(1 for v in plan if v is not None),
    }
    if dry_run:
        return json.dumps({
            "dry_run": True,
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": sheet_name,
            "column": column.upper(),
            "preview": preview,
        }, ensure_ascii=False)

    cells = [
        {
            "values": [
                {
                    "userEnteredValue": (
                        {"numberValue": v} if v is not None else {}
                    )
                }
            ]
        }
        for v in plan
    ]
    req = {
        "updateCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row - 1,
                "endRowIndex": end_row,
                "startColumnIndex": col_idx,
                "endColumnIndex": col_idx + 1,
            },
            "rows": cells,
            "fields": "userEnteredValue",
        }
    }
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [req]})
        .execute
    )
    return json.dumps({
        "spreadsheet_id": spreadsheet_id,
        "sheet_name": sheet_name,
        "column": column.upper(),
        "applied_rows": len(plan),
        "preview": preview,
    }, ensure_ascii=False)


# ============================================================================
# Phase 6: update_cell_rich_text — partial formatting inside a single cell
# ============================================================================


def _runs_to_api_format(runs: List[dict]) -> List[dict]:
    """Convert user-friendly run specs to Sheets API textFormatRuns.

    Each run must include start_index (0-based offset within the cell text).
    Optional formatting keys: bold, italic, underline, strikethrough,
    font_size, font_family, foreground_color (#RRGGBB).
    """
    api_runs = []
    for r in runs:
        if "start_index" not in r:
            raise UserInputError("Each run must have 'start_index'.")
        fmt: dict = {}
        for src, dst in [
            ("bold", "bold"),
            ("italic", "italic"),
            ("underline", "underline"),
            ("strikethrough", "strikethrough"),
            ("font_size", "fontSize"),
            ("font_family", "fontFamily"),
        ]:
            if src in r and r[src] is not None:
                fmt[dst] = r[src]
        if "foreground_color" in r and r["foreground_color"]:
            fmt["foregroundColor"] = _parse_hex_color(r["foreground_color"])
        api_runs.append({
            "startIndex": int(r["start_index"]),
            "format": fmt,
        })
    api_runs.sort(key=lambda x: x["startIndex"])
    if api_runs and api_runs[0]["startIndex"] != 0:
        # Sheets API requires the first run to start at 0; prepend a default
        # empty-format run so the cell's default style applies up to the
        # first user-supplied run.
        api_runs.insert(0, {"startIndex": 0, "format": {}})
    return api_runs


@server.tool()
@handle_http_errors("update_cell_rich_text", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def update_cell_rich_text(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
    cell: str,
    text: str,
    runs: Optional[List[dict]] = None,
    bold_prefix_length: Optional[int] = None,
) -> str:
    """
    Write text into a single cell with partial inline formatting.

    Sheets API ``textFormatRuns`` lets a single cell carry multiple styled
    segments (bold start + plain rest, coloured fragments, etc.). The
    standard format_sheet_range applies one style to the whole cell; this
    tool covers the in-cell mixed-format case.

    Args:
        sheet_name: Target sheet.
        cell: Target cell in A1 (e.g. "A3"). Must be one cell, not a range.
        text: Full text to write.
        runs: List of run specs. Each: {start_index, bold?, italic?,
            underline?, strikethrough?, font_size?, font_family?,
            foreground_color?}. start_index is 0-based offset within text.
        bold_prefix_length: Shortcut for the common case "first N chars bold,
            rest default". Equivalent to runs=[{start_index:0, bold:true},
            {start_index:N, bold:false}]. Ignored if runs is also given.

    Returns:
        str: Confirmation envelope with cell, char count, and run count.
    """
    if not text:
        raise UserInputError("text must be non-empty.")
    rc = cell.strip()
    if "!" in rc:
        raise UserInputError(
            "Pass sheet_name separately; cell must be a bare A1 like 'B3'."
        )
    col_letters = ""
    row_digits = ""
    for ch in rc:
        if ch.isalpha():
            col_letters += ch
        elif ch.isdigit():
            row_digits += ch
        else:
            raise UserInputError(f"Bad cell reference '{cell}'.")
    if not col_letters or not row_digits:
        raise UserInputError(f"Bad cell reference '{cell}'.")
    col_idx = _column_to_index(col_letters)
    row_idx = int(row_digits) - 1

    if runs is None and bold_prefix_length is not None:
        if bold_prefix_length <= 0 or bold_prefix_length > len(text):
            raise UserInputError(
                "bold_prefix_length must be in (0, len(text)]."
            )
        runs = [
            {"start_index": 0, "bold": True},
            {"start_index": bold_prefix_length, "bold": False},
        ]

    sheets = await _fetch_sheets_metadata(service, spreadsheet_id)
    target = _select_sheet(sheets, sheet_name)
    sheet_id = target["properties"]["sheetId"]

    cell_data: dict = {"userEnteredValue": {"stringValue": text}}
    api_runs = _runs_to_api_format(runs) if runs else []
    if api_runs:
        cell_data["textFormatRuns"] = api_runs
    fields = "userEnteredValue"
    if api_runs:
        fields += ",textFormatRuns"

    req = {
        "updateCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row_idx,
                "endRowIndex": row_idx + 1,
                "startColumnIndex": col_idx,
                "endColumnIndex": col_idx + 1,
            },
            "rows": [{"values": [cell_data]}],
            "fields": fields,
        }
    }
    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [req]})
        .execute
    )
    return json.dumps({
        "spreadsheet_id": spreadsheet_id,
        "sheet_name": sheet_name,
        "cell": rc.upper(),
        "char_count": len(text),
        "run_count": len(api_runs),
    }, ensure_ascii=False)
