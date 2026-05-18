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
