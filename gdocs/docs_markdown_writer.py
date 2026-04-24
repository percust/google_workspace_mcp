"""Markdown to Google Docs API batchUpdate request converter.

Parses CommonMark+GFM markdown and emits a list of Docs API request dicts
that, when applied in order, render the markdown into a document or a
specific tab within a document.

Primary entry point - markdown_to_docs_requests(markdown_text, tab_id=None).
"""

from __future__ import annotations

from typing import Optional

from markdown_it import MarkdownIt


def markdown_to_docs_requests(
    markdown_text: str,
    tab_id: Optional[str] = None,
    start_index: int = 1,
) -> list[dict]:
    """Convert markdown to a list of Docs API batchUpdate request dicts.

    Args:
        markdown_text - the markdown source
        tab_id - optional tab ID; when provided, every range targets this tab
        start_index - document index at which content insertion begins

    Returns:
        Ordered list of request dicts. Empty list for empty input.
    """
    if not markdown_text.strip():
        return []

    md = MarkdownIt("commonmark")
    tokens = md.parse(markdown_text)

    requests: list[dict] = []
    _emit_requests(tokens, requests, tab_id, start_index)
    return requests


def _emit_requests(tokens, requests, tab_id, start_index):
    """Walk markdown-it tokens and append Docs API requests.

    Maintains a running `cursor` that represents the current insertion point
    in the document. Each insertText advances cursor by len(text).
    """
    cursor = [start_index]  # mutable via list so helpers can advance it

    i = 0
    while i < len(tokens):
        tok = tokens[i]

        if tok.type == "heading_open":
            level = int(tok.tag[1])  # 'h1' -> 1
            inline_tok = tokens[i + 1]
            text, inline_styles = _render_inline_with_styles(
                inline_tok.children or [], cursor[0], tab_id
            )
            text += "\n"
            range_start = cursor[0]
            requests.append(_build_insert_text(cursor[0], text, tab_id))
            cursor[0] += len(text)
            requests.append(_build_heading_style(range_start, cursor[0], level, tab_id))
            requests.extend(inline_styles)
            i += 3
            continue

        if tok.type in ("bullet_list_open", "ordered_list_open"):
            preset = (
                "BULLET_DISC_CIRCLE_SQUARE"
                if tok.type == "bullet_list_open"
                else "NUMBERED_DECIMAL_ALPHA_ROMAN"
            )
            list_start = cursor[0]
            # Find the matching closing token
            close_type = tok.type.replace("_open", "_close")
            depth = 1
            j = i + 1
            while j < len(tokens) and depth > 0:
                if tokens[j].type == tok.type:
                    depth += 1
                elif tokens[j].type == close_type:
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            # Iterate items between i and j
            k = i + 1
            while k < j:
                item = tokens[k]
                if item.type == "list_item_open":
                    # Inner structure typically - list_item_open, paragraph_open, inline, paragraph_close, list_item_close
                    # Find the inline token within this list_item
                    if k + 2 < j and tokens[k + 2].type == "inline":
                        inline_tok = tokens[k + 2]
                        text, inline_styles = _render_inline_with_styles(
                            inline_tok.children or [], cursor[0], tab_id
                        )
                        text += "\n"
                        requests.append(_build_insert_text(cursor[0], text, tab_id))
                        cursor[0] += len(text)
                        requests.extend(inline_styles)
                k += 1
            list_end = cursor[0]
            # One createParagraphBullets covering the full list range
            rng = {"startIndex": list_start, "endIndex": list_end}
            if tab_id:
                rng["tabId"] = tab_id
            requests.append({
                "createParagraphBullets": {
                    "range": rng,
                    "bulletPreset": preset,
                }
            })
            i = j + 1
            continue

        if tok.type == "paragraph_open":
            # paragraph_open is followed by inline (children), then paragraph_close
            inline_tok = tokens[i + 1]
            text, inline_styles = _render_inline_with_styles(
                inline_tok.children or [], cursor[0], tab_id
            )
            text += "\n"
            requests.append(_build_insert_text(cursor[0], text, tab_id))
            cursor[0] += len(text)
            requests.extend(inline_styles)
            i += 3  # skip paragraph_open, inline, paragraph_close
            continue

        i += 1


def _render_inline_with_styles(
    children,
    base_index: int,
    tab_id: Optional[str],
) -> tuple[str, list[dict]]:
    """Walk inline tokens, returning plain text and style requests.

    Args:
        children - inline tokens from markdown-it
        base_index - the document index where this inline block starts
        tab_id - optional tab ID for ranges

    Returns:
        (plain_text, style_requests). The caller emits insertText with
        plain_text starting at base_index, then appends the style_requests.
    """
    text_parts: list[str] = []
    style_requests: list[dict] = []
    local_pos = 0  # position within this inline block (0-based)
    # Stack entries are tuples. For strong/em: (style_name, start_local_pos).
    # For link: (style_name, start_local_pos, href).
    stack: list[tuple] = []

    for tok in children:
        if tok.type == "text":
            text_parts.append(tok.content)
            local_pos += len(tok.content)
        elif tok.type == "softbreak":
            text_parts.append(" ")
            local_pos += 1
        elif tok.type == "hardbreak":
            text_parts.append("\n")
            local_pos += 1
        elif tok.type == "code_inline":
            # self-contained - emit style immediately
            start_local = local_pos
            text_parts.append(tok.content)
            local_pos += len(tok.content)
            style_requests.append(
                _build_text_style(
                    base_index + start_local,
                    base_index + local_pos,
                    {"weightedFontFamily": {"fontFamily": "Courier New", "weight": 400}},
                    "weightedFontFamily",
                    tab_id,
                )
            )
        elif tok.type in ("strong_open", "em_open"):
            stack.append((tok.type, local_pos))
        elif tok.type in ("strong_close", "em_close"):
            opener_type = tok.type.replace("_close", "_open")
            for idx in range(len(stack) - 1, -1, -1):
                if stack[idx][0] == opener_type:
                    _, start_local = stack.pop(idx)
                    style_key = "bold" if opener_type == "strong_open" else "italic"
                    style_requests.append(
                        _build_text_style(
                            base_index + start_local,
                            base_index + local_pos,
                            {style_key: True},
                            style_key,
                            tab_id,
                        )
                    )
                    break
        elif tok.type == "link_open":
            # tok.attrs may be a dict (newer markdown-it-py) or list of [key, val]
            # pairs (older). Support both.
            attrs = tok.attrs
            if isinstance(attrs, dict):
                href = attrs.get("href")
            else:
                href = next((a[1] for a in attrs if a[0] == "href"), None)
            stack.append(("link_open", local_pos, href))
        elif tok.type == "link_close":
            for idx in range(len(stack) - 1, -1, -1):
                if stack[idx][0] == "link_open":
                    _, start_local, href = stack.pop(idx)
                    style_requests.append(
                        _build_text_style(
                            base_index + start_local,
                            base_index + local_pos,
                            {"link": {"url": href}},
                            "link",
                            tab_id,
                        )
                    )
                    break

    return "".join(text_parts), style_requests


def _build_text_style(
    start: int,
    end: int,
    style: dict,
    fields: str,
    tab_id: Optional[str],
) -> dict:
    """Build an updateTextStyle request."""
    rng = {"startIndex": start, "endIndex": end}
    if tab_id:
        rng["tabId"] = tab_id
    return {
        "updateTextStyle": {
            "range": rng,
            "textStyle": style,
            "fields": fields,
        }
    }


def _build_insert_text(index: int, text: str, tab_id: Optional[str]) -> dict:
    """Build an insertText request dict, threading tab_id if provided."""
    location = {"index": index}
    if tab_id:
        location["tabId"] = tab_id
    return {"insertText": {"location": location, "text": text}}


def _build_heading_style(
    start: int, end: int, level: int, tab_id: Optional[str]
) -> dict:
    """Build updateParagraphStyle request setting HEADING_N named style."""
    rng = {"startIndex": start, "endIndex": end}
    if tab_id:
        rng["tabId"] = tab_id
    return {
        "updateParagraphStyle": {
            "range": rng,
            "paragraphStyle": {"namedStyleType": f"HEADING_{level}"},
            "fields": "namedStyleType",
        }
    }
