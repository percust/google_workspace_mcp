"""Unit tests for gdocs.docs_markdown_writer."""

import pytest

from gdocs.docs_markdown_writer import markdown_to_docs_requests


def test_empty_markdown_returns_empty_list():
    requests = markdown_to_docs_requests("")
    assert requests == []


def test_returns_list_of_dicts():
    requests = markdown_to_docs_requests("Hello world")
    assert isinstance(requests, list)
    assert len(requests) >= 1, "Non-empty input should produce at least one request"
    assert all(isinstance(r, dict) for r in requests)


def test_single_paragraph_emits_insert_text():
    requests = markdown_to_docs_requests("Hello world")
    inserts = [r for r in requests if "insertText" in r]
    assert len(inserts) == 1
    assert inserts[0]["insertText"]["text"] == "Hello world\n"
    assert inserts[0]["insertText"]["location"]["index"] == 1


def test_two_paragraphs_emit_two_inserts_with_correct_indices():
    requests = markdown_to_docs_requests("First para\n\nSecond para")
    inserts = [r for r in requests if "insertText" in r]
    assert len(inserts) == 2
    assert inserts[0]["insertText"]["text"] == "First para\n"
    assert inserts[0]["insertText"]["location"]["index"] == 1
    # Second paragraph starts after first's text + newline
    assert inserts[1]["insertText"]["text"] == "Second para\n"
    assert inserts[1]["insertText"]["location"]["index"] == 1 + len("First para\n")


def test_h1_emits_insert_and_heading_style():
    requests = markdown_to_docs_requests("# My Title")
    inserts = [r for r in requests if "insertText" in r]
    styles = [r for r in requests if "updateParagraphStyle" in r]
    assert len(inserts) == 1
    assert inserts[0]["insertText"]["text"] == "My Title\n"
    assert len(styles) == 1
    assert styles[0]["updateParagraphStyle"]["paragraphStyle"]["namedStyleType"] == "HEADING_1"
    # Range should cover the heading text
    rng = styles[0]["updateParagraphStyle"]["range"]
    assert rng["startIndex"] == 1
    assert rng["endIndex"] == 1 + len("My Title\n")


def test_h2_h3_h4_h5_h6_all_emit_correct_named_style():
    for level in range(2, 7):
        hashes = "#" * level
        md = f"{hashes} Heading L{level}"
        requests = markdown_to_docs_requests(md)
        styles = [r for r in requests if "updateParagraphStyle" in r]
        assert len(styles) == 1
        assert styles[0]["updateParagraphStyle"]["paragraphStyle"]["namedStyleType"] == f"HEADING_{level}"


def test_bold_span_emits_update_text_style():
    requests = markdown_to_docs_requests("This is **bold** text.")
    inserts = [r for r in requests if "insertText" in r]
    styles = [r for r in requests if "updateTextStyle" in r]
    assert len(inserts) == 1
    assert inserts[0]["insertText"]["text"] == "This is bold text.\n"
    assert len(styles) == 1
    ts = styles[0]["updateTextStyle"]
    assert ts["textStyle"]["bold"] is True
    rng = ts["range"]
    assert rng["startIndex"] == 1 + len("This is ")
    assert rng["endIndex"] == rng["startIndex"] + len("bold")


def test_italic_span_emits_italic_style():
    requests = markdown_to_docs_requests("Some *italic* word.")
    styles = [r for r in requests if "updateTextStyle" in r]
    assert len(styles) == 1
    assert styles[0]["updateTextStyle"]["textStyle"]["italic"] is True


def test_inline_code_emits_monospace_style():
    requests = markdown_to_docs_requests("Use the `foo()` function.")
    styles = [r for r in requests if "updateTextStyle" in r]
    assert len(styles) == 1
    ts = styles[0]["updateTextStyle"]["textStyle"]
    assert ts.get("weightedFontFamily", {}).get("fontFamily") in (
        "Courier New",
        "Roboto Mono",
        "Consolas",
    )


def test_link_emits_link_style():
    requests = markdown_to_docs_requests("See [docs](https://example.com) here.")
    styles = [r for r in requests if "updateTextStyle" in r]
    assert len(styles) == 1
    assert styles[0]["updateTextStyle"]["textStyle"]["link"]["url"] == "https://example.com"


def test_combined_bold_and_italic_spans():
    requests = markdown_to_docs_requests("A **bold** and *italic* mix.")
    styles = [r for r in requests if "updateTextStyle" in r]
    assert len(styles) == 2
    style_types = sorted([
        "bold" if s["updateTextStyle"]["textStyle"].get("bold") else "italic"
        for s in styles
    ])
    assert style_types == ["bold", "italic"]


def test_unordered_list_emits_bullets():
    md = "- Item one\n- Item two\n- Item three"
    requests = markdown_to_docs_requests(md)
    inserts = [r for r in requests if "insertText" in r]
    bullets = [r for r in requests if "createParagraphBullets" in r]
    assert len(inserts) == 3
    assert inserts[0]["insertText"]["text"] == "Item one\n"
    assert inserts[1]["insertText"]["text"] == "Item two\n"
    # One bullet creation request covering all three items
    assert len(bullets) == 1
    preset = bullets[0]["createParagraphBullets"]["bulletPreset"]
    assert preset == "BULLET_DISC_CIRCLE_SQUARE"


def test_ordered_list_emits_numbered_preset():
    md = "1. First\n2. Second\n3. Third"
    requests = markdown_to_docs_requests(md)
    bullets = [r for r in requests if "createParagraphBullets" in r]
    assert len(bullets) == 1
    preset = bullets[0]["createParagraphBullets"]["bulletPreset"]
    assert preset == "NUMBERED_DECIMAL_ALPHA_ROMAN"
