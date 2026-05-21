# Pending TODO для форка workspace-mcp

Список изменений, которые планировались, но отложены. Заводится по запросу Кирилла («запомни на будущее»).

---

## Phase 6 — `update_cell_rich_text` (отложено 18.05.2026)

**Что:** добавить tool для записи в **одну ячейку** Sheets текста + `textFormatRuns` — rich text runs внутри ячейки.

**Зачем:** нужен для смешанного форматирования внутри одной ячейки:
- жирное «Примечание» в начале + остальной обычный текст;
- цветной/курсивный фрагмент в середине текста;
- любые комбинации стилей на отдельных диапазонах символов.

Текущий `format_sheet_range` применяет формат **ко всей ячейке целиком**, не на часть текста — этот пробел и закрывается новым tool.

**API под капотом:** Sheets `batchUpdate` с request `updateCells`, в `CellData`:
- `userEnteredValue.stringValue` — полный текст ячейки;
- `textFormatRuns` — массив `{startIndex, format: {bold, italic, underline, strikethrough, fontSize, fontFamily, foregroundColor}}`;
- `fields: "userEnteredValue,textFormatRuns"`.

Первый run обычно `startIndex=0` с одним стилем (например, `bold=true` на длину «Примечание» = 10), второй run — `startIndex=10` с обычным стилем. Дальнейшие runs аналогично.

**Предлагаемая сигнатура tool:**

```python
@server.tool()
@handle_http_errors("update_cell_rich_text", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def update_cell_rich_text(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,      # A1-style ОДНА ячейка, e.g. "Sheet!A3"
    text: str,            # полный текст с \n для переносов
    runs: Optional[Union[str, List[dict]]] = None,
        # список {start, format: {bold/italic/underline/strikethrough/
        #                          foreground_color/font_size/font_family}}
) -> str:
    ...
```

Парсинг `range_name` через существующий `_parse_a1_range`. Проверка что это одна ячейка (endRow-startRow=1 && endCol-startCol=1) — иначе UserInputError.

Цвет — через существующий `_parse_hex_color`. JSON-парсинг runs — через существующий `_parse_json`.

**Регистрация:** `core/tool_tiers.yaml` → `sheets:complete:` под именем `update_cell_rich_text`.

**Workflow деплоя** (как Phase 5):
1. `docker tag workspace-mcp:local workspace-mcp:pre-phase6`
2. правка `gsheets/sheets_tools_ext.py` + `core/tool_tiers.yaml`
3. `python3 -c "import ast; ast.parse(...)"` smoke
4. `docker compose build workspace-mcp && docker compose up -d workspace-mcp`
5. `git add → commit → push`
6. **Reconnect коннектора Кюст в Claude Desktop** (полное удаление + добавление).

**Где будет применяться сразу после внедрения:**
- Таблица «Посты × операции» (id `1kHpqtWr4HxNcAsqJWEYBEX_ZGMoKXWunxyIBf-bue2k`), Row 3 (примечания) на всех 4 листах: жирное «Примечание» + остальной обычный текст с font_size=8.

**Источник запроса:** диалог с Кириллом 18.05.2026 по таблице «Посты × операции», требование «примечание начинай с жирного Примечание».

**Решение Кирилла:** не делать сейчас («не делай, запомни»).
