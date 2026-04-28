"""Запись заявок в Google Sheets через gspread.

Лист `Applications` создаётся автоматически при первом вызове, если его ещё нет.
Все операции синхронные — оборачивай в asyncio.to_thread из бота.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials

WORKSHEET_NAME = "Applications"
# Новые колонки добавляем В КОНЕЦ — auto-upgrade в _get_ws() переписывает только
# шапку, не двигает данные. Если вставить в середину, старые ряды поедут.
HEADERS = [
    "timestamp", "tg_id", "tg_username",
    "examples", "experience", "contact", "name",
    "tg_first_name", "tg_last_name",
    # follow-up MVP (этап 1, без AI):
    "followup_state",         # pending|approved|sending|sent|blocked|skipped|dry_run_sent
    "followup_draft",         # сгенерированный текст follow-up
    "followup_send_after",    # ISO-таймстемп когда можно слать
    "followup_sent_at",       # ISO-таймстемп фактической отправки
    "followup_error_reason",  # текст ошибки если followup_state=skipped по сбою
    "dry_run_sent_at",        # ISO когда был dry-run прогон (followup_state не трогает)
    "test_response",          # accepted|wants_terms|declined|submitted
    "test_video_url",         # ссылка или telegram file_id
]

EVENTS_WS_NAME = "Events"
EVENTS_HEADERS = ["timestamp", "tg_id", "tg_username", "event", "extra"]

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_ws = None  # кэш worksheet между вызовами
_events_ws = None


def _load_credentials() -> Credentials:
    # Приоритет: GOOGLE_CREDS_JSON (для Render — кладём JSON в env var,
    # потому что Render API не даёт надёжно заливать Secret Files).
    raw = os.getenv("GOOGLE_CREDS_JSON")
    if raw:
        return Credentials.from_service_account_info(json.loads(raw), scopes=_SCOPES)
    creds_file = os.getenv("GOOGLE_CREDS_FILE", "credentials.json")
    return Credentials.from_service_account_file(creds_file, scopes=_SCOPES)


def _get_ws():
    global _ws
    if _ws is not None:
        return _ws

    sheet_id = os.getenv("SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("SHEET_ID не задан")

    gc = gspread.authorize(_load_credentials())
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=len(HEADERS))
        ws.append_row(HEADERS)

    # Авто-апгрейд шапки если в листе старая схема (например после добавления колонки)
    actual = ws.row_values(1)
    if actual != HEADERS:
        ws.update("A1", [HEADERS])

    _ws = ws
    return ws


_RANGE_ROW_RE = re.compile(r"![A-Z]+(\d+)")


def _ensure_grid_rows(ws, target_row: int) -> None:
    """Если grid меньше target_row — расширяем с запасом. Грид не уменьшаем."""
    if ws.row_count >= target_row:
        return
    new_rows = max(target_row + 200, ws.row_count + 200)
    ws.resize(rows=new_rows)


def append_application(data: dict) -> int:
    """Добавляет строку и возвращает её 1-based row index, точно как Sheets API
    его положил (парсим из updatedRange ответа values:append)."""
    ws = _get_ws()
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    row = [
        ts,
        str(data.get("tg_id", "")),
        data.get("tg_username", ""),
        data.get("examples", ""),
        data.get("experience", ""),
        data.get("contact", ""),
        data.get("name", ""),
        data.get("tg_first_name", ""),
        data.get("tg_last_name", ""),
    ]
    # Дополним пустыми ячейками под follow-up колонки, чтобы шапка матчилась.
    row += [""] * (len(HEADERS) - len(row))
    # Передаём начальные значения follow-up если их прокинули в data.
    for i, col in enumerate(HEADERS):
        if col in data and data[col] != "" and not row[i]:
            row[i] = data[col]
    # Заранее даём grid'у запас, чтобы append не упёрся в лимит.
    populated = len(ws.get_all_values())
    _ensure_grid_rows(ws, populated + 2)
    res = ws.append_row(
        row,
        value_input_option="USER_ENTERED",
        insert_data_option="INSERT_ROWS",
    )
    updated_range = (res or {}).get("updates", {}).get("updatedRange", "")
    m = _RANGE_ROW_RE.search(updated_range)
    if not m:
        # Фолбэк — посчитаем populated rows. Менее надёжно, но не упадём.
        return len(ws.get_all_values())
    return int(m.group(1))


def update_application_fields(row_idx: int, fields: dict) -> None:
    """Обновляет конкретные ячейки строки. row_idx — 1-based (как в Sheets)."""
    ws = _get_ws()
    if row_idx <= 1:
        raise ValueError(f"row_idx={row_idx}: нельзя писать в шапку")
    # Если grid не дотягивает до этой строки — расширяем. Иначе Sheets API вернёт
    # "Range exceeds grid limits".
    _ensure_grid_rows(ws, row_idx)
    updates = []
    for col_name, value in fields.items():
        if col_name not in HEADERS:
            raise ValueError(f"unknown column: {col_name}")
        col_idx = HEADERS.index(col_name) + 1  # 1-based
        a1 = gspread.utils.rowcol_to_a1(row_idx, col_idx)
        updates.append({"range": a1, "values": [[str(value)]]})
    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")


def find_application_row(tg_id: int | str) -> int | None:
    """Ищет последнюю по времени строку юзера. 1-based row idx или None."""
    ws = _get_ws()
    rows = ws.get_all_values()
    if not rows:
        return None
    headers = rows[0]
    try:
        col_tg = headers.index("tg_id")
    except ValueError:
        return None
    target = str(tg_id)
    # Идём с конца — самая свежая запись юзера.
    for i in range(len(rows) - 1, 0, -1):
        if rows[i][col_tg] == target:
            return i + 1  # 1-based
    return None


def has_active_followup(tg_id: int | str) -> bool:
    """True если у tg_id уже есть незавершённый follow-up.

    Терминальные статусы (dry_run_sent/skipped/blocked) и завершённые ответы
    кандидата не блокируют новую заявку. Это важно для dry-run: одна тестовая
    песочница не должна навсегда запрещать повторную заявку с того же аккаунта.
    """
    ws = _get_ws()
    rows = ws.get_all_values()
    if len(rows) < 2:
        return False
    headers = rows[0]
    try:
        col_tg = headers.index("tg_id")
        col_state = headers.index("followup_state")
    except ValueError:
        return False
    try:
        col_response = headers.index("test_response")
    except ValueError:
        col_response = None
    target = str(tg_id)
    active_states = {"pending", "approved", "sending"}
    terminal_responses = {"declined", "submitted"}
    for r in rows[1:]:
        if len(r) <= max(col_tg, col_state):
            continue
        if r[col_tg] != target:
            continue
        state = r[col_state].strip()
        response = ""
        if col_response is not None and len(r) > col_response:
            response = r[col_response].strip()
        if state in active_states:
            return True
        if state == "sent" and response not in terminal_responses:
            return True
    return False


def read_applications_with_index() -> list[tuple[int, dict]]:
    """Возвращает [(row_idx, record_dict), ...]. row_idx — 1-based."""
    ws = _get_ws()
    records = ws.get_all_records()
    # get_all_records пропускает шапку, индексация с 0 → реальный row_idx = i+2
    return [(i + 2, rec) for i, rec in enumerate(records)]


def schema_check() -> list[str]:
    """Проверяет, что в листе есть все ожидаемые колонки. Возвращает список
    отсутствующих (если пусто — всё ок). Для вызова на старте бота."""
    ws = _get_ws()  # _get_ws сам апгрейдит шапку, так что после вызова всё ок.
    actual = ws.row_values(1)
    return [h for h in HEADERS if h not in actual]


def _get_events_ws():
    global _events_ws
    if _events_ws is not None:
        return _events_ws

    sheet_id = os.getenv("SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("SHEET_ID не задан")

    gc = gspread.authorize(_load_credentials())
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(EVENTS_WS_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=EVENTS_WS_NAME, rows=10000, cols=len(EVENTS_HEADERS))
        ws.append_row(EVENTS_HEADERS)

    actual = ws.row_values(1)
    if actual != EVENTS_HEADERS:
        ws.update("A1", [EVENTS_HEADERS])

    _events_ws = ws
    return ws


def append_event(tg_id: int | str, tg_username: str, event: str, extra: str = "") -> None:
    ws = _get_events_ws()
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ws.append_row(
        [ts, str(tg_id), tg_username or "", event, extra or ""],
        value_input_option="RAW",
    )


def read_events() -> list[dict]:
    """Все события из листа Events. Для агрегации в /stats."""
    ws = _get_events_ws()
    return ws.get_all_records()


def read_applications() -> list[dict]:
    """Все заявки из листа Applications."""
    ws = _get_ws()
    return ws.get_all_records()
