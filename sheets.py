"""Запись заявок в Google Sheets через gspread.

Лист `Applications` создаётся автоматически при первом вызове, если его ещё нет.
Все операции синхронные — оборачивай в asyncio.to_thread из бота.
"""
from __future__ import annotations

import json
import os
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


def append_application(data: dict) -> None:
    ws = _get_ws()
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ws.append_row(
        [
            ts,
            str(data.get("tg_id", "")),
            data.get("tg_username", ""),
            data.get("examples", ""),
            data.get("experience", ""),
            data.get("contact", ""),
            data.get("name", ""),
            data.get("tg_first_name", ""),
            data.get("tg_last_name", ""),
        ],
        value_input_option="USER_ENTERED",
    )


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
