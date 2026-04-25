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
HEADERS = ["timestamp", "tg_id", "tg_username", "examples", "experience", "contact"]
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_ws = None  # кэш worksheet между вызовами


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
        ],
        value_input_option="USER_ENTERED",
    )
