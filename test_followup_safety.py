"""Safety checks for follow-up dry-run.

No Telegram API and no Google Sheets calls.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import followup
import sheets


class FakeBot:
    def __init__(self, row: dict):
        self.row = row
        self.sent: list[dict] = []

    async def send_message(self, chat_id, text, reply_markup=None):
        # Dry-run must mark the row before sending, otherwise a failed update
        # after send can create an every-tick spam loop.
        assert self.row.get("followup_state") == "dry_run_sent"
        assert self.row.get("dry_run_sent_at")
        self.sent.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})


class FakeBriefMessage:
    def __init__(self):
        self.replies: list[str] = []
        self.documents: list[dict] = []

    async def reply(self, text, **kwargs):
        self.replies.append(text)

    async def answer_document(self, document, caption=None, **kwargs):
        self.documents.append({"document": document, "caption": caption})


class FakeWorksheet:
    def __init__(self, rows: list[list[str]]):
        self.rows = rows

    def get_all_values(self):
        return self.rows


def _install_fake_sheet(row: dict):
    def read_applications_with_index():
        return [(2, dict(row))]

    def update_application_fields(row_idx: int, fields: dict):
        assert row_idx == 2
        row.update(fields)

    followup.read_applications_with_index = read_applications_with_index
    followup.update_application_fields = update_application_fields


async def test_dry_run_is_at_most_once():
    row = {
        "tg_id": "123",
        "tg_username": "candidate",
        "followup_state": "approved",
        "followup_send_after": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(timespec="seconds"),
        "followup_draft": "Тестовый follow-up",
        "dry_run_sent_at": "",
    }
    _install_fake_sheet(row)
    followup.FOLLOWUP_DRY_RUN = True
    followup.ADMIN_IDS = [-1001]
    followup.DRY_RUN_ADMIN_IDS = [647035299]

    bot = FakeBot(row)
    counts = await followup._process_one_tick(bot)
    assert counts["dry_run"] == 1
    assert row["followup_state"] == "dry_run_sent"
    assert row["dry_run_sent_at"]
    assert len(bot.sent) == 1
    assert bot.sent[0]["chat_id"] == 647035299
    assert bot.sent[0]["reply_markup"] is not None
    assert bot.sent[0]["reply_markup"].inline_keyboard[0][0].callback_data == "fu_demo:ready"

    counts = await followup._process_one_tick(bot)
    assert counts["dry_run"] == 0
    assert len(bot.sent) == 1


async def test_old_dry_run_row_never_goes_real():
    row = {
        "tg_id": "123",
        "tg_username": "candidate",
        "followup_state": "approved",
        "followup_send_after": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(timespec="seconds"),
        "followup_draft": "Тестовый follow-up",
        "dry_run_sent_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _install_fake_sheet(row)
    followup.FOLLOWUP_DRY_RUN = False

    bot = FakeBot(row)
    counts = await followup._process_one_tick(bot)
    assert counts["errors"] == 1
    assert row["followup_state"] == "skipped"
    assert row["followup_error_reason"] == "dry_run_sent_requires_new_approval"
    assert not bot.sent


def test_approve_button_uses_actual_delay():
    old = followup.FOLLOWUP_DELAY_SEC
    try:
        followup.FOLLOWUP_DELAY_SEC = 60
        kb = followup._admin_kb(123)
        assert kb.inline_keyboard[0][0].text == "✅ Одобрить +1мин"
    finally:
        followup.FOLLOWUP_DELAY_SEC = old


def test_candidate_brief_is_complete():
    assert "700 ₽" in followup.TZ_TEXT
    assert followup.SITE_URL in followup.TZ_TEXT
    assert "файлом или ссылкой" in followup.TZ_TEXT
    assert "не используем тест без вашего разрешения" in followup.TERMS_TEXT
    assert followup.TZ_FILE_PATH.exists()

    kb = followup._terms_kb(123)
    assert kb.inline_keyboard[0][0].text == "✅ Ок, пришлите ТЗ"
    assert kb.inline_keyboard[0][0].callback_data == "fu_u:ready:123"

    demo_kb = followup._demo_terms_kb()
    assert demo_kb.inline_keyboard[0][0].callback_data == "fu_demo:ready"

    decline_kb = followup._decline_return_kb(123)
    assert decline_kb.inline_keyboard[0][0].callback_data == "fu_u:ready:123"
    assert decline_kb.inline_keyboard[1][0].callback_data == "fu_u:terms:123"


async def test_send_test_brief_sends_text_and_pdf():
    message = FakeBriefMessage()
    await followup._send_test_brief(message)
    assert len(message.replies) == 1
    assert "700 ₽" in message.replies[0]
    assert len(message.documents) == 1
    assert "Комикс-ТЗ" in message.documents[0]["caption"]


def test_dry_run_does_not_block_new_application():
    old_get_ws = sheets._get_ws
    headers = sheets.HEADERS
    rows = [headers]
    for state, response in [
        ("dry_run_sent", ""),
        ("skipped", ""),
        ("blocked", ""),
        ("sent", "submitted"),
        ("sent", "declined"),
    ]:
        row = [""] * len(headers)
        row[headers.index("tg_id")] = "123"
        row[headers.index("followup_state")] = state
        row[headers.index("test_response")] = response
        rows.append(row)
    try:
        sheets._get_ws = lambda: FakeWorksheet(rows)
        assert sheets.has_active_followup("123") is False

        pending = [""] * len(headers)
        pending[headers.index("tg_id")] = "123"
        pending[headers.index("followup_state")] = "pending"
        rows.append(pending)
        assert sheets.has_active_followup("123") is True
    finally:
        sheets._get_ws = old_get_ws


def test_moscow_work_window_schedule():
    old_delay = followup.FOLLOWUP_DELAY_SEC
    try:
        followup.FOLLOWUP_DELAY_SEC = 3 * 3600

        # 12:00 МСК approve +3ч = 15:00 МСК, остаётся сегодня.
        send_after = followup._calculate_send_after(
            datetime(2026, 4, 28, 9, 0, tzinfo=timezone.utc)
        )
        assert send_after == datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc)
        assert followup._format_send_after_msk(send_after) == "28.04 15:00 МСК"

        # 16:30 МСК approve +3ч = 19:30 МСК, перенос на завтра 09:00 МСК.
        send_after = followup._calculate_send_after(
            datetime(2026, 4, 28, 13, 30, tzinfo=timezone.utc)
        )
        assert send_after == datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc)
        assert followup._format_send_after_msk(send_after) == "29.04 09:00 МСК"

        # 05:00 МСК approve +3ч = 08:00 МСК, ждём до 09:00 МСК.
        send_after = followup._calculate_send_after(
            datetime(2026, 4, 28, 2, 0, tzinfo=timezone.utc)
        )
        assert send_after == datetime(2026, 4, 28, 6, 0, tzinfo=timezone.utc)
        assert followup._format_send_after_msk(send_after) == "28.04 09:00 МСК"
    finally:
        followup.FOLLOWUP_DELAY_SEC = old_delay


async def main() -> int:
    await test_dry_run_is_at_most_once()
    await test_old_dry_run_row_never_goes_real()
    test_approve_button_uses_actual_delay()
    test_candidate_brief_is_complete()
    await test_send_test_brief_sends_text_and_pdf()
    test_dry_run_does_not_block_new_application()
    test_moscow_work_window_schedule()
    print("ALL FOLLOWUP SAFETY TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
