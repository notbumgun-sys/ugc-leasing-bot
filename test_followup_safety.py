"""Safety checks for follow-up dry-run.

No Telegram API and no Google Sheets calls.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import followup


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


async def main() -> int:
    await test_dry_run_is_at_most_once()
    await test_old_dry_run_row_never_goes_real()
    test_approve_button_uses_actual_delay()
    print("ALL FOLLOWUP SAFETY TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
