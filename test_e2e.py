"""E2E прогон бота — вызывает реальные хэндлеры из bot.py с фейковыми
Message/User/Chat. Реально пишет тестовую строку в Google Sheets через
sheets.append_application. Потом читает её обратно для проверки и удаляет.

Запуск:
    PYTHONIOENCODING=utf-8 venv/Scripts/python.exe test_e2e.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, str(Path(__file__).parent))

import bot as bot_mod  # noqa: E402  загружает .env, регистрирует хэндлеры
import sheets as sheets_mod  # noqa: E402
from aiogram.fsm.context import FSMContext  # noqa: E402
from aiogram.fsm.storage.base import StorageKey  # noqa: E402
from aiogram.fsm.storage.memory import MemoryStorage  # noqa: E402

TEST_UID = 999_888_777
TEST_USERNAME = "e2e_claude_test"

EXAMPLES = (
    "[E2E TEST] https://www.youtube.com/watch?v=dQw4w9WgXcQ "
    "https://t.me/leasing_stock_demo "
    "https://www.tiktok.com/@autoblogger/video/123"
)
EXPERIENCE = (
    "[E2E TEST] Снимаю авто-обзоры 3+ года для YouTube и Reels. "
    "Камера Sony A7IV, объективы 24-70 + 85mm, петличка Rode Wireless Go II, "
    "дрон DJI Mini 4 Pro. Делал контент для официальных дилеров."
)
CONTACT = "[E2E TEST] @stockauto_demo, +7 999 000-00-00"


async def run() -> int:
    captured_admin_msgs: list[str] = []

    async def fake_notify(bot, text):
        captured_admin_msgs.append(text)

    bot_mod._notify_admins = fake_notify  # noqa: SLF001
    bot_mod._last_submission.pop(TEST_UID, None)

    storage = MemoryStorage()
    key = StorageKey(bot_id=0, chat_id=TEST_UID, user_id=TEST_UID)
    state = FSMContext(storage=storage, key=key)

    user = MagicMock()
    user.id = TEST_UID
    user.username = TEST_USERNAME
    user.first_name = "E2E"
    chat = MagicMock()
    chat.id = TEST_UID

    sent: list[tuple[str, str]] = []

    def make_msg(text: str):
        m = MagicMock()
        m.from_user = user
        m.chat = chat
        m.text = text
        m.contact = None  # текстовый ввод, не «Поделиться контактом»

        async def answer(t, reply_markup=None, **kw):
            sent.append(("BOT", t))
            return MagicMock(from_user=user, chat=chat, text=t)

        m.answer = answer
        return m

    print("─" * 70)
    print("E2E прогон UGC-бота")
    print("─" * 70)

    sent.append(("USER", "/start"))
    await bot_mod.cmd_start(make_msg("/start"), state)
    assert (await state.get_state()) == bot_mod.Form.examples.state, "after /start"

    sent.append(("USER", EXAMPLES))
    await bot_mod.got_examples(make_msg(EXAMPLES), state)
    assert (await state.get_state()) == bot_mod.Form.experience.state, "after examples"

    sent.append(("USER", EXPERIENCE))
    await bot_mod.got_experience(make_msg(EXPERIENCE), state)
    assert (await state.get_state()) == bot_mod.Form.contact.state, "after experience"

    sent.append(("USER", CONTACT))
    fake_bot = MagicMock()
    fake_bot.send_message = AsyncMock()
    await bot_mod.got_contact(make_msg(CONTACT), state, fake_bot)
    assert (await state.get_state()) is None, "state cleared after submission"

    for who, text in sent:
        prefix = "→" if who == "USER" else "←"
        print(f"  {prefix} {who:4} {text[:120]}")

    print("\nNotify-admin payload:")
    for n in captured_admin_msgs:
        print("  " + n.replace("\n", "\n  "))

    print("\n=== Verifying Google Sheets row ===")
    ws = sheets_mod._get_ws()  # noqa: SLF001
    rows = ws.get_all_values()
    last = rows[-1] if rows else []
    print(f"  total rows: {len(rows)}  | last row: {last}")

    ok = (
        last
        and str(last[1]) == str(TEST_UID)
        and last[2] == TEST_USERNAME
        and last[3] == EXAMPLES
        and last[4] == EXPERIENCE
        and last[5] == CONTACT
    )
    if not ok:
        print("  ❌ FAIL — последняя строка не совпадает с тестовыми данными")
        return 1

    print("  ✅ PASS — строка записана корректно")

    last_row_idx = len(rows)
    print(f"\nЧистка: удаляю тестовую строку #{last_row_idx}")
    ws.delete_rows(last_row_idx)
    print("  ✅ удалено")

    # --- Сценарий 2: кнопка «↩️ Назад» ---
    print("\n─── Сценарий 2: «↩️ Назад» с шага 3 → шаг 2 → шаг 1 ───")
    state2 = FSMContext(storage=storage, key=StorageKey(bot_id=0, chat_id=2, user_id=2))
    user2 = MagicMock(); user2.id = 2; user2.username = "back_tester"
    chat2 = MagicMock(); chat2.id = 2

    def mk(text, contact=None):
        m = MagicMock(); m.from_user = user2; m.chat = chat2
        m.text = text; m.contact = contact
        async def answer(t, reply_markup=None, **kw): pass
        m.answer = answer
        return m

    await bot_mod.cmd_start(mk("/start"), state2)
    await bot_mod.got_examples(mk("https://t.me/x"), state2)
    await bot_mod.got_experience(mk("snimayu давно"), state2)
    assert (await state2.get_state()) == bot_mod.Form.contact.state

    await bot_mod.cmd_back(mk(bot_mod.BTN_BACK), state2)
    assert (await state2.get_state()) == bot_mod.Form.experience.state, "back to experience"
    print("  ✅ contact → experience")

    await bot_mod.cmd_back(mk(bot_mod.BTN_BACK), state2)
    assert (await state2.get_state()) == bot_mod.Form.examples.state, "back to examples"
    print("  ✅ experience → examples")

    await bot_mod.cmd_back(mk(bot_mod.BTN_BACK), state2)
    assert (await state2.get_state()) == bot_mod.Form.examples.state, "stays on examples"
    print("  ✅ examples: остаётся на месте (некуда назад)")

    # --- Сценарий 3: «📱 Отправить мой контакт» ---
    print("\n─── Сценарий 3: контакт через request_contact ───")
    state3 = FSMContext(storage=storage, key=StorageKey(bot_id=0, chat_id=3, user_id=3))
    user3 = MagicMock(); user3.id = 3; user3.username = "contact_tester"
    chat3 = MagicMock(); chat3.id = 3

    def mk3(text, contact=None):
        m = MagicMock(); m.from_user = user3; m.chat = chat3
        m.text = text; m.contact = contact
        async def answer(t, reply_markup=None, **kw): pass
        m.answer = answer
        return m

    await bot_mod.cmd_start(mk3("/start"), state3)
    await bot_mod.got_examples(mk3("[E2E TEST] https://t.me/contact_test"), state3)
    await bot_mod.got_experience(mk3("[E2E TEST] контакт-сценарий"), state3)

    fake_contact = MagicMock()
    fake_contact.first_name = "Иван"
    fake_contact.last_name = "Петров"
    fake_contact.phone_number = "+79991234567"
    bot_mod._last_submission.pop(3, None)
    await bot_mod.got_contact(mk3(None, contact=fake_contact), state3, fake_bot)

    rows = ws.get_all_values()
    last = rows[-1]
    expected_contact = "Иван Петров, тел.: +79991234567"
    if last[5] == expected_contact:
        print(f"  ✅ contact записан: {last[5]}")
    else:
        print(f"  ❌ FAIL — got: {last[5]}")
        return 1
    ws.delete_rows(len(rows))
    print(f"  ✅ тестовая строка удалена")

    print("\n══════════ ALL SCENARIOS PASSED ══════════")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
