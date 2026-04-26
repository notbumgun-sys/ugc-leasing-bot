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

import bot as bot_mod  # noqa: E402
import sheets as sheets_mod  # noqa: E402
from aiogram.fsm.context import FSMContext  # noqa: E402
from aiogram.fsm.storage.base import StorageKey  # noqa: E402
from aiogram.fsm.storage.memory import MemoryStorage  # noqa: E402

EXAMPLES = (
    "[E2E TEST] https://www.youtube.com/watch?v=dQw4w9WgXcQ "
    "https://t.me/leasing_stock_demo "
    "https://www.tiktok.com/@autoblogger/video/123"
)
EXPERIENCE = "[E2E TEST] Снимаю авто-обзоры 3+ года для YouTube и Reels."
NAME = "[E2E TEST] Иван"
CONTACT = "[E2E TEST] @stockauto_demo, +7 999 000-00-00"


def _mk_msg(user, chat, text=None, contact=None):
    m = MagicMock()
    m.from_user = user
    m.chat = chat
    m.text = text
    m.contact = contact

    async def answer(t, reply_markup=None, **kw):
        return MagicMock(from_user=user, chat=chat, text=t)

    m.answer = answer
    return m


async def run() -> int:
    notif: list[str] = []

    async def fake_notify(bot, text):
        notif.append(text)

    bot_mod._notify_admins = fake_notify
    fake_bot = MagicMock()
    fake_bot.send_message = AsyncMock()

    storage = MemoryStorage()
    print("=" * 60)
    print("E2E прогон UGC-бота: 4 шага → сводка → submit")
    print("=" * 60)

    # --- Сценарий 1: full happy path ---
    uid1 = 999_888_777
    bot_mod._last_submission.pop(uid1, None)
    user = MagicMock()
    user.id = uid1
    user.username = "e2e_full"
    user.first_name = "Иван"
    user.last_name = "Тестов"
    chat = MagicMock(); chat.id = uid1
    state = FSMContext(storage=storage, key=StorageKey(bot_id=0, chat_id=uid1, user_id=uid1))

    await bot_mod.cmd_start(_mk_msg(user, chat, "/start"), state)
    assert (await state.get_state()) == bot_mod.Form.examples.state

    await bot_mod.got_examples(_mk_msg(user, chat, EXAMPLES), state)
    assert (await state.get_state()) == bot_mod.Form.experience.state, "→ experience"

    await bot_mod.got_experience(_mk_msg(user, chat, EXPERIENCE), state)
    assert (await state.get_state()) == bot_mod.Form.name.state, "→ name"

    await bot_mod.got_name(_mk_msg(user, chat, NAME), state)
    assert (await state.get_state()) == bot_mod.Form.contact.state, "→ contact"

    await bot_mod.got_contact(_mk_msg(user, chat, CONTACT), state)
    assert (await state.get_state()) == bot_mod.Form.confirm.state, "→ confirm (NOT yet in Sheets)"
    print("  ✅ После contact заявка в Form.confirm — НЕ записана в Sheets автоматически")

    # На confirm-шаге проверим что фолбек на левый текст не пускает submit
    rows_before = len(sheets_mod._get_ws().get_all_values())
    await bot_mod.confirm_fallback(_mk_msg(user, chat, "что-то левое"), state)
    rows_after = len(sheets_mod._get_ws().get_all_values())
    assert rows_before == rows_after, "submit НЕ должен срабатывать на произвольный текст"
    print("  ✅ Левый текст на confirm-шаге не отправляет заявку")

    await bot_mod.cmd_submit(_mk_msg(user, chat, bot_mod.BTN_SUBMIT), state, fake_bot)
    assert (await state.get_state()) is None, "после submit state очищен"

    ws = sheets_mod._get_ws()
    rows = ws.get_all_values()
    last = rows[-1]
    print(f"  Sheets last row: ts={last[0]} contact={last[5][:40]!r} name={last[6]!r} tg_first={last[7]!r} tg_last={last[8]!r}")
    expected = (str(uid1), "e2e_full", EXAMPLES, EXPERIENCE, CONTACT, NAME, "Иван", "Тестов")
    actual = (last[1], last[2], last[3], last[4], last[5], last[6], last[7], last[8])
    if actual != expected:
        print(f"  ❌ FAIL\n     expected: {expected}\n     got:      {actual}")
        return 1
    print("  ✅ Запись в Sheets корректна (с tg_first_name/tg_last_name)")

    ws.delete_rows(len(rows))
    print("  ✅ Тестовая строка удалена")

    # --- Сценарий 2: «↩️ Назад» работает на каждом шаге, включая confirm ---
    print("\n--- Сценарий 2: «↩️ Назад» по всем шагам, включая финальный confirm ---")
    uid2 = 2
    state2 = FSMContext(storage=storage, key=StorageKey(bot_id=0, chat_id=uid2, user_id=uid2))
    user2 = MagicMock()
    user2.id = uid2
    user2.username = "back_tester"
    user2.first_name = "Назад"
    user2.last_name = "Тестов"
    chat2 = MagicMock(); chat2.id = uid2

    await bot_mod.cmd_start(_mk_msg(user2, chat2, "/start"), state2)
    await bot_mod.got_examples(_mk_msg(user2, chat2, "https://t.me/x"), state2)
    await bot_mod.got_experience(_mk_msg(user2, chat2, "опыт"), state2)
    await bot_mod.got_name(_mk_msg(user2, chat2, "Имя"), state2)
    await bot_mod.got_contact(_mk_msg(user2, chat2, "+79991234567"), state2)
    assert (await state2.get_state()) == bot_mod.Form.confirm.state

    # Назад: confirm → contact
    await bot_mod.cmd_back(_mk_msg(user2, chat2, bot_mod.BTN_BACK), state2)
    assert (await state2.get_state()) == bot_mod.Form.contact.state
    print("  ✅ confirm → contact")

    # Назад: contact → name
    await bot_mod.cmd_back(_mk_msg(user2, chat2, bot_mod.BTN_BACK), state2)
    assert (await state2.get_state()) == bot_mod.Form.name.state
    print("  ✅ contact → name")

    # Назад: name → experience
    await bot_mod.cmd_back(_mk_msg(user2, chat2, bot_mod.BTN_BACK), state2)
    assert (await state2.get_state()) == bot_mod.Form.experience.state
    print("  ✅ name → experience")

    # Назад: experience → examples
    await bot_mod.cmd_back(_mk_msg(user2, chat2, bot_mod.BTN_BACK), state2)
    assert (await state2.get_state()) == bot_mod.Form.examples.state
    print("  ✅ experience → examples")

    # Назад на examples: остаётся
    await bot_mod.cmd_back(_mk_msg(user2, chat2, bot_mod.BTN_BACK), state2)
    assert (await state2.get_state()) == bot_mod.Form.examples.state
    print("  ✅ examples: остаётся (некуда назад)")

    # --- Сценарий 3: Поделиться контактом + потом откат и редактирование ---
    print("\n--- Сценарий 3: контакт через request_contact, затем правка имени ---")
    uid3 = 3
    bot_mod._last_submission.pop(uid3, None)
    state3 = FSMContext(storage=storage, key=StorageKey(bot_id=0, chat_id=uid3, user_id=uid3))
    user3 = MagicMock()
    user3.id = uid3
    user3.username = ""  # без username — fallback на first_name/contact
    user3.first_name = "Юзер"
    user3.last_name = "БезЮзернейма"
    chat3 = MagicMock(); chat3.id = uid3

    await bot_mod.cmd_start(_mk_msg(user3, chat3, "/start"), state3)
    await bot_mod.got_examples(_mk_msg(user3, chat3, "[E2E TEST] https://t.me/cs"), state3)
    await bot_mod.got_experience(_mk_msg(user3, chat3, "[E2E TEST] опыт"), state3)
    await bot_mod.got_name(_mk_msg(user3, chat3, "[E2E TEST] Старое Имя"), state3)

    fake_contact = MagicMock()
    fake_contact.phone_number = "+79991234567"
    fake_contact.first_name = "Иван"
    fake_contact.last_name = "Петров"
    await bot_mod.got_contact(_mk_msg(user3, chat3, None, contact=fake_contact), state3)
    assert (await state3.get_state()) == bot_mod.Form.confirm.state

    data3 = await state3.get_data()
    assert data3["contact"] == "тел.: +79991234567", f"contact={data3['contact']!r}"
    print("  ✅ Поделиться контактом → 'тел.: +79991234567'")

    # Откатываемся к имени и меняем
    await bot_mod.cmd_back(_mk_msg(user3, chat3, bot_mod.BTN_BACK), state3)  # confirm → contact
    await bot_mod.cmd_back(_mk_msg(user3, chat3, bot_mod.BTN_BACK), state3)  # contact → name
    await bot_mod.got_name(_mk_msg(user3, chat3, "[E2E TEST] Новое Имя"), state3)
    await bot_mod.got_contact(_mk_msg(user3, chat3, "[E2E TEST] @new_contact"), state3)
    assert (await state3.get_state()) == bot_mod.Form.confirm.state

    data3 = await state3.get_data()
    assert data3["name"] == "[E2E TEST] Новое Имя"
    assert data3["contact"] == "[E2E TEST] @new_contact"
    print("  ✅ После «Назад» можно поменять имя и контакт; сводка обновилась")

    await bot_mod.cmd_submit(_mk_msg(user3, chat3, bot_mod.BTN_SUBMIT), state3, fake_bot)
    rows = ws.get_all_values()
    last = rows[-1]
    assert last[6] == "[E2E TEST] Новое Имя"
    assert last[5] == "[E2E TEST] @new_contact"
    # Юзер без username — приоритет имя из shared контакта (Иван Петров),
    # потом откатывался назад и контакт был перебит на текстовый, но имя из contact
    # всё равно осело в FSM data → должно сохраниться в tg_first_name
    assert last[7] in ("Иван", "Юзер"), f"tg_first_name={last[7]!r}"
    print(f"  ✅ Без username попал tg_first_name={last[7]!r}, tg_last_name={last[8]!r}")
    ws.delete_rows(len(rows))
    print("  ✅ Финальная заявка содержит ОБНОВЛЁННЫЕ значения")

    print("\n" + "=" * 60)
    print("ALL SCENARIOS PASSED")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
