"""UGC Leasing — бот сбора заявок от криэйторов.

Простой линейный диалог на 3 шага (примеры работ → опыт → контакт),
запись в Google Sheets, уведомление админов. Long polling.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from pathlib import Path

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from dotenv import load_dotenv

from sheets import append_application

# --- Конфиг -----------------------------------------------------------------

ENV_DIR = Path(__file__).parent / "env"
load_dotenv(ENV_DIR / ".env")
# Если в .env не задан GOOGLE_CREDS_FILE — берём из той же папки env/
os.environ.setdefault("GOOGLE_CREDS_FILE", str(ENV_DIR / "credentials.json"))

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
SPAM_COOLDOWN_SEC = 600   # повторная заявка от одного юзера — раз в 10 минут
MAX_TEXT_LEN = 4000       # лимит длины одного сообщения от юзера

# --- Логирование ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("ugc-bot")

# --- FSM --------------------------------------------------------------------


class Form(StatesGroup):
    examples = State()
    experience = State()
    contact = State()


# user_id -> timestamp последней принятой заявки (антиспам)
_last_submission: dict[int, float] = {}

URL_RE = re.compile(r"https?://\S+")

BTN_RESTART = "🔄 Начать заново"
BTN_CANCEL = "❌ Отмена"
BTN_BACK = "↩️ Назад"
BTN_CONTACT = "📱 Отправить мой контакт"


def _ctrl_row() -> list[KeyboardButton]:
    return [KeyboardButton(text=BTN_RESTART), KeyboardButton(text=BTN_CANCEL)]


def kb() -> ReplyKeyboardMarkup:
    """Клавиатура для первого шага — назад некуда."""
    return ReplyKeyboardMarkup(keyboard=[_ctrl_row()], resize_keyboard=True)


def kb_with_back() -> ReplyKeyboardMarkup:
    """Шаги 2 и 3 — добавляется кнопка «Назад» к предыдущему вопросу."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_BACK)], _ctrl_row()],
        resize_keyboard=True,
    )


def kb_contact() -> ReplyKeyboardMarkup:
    """Шаг 3 — добавляется кнопка отправки контакта из Telegram."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CONTACT, request_contact=True)],
            [KeyboardButton(text=BTN_BACK)],
            _ctrl_row(),
        ],
        resize_keyboard=True,
    )


# --- Тексты -----------------------------------------------------------------

WELCOME = (
    "Привет, брат! 👋\n\n"
    "Мы — <b>ЛизингСток</b>. Платформа с выгодным стоком авто из лизинговых "
    "компаний: легковые, грузовые, коммерческий транспорт и спецтехника. "
    "Цены ниже рынка на <b>15–25%</b>, много уникальных вариантов, которых нет на Авито.\n\n"
    "Ищем <b>мужчин-автоэнтузиастов</b> для съёмки коротких видео. "
    "Заполни короткую анкету — это 2 минуты.\n\n"
    "<b>Шаг 1 из 3.</b> Пришли <b>ссылки</b> на 2–3 примера своих работ "
    "(YouTube, VK Video, Rutube, Telegram, Instagram — любые). "
    "Одним сообщением. Только ссылки, без вложений."
)

ASK_EXPERIENCE = (
    "<b>Шаг 2 из 3.</b> Коротко расскажи про опыт: "
    "сколько снимаешь, какие темы ведёшь, какая техника. Одним сообщением."
)

ASK_CONTACT = (
    "<b>Шаг 3 из 3.</b> Как с тобой связаться? "
    "Оставь Telegram-ник (@username) или имя + любой удобный контакт."
)

THANKS = (
    "✅ Заявку получил, спасибо!\n\n"
    "Посмотрим работы и свяжемся с тобой в течение 24 часов."
)

CANCELED = "Ок, отменил. Если захочешь заполнить — просто нажми /start"

ASK_EXAMPLES_AGAIN = (
    "Окей, вернулся к <b>Шагу 1 из 3</b>. Пришли ссылки на 2–3 примера "
    "своих работ одним сообщением (YouTube, VK Video, Rutube, Telegram, Instagram)."
)

# --- Роутер -----------------------------------------------------------------

router = Router()


# Команды и кнопки управления — регистрируем ДО FSM-хэндлеров,
# чтобы они срабатывали в любом состоянии.

@router.message(Command("start"))
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await state.set_state(Form.examples)
    await m.answer(WELCOME, reply_markup=kb())


@router.message(F.text == BTN_CANCEL)
@router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(CANCELED, reply_markup=ReplyKeyboardRemove())


@router.message(F.text == BTN_RESTART)
async def cmd_restart(m: Message, state: FSMContext):
    await cmd_start(m, state)


@router.message(F.text == BTN_BACK)
async def cmd_back(m: Message, state: FSMContext):
    cur = await state.get_state()
    if cur == Form.experience.state:
        await state.set_state(Form.examples)
        await m.answer(ASK_EXAMPLES_AGAIN, reply_markup=kb())
    elif cur == Form.contact.state:
        await state.set_state(Form.experience)
        await m.answer(ASK_EXPERIENCE, reply_markup=kb_with_back())
    else:
        await m.answer(
            "Это первый шаг — назад некуда. Пришли ссылки 👇", reply_markup=kb()
        )


def _validate_text(m: Message) -> str | None:
    """Вернуть текст ошибки или None если всё ок."""
    if not m.text:
        return "Пришли текст, пожалуйста — без вложений, стикеров и голосовых."
    if len(m.text) > MAX_TEXT_LEN:
        return f"Слишком длинно ({len(m.text)} символов). Уложись в {MAX_TEXT_LEN}."
    return None


@router.message(Form.examples)
async def got_examples(m: Message, state: FSMContext):
    err = _validate_text(m)
    if err:
        await m.answer(err, reply_markup=kb())
        return
    if not URL_RE.search(m.text):
        await m.answer(
            "Не нашёл ни одной ссылки. Пришли URL своих работ — "
            "YouTube, VK Video, Rutube, Telegram или Instagram.",
            reply_markup=kb(),
        )
        return
    await state.update_data(examples=m.text.strip())
    await state.set_state(Form.experience)
    await m.answer(ASK_EXPERIENCE, reply_markup=kb_with_back())


@router.message(Form.experience)
async def got_experience(m: Message, state: FSMContext):
    err = _validate_text(m)
    if err:
        await m.answer(err, reply_markup=kb_with_back())
        return
    await state.update_data(experience=m.text.strip())
    await state.set_state(Form.contact)
    await m.answer(ASK_CONTACT, reply_markup=kb_contact())


@router.message(Form.contact)
async def got_contact(m: Message, state: FSMContext, bot: Bot):
    if m.contact:
        first = (m.contact.first_name or "").strip()
        last = (m.contact.last_name or "").strip()
        phone = m.contact.phone_number or ""
        contact_text = f"{first} {last}".strip()
        if phone:
            contact_text = (contact_text + f", тел.: {phone}").strip(", ").strip()
    else:
        err = _validate_text(m)
        if err:
            await m.answer(err, reply_markup=kb_contact())
            return
        contact_text = m.text.strip()

    # Антиспам: не чаще одной заявки в 10 минут от одного юзера
    now = time.time()
    last = _last_submission.get(m.from_user.id, 0)
    if now - last < SPAM_COOLDOWN_SEC:
        await state.clear()
        await m.answer(
            "Мы уже получили твою заявку совсем недавно. "
            "Напиши чуть позже, если хочешь дополнить.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    data = await state.get_data()
    payload = {
        "tg_id": m.from_user.id,
        "tg_username": m.from_user.username or "",
        "examples": data.get("examples", ""),
        "experience": data.get("experience", ""),
        "contact": contact_text,
    }

    try:
        await asyncio.to_thread(append_application, payload)
    except Exception as e:
        log.exception("Ошибка записи в Google Sheets")
        await m.answer(
            "У нас сбой на нашей стороне. Уже чиним — возвращайся чуть позже, пожалуйста.",
            reply_markup=ReplyKeyboardRemove(),
        )
        await _notify_admins(
            bot,
            f"⚠️ Ошибка Sheets: {e}\n"
            f"Юзер: @{payload['tg_username'] or '—'} (id {payload['tg_id']})",
        )
        await state.clear()
        return

    # Cooldown ставим только после успешной записи —
    # если Sheets упал, юзер сможет попробовать снова.
    _last_submission[m.from_user.id] = now
    await state.clear()
    await m.answer(THANKS, reply_markup=ReplyKeyboardRemove())

    log.info("Новая заявка от @%s (id=%s)", payload["tg_username"], payload["tg_id"])
    await _notify_admins(
        bot,
        "🆕 Новая заявка UGC\n\n"
        f"TG: @{payload['tg_username'] or '—'} (id {payload['tg_id']})\n"
        f"Контакт: {payload['contact']}\n\n"
        f"Примеры:\n{payload['examples'][:500]}\n\n"
        f"Опыт:\n{payload['experience'][:500]}",
    )


@router.message()
async def fallback(m: Message, state: FSMContext):
    cur = await state.get_state()
    if cur is None:
        await m.answer(
            "Чтобы оставить заявку — нажми /start",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await m.answer(
            "Не понял. Ответь текстом по текущему шагу или нажми "
            "«🔄 Начать заново» / «❌ Отмена».",
            reply_markup=kb(),
        )


# --- Утилиты ----------------------------------------------------------------


async def _notify_admins(bot: Bot, text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            log.exception("Не смог отправить админу %s", admin_id)


# --- Точка входа ------------------------------------------------------------


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан в .env")
    if not ADMIN_IDS:
        log.warning("ADMIN_IDS пуст — уведомления админам уходить не будут")

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Render автоматически выставляет RENDER_EXTERNAL_URL для web services.
    # Если переменной нет (локально) — падаем в polling.
    base_url = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not base_url:
        log.info("Локальный режим: long polling, админов: %s", len(ADMIN_IDS))
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
        return

    webhook_path = "/webhook"
    webhook_url = f"{base_url}{webhook_path}"
    webhook_secret = os.getenv("WEBHOOK_SECRET", "ugc-leasing-secret")

    await bot.set_webhook(
        webhook_url, secret_token=webhook_secret, drop_pending_updates=True
    )
    log.info("Webhook установлен: %s, админов: %s", webhook_url, len(ADMIN_IDS))

    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=webhook_secret).register(
        app, path=webhook_path
    )
    # Health-check для Render — без него free web service считается «непросыпающимся»
    app.router.add_get("/", lambda r: web.Response(text="OK"))
    setup_application(app, dp, bot=bot)

    port = int(os.getenv("PORT", "10000"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
