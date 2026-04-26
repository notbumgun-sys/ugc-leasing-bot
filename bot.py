"""UGC Leasing — бот сбора заявок от криэйторов.

Простой линейный диалог на 3 шага (примеры работ → опыт → контакт),
запись в Google Sheets, уведомление админов. Long polling.
"""
from __future__ import annotations

import asyncio
import hmac
import html
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from dotenv import load_dotenv

from sheets import append_application, append_event, read_applications, read_events

# --- Конфиг -----------------------------------------------------------------

ENV_DIR = Path(__file__).parent / "env"
load_dotenv(ENV_DIR / ".env")
# Если в .env не задан GOOGLE_CREDS_FILE — берём из той же папки env/
os.environ.setdefault("GOOGLE_CREDS_FILE", str(ENV_DIR / "credentials.json"))

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "stockauto_ugc_bot")
# ADMIN_IDS — куда уходят уведомления о новых заявках (обычно группа).
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
# OWNER_IDS — кто может вызвать /stats (личные tg_id владельцев).
OWNER_IDS = [int(x) for x in os.getenv("OWNER_IDS", "").split(",") if x.strip()]
# Токен для веб-админки /admin?token=...
ADMIN_WEB_TOKEN = os.getenv("ADMIN_WEB_TOKEN", "")
SPAM_COOLDOWN_SEC = int(os.getenv("SPAM_COOLDOWN_SEC", "30"))  # дефолт: 30 сек между submit'ами
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
    name = State()
    contact = State()
    confirm = State()


# user_id -> timestamp последней принятой заявки (антиспам)
_last_submission: dict[int, float] = {}

URL_RE = re.compile(r"https?://\S+")

BTN_RESTART = "🔄 Начать заново"
BTN_CANCEL = "❌ Отмена"
BTN_BACK = "↩️ Назад"
BTN_CONTACT = "📱 Отправить мой контакт"
BTN_SUBMIT = "✅ Отправить"


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
    """Шаг 4 — добавляется кнопка отправки контакта из Telegram."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CONTACT, request_contact=True)],
            [KeyboardButton(text=BTN_BACK)],
            _ctrl_row(),
        ],
        resize_keyboard=True,
    )


def kb_confirm() -> ReplyKeyboardMarkup:
    """Финальный экран — отправить или назад."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SUBMIT)],
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
    "<b>Шаг 1 из 4.</b> Пришли <b>ссылки</b> на свои соцсети или конкретные видео — "
    "TikTok, YouTube, Reels, Shorts, Instagram, Telegram-канал, "
    "VK Video, Rutube, Snapchat, Facebook или любая другая платформа. "
    "Одним сообщением."
)

ASK_EXPERIENCE = (
    "<b>Шаг 2 из 4.</b> Коротко расскажи про опыт: "
    "сколько снимаешь, какие темы ведёшь, какая техника. Одним сообщением."
)

ASK_NAME = "<b>Шаг 3 из 4.</b> Как тебя зовут? Имя одной строкой."

ASK_CONTACT = (
    "<b>Шаг 4 из 4.</b> Контакт для связи: оставь Telegram-ник (@username), "
    "номер телефона или нажми кнопку <b>«📱 Отправить мой контакт»</b> ниже."
)

THANKS = (
    "✅ Заявку получил, спасибо!\n\n"
    "Посмотрим работы и свяжемся с тобой в течение 24 часов."
)

CANCELED = "Ок, отменил. Если захочешь заполнить — просто нажми /start"

ASK_EXAMPLES_AGAIN = (
    "Окей, вернулся к <b>Шагу 1 из 4</b>. Пришли ссылки — на свои соцсети "
    "или конкретные видео (TikTok, YouTube, Reels, Instagram, Telegram, "
    "VK или любая платформа)."
)


_FUNNEL_STEPS = [
    ("start", "🚀 Запустили /start"),
    ("step_examples", "1️⃣ Прислали ссылки"),
    ("step_experience", "2️⃣ Рассказали опыт"),
    ("step_name", "3️⃣ Указали имя"),
    ("step_contact", "4️⃣ Дали контакт"),
    ("submitted", "✅ Отправили заявку"),
]


def _aggregate(events: list[dict], since_ts: str | None = None) -> dict[str, set[int]]:
    """Возвращает {event: set(tg_id)} — множества уникальных юзеров на каждом шаге."""
    out: dict[str, set[int]] = {ev: set() for ev, _ in _FUNNEL_STEPS}
    out["cancelled"] = set()
    for r in events:
        ts = str(r.get("timestamp", ""))
        if since_ts and ts < since_ts:
            continue
        ev = str(r.get("event", ""))
        try:
            uid = int(r.get("tg_id", 0))
        except (TypeError, ValueError):
            continue
        if ev in out:
            out[ev].add(uid)
    return out


def _format_funnel_block(title: str, agg: dict[str, set[int]]) -> str:
    base = len(agg.get("start", set())) or 1  # чтобы не делить на 0
    lines = [f"<b>{title}</b>"]
    prev = None
    for ev, label in _FUNNEL_STEPS:
        n = len(agg.get(ev, set()))
        pct_total = n / base * 100
        if prev is None:
            lines.append(f"{label}: <b>{n}</b>")
        else:
            step_pct = (n / prev * 100) if prev else 0
            lines.append(f"{label}: <b>{n}</b>  ({pct_total:.0f}% всего, {step_pct:.0f}% шаг)")
        prev = n
    cancelled = len(agg.get("cancelled", set()))
    if cancelled:
        lines.append(f"❌ Отменили: <b>{cancelled}</b>")
    return "\n".join(lines)


def _format_stats(events: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    since_24h = (now - timedelta(hours=24)).isoformat(timespec="seconds")

    agg_all = _aggregate(events)
    agg_24h = _aggregate(events, since_ts=since_24h)

    return (
        f"📊 <b>Статистика бота</b>\n"
        f"<i>Записей в Events: {len(events)}</i>\n\n"
        f"{_format_funnel_block('За 24 часа', agg_24h)}\n\n"
        f"{_format_funnel_block('За всё время (с момента трекинга)', agg_all)}"
    )


def _render_admin_html(events: list[dict], apps: list[dict]) -> str:
    """Минимальная HTML-страница админки: воронка 24h+all, последние заявки,
    кнопка открыть бота для ручного тестирования. Без JS."""
    now = datetime.now(timezone.utc)
    since_24h = (now - timedelta(hours=24)).isoformat(timespec="seconds")
    agg_24h = _aggregate(events, since_ts=since_24h)
    agg_all = _aggregate(events)

    def funnel_table(title: str, agg: dict[str, set[int]]) -> str:
        base = len(agg.get("start", set())) or 1
        rows_html = []
        prev = None
        for ev, label in _FUNNEL_STEPS:
            n = len(agg.get(ev, set()))
            pct_total = n / base * 100
            if prev is None:
                pct_html = ""
            else:
                step_pct = (n / prev * 100) if prev else 0
                pct_html = f'<span class="pct">{pct_total:.0f}% всего · {step_pct:.0f}% шаг</span>'
            rows_html.append(
                f"<tr><td>{label}</td><td class='n'>{n}</td><td>{pct_html}</td></tr>"
            )
            prev = n
        cancelled = len(agg.get("cancelled", set()))
        return (
            f"<h3>{html.escape(title)}</h3>"
            f"<table class='funnel'>{''.join(rows_html)}</table>"
            f"<p class='cancel'>❌ Отменили: <b>{cancelled}</b></p>"
        )

    apps_blocks = []
    for a in list(apps)[-15:][::-1]:
        ts = str(a.get("timestamp", ""))[:19].replace("T", " ")
        name = html.escape(str(a.get("name", "") or "—"))
        contact = html.escape(str(a.get("contact", "")))
        username = html.escape(str(a.get("tg_username", "") or ""))
        tg_first = html.escape(str(a.get("tg_first_name", "") or ""))
        tg_last = html.escape(str(a.get("tg_last_name", "") or ""))
        tg_full = (tg_first + " " + tg_last).strip()
        examples = html.escape(str(a.get("examples", "") or ""))[:300]
        experience = html.escape(str(a.get("experience", "") or ""))[:200]
        if username:
            uname_html = f'<a href="https://t.me/{username}" target="_blank">@{username}</a>'
        else:
            tg_id = a.get("tg_id", "")
            uname_html = f'tg://user?id={tg_id}' if tg_id else "—"
            uname_html = f'<a href="{uname_html}">id {tg_id}</a>' if tg_id else "—"
        if tg_full:
            uname_html += f" · {tg_full}"
        apps_blocks.append(
            f"""<div class="app">
                <div class="app-head"><b>{name}</b> · {contact} · {uname_html}</div>
                <div class="ts">{ts} UTC</div>
                <div class="ex">📎 {examples}</div>
                <div class="ex">💬 {experience}</div>
            </div>"""
        )
    apps_html = "\n".join(apps_blocks) or "<p><i>Заявок пока нет.</i></p>"

    return f"""<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>UGC бот · админка</title>
<style>
  body {{ font: 15px/1.5 -apple-system, Segoe UI, Roboto, sans-serif;
         max-width: 720px; margin: 24px auto; padding: 0 16px; color: #222; }}
  h1 {{ font-size: 22px; margin: 0 0 4px; }}
  h3 {{ margin: 24px 0 8px; }}
  .sub {{ color: #777; font-size: 13px; margin-bottom: 20px; }}
  .cta {{ display: inline-block; background: #0088cc; color: #fff;
          padding: 10px 18px; border-radius: 8px; text-decoration: none;
          font-weight: 500; margin-right: 8px; }}
  .cta.alt {{ background: #eee; color: #222; }}
  table.funnel {{ border-collapse: collapse; width: 100%; }}
  table.funnel td {{ padding: 6px 8px; border-bottom: 1px solid #eee; }}
  table.funnel td.n {{ font-weight: 600; text-align: right; width: 60px; }}
  .pct {{ color: #888; font-size: 13px; }}
  .cancel {{ color: #888; font-size: 13px; margin: 6px 0 0; }}
  .app {{ border: 1px solid #ddd; border-radius: 8px;
          padding: 10px 12px; margin: 8px 0; background: #fafafa; }}
  .app-head {{ font-size: 14px; }}
  .ts {{ color: #999; font-size: 12px; margin: 2px 0 6px; }}
  .ex {{ color: #555; font-size: 13px; word-break: break-word; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
  @media (max-width: 600px) {{ .grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
  <h1>📊 UGC бот · админка</h1>
  <div class="sub">Обновляется автоматически каждые 60 сек · записей в Events: {len(events)} · заявок в Sheet: {len(apps)}</div>

  <div>
    <a class="cta" href="https://t.me/{BOT_USERNAME}" target="_blank">🤖 Открыть @{BOT_USERNAME}</a>
    <a class="cta alt" href="">🔄 Обновить</a>
  </div>

  <div class="grid">
    <div>{funnel_table('За 24 часа', agg_24h)}</div>
    <div>{funnel_table('За всё время', agg_all)}</div>
  </div>

  <h3>Последние заявки ({len(apps)} всего)</h3>
  {apps_html}

</body></html>"""


def _build_summary(data: dict) -> str:
    def short(s: str, n: int = 300) -> str:
        s = s or ""
        return s if len(s) <= n else s[:n] + "…"
    return (
        "<b>📋 Проверь заявку:</b>\n\n"
        f"• <b>Имя:</b> {short(data.get('name',''), 80)}\n"
        f"• <b>Контакт:</b> {short(data.get('contact',''), 100)}\n"
        f"• <b>Примеры:</b> {short(data.get('examples',''))}\n"
        f"• <b>Опыт:</b> {short(data.get('experience',''))}\n\n"
        "Всё верно? Жми «✅ Отправить». Ошибся — «↩️ Назад» (можно вернуться "
        "хоть к шагу 1)."
    )

# --- Роутер -----------------------------------------------------------------

router = Router()
# Бот живёт в группе уведомлений (ADMIN_IDS), но диалог ведёт только в личке —
# чтобы команды и FSM не срабатывали на сообщения в группе.
router.message.filter(F.chat.type == "private")


# Команды и кнопки управления — регистрируем ДО FSM-хэндлеров,
# чтобы они срабатывали в любом состоянии.

@router.message(Command("stats"))
async def cmd_stats(m: Message):
    if m.from_user.id not in OWNER_IDS:
        return  # тихо игнорим — пусть выглядит как неизвестная команда
    try:
        events = await asyncio.to_thread(read_events)
    except Exception as e:
        log.exception("read_events failed")
        await m.answer(f"Не смог прочитать Events: {e}")
        return
    text = _format_stats(events)
    await m.answer(text)


@router.message(Command("start"))
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await state.set_state(Form.examples)
    _track(m, "start")
    await m.answer(WELCOME, reply_markup=kb())


@router.message(F.text == BTN_CANCEL)
@router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    cur = await state.get_state()
    await state.clear()
    _track(m, "cancelled", extra=cur or "no_state")
    await m.answer(CANCELED, reply_markup=kb())


@router.message(F.text == BTN_RESTART)
async def cmd_restart(m: Message, state: FSMContext):
    await cmd_start(m, state)


@router.message(F.text == BTN_BACK)
async def cmd_back(m: Message, state: FSMContext):
    cur = await state.get_state()
    if cur == Form.experience.state:
        await state.set_state(Form.examples)
        await m.answer(ASK_EXAMPLES_AGAIN, reply_markup=kb())
    elif cur == Form.name.state:
        await state.set_state(Form.experience)
        await m.answer(ASK_EXPERIENCE, reply_markup=kb_with_back())
    elif cur == Form.contact.state:
        await state.set_state(Form.name)
        await m.answer(ASK_NAME, reply_markup=kb_with_back())
    elif cur == Form.confirm.state:
        await state.set_state(Form.contact)
        await m.answer(ASK_CONTACT, reply_markup=kb_contact())
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
            "Не нашёл ссылку — пришли URL на свою соцсеть или конкретное "
            "видео (TikTok, YouTube, Reels, Instagram, Telegram или любая "
            "другая платформа).",
            reply_markup=kb(),
        )
        return
    await state.update_data(examples=m.text.strip())
    await state.set_state(Form.experience)
    _track(m, "step_examples")
    await m.answer(ASK_EXPERIENCE, reply_markup=kb_with_back())


@router.message(Form.experience)
async def got_experience(m: Message, state: FSMContext):
    err = _validate_text(m)
    if err:
        await m.answer(err, reply_markup=kb_with_back())
        return
    await state.update_data(experience=m.text.strip())
    await state.set_state(Form.name)
    _track(m, "step_experience")
    await m.answer(ASK_NAME, reply_markup=kb_with_back())


@router.message(Form.name)
async def got_name(m: Message, state: FSMContext):
    err = _validate_text(m)
    if err:
        await m.answer(err, reply_markup=kb_with_back())
        return
    await state.update_data(name=m.text.strip())
    await state.set_state(Form.contact)
    _track(m, "step_name")
    await m.answer(ASK_CONTACT, reply_markup=kb_contact())


@router.message(Form.contact)
async def got_contact(m: Message, state: FSMContext):
    update = {}
    if m.contact:
        phone = m.contact.phone_number or ""
        contact_text = f"тел.: {phone}" if phone else ""
        # Telegram при request_contact кладёт имя самого пользователя в contact —
        # сохраняем как доп. источник, если поле name юзер ввёл небрежно
        if m.contact.first_name:
            update["contact_first_name"] = m.contact.first_name
        if m.contact.last_name:
            update["contact_last_name"] = m.contact.last_name
    else:
        err = _validate_text(m)
        if err:
            await m.answer(err, reply_markup=kb_contact())
            return
        contact_text = m.text.strip()

    if not contact_text:
        await m.answer("Не понял контакт — попробуй ещё раз.", reply_markup=kb_contact())
        return

    update["contact"] = contact_text
    await state.update_data(**update)
    await state.set_state(Form.confirm)
    data = await state.get_data()
    _track(m, "step_contact")
    await m.answer(_build_summary(data), reply_markup=kb_confirm())


@router.message(F.text == BTN_SUBMIT)
async def cmd_submit(m: Message, state: FSMContext, bot: Bot):
    cur = await state.get_state()
    if cur != Form.confirm.state:
        # Кнопку нажали не на сводке — игнорируем мягко
        return

    now = time.time()
    last = _last_submission.get(m.from_user.id, 0)
    if now - last < SPAM_COOLDOWN_SEC:
        await state.clear()
        await m.answer(
            "⏳ Заявка от тебя только что отправлена. "
            "Если хочешь оставить ещё одну — попробуй через минуту.",
            reply_markup=kb(),
        )
        return

    data = await state.get_data()
    # Telegram-имя: приоритет у того что сам пользователь вбил в request_contact
    # (это часто более полное), иначе берём from_user (что выставлено в профиле).
    tg_first = data.get("contact_first_name") or (m.from_user.first_name or "")
    tg_last = data.get("contact_last_name") or (m.from_user.last_name or "")
    payload = {
        "tg_id": m.from_user.id,
        "tg_username": m.from_user.username or "",
        "tg_first_name": tg_first,
        "tg_last_name": tg_last,
        "examples": data.get("examples", ""),
        "experience": data.get("experience", ""),
        "contact": data.get("contact", ""),
        "name": data.get("name", ""),
    }

    try:
        await asyncio.to_thread(append_application, payload)
    except Exception as e:
        log.exception("Ошибка записи в Google Sheets")
        await m.answer(
            "У нас сбой на нашей стороне. Уже чиним — возвращайся чуть позже, пожалуйста.",
            reply_markup=kb(),
        )
        await _notify_admins(
            bot,
            f"⚠️ Ошибка Sheets: {e}\n"
            f"Юзер: @{payload['tg_username'] or '—'} (id {payload['tg_id']})",
        )
        await state.clear()
        return

    _last_submission[m.from_user.id] = now
    await state.clear()
    _track(m, "submitted")
    await m.answer(THANKS, reply_markup=kb())

    log.info("Новая заявка от @%s (id=%s)", payload["tg_username"], payload["tg_id"])
    tg_full = (payload["tg_first_name"] + " " + payload["tg_last_name"]).strip()
    tg_handle = f"@{payload['tg_username']}" if payload['tg_username'] else "(без username)"
    tg_line = f"TG: {tg_handle}"
    if tg_full:
        tg_line += f" — {tg_full}"
    tg_line += f" (id {payload['tg_id']})"
    await _notify_admins(
        bot,
        "🆕 Новая заявка UGC\n\n"
        f"Имя: {payload['name']}\n"
        f"{tg_line}\n"
        f"Контакт: {payload['contact']}\n\n"
        f"Примеры:\n{payload['examples'][:500]}\n\n"
        f"Опыт:\n{payload['experience'][:500]}",
    )


@router.message(Form.confirm)
async def confirm_fallback(m: Message, state: FSMContext):
    """На сводке жмут кнопки. Любой текст — мягкое напоминание."""
    await m.answer(
        "Жми «✅ Отправить» чтобы отправить заявку или «↩️ Назад» чтобы поправить.",
        reply_markup=kb_confirm(),
    )


@router.message()
async def fallback(m: Message, state: FSMContext):
    cur = await state.get_state()
    if cur is None:
        await m.answer(
            "Чтобы оставить заявку — нажми /start",
            reply_markup=kb(),
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


def _track(m: Message, event: str, extra: str = "") -> None:
    """Пишет событие в Events sheet асинхронно (fire-and-forget).
    Падение Sheets не должно ломать пользовательский flow."""
    tg_id = m.from_user.id if m.from_user else 0
    username = (m.from_user.username if m.from_user else "") or ""

    async def _do():
        try:
            await asyncio.to_thread(append_event, tg_id, username, event, extra)
        except Exception:
            log.exception("track event failed: %s", event)

    asyncio.create_task(_do())


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

    async def admin_handler(request: web.Request) -> web.Response:
        token = request.query.get("token", "")
        if not ADMIN_WEB_TOKEN or not hmac.compare_digest(token, ADMIN_WEB_TOKEN):
            return web.Response(text="forbidden", status=403)
        try:
            events = await asyncio.to_thread(read_events)
            apps = await asyncio.to_thread(read_applications)
        except Exception:
            log.exception("admin: read failed")
            return web.Response(text="sheets read failed, check logs", status=500)
        body = _render_admin_html(events, apps)
        return web.Response(text=body, content_type="text/html", charset="utf-8")

    app.router.add_get("/admin", admin_handler)

    setup_application(app, dp, bot=bot)

    port = int(os.getenv("PORT", "10000"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
