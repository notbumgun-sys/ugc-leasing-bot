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
from urllib.parse import urlencode

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

# Подписи для лога событий — для шагов берутся из _FUNNEL_STEPS, дополнительные
# события (валидаторы, кнопки) — здесь.
_EVENT_LABELS = {ev: lbl for ev, lbl in _FUNNEL_STEPS}
_EVENT_LABELS.update({
    "cancelled": "❌ Отменил",
    "examples_invalid": "⚠️ Шаг 1: невалидно",
    "experience_invalid": "⚠️ Шаг 2: невалидно",
    "name_invalid": "⚠️ Шаг 3: невалидно",
    "contact_invalid": "⚠️ Шаг 4: невалидно",
    "back_pressed": "↩️ Нажал «Назад»",
    "cooldown_hit": "⏳ Cooldown сработал",
    "fallback": "❓ Сообщение мимо FSM",
    "confirm_fallback": "❓ На сводке вместо кнопки",
})


def _event_label(ev: str) -> str:
    return _EVENT_LABELS.get(ev, ev)


_RANGE_LABELS = {
    "today": "Сегодня",
    "7d": "7 дней",
    "30d": "30 дней",
    "all": "Всё время",
}


def _range_since_iso(key: str) -> str:
    """Возвращает ISO-строку начала диапазона (для сравнения с timestamp)."""
    now = datetime.now(timezone.utc)
    if key == "today":
        d = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return d.isoformat(timespec="seconds")
    if key == "7d":
        return (now - timedelta(days=7)).isoformat(timespec="seconds")
    if key == "30d":
        return (now - timedelta(days=30)).isoformat(timespec="seconds")
    return ""  # all


def _format_delta_secs(secs: int) -> str:
    if secs < 60:
        return f"{secs}с"
    if secs < 3600:
        return f"{secs // 60}м {secs % 60}с"
    if secs < 86400:
        return f"{secs // 3600}ч {(secs % 3600) // 60}м"
    return f"{secs // 86400}д"


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


def _build_users(events: list[dict], apps: list[dict]) -> list[dict]:
    """Склейка событий и заявок по tg_id. Возвращает список юзеров,
    отсортированных по последнему действию."""
    users: dict[int, dict] = {}

    def ensure(uid: int) -> dict:
        u = users.get(uid)
        if u is None:
            u = {
                "tg_id": uid, "username": "", "name": "",
                "tg_first_name": "", "tg_last_name": "",
                "events": [], "leads": [],
                "first_ts": "", "last_ts": "",
            }
            users[uid] = u
        return u

    for e in events:
        try:
            uid = int(e.get("tg_id", 0))
        except (TypeError, ValueError):
            continue
        if not uid:
            continue
        u = ensure(uid)
        u["events"].append(e)
        un = str(e.get("tg_username", "") or "")
        if un and not u["username"]:
            u["username"] = un
        ts = str(e.get("timestamp", ""))
        if ts:
            if not u["first_ts"] or ts < u["first_ts"]:
                u["first_ts"] = ts
            if ts > u["last_ts"]:
                u["last_ts"] = ts

    for a in apps:
        try:
            uid = int(a.get("tg_id", 0))
        except (TypeError, ValueError):
            continue
        if not uid:
            continue
        u = ensure(uid)
        u["leads"].append(a)
        if not u["name"] and a.get("name"):
            u["name"] = str(a["name"])
        if not u["username"] and a.get("tg_username"):
            u["username"] = str(a["tg_username"])
        if not u["tg_first_name"] and a.get("tg_first_name"):
            u["tg_first_name"] = str(a["tg_first_name"])
        if not u["tg_last_name"] and a.get("tg_last_name"):
            u["tg_last_name"] = str(a["tg_last_name"])
        ts = str(a.get("timestamp", ""))
        if ts and ts > u["last_ts"]:
            u["last_ts"] = ts

    return sorted(users.values(), key=lambda u: u["last_ts"], reverse=True)


def _format_user_timeline(events: list[dict]) -> str:
    """Хронологический таймлайн событий одного юзера с дельтами."""
    asc = sorted(events, key=lambda e: str(e.get("timestamp", "")))
    if not asc:
        return "<p class='muted'>Событий нет.</p>"
    rows = []
    prev_ts = None
    for e in asc:
        ts_iso = str(e.get("timestamp", ""))
        ts_short = ts_iso[:19].replace("T", " ")
        delta_html = ""
        if prev_ts and ts_iso:
            try:
                d1 = datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
                d2 = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
                secs = max(0, int((d2 - d1).total_seconds()))
                delta_html = f"<span class='delta'>+{_format_delta_secs(secs)}</span>"
            except (ValueError, TypeError):
                pass
        ev = str(e.get("event", ""))
        extra = str(e.get("extra", "") or "")
        label = _event_label(ev)
        extra_html = f"<span class='extra'>{html.escape(extra[:120])}</span>" if extra else ""
        rows.append(
            f"<div class='tl-row'>"
            f"<span class='tl-time'>{html.escape(ts_short)}</span>"
            f"<span class='tl-event'>{html.escape(label)}</span>"
            f"{extra_html}"
            f"{delta_html}"
            f"</div>"
        )
        prev_ts = ts_iso
    return "<div class='timeline'>" + "".join(rows) + "</div>"


def _filter_events_by_range(events: list[dict], since_iso: str) -> list[dict]:
    if not since_iso:
        return list(events)
    return [e for e in events if str(e.get("timestamp", "")) >= since_iso]


def _filter_apps_by_range(apps: list[dict], since_iso: str) -> list[dict]:
    if not since_iso:
        return list(apps)
    return [a for a in apps if str(a.get("timestamp", "")) >= since_iso]


def _funnel_rows(agg: dict[str, set[int]]) -> list[tuple]:
    """Возвращает строки воронки: (label, count, pct_total, drop_pct, bar_pct)."""
    base = len(agg.get("start", set())) or 1
    rows = []
    prev = None
    for ev, label in _FUNNEL_STEPS:
        n = len(agg.get(ev, set()))
        pct_total = n / base * 100
        bar_pct = pct_total if prev is None else pct_total
        drop_pct = None
        if prev is not None and prev > 0:
            drop_pct = (prev - n) / prev * 100
        rows.append((label, n, pct_total, drop_pct, bar_pct))
        prev = n
    return rows


def _render_admin_html(
    events: list[dict],
    apps: list[dict],
    *,
    token: str,
    range_key: str,
    tab: str,
) -> str:
    """Админка с табами и range-селектором. Серверный рендер, без JS."""
    since_iso = _range_since_iso(range_key)
    f_events = _filter_events_by_range(events, since_iso)
    f_apps = _filter_apps_by_range(apps, since_iso)

    # ---- Хедер: range + табы ----
    def link(extra_params: dict) -> str:
        params = {"token": token, "range": range_key, "tab": tab, **extra_params}
        return f"/admin?{urlencode(params)}"

    range_links = " ".join(
        f'<a class="range {"active" if k == range_key else ""}" href="{link({"range": k})}">{html.escape(v)}</a>'
        for k, v in _RANGE_LABELS.items()
    )

    tabs = [
        ("funnel", "📊 Воронка"),
        ("users", "👥 Пользователи"),
        ("leads", "📨 Заявки"),
        ("events", "📜 Лог событий"),
    ]
    tab_links = " ".join(
        f'<a class="tab {"active" if k == tab else ""}" href="{link({"tab": k})}">{html.escape(v)}</a>'
        for k, v in tabs
    )

    # ---- Контент ----
    if tab == "funnel":
        body = _render_tab_funnel(f_events, range_key)
    elif tab == "users":
        body = _render_tab_users(f_events, f_apps)
    elif tab == "leads":
        body = _render_tab_leads(f_apps)
    elif tab == "events":
        body = _render_tab_events(f_events)
    else:
        body = "<p>Неизвестный таб.</p>"

    return f"""<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>UGC бот · админка</title>
<style>
  body {{ font: 15px/1.5 -apple-system, Segoe UI, Roboto, sans-serif;
         max-width: 980px; margin: 0 auto; padding: 16px; color: #222; background: #f6f7f9; }}
  h1 {{ font-size: 22px; margin: 0 0 4px; }}
  h2 {{ font-size: 18px; margin: 18px 0 8px; }}
  h3 {{ margin: 18px 0 8px; font-size: 16px; }}
  .sub {{ color: #777; font-size: 13px; margin-bottom: 16px; }}
  .muted {{ color: #888; font-size: 13px; }}
  .cta {{ display: inline-block; background: #0088cc; color: #fff;
          padding: 8px 14px; border-radius: 8px; text-decoration: none;
          font-weight: 500; margin-right: 6px; font-size: 14px; }}
  .cta.alt {{ background: #eee; color: #222; }}
  .topbar {{ display: flex; justify-content: space-between; align-items: center;
             flex-wrap: wrap; gap: 12px; margin-bottom: 8px; }}
  .ranges {{ display: inline-flex; background: #fff; border-radius: 8px;
             padding: 4px; border: 1px solid #e3e6eb; }}
  .range {{ padding: 6px 12px; border-radius: 6px; text-decoration: none;
            color: #555; font-size: 13px; font-weight: 500; }}
  .range.active {{ background: #0088cc; color: #fff; }}
  .tabs {{ display: flex; flex-wrap: wrap; gap: 4px;
           border-bottom: 2px solid #e3e6eb; margin: 16px 0 20px; }}
  .tab {{ padding: 10px 14px; text-decoration: none; color: #555;
          font-size: 14px; font-weight: 500;
          border-bottom: 2px solid transparent; margin-bottom: -2px; }}
  .tab.active {{ color: #0088cc; border-bottom-color: #0088cc; }}
  .card {{ background: #fff; border: 1px solid #e3e6eb; border-radius: 10px;
           padding: 14px 16px; margin-bottom: 12px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
               gap: 12px; margin-bottom: 16px; }}
  .kpi {{ background: #fff; border: 1px solid #e3e6eb; border-radius: 10px; padding: 14px; }}
  .kpi-label {{ font-size: 12px; color: #777; text-transform: uppercase;
                letter-spacing: 0.5px; }}
  .kpi-value {{ font-size: 26px; font-weight: 700; margin: 4px 0 2px; }}
  .kpi-sub {{ font-size: 12px; color: #888; }}
  .kpi.accent .kpi-value {{ color: #0088cc; }}
  .funnel-row {{ position: relative; margin: 6px 0; padding: 8px 12px;
                 background: #f0f3f7; border-radius: 6px; overflow: hidden; }}
  .funnel-fill {{ position: absolute; left: 0; top: 0; bottom: 0;
                  background: linear-gradient(90deg, #0088cc 0%, #00aaff 100%);
                  opacity: 0.18; }}
  .funnel-text {{ position: relative; display: flex; justify-content: space-between;
                  font-size: 14px; }}
  .funnel-text .label {{ font-weight: 500; }}
  .funnel-text .num {{ font-weight: 700; }}
  .drop {{ color: #c44; font-size: 12px; margin-left: 8px; }}
  details.user, details.lead {{ background: #fff; border: 1px solid #e3e6eb;
                                border-radius: 8px; margin: 6px 0; padding: 0; }}
  details.user > summary, details.lead > summary {{
    list-style: none; padding: 10px 14px; cursor: pointer; user-select: none;
    display: flex; flex-wrap: wrap; gap: 12px; align-items: center;
  }}
  details.user > summary::-webkit-details-marker, details.lead > summary::-webkit-details-marker {{ display: none; }}
  details.user > summary::before, details.lead > summary::before {{
    content: "▶"; color: #aaa; font-size: 11px;
    transition: transform 0.15s; display: inline-block;
  }}
  details[open] > summary::before {{ transform: rotate(90deg); }}
  details[open] > summary {{ border-bottom: 1px solid #eef0f4; background: #fafbfc; }}
  .summary-name {{ font-weight: 600; }}
  .summary-meta {{ color: #888; font-size: 13px; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px;
            font-size: 11px; font-weight: 600; }}
  .badge.lead {{ background: #d4edda; color: #155724; }}
  .badge.start {{ background: #e2e3e5; color: #383d41; }}
  .body-pad {{ padding: 12px 14px; }}
  .timeline {{ display: grid; gap: 4px; }}
  .tl-row {{ display: grid; grid-template-columns: 130px 1fr auto auto; gap: 10px;
             font-size: 13px; padding: 4px 0; border-bottom: 1px dashed #eef0f4; }}
  .tl-time {{ color: #999; font-family: ui-monospace, Consolas, monospace; font-size: 12px; }}
  .tl-event {{ font-weight: 500; }}
  .extra {{ color: #888; font-size: 12px; font-family: ui-monospace, Consolas, monospace;
           overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .delta {{ color: #aaa; font-size: 12px; }}
  table.events-tbl {{ width: 100%; border-collapse: collapse; }}
  table.events-tbl th, table.events-tbl td {{ padding: 6px 8px; text-align: left;
                                              border-bottom: 1px solid #f0f1f3; font-size: 13px; }}
  table.events-tbl th {{ color: #777; font-weight: 600; font-size: 12px;
                         text-transform: uppercase; letter-spacing: 0.4px; }}
  .lead-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px 16px; }}
  .field-label {{ font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.4px; }}
  .field-value {{ font-size: 14px; word-break: break-word; }}
  @media (max-width: 600px) {{
    .lead-grid {{ grid-template-columns: 1fr; }}
    .tl-row {{ grid-template-columns: 100px 1fr; }}
    .extra, .delta {{ grid-column: 1 / -1; padding-left: 110px; }}
  }}
</style>
</head>
<body>
  <div class="topbar">
    <div>
      <h1>📊 UGC бот · админка</h1>
      <div class="sub">Events: {len(events)} · Заявок: {len(apps)} · период: <b>{html.escape(_RANGE_LABELS.get(range_key, range_key))}</b></div>
    </div>
    <div class="ranges">{range_links}</div>
  </div>

  <div>
    <a class="cta" href="https://t.me/{BOT_USERNAME}" target="_blank">🤖 Открыть @{BOT_USERNAME}</a>
    <a class="cta alt" href="{link({})}">🔄 Обновить</a>
  </div>

  <div class="tabs">{tab_links}</div>

  {body}
</body></html>"""


def _render_tab_funnel(events: list[dict], range_key: str) -> str:
    agg = _aggregate(events)  # events уже отфильтрованы
    rows = _funnel_rows(agg)
    base = max((r[1] for r in rows), default=0)

    starts = len(agg.get("start", set()))
    submits = len(agg.get("submitted", set()))
    cancelled = len(agg.get("cancelled", set()))
    cr = (submits / starts * 100) if starts else 0

    invalid_count = sum(
        1 for e in events
        if str(e.get("event", "")).endswith("_invalid")
        or str(e.get("event", "")) == "fallback"
        or str(e.get("event", "")) == "confirm_fallback"
    )

    kpi = (
        '<div class="kpi-grid">'
        f'<div class="kpi accent"><div class="kpi-label">Конверсия</div>'
        f'<div class="kpi-value">{cr:.1f}%</div>'
        f'<div class="kpi-sub">submitted / start</div></div>'
        f'<div class="kpi"><div class="kpi-label">Запустили</div>'
        f'<div class="kpi-value">{starts}</div>'
        f'<div class="kpi-sub">уникальных юзеров</div></div>'
        f'<div class="kpi"><div class="kpi-label">Заявок</div>'
        f'<div class="kpi-value">{submits}</div>'
        f'<div class="kpi-sub">Отмен: {cancelled}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Сигналов о баге</div>'
        f'<div class="kpi-value">{invalid_count}</div>'
        f'<div class="kpi-sub">невалидных вводов / fallback\'ов</div></div>'
        '</div>'
    )

    funnel_html = []
    for label, n, pct_total, drop_pct, bar_pct in rows:
        bar_w = (n / base * 100) if base else 0
        drop_html = f' <span class="drop">−{drop_pct:.0f}%</span>' if drop_pct and drop_pct > 0 else ""
        funnel_html.append(
            f'<div class="funnel-row">'
            f'<div class="funnel-fill" style="width:{bar_w:.1f}%"></div>'
            f'<div class="funnel-text">'
            f'<span class="label">{html.escape(label)}</span>'
            f'<span><span class="num">{n}</span>{drop_html}</span>'
            f'</div></div>'
        )

    return (
        f"{kpi}"
        f'<div class="card">'
        f'<h2>Воронка · {html.escape(_RANGE_LABELS.get(range_key, range_key))}</h2>'
        f'<p class="muted">Уникальные tg_id на каждом шаге. Drop% — потеря между шагами.</p>'
        f'{"".join(funnel_html)}'
        f'</div>'
    )


def _render_tab_users(events: list[dict], apps: list[dict]) -> str:
    users = _build_users(events, apps)
    if not users:
        return '<div class="card"><p class="muted">За выбранный период активности нет.</p></div>'

    blocks = []
    for u in users:
        username = u["username"]
        tg_full = (u["tg_first_name"] + " " + u["tg_last_name"]).strip()
        # Имя для шапки: что юзер ввёл в форме > tg_first/last > username > id
        display = (
            u["name"] or tg_full or
            (f"@{username}" if username else "") or
            f"id {u['tg_id']}"
        )
        # В summary — только ТЕКСТ контакта (без <a>): иначе клик по ссылке
        # перехватывает event и <details> не раскрывается. Ссылка — в теле.
        contact_label = (
            f'@{html.escape(username)}' if username
            else f'id {u["tg_id"]}'
        )
        contact_link_html = (
            f'<a href="https://t.me/{html.escape(username)}" target="_blank">@{html.escape(username)} →</a>'
            if username else f'<a href="tg://user?id={u["tg_id"]}">id {u["tg_id"]} →</a>'
        )
        last_short = u["last_ts"][:19].replace("T", " ")
        first_short = u["first_ts"][:19].replace("T", " ")

        # Бейджи
        badges = []
        if u["leads"]:
            badges.append(f'<span class="badge lead">Заявок: {len(u["leads"])}</span>')
        else:
            badges.append('<span class="badge start">Без заявки</span>')
        badges_html = " ".join(badges)

        # Тело — таймлайн
        timeline = _format_user_timeline(u["events"])
        leads_html = ""
        if u["leads"]:
            lead_items = []
            for a in u["leads"]:
                lead_items.append(
                    f'<div class="card" style="margin:6px 0">'
                    f'<div class="lead-grid">'
                    f'<div><div class="field-label">Имя</div><div class="field-value">{html.escape(str(a.get("name", "—")))}</div></div>'
                    f'<div><div class="field-label">Контакт</div><div class="field-value">{html.escape(str(a.get("contact", "—")))}</div></div>'
                    f'<div style="grid-column: 1 / -1"><div class="field-label">Примеры</div><div class="field-value">{html.escape(str(a.get("examples", ""))[:400])}</div></div>'
                    f'<div style="grid-column: 1 / -1"><div class="field-label">Опыт</div><div class="field-value">{html.escape(str(a.get("experience", ""))[:300])}</div></div>'
                    f'</div></div>'
                )
            leads_html = f'<h3>Заявки</h3>{"".join(lead_items)}'

        blocks.append(
            f'<details class="user">'
            f'<summary>'
            f'<span class="summary-name">{html.escape(display)}</span>'
            f'<span class="summary-meta">{contact_label}</span>'
            f'<span class="summary-meta">· {len(u["events"])} событий</span>'
            f'<span class="summary-meta">· последнее: {html.escape(last_short)}</span>'
            f'{badges_html}'
            f'</summary>'
            f'<div class="body-pad">'
            f'<p class="muted">Контакт: {contact_link_html} · первое: {html.escape(first_short)} · последнее: {html.escape(last_short)}</p>'
            f'{leads_html}'
            f'<h3>Таймлайн ({len(u["events"])} событий)</h3>'
            f'{timeline}'
            f'</div>'
            f'</details>'
        )

    return (
        f'<div class="card"><h2>Пользователи · {len(users)}</h2>'
        f'<p class="muted">Кликни на юзера чтобы развернуть таймлайн всех его действий с дельтами по времени.</p></div>'
        f'{"".join(blocks)}'
    )


def _render_tab_leads(apps: list[dict]) -> str:
    if not apps:
        return '<div class="card"><p class="muted">За выбранный период заявок нет.</p></div>'
    blocks = []
    for a in sorted(apps, key=lambda x: str(x.get("timestamp", "")), reverse=True):
        ts = str(a.get("timestamp", ""))[:19].replace("T", " ")
        name = html.escape(str(a.get("name", "") or "—"))
        contact = html.escape(str(a.get("contact", "")))
        username = str(a.get("tg_username", "") or "")
        tg_id = a.get("tg_id", "")
        tg_first = html.escape(str(a.get("tg_first_name", "") or ""))
        tg_last = html.escape(str(a.get("tg_last_name", "") or ""))
        tg_full = (tg_first + " " + tg_last).strip()
        examples = html.escape(str(a.get("examples", "") or ""))
        experience = html.escape(str(a.get("experience", "") or ""))
        # Текстовый лейбл для summary (без <a>) и кликабельная ссылка для тела
        if username:
            uname_label = f'@{html.escape(username)}'
            uname_link = f'<a href="https://t.me/{html.escape(username)}" target="_blank">@{html.escape(username)} →</a>'
        elif tg_id:
            uname_label = f'id {tg_id}'
            uname_link = f'<a href="tg://user?id={tg_id}">id {tg_id} →</a>'
        else:
            uname_label = "—"
            uname_link = "—"
        if tg_full:
            uname_label += f' · {tg_full}'
            uname_link += f' · <span class="muted">{tg_full}</span>'

        blocks.append(
            f'<details class="lead">'
            f'<summary>'
            f'<span class="summary-name">{name}</span>'
            f'<span class="summary-meta">· {contact}</span>'
            f'<span class="summary-meta">· {uname_label}</span>'
            f'<span class="summary-meta" style="margin-left:auto">{html.escape(ts)}</span>'
            f'</summary>'
            f'<div class="body-pad lead-grid">'
            f'<div style="grid-column: 1 / -1"><div class="field-label">TG</div><div class="field-value">{uname_link}</div></div>'
            f'<div style="grid-column: 1 / -1"><div class="field-label">Примеры</div><div class="field-value">{examples}</div></div>'
            f'<div style="grid-column: 1 / -1"><div class="field-label">Опыт</div><div class="field-value">{experience}</div></div>'
            f'</div></details>'
        )
    return (
        f'<div class="card"><h2>Заявки · {len(apps)}</h2></div>'
        f'{"".join(blocks)}'
    )


def _render_tab_events(events: list[dict]) -> str:
    if not events:
        return '<div class="card"><p class="muted">За выбранный период событий нет.</p></div>'
    sorted_evs = sorted(events, key=lambda e: str(e.get("timestamp", "")), reverse=True)
    rows = []
    for e in sorted_evs[:500]:
        ts = str(e.get("timestamp", ""))[:19].replace("T", " ")
        username = str(e.get("tg_username", "") or "")
        tg_id = e.get("tg_id", "")
        ev = str(e.get("event", ""))
        extra = str(e.get("extra", "") or "")
        user_html = (
            f'<a href="https://t.me/{html.escape(username)}" target="_blank">@{html.escape(username)}</a>'
            if username else (f'id {html.escape(str(tg_id))}' if tg_id else "—")
        )
        rows.append(
            f'<tr>'
            f'<td><code style="font-size:12px;color:#999">{html.escape(ts)}</code></td>'
            f'<td>{user_html}</td>'
            f'<td>{html.escape(_event_label(ev))}</td>'
            f'<td><span class="extra">{html.escape(extra[:140])}</span></td>'
            f'</tr>'
        )
    note = ""
    if len(sorted_evs) > 500:
        note = f'<p class="muted">Показано 500 из {len(sorted_evs)} событий.</p>'
    return (
        f'<div class="card">'
        f'<h2>Лог событий · {len(sorted_evs)}</h2>'
        f'{note}'
        f'<table class="events-tbl">'
        f'<thead><tr><th>Время</th><th>Юзер</th><th>Событие</th><th>Detail</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        f'</table></div>'
    )


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
    # /start <payload> — deep-link source (для рекламы)
    parts = (m.text or "").split(maxsplit=1)
    start_param = parts[1].strip() if len(parts) > 1 else ""
    _track(m, "start", extra=start_param)
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
    _track(m, "back_pressed", extra=(cur or "no_state").replace("Form:", ""))
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
        _track(m, "examples_invalid", extra=f"validate: {err[:80]}")
        await m.answer(err, reply_markup=kb())
        return
    if not URL_RE.search(m.text):
        _track(m, "examples_invalid", extra=f"no_url: {m.text[:80]}")
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
        _track(m, "experience_invalid", extra=f"validate: {err[:80]}")
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
        _track(m, "name_invalid", extra=f"validate: {err[:80]}")
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
            _track(m, "contact_invalid", extra=f"validate: {err[:80]}")
            await m.answer(err, reply_markup=kb_contact())
            return
        contact_text = m.text.strip()

    if not contact_text:
        _track(m, "contact_invalid", extra="empty")
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
        _track(m, "cooldown_hit", extra=f"{int(now - last)}s_since_last")
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
    _track(m, "confirm_fallback", extra=(m.text or "")[:80])
    await m.answer(
        "Жми «✅ Отправить» чтобы отправить заявку или «↩️ Назад» чтобы поправить.",
        reply_markup=kb_confirm(),
    )


@router.message()
async def fallback(m: Message, state: FSMContext):
    cur = await state.get_state()
    state_label = (cur or "no_state").replace("Form:", "")
    _track(m, "fallback", extra=f"state={state_label}; text={(m.text or '')[:60]}")
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
        range_key = request.query.get("range", "7d")
        if range_key not in _RANGE_LABELS:
            range_key = "7d"
        tab = request.query.get("tab", "funnel")
        if tab not in {"funnel", "users", "leads", "events"}:
            tab = "funnel"
        try:
            events = await asyncio.to_thread(read_events)
            apps = await asyncio.to_thread(read_applications)
        except Exception:
            log.exception("admin: read failed")
            return web.Response(text="sheets read failed, check logs", status=500)
        body = _render_admin_html(events, apps, token=token, range_key=range_key, tab=tab)
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
