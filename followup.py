"""Follow-up MVP: после заявки бот предлагает кандидату оплачиваемый тест.

Поток:
  1. Юзер шлёт заявку → cmd_submit (в bot.py) → on_application_submitted(...).
     Здесь генерируется followup_draft, пишется в Sheets (state=pending),
     админу уходит сообщение с inline-кнопками [Одобрить +3ч] [Пропустить].
  2. Админ жмёт Одобрить → state=approved, send_after=now+3h.
     Если время выходит за 09:00–18:00 МСК — переносит на ближайшее рабочее окно.
  3. Scheduler в основном процессе раз в 5 минут читает Sheets, ищет
     approved-строки с send_after <= now. Перед отправкой ставит state=sending,
     потом sendMessage кандидату, потом state=sent.
  4. Если FOLLOWUP_DRY_RUN=true — вместо кандидата шлёт в админ-группу,
     заранее ставит followup_state=dry_run_sent и dry_run_sent_at, чтобы не
     спамить повторно и не отправить эту строку кандидату при выключении dry-run.
  5. Кандидат жмёт «Готов» → бот спрашивает видео, test_response=accepted →
     ждёт файл/ссылку → test_response=submitted, test_video_url=...

Идемпотентность:
  - cmd_submit вызывает has_active_followup до создания черновика.
  - Approve callback идемпотентен: повторный клик ничего не двигает.
  - Scheduler ставит sending ДО sendMessage. Recovery на старте: stuck
    sending старше 60 сек → sent (at-most-once, пользователь скорее не получит,
    чем получит дважды).

Все вызовы Sheets — через asyncio.to_thread.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from followup_hooks import build_hook
from sheets import (
    find_application_row,
    has_active_followup,
    read_applications_with_index,
    update_application_fields,
)

log = logging.getLogger("ugc-bot.followup")
router = Router(name="followup")


# --- Конфиг -----------------------------------------------------------------

def _env_bool(name: str, default: bool) -> bool:
    """Жёсткий парсинг булевых env. Любое значение кроме точного 'true' → False."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() == "true"


# Дефолты максимально безопасные: бот не шлёт ни одного follow-up без явного
# выставления обоих флагов. Включить реальную рассылку = ENABLED=true И DRY_RUN=false.
FOLLOWUP_ENABLED = _env_bool("FOLLOWUP_ENABLED", default=False)
FOLLOWUP_DRY_RUN = _env_bool("FOLLOWUP_DRY_RUN", default=True)

# Задержка от Approve до отправки. Дефолт 3 часа. В тестах подменяем на 0.
FOLLOWUP_DELAY_SEC = int(os.getenv("FOLLOWUP_DELAY_SEC", str(3 * 3600)))
# Период опроса Sheets. Меньше 300с упрётся в квоты Sheets API.
FOLLOWUP_TICK_SEC = int(os.getenv("FOLLOWUP_TICK_SEC", "300"))
# Recovery: stuck `sending` старше этого считаем доставленными (at-most-once).
SENDING_TIMEOUT_SEC = 60
FOLLOWUP_TIMEZONE = os.getenv("FOLLOWUP_TIMEZONE", "Europe/Moscow")
WORK_START_HOUR = int(os.getenv("FOLLOWUP_WORK_START_HOUR", "9"))
WORK_END_HOUR = int(os.getenv("FOLLOWUP_WORK_END_HOUR", "18"))


def _load_work_tz(name: str):
    try:
        return ZoneInfo(name)
    except Exception:
        # На Windows в локальном venv может не быть пакета tzdata. Для Москвы
        # безопасный fallback — фиксированный UTC+3, т.к. сезонного перевода нет.
        if name in {"Europe/Moscow", "MSK"}:
            return timezone(timedelta(hours=3), name="MSK")
        raise


WORK_TZ = _load_work_tz(FOLLOWUP_TIMEZONE)

ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DRY_RUN_ADMIN_IDS = [
    int(x) for x in os.getenv("FOLLOWUP_DRY_RUN_ADMIN_IDS", "").split(",") if x.strip()
] or ADMIN_IDS

SITE_URL = "https://xn--c1aeedbcapcxc2dyb.xn--p1ai/"
TEST_PRICE_RUB = 700
_tz_file_env = os.getenv("FOLLOWUP_TZ_FILE", "").strip()
TZ_FILE_PATH = Path(_tz_file_env) if _tz_file_env else (
    Path(__file__).parent / "assets" / "ugc_leasing_comic_5_slides_9x16.pdf"
)


# --- FSM для приёма видео от кандидата --------------------------------------

class FollowupForm(StatesGroup):
    waiting_video = State()


# --- Текст follow-up ---------------------------------------------------------

def build_draft(name: str, examples: str, experience: str) -> str:
    """Шаблонный draft. Если зацепка fallback (saw_examples=False) — фразу про
    «посмотрели то что вы прислали» НЕ вставляем, чтобы не врать."""
    hook = build_hook(examples, experience)
    name_line = (name or "").strip() or "Привет"
    if hook.saw_examples:
        seen_line = "Спасибо за заявку. Мы посмотрели то, что вы прислали."
    else:
        seen_line = "Спасибо за заявку."
    return (
        f"{name_line}, привет!\n\n"
        f"{seen_line}\n\n"
        f"По заявке видно, что {hook.text}.\n\n"
        "Хотим предложить оплачиваемое тестовое задание: видео 15–30 секунд по нашему ТЗ.\n\n"
        "Тест нужен, чтобы понять, получится ли у вас именно наш формат. "
        "Мы не будем публиковать или использовать тестовое видео без вашего отдельного согласия.\n\n"
        "Если интересно — пришлём ТЗ и условия оплаты.\n\n"
        "Готовы попробовать?"
    )


# --- Inline keyboards --------------------------------------------------------

def _delay_label(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}с"
    if seconds < 3600:
        minutes = max(1, round(seconds / 60))
        return f"{minutes}мин"
    hours = seconds / 3600
    if hours.is_integer():
        return f"{int(hours)}ч"
    return f"{hours:.1f}ч"


def _apply_work_window(dt_utc: datetime) -> datetime:
    """Возвращает ближайшее разрешённое время отправки в рабочем окне МСК.

    dt_utc уже включает задержку после approve. Если время попало до 09:00 МСК,
    переносим на 09:00 того же дня. Если после/в 18:00 — на 09:00 следующего дня.
    """
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    local = dt_utc.astimezone(WORK_TZ)
    start = local.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)
    end = local.replace(hour=WORK_END_HOUR, minute=0, second=0, microsecond=0)
    if local < start:
        scheduled = start
    elif local >= end:
        scheduled = (local + timedelta(days=1)).replace(
            hour=WORK_START_HOUR, minute=0, second=0, microsecond=0
        )
    else:
        scheduled = local
    return scheduled.astimezone(timezone.utc)


def _calculate_send_after(now_utc: datetime | None = None) -> datetime:
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    delayed = now_utc.astimezone(timezone.utc) + timedelta(seconds=FOLLOWUP_DELAY_SEC)
    return _apply_work_window(delayed)


def _format_send_after_msk(dt_utc: datetime) -> str:
    return dt_utc.astimezone(WORK_TZ).strftime("%d.%m %H:%M МСК")


def _admin_kb(tg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=f"✅ Одобрить +{_delay_label(FOLLOWUP_DELAY_SEC)}",
            callback_data=f"fu:approve:{tg_id}",
        ),
        InlineKeyboardButton(text="❌ Пропустить", callback_data=f"fu:skip:{tg_id}"),
    ]])


def _user_kb(tg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Готов(а), пришлите ТЗ", callback_data=f"fu_u:ready:{tg_id}")],
        [InlineKeyboardButton(text="💬 Сначала условия", callback_data=f"fu_u:terms:{tg_id}")],
        [InlineKeyboardButton(text="❌ Не актуально", callback_data=f"fu_u:decline:{tg_id}")],
    ])


def _terms_kb(tg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ок, пришлите ТЗ", callback_data=f"fu_u:ready:{tg_id}")],
        [InlineKeyboardButton(text="❌ Не актуально", callback_data=f"fu_u:decline:{tg_id}")],
    ])


def _decline_return_kb(tg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Передумал(а), пришлите ТЗ", callback_data=f"fu_u:ready:{tg_id}")],
        [InlineKeyboardButton(text="💬 Посмотреть условия", callback_data=f"fu_u:terms:{tg_id}")],
    ])


# --- Hook из cmd_submit ------------------------------------------------------

async def on_application_submitted(bot: Bot, payload: dict, row_idx: int) -> None:
    """Вызывается из bot.py:cmd_submit ПОСЛЕ append_application.

    Все ошибки гасим (логируем) — flow заявки не должен ломаться из-за follow-up.
    """
    if not FOLLOWUP_ENABLED:
        return
    tg_id = payload.get("tg_id")
    if not tg_id:
        return
    try:
        # Дедуп: если у юзера уже есть активный follow-up — не плодим второй.
        # Учти: append_application уже создал новую строку, и has_active_followup
        # сейчас увидит её только если followup_state уже выставлен. На свежей
        # строке state пустой → has_active_followup проверяет ПРЕДЫДУЩИЕ строки.
        if await asyncio.to_thread(has_active_followup, tg_id):
            log.info("follow-up skipped (duplicate): tg_id=%s", tg_id)
            return
        draft = build_draft(payload.get("name", ""), payload.get("examples", ""), payload.get("experience", ""))
        await asyncio.to_thread(
            update_application_fields,
            row_idx,
            {"followup_state": "pending", "followup_draft": draft},
        )
        # Уведомляем админ-группу с draft + кнопками.
        username = payload.get("tg_username", "")
        handle = f"@{username}" if username else f"id {tg_id}"
        admin_text = (
            "🆕 Follow-up черновик\n"
            f"Кандидат: {handle}\n"
            f"Имя: {payload.get('name', '—')}\n\n"
            "— Текст черновика —\n"
            f"{draft}"
        )
        kb = _admin_kb(tg_id)
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, admin_text, reply_markup=kb)
            except Exception:
                log.exception("follow-up: не смог отправить админу %s", admin_id)
    except Exception:
        log.exception("on_application_submitted failed for tg_id=%s", tg_id)


# --- Callback handlers (админ) -----------------------------------------------

@router.callback_query(F.data.startswith("fu:approve:"))
async def cb_admin_approve(cq: CallbackQuery) -> None:
    tg_id = int(cq.data.split(":")[2])
    row_idx = await asyncio.to_thread(find_application_row, tg_id)
    if not row_idx:
        await cq.answer("Не нашёл строку в Sheets", show_alert=True)
        return
    rows = await asyncio.to_thread(read_applications_with_index)
    rec = next((r for ri, r in rows if ri == row_idx), None)
    if rec is None:
        await cq.answer("Строка не найдена", show_alert=True)
        return
    cur_state = str(rec.get("followup_state", "")).strip()
    # Идемпотентность: если уже approved/sending/sent — игнор.
    if cur_state in {"approved", "sending", "sent", "blocked", "dry_run_sent"}:
        await cq.answer(f"Уже: {cur_state}", show_alert=False)
        return
    if cur_state != "pending":
        await cq.answer(f"Состояние {cur_state!r}, approve не применим", show_alert=True)
        return
    send_after_dt = _calculate_send_after()
    send_after = send_after_dt.isoformat(timespec="seconds")
    send_after_label = _format_send_after_msk(send_after_dt)
    await asyncio.to_thread(
        update_application_fields,
        row_idx,
        {"followup_state": "approved", "followup_send_after": send_after},
    )
    await cq.answer(f"Одобрено, отправка после {send_after_label}")
    if cq.message:
        try:
            await cq.message.edit_reply_markup(reply_markup=None)
            await cq.message.reply(f"✅ Одобрено. Уйдёт после {send_after_label}")
        except Exception:
            pass


@router.callback_query(F.data.startswith("fu:skip:"))
async def cb_admin_skip(cq: CallbackQuery) -> None:
    tg_id = int(cq.data.split(":")[2])
    row_idx = await asyncio.to_thread(find_application_row, tg_id)
    if not row_idx:
        await cq.answer("Не нашёл строку", show_alert=True)
        return
    await asyncio.to_thread(
        update_application_fields,
        row_idx,
        {"followup_state": "skipped", "followup_error_reason": "admin_skip"},
    )
    await cq.answer("Пропущено")
    if cq.message:
        try:
            await cq.message.edit_reply_markup(reply_markup=None)
            await cq.message.reply("❌ Пропущено")
        except Exception:
            pass


# --- Callback handlers (кандидат) --------------------------------------------

TERMS_TEXT = (
    "Конечно.\n\n"
    f"Тестовое оплачиваемое: {TEST_PRICE_RUB} ₽ за ролик.\n\n"
    "Формат: короткое вертикальное видео 15–30 секунд.\n"
    f"Тема: ЛизингСток — сервис с авто и техникой из лизинга:\n{SITE_URL}\n\n"
    "Мы не публикуем и не используем тест без вашего разрешения.\n"
    "Нам нужно понять, получится ли у вас делать живой контент под нашу нишу.\n\n"
    "Если готовы — пришлём комикс-ТЗ."
)

TZ_TEXT = (
    "Отлично, спасибо!\n\n"
    f"Тестовое задание оплачиваемое: {TEST_PRICE_RUB} ₽ за ролик.\n\n"
    "Нужно сделать короткое вертикальное видео 15–30 секунд для ЛизингСток:\n"
    f"{SITE_URL}\n\n"
    "Прикрепляем комикс-ТЗ как ориентир. Не нужно копировать его один в один — "
    "это скорее пример логики: хук → проблема → выгодный лот/идея → интерес к сервису.\n\n"
    "Можно сделать по-своему: с вашей подачей, юмором, монтажом, голосом, текстом на экране — как видите.\n\n"
    "Так как это тест, не нужен дорогой продакшн или идеальная съёмка. "
    "Нам важно понять вашу креативность, подачу и сможете ли вы делать живой контент в нашей нише.\n\n"
    "Видео можно прислать сюда файлом или ссылкой — как удобнее."
)


async def _clear_message_keyboard(cq: CallbackQuery) -> None:
    if not cq.message:
        return
    try:
        await cq.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


async def _send_test_brief(message: Message) -> None:
    await message.reply(TZ_TEXT)
    if TZ_FILE_PATH.exists():
        await message.answer_document(
            FSInputFile(TZ_FILE_PATH),
            caption="Комикс-ТЗ. Используйте как ориентир, не как жёсткий шаблон.",
        )
    else:
        log.error("follow-up TZ file is missing: %s", TZ_FILE_PATH)
        await message.reply(
            "Файл с комикс-ТЗ сейчас не прикрепился технически, но можно делать по описанию выше. "
            "Мы отдельно проверим файл."
        )


@router.callback_query(F.data.startswith("fu_u:ready:"))
async def cb_user_ready(cq: CallbackQuery, state: FSMContext) -> None:
    tg_id = int(cq.data.split(":")[2])
    row_idx = await asyncio.to_thread(find_application_row, tg_id)
    if row_idx:
        await asyncio.to_thread(update_application_fields, row_idx, {"test_response": "accepted"})
    await cq.answer()
    if cq.message:
        await _clear_message_keyboard(cq)
        await _send_test_brief(cq.message)
    await state.set_state(FollowupForm.waiting_video)


@router.callback_query(F.data.startswith("fu_u:terms:"))
async def cb_user_terms(cq: CallbackQuery, state: FSMContext) -> None:
    tg_id = int(cq.data.split(":")[2])
    row_idx = await asyncio.to_thread(find_application_row, tg_id)
    if row_idx:
        await asyncio.to_thread(update_application_fields, row_idx, {"test_response": "wants_terms"})
    await cq.answer()
    if cq.message:
        await _clear_message_keyboard(cq)
        await cq.message.reply(TERMS_TEXT, reply_markup=_terms_kb(tg_id))


@router.callback_query(F.data == "fu_demo")
async def cb_demo(cq: CallbackQuery) -> None:
    await cq.answer(
        "🧪 Демо: в dry-run кнопки только показывают, как это выглядит. "
        "У реального кандидата эта кнопка будет рабочей.",
        show_alert=True,
    )


def _demo_terms_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ок, пришлите ТЗ", callback_data="fu_demo:ready")],
        [InlineKeyboardButton(text="❌ Не актуально", callback_data="fu_demo:decline")],
    ])


def _demo_decline_return_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Передумал(а), пришлите ТЗ", callback_data="fu_demo:ready")],
        [InlineKeyboardButton(text="💬 Посмотреть условия", callback_data="fu_demo:terms")],
    ])


@router.callback_query(F.data == "fu_demo:ready")
async def cb_demo_ready(cq: CallbackQuery) -> None:
    await cq.answer("🧪 Dry-run: показываю сценарий кандидата, Sheets не меняю.")
    if cq.message:
        await _clear_message_keyboard(cq)
        await _send_test_brief(cq.message)


@router.callback_query(F.data == "fu_demo:terms")
async def cb_demo_terms(cq: CallbackQuery) -> None:
    await cq.answer("🧪 Dry-run: показываю условия, Sheets не меняю.")
    if cq.message:
        await _clear_message_keyboard(cq)
        await cq.message.reply(TERMS_TEXT, reply_markup=_demo_terms_kb())


@router.callback_query(F.data == "fu_demo:decline")
async def cb_demo_decline(cq: CallbackQuery) -> None:
    await cq.answer("🧪 Dry-run: показываю отказ, Sheets не меняю.")
    if cq.message:
        await _clear_message_keyboard(cq)
        await cq.message.reply(
            "Понял, спасибо за ответ.\n\n"
            "Если передумаете, можно вернуться к тестовому заданию ниже.",
            reply_markup=_demo_decline_return_kb(),
        )


@router.callback_query(F.data.startswith("fu_u:decline:"))
async def cb_user_decline(cq: CallbackQuery, state: FSMContext) -> None:
    tg_id = int(cq.data.split(":")[2])
    row_idx = await asyncio.to_thread(find_application_row, tg_id)
    if row_idx:
        await asyncio.to_thread(update_application_fields, row_idx, {"test_response": "declined"})
    await cq.answer()
    if cq.message:
        await _clear_message_keyboard(cq)
        await cq.message.reply(
            "Понял, спасибо за ответ.\n\n"
            "Если передумаете, можно вернуться к тестовому заданию ниже.",
            reply_markup=_decline_return_kb(tg_id),
        )
    await state.clear()


# --- Приём видео ------------------------------------------------------------

@router.message(FollowupForm.waiting_video)
async def on_test_video(m: Message, state: FSMContext) -> None:
    if not m.from_user:
        return
    video_ref = ""
    should_copy_media = False
    if m.video:
        video_ref = f"file_id:{m.video.file_id}"
        should_copy_media = True
    elif m.video_note:
        video_ref = f"video_note:{m.video_note.file_id}"
        should_copy_media = True
    elif m.document:
        file_name = (m.document.file_name or "").lower()
        mime_type = (m.document.mime_type or "").lower()
        is_video_doc = mime_type.startswith("video/") or file_name.endswith((".mp4", ".mov", ".m4v", ".webm"))
        if is_video_doc:
            video_ref = f"document:{m.document.file_id}"
            should_copy_media = True
    elif m.text and ("http://" in m.text or "https://" in m.text):
        video_ref = m.text.strip()[:500]
    else:
        await m.answer(
            "Жду видео файлом или ссылкой (Drive / Я.Диск / YouTube / VK / Telegram / прямая https-ссылка). "
            "Если хочешь отменить — напиши /start."
        )
        return
    row_idx = await asyncio.to_thread(find_application_row, m.from_user.id)
    if row_idx:
        await asyncio.to_thread(
            update_application_fields,
            row_idx,
            {"test_response": "submitted", "test_video_url": video_ref},
        )
    await m.answer("Принял! Посмотрим и вернёмся с фидбэком.")
    # Уведомляем админов — пришло тестовое.
    bot = m.bot
    if bot:
        username = m.from_user.username or ""
        handle = f"@{username}" if username else f"id {m.from_user.id}"
        for admin_id in ADMIN_IDS:
            try:
                note = (
                    f"📹 Пришло тестовое видео от {handle}\n"
                    f"Строка Sheets: {row_idx or 'не найдена'}\n"
                    f"ref: {video_ref[:200]}"
                )
                await bot.send_message(admin_id, note)
                if should_copy_media:
                    await bot.copy_message(
                        chat_id=admin_id,
                        from_chat_id=m.chat.id,
                        message_id=m.message_id,
                    )
            except Exception:
                log.exception("test_video: не смог отправить админу %s", admin_id)
    await state.clear()


# --- Scheduler --------------------------------------------------------------

def _parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        # Sheets может вернуть с Z или с +00:00 — оба варианта валидные.
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


async def _send_to_user(bot: Bot, tg_id: int, text: str) -> None:
    await bot.send_message(tg_id, text, reply_markup=_user_kb(tg_id))


def _demo_user_kb() -> InlineKeyboardMarkup:
    """Демо-кнопки для dry-run: показывают сценарий кандидата, но не меняют Sheets."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Готов(а), пришлите ТЗ", callback_data="fu_demo:ready")],
        [InlineKeyboardButton(text="💬 Сначала условия", callback_data="fu_demo:terms")],
        [InlineKeyboardButton(text="❌ Не актуально", callback_data="fu_demo:decline")],
    ])


async def _send_dry_run(bot: Bot, candidate_tg_id: int, candidate_username: str, text: str) -> None:
    handle = f"@{candidate_username}" if candidate_username else f"id {candidate_tg_id}"
    body = (
        f"🧪 DRY RUN: должно было уйти кандидату {handle}\n\n"
        f"— Текст —\n{text}"
    )
    for admin_id in DRY_RUN_ADMIN_IDS:
        try:
            await bot.send_message(admin_id, body, reply_markup=_demo_user_kb())
        except Exception:
            log.exception("dry_run: не смог отправить админу %s", admin_id)


async def _recover_stuck_sending(now: datetime) -> int:
    """At-most-once recovery: всё что застряло в sending дольше SENDING_TIMEOUT_SEC
    помечаем как sent (с recovered=true в reason). Лучше не отправить, чем отправить дважды."""
    rows = await asyncio.to_thread(read_applications_with_index)
    fixed = 0
    for row_idx, rec in rows:
        if str(rec.get("followup_state", "")).strip() != "sending":
            continue
        # Если sending без send_after — recovery всё равно применим.
        # Берём send_after как отметку начала sending. Если он пустой — считаем сейчас.
        started = _parse_iso(str(rec.get("followup_send_after", "")))
        if started and (now - started).total_seconds() < SENDING_TIMEOUT_SEC:
            continue
        await asyncio.to_thread(
            update_application_fields,
            row_idx,
            {
                "followup_state": "sent",
                "followup_sent_at": now.isoformat(timespec="seconds"),
                "followup_error_reason": "recovered_from_stuck_sending",
            },
        )
        fixed += 1
    if fixed:
        log.warning("scheduler recovery: %s stuck sending → sent", fixed)
    return fixed


async def _process_one_tick(bot: Bot) -> dict:
    """Один проход scheduler. Возвращает счётчики для логов."""
    now = datetime.now(timezone.utc)
    counts = {"approved": 0, "sent": 0, "blocked": 0, "dry_run": 0, "errors": 0}
    rows = await asyncio.to_thread(read_applications_with_index)
    for row_idx, rec in rows:
        state = str(rec.get("followup_state", "")).strip()
        if state != "approved":
            continue
        counts["approved"] += 1
        send_after = _parse_iso(str(rec.get("followup_send_after", "")))
        if send_after is None or send_after > now:
            continue
        try:
            tg_id = int(str(rec.get("tg_id", "")).strip() or 0)
        except ValueError:
            tg_id = 0
        if not tg_id:
            counts["errors"] += 1
            await asyncio.to_thread(
                update_application_fields,
                row_idx,
                {"followup_state": "skipped", "followup_error_reason": "no_tg_id"},
            )
            continue
        draft = str(rec.get("followup_draft", "")).strip()
        if not draft:
            counts["errors"] += 1
            await asyncio.to_thread(
                update_application_fields,
                row_idx,
                {"followup_state": "skipped", "followup_error_reason": "empty_draft"},
            )
            continue

        # Dry-run: заранее помечаем строку как обработанную, потом шлём в админ-чат.
        # Это at-most-once: лучше один dry-run не увидеть, чем спамить каждую минуту.
        if FOLLOWUP_DRY_RUN:
            if str(rec.get("dry_run_sent_at", "")).strip():
                continue
            await asyncio.to_thread(
                update_application_fields,
                row_idx,
                {
                    "followup_state": "dry_run_sent",
                    "dry_run_sent_at": now.isoformat(timespec="seconds"),
                },
            )
            await _send_dry_run(bot, tg_id, str(rec.get("tg_username", "")), draft)
            counts["dry_run"] += 1
            await asyncio.sleep(0.05)  # rate-limit: <=20/sec
            continue

        # Safety rail: строки, уже прошедшие dry-run, нельзя отправлять кандидату
        # автоматически после переключения FOLLOWUP_DRY_RUN=false.
        if str(rec.get("dry_run_sent_at", "")).strip():
            counts["errors"] += 1
            await asyncio.to_thread(
                update_application_fields,
                row_idx,
                {
                    "followup_state": "skipped",
                    "followup_error_reason": "dry_run_sent_requires_new_approval",
                },
            )
            continue

        # Реальная отправка. Сначала ставим sending — это lock для второго прохода.
        await asyncio.to_thread(update_application_fields, row_idx, {"followup_state": "sending"})
        try:
            await _send_to_user(bot, tg_id, draft)
        except TelegramForbiddenError:
            counts["blocked"] += 1
            await asyncio.to_thread(
                update_application_fields,
                row_idx,
                {"followup_state": "blocked", "followup_error_reason": "user_blocked_bot"},
            )
            continue
        except TelegramRetryAfter as e:
            log.warning("rate limit: sleeping %s", e.retry_after)
            await asyncio.sleep(int(e.retry_after) + 1)
            # Откатываем sending → approved, чтобы взяли в следующем тике.
            await asyncio.to_thread(
                update_application_fields,
                row_idx,
                {"followup_state": "approved"},
            )
            continue
        except Exception as e:
            log.exception("send failed for tg_id=%s", tg_id)
            counts["errors"] += 1
            await asyncio.to_thread(
                update_application_fields,
                row_idx,
                {"followup_state": "skipped", "followup_error_reason": f"send_error: {type(e).__name__}"},
            )
            continue
        await asyncio.to_thread(
            update_application_fields,
            row_idx,
            {"followup_state": "sent", "followup_sent_at": now.isoformat(timespec="seconds")},
        )
        counts["sent"] += 1
        await asyncio.sleep(0.05)  # 20 msg/sec safety
    return counts


async def _scheduler_loop(bot: Bot) -> None:
    log.info(
        "follow-up scheduler started: ENABLED=%s DRY_RUN=%s tick=%ss delay=%ss",
        FOLLOWUP_ENABLED, FOLLOWUP_DRY_RUN, FOLLOWUP_TICK_SEC, FOLLOWUP_DELAY_SEC,
    )
    # Recovery один раз на старте.
    try:
        await _recover_stuck_sending(datetime.now(timezone.utc))
    except Exception:
        log.exception("scheduler recovery failed")
    while True:
        try:
            counts = await _process_one_tick(bot)
            if any(counts.values()):
                log.info("follow-up tick: %s", counts)
        except Exception:
            log.exception("follow-up tick failed")
        await asyncio.sleep(FOLLOWUP_TICK_SEC)


def start_scheduler(bot: Bot) -> asyncio.Task | None:
    """Запускает scheduler в фоне. Если ENABLED=false — ничего не делает."""
    if not FOLLOWUP_ENABLED:
        log.info("follow-up scheduler disabled (FOLLOWUP_ENABLED=false)")
        return None
    return asyncio.create_task(_scheduler_loop(bot), name="follow-up-scheduler")
