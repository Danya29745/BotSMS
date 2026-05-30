#!/usr/bin/env python3
"""
👁 ShadowWatch Bot — единый файл для деплоя на BotHost
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Callable, Awaitable, Any, Dict

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup,
    InlineKeyboardButton, Update, LabeledPrice, PreCheckoutQuery,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    BusinessConnection
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# КОНФИГ
# ══════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан!")

_admin_ids_raw = os.getenv("ADMIN_IDS", "7965055989")
ADMIN_IDS = [int(x.strip()) for x in _admin_ids_raw.split(",") if x.strip()]
DB_PATH = os.getenv("DB_PATH", "shadowwatch.db")

BOT_NAME = "ShadowWatch"

# Тарифы
PLANS = {
    "trial": {"label": "🎁 Пробный период",  "days": 7,   "stars": 0,   "desc": "7 дней бесплатно"},
    "month": {"label": "📅 1 месяц",          "days": 30,  "stars": 35,  "desc": "1 месяц доступа"},
    "three": {"label": "📦 3 месяца",         "days": 90,  "stars": 89,  "desc": "3 месяца • скидка 15%"},
    "year":  {"label": "👑 1 год",            "days": 365, "stars": 299, "desc": "12 месяцев • скидка 29%"},
}

# ══════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════

def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _init_db_sync():
    conn = _get_conn(); c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
        registered TEXT DEFAULT (datetime('now')), last_seen TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS subscriptions (
        user_id INTEGER PRIMARY KEY, expires_at TEXT NOT NULL,
        granted_by INTEGER, granted_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS trial_used (
        user_id INTEGER PRIMARY KEY, used_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS message_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
        user_id INTEGER, username TEXT, first_name TEXT,
        text TEXT, media_type TEXT, file_id TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(chat_id, message_id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS user_settings (
        user_id INTEGER PRIMARY KEY,
        notify_delete INTEGER DEFAULT 1,
        notify_edit INTEGER DEFAULT 1,
        notify_self_destruct INTEGER DEFAULT 1
    )""")
    conn.commit(); conn.close()

async def init_db():
    await asyncio.get_event_loop().run_in_executor(None, _init_db_sync)

def _run(fn):
    return asyncio.get_event_loop().run_in_executor(None, fn)

async def upsert_user(user_id, username=None, first_name=None):
    def _f():
        conn = _get_conn()
        conn.execute("""INSERT INTO users (user_id, username, first_name, last_seen) VALUES (?,?,?,datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,
            first_name=excluded.first_name, last_seen=datetime('now')""",
            (user_id, username, first_name))
        conn.commit(); conn.close()
    await _run(_f)

async def get_all_users():
    def _f():
        conn = _get_conn()
        rows = conn.execute("SELECT * FROM users ORDER BY registered DESC").fetchall()
        conn.close(); return [dict(r) for r in rows]
    return await _run(_f)

async def grant_subscription(user_id, days, granted_by):
    def _f():
        conn = _get_conn()
        expires = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""INSERT INTO subscriptions (user_id, expires_at, granted_by) VALUES (?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET expires_at=?, granted_by=?, granted_at=datetime('now')""",
            (user_id, expires, granted_by, expires, granted_by))
        conn.commit(); conn.close(); return expires
    return await _run(_f)

async def revoke_subscription(user_id):
    def _f():
        conn = _get_conn()
        conn.execute("DELETE FROM subscriptions WHERE user_id=?", (user_id,))
        conn.commit(); conn.close()
    await _run(_f)

async def is_subscribed(user_id) -> bool:
    def _f():
        conn = _get_conn()
        row = conn.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
        conn.close()
        if not row: return False
        return datetime.strptime(row["expires_at"], "%Y-%m-%d %H:%M:%S") > datetime.now()
    return await _run(_f)

async def get_subscription(user_id):
    def _f():
        conn = _get_conn()
        row = conn.execute("SELECT * FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
        conn.close(); return dict(row) if row else None
    return await _run(_f)

async def get_all_subscriptions():
    def _f():
        conn = _get_conn()
        rows = conn.execute("""SELECT s.*, u.username, u.first_name FROM subscriptions s
            LEFT JOIN users u ON u.user_id=s.user_id ORDER BY s.expires_at DESC""").fetchall()
        conn.close(); return [dict(r) for r in rows]
    return await _run(_f)

async def has_used_trial(user_id) -> bool:
    def _f():
        conn = _get_conn()
        row = conn.execute("SELECT 1 FROM trial_used WHERE user_id=?", (user_id,)).fetchone()
        conn.close(); return row is not None
    return await _run(_f)

async def mark_trial_used(user_id):
    def _f():
        conn = _get_conn()
        conn.execute("INSERT OR IGNORE INTO trial_used (user_id) VALUES (?)", (user_id,))
        conn.commit(); conn.close()
    await _run(_f)

async def cache_message(chat_id, message_id, user_id, username, first_name,
                         text=None, media_type=None, file_id=None):
    def _f():
        conn = _get_conn()
        conn.execute("""INSERT OR REPLACE INTO message_cache
            (chat_id,message_id,user_id,username,first_name,text,media_type,file_id)
            VALUES (?,?,?,?,?,?,?,?)""",
            (chat_id, message_id, user_id, username, first_name, text, media_type, file_id))
        conn.commit(); conn.close()
    await _run(_f)

async def get_cached_message(chat_id, message_id):
    def _f():
        conn = _get_conn()
        row = conn.execute("SELECT * FROM message_cache WHERE chat_id=? AND message_id=?",
                           (chat_id, message_id)).fetchone()
        conn.close(); return dict(row) if row else None
    return await _run(_f)

async def delete_cached_message(chat_id, message_id):
    def _f():
        conn = _get_conn()
        conn.execute("DELETE FROM message_cache WHERE chat_id=? AND message_id=?", (chat_id, message_id))
        conn.commit(); conn.close()
    await _run(_f)

async def get_user_settings(user_id) -> dict:
    def _f():
        conn = _get_conn()
        row = conn.execute("SELECT * FROM user_settings WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            conn.execute("INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (user_id,))
            conn.commit()
            row = conn.execute("SELECT * FROM user_settings WHERE user_id=?", (user_id,)).fetchone()
        conn.close(); return dict(row)
    return await _run(_f)

async def toggle_user_setting(user_id, field):
    if field not in {"notify_delete", "notify_edit", "notify_self_destruct"}: return
    def _f():
        conn = _get_conn()
        conn.execute(f"""INSERT INTO user_settings (user_id, {field}) VALUES (?, 1)
            ON CONFLICT(user_id) DO UPDATE SET {field} = 1 - {field}""", (user_id,))
        conn.commit(); conn.close()
    await _run(_f)

# ══════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════

def is_admin(user_id): return user_id in ADMIN_IDS

def user_link(user_id, first_name, username=None):
    name = first_name or "Пользователь"
    uname = f" (@{username})" if username else ""
    return f'<a href="tg://user?id={user_id}">{name}</a>{uname}'

def extract_media(msg: Message):
    if msg.photo:      return "photo",      msg.photo[-1].file_id
    if msg.video:      return "video",      msg.video.file_id
    if msg.video_note: return "video_note", msg.video_note.file_id
    if msg.voice:      return "voice",      msg.voice.file_id
    if msg.audio:      return "audio",      msg.audio.file_id
    if msg.document:   return "document",   msg.document.file_id
    if msg.sticker:    return "sticker",    msg.sticker.file_id
    if msg.animation:  return "animation",  msg.animation.file_id
    return None, None

MEDIA_EMOJI = {"photo":"🖼","video":"🎬","video_note":"📹","voice":"🎙",
               "audio":"🎵","document":"📄","sticker":"🎭","animation":"🎞"}
MEDIA_SEND  = {"photo":"send_photo","video":"send_video","video_note":"send_video_note",
               "voice":"send_voice","audio":"send_audio","document":"send_document",
               "sticker":"send_sticker","animation":"send_animation"}

async def notify_user(bot, user_id, **kwargs):
    try: await bot.send_message(user_id, **kwargs)
    except Exception as e: logger.warning(f"notify {user_id}: {e}")

async def send_media(bot, user_id, media_type, file_id, caption=None):
    method = getattr(bot, MEDIA_SEND.get(media_type, ""), None)
    if not method: return
    kwargs = {"chat_id": user_id}
    if media_type in ("video_note", "sticker"): kwargs[media_type] = file_id
    else:
        kwargs[media_type] = file_id
        if caption: kwargs["caption"] = caption
    try: await method(**kwargs)
    except Exception as e: logger.warning(f"send_media {user_id}: {e}")

async def should_notify(user_id, setting) -> bool:
    if user_id in ADMIN_IDS: return True
    if not await is_subscribed(user_id): return False
    s = await get_user_settings(user_id)
    return bool(s.get(setting, 1))

def back_kb_adm():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
    ])

def main_back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="user:back")]
    ])

def reply_keyboard():
    """Постоянная клавиатура внизу экрана"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💳 Тарифы"), KeyboardButton(text="📋 Моя подписка")],
            [KeyboardButton(text="⚙️ Настройки"),  KeyboardButton(text="❓ Помощь")],
        ],
        resize_keyboard=True,
        persistent=True
    )

# ══════════════════════════════════════════════
# РОУТЕРЫ
# ══════════════════════════════════════════════

admin_router  = Router()
user_router   = Router()
event_router  = Router()
payment_router = Router()

# ══════════════════════════════════════════════
# FSM
# ══════════════════════════════════════════════

class AdminStates(StatesGroup):
    waiting_user_id   = State()
    waiting_days      = State()
    waiting_revoke_id = State()

# ══════════════════════════════════════════════
# ГЛАВНОЕ МЕНЮ
# ══════════════════════════════════════════════

def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Тарифы и подписка",     callback_data="user:plans")],
        [InlineKeyboardButton(text="📋 Моя подписка",           callback_data="user:sub")],
        [InlineKeyboardButton(text="⚙️ Настройки уведомлений", callback_data="user:settings")],
        [InlineKeyboardButton(text="❓ Как пользоваться",       callback_data="user:help")],
    ])

@user_router.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    subscribed = await is_subscribed(u.id)

    if is_admin(u.id):
        status = "👑 Администратор — безлимитный доступ"
    elif subscribed:
        sub = await get_subscription(u.id)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        status = f"✅ Подписка активна · осталось {days_left} дн."
    else:
        status = "❌ Подписка не активна"

    await msg.answer(
        f"👁 <b>ShadowWatch</b>\n\n"
        f"Привет, {u.first_name}! 👋\n\n"
        f"<b>Статус:</b> {status}\n\n"
        f"🗑 Вижу удалённые сообщения\n"
        f"✏️ Замечаю все редактирования\n"
        f"💣 Перехватываю исчезающие медиа\n\n"
        f"<i>Выбери действие ниже 👇</i>",
        reply_markup=reply_keyboard()
    )
    await msg.answer("Меню:", reply_markup=main_keyboard())

@user_router.callback_query(F.data == "user:back")
async def cb_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    u = call.from_user
    subscribed = await is_subscribed(u.id)
    if is_admin(u.id):        status = "👑 Администратор — безлимитный доступ"
    elif subscribed:           status = "✅ Подписка активна"
    else:                      status = "❌ Подписка не активна"
    await call.message.edit_text(
        f"👁 <b>ShadowWatch</b>\n\n"
        f"<b>Статус:</b> {status}\n\n"
        f"🗑 Вижу удалённые сообщения\n"
        f"✏️ Замечаю все редактирования\n"
        f"💣 Перехватываю исчезающие медиа\n\n"
        f"<i>Выбери действие ниже 👇</i>",
        reply_markup=main_keyboard()
    )
    await call.answer()

# ══════════════════════════════════════════════
# ТАРИФЫ И ОПЛАТА
# ══════════════════════════════════════════════

def plans_keyboard(trial_available: bool):
    rows = []
    if trial_available:
        rows.append([InlineKeyboardButton(text="🎁 Пробный период · 7 дней БЕСПЛАТНО", callback_data="plan:trial")])
    rows.append([InlineKeyboardButton(text="📅 1 месяц · 35 ⭐",            callback_data="plan:month")])
    rows.append([InlineKeyboardButton(text="📦 3 месяца · 89 ⭐  (-15%)",   callback_data="plan:three")])
    rows.append([InlineKeyboardButton(text="👑 1 год · 299 ⭐  (-29%)",     callback_data="plan:year")])
    rows.append([InlineKeyboardButton(text="◀️ Назад",                      callback_data="user:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@user_router.callback_query(F.data == "user:plans")
async def cb_plans(call: CallbackQuery):
    uid = call.from_user.id
    trial_ok = not await has_used_trial(uid) and not is_admin(uid)
    subscribed = await is_subscribed(uid)

    sub_info = ""
    if subscribed:
        sub = await get_subscription(uid)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        sub_info = f"\n\n✅ <b>Текущая подписка:</b> до {exp.strftime('%d.%m.%Y')} ({days_left} дн.)"

    trial_note = ""
    if not trial_ok and not is_admin(uid):
        trial_note = "\n<i>* Пробный период уже использован</i>"

    await call.message.edit_text(
        f"💳 <b>Тарифы ShadowWatch</b>{sub_info}\n\n"
        f"{'🎁 <b>Пробный период</b> · 7 дней бесплатно' + chr(10) if trial_ok else ''}"
        f"📅 <b>1 месяц</b> · 35 ⭐\n"
        f"📦 <b>3 месяца</b> · 89 ⭐ <i>(скидка 15%)</i>\n"
        f"👑 <b>1 год</b> · 299 ⭐ <i>(скидка 29%)</i>\n"
        f"{trial_note}\n\n"
        f"<i>⭐ Оплата через Telegram Stars — безопасно и мгновенно</i>",
        reply_markup=plans_keyboard(trial_ok)
    )
    await call.answer()

@user_router.callback_query(F.data.startswith("plan:"))
async def cb_plan_select(call: CallbackQuery, bot: Bot):
    plan_key = call.data.split(":")[1]
    uid = call.from_user.id
    plan = PLANS.get(plan_key)
    if not plan:
        return await call.answer("Неизвестный тариф", show_alert=True)

    # Пробный период
    if plan_key == "trial":
        if await has_used_trial(uid):
            return await call.answer("❌ Пробный период уже использован!", show_alert=True)
        await mark_trial_used(uid)
        expires = await grant_subscription(uid, plan["days"], 0)
        exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
        await call.message.edit_text(
            f"🎁 <b>Пробный период активирован!</b>\n\n"
            f"⏳ Срок: <b>7 дней</b>\n"
            f"📅 Истекает: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Все функции ShadowWatch теперь доступны. Удачи! 👁",
            reply_markup=main_back_kb()
        )
        await call.answer("✅ Активировано!")
        # Уведомляем себя
        for admin_id in ADMIN_IDS:
            await notify_user(bot, admin_id,
                text=f"🎁 Новый пробный период\n👤 {user_link(uid, call.from_user.first_name, call.from_user.username)}",
                parse_mode="HTML")
        return

    # Платные тарифы — отправляем инвойс со Stars
    await call.message.delete()
    await bot.send_invoice(
        chat_id=uid,
        title=f"ShadowWatch · {plan['label']}",
        description=f"Доступ ко всем функциям ShadowWatch на {plan['desc']}",
        payload=f"sub_{plan_key}_{uid}",
        currency="XTR",  # Telegram Stars
        prices=[LabeledPrice(label=plan["label"], amount=plan["stars"])],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⭐ Оплатить {plan['stars']} Stars", pay=True)],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="user:plans")],
        ])
    )
    await call.answer()

# ── Обработка оплаты ──

@payment_router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)

@payment_router.message(F.successful_payment)
async def successful_payment(msg: Message, bot: Bot):
    payload = msg.successful_payment.invoice_payload  # sub_month_123456
    parts = payload.split("_")
    if len(parts) < 2:
        return
    plan_key = parts[1]
    uid = msg.from_user.id
    plan = PLANS.get(plan_key)
    if not plan:
        return

    expires = await grant_subscription(uid, plan["days"], 0)
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    stars = msg.successful_payment.total_amount

    await msg.answer(
        f"✅ <b>Оплата прошла успешно!</b>\n\n"
        f"💳 Тариф: <b>{plan['label']}</b>\n"
        f"⭐ Списано: <b>{stars} Stars</b>\n"
        f"📅 Подписка до: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
        f"Все функции ShadowWatch активированы! 👁",
        reply_markup=main_back_kb()
    )

    # Уведомляем себя о новой оплате
    for admin_id in ADMIN_IDS:
        await notify_user(bot, admin_id,
            text=f"💰 <b>Новая оплата!</b>\n\n"
                 f"👤 {user_link(uid, msg.from_user.first_name, msg.from_user.username)}\n"
                 f"💳 Тариф: {plan['label']}\n"
                 f"⭐ Stars: {stars}\n"
                 f"📅 До: {exp_dt.strftime('%d.%m.%Y %H:%M')}",
            parse_mode="HTML")

# ══════════════════════════════════════════════
# МОЯ ПОДПИСКА
# ══════════════════════════════════════════════

@user_router.callback_query(F.data == "user:sub")
async def cb_sub(call: CallbackQuery):
    uid = call.from_user.id
    if is_admin(uid):
        text = "👑 <b>Администратор</b>\nБезлимитный доступ ко всем функциям"
    else:
        sub = await get_subscription(uid)
        if not sub:
            text = (
                "❌ <b>Подписка не активна</b>\n\n"
                "Оформи подписку чтобы получить доступ ко всем функциям ShadowWatch."
            )
        else:
            exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if exp > now:
                days_left = (exp - now).days
                text = (
                    f"✅ <b>Подписка активна</b>\n\n"
                    f"📅 Истекает: <b>{exp.strftime('%d.%m.%Y %H:%M')}</b>\n"
                    f"⏳ Осталось: <b>{days_left} дн.</b>"
                )
            else:
                text = (
                    f"⏰ <b>Подписка истекла</b>\n\n"
                    f"Дата: {exp.strftime('%d.%m.%Y %H:%M')}\n"
                    f"Оформи новую подписку для продолжения."
                )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Продлить / Купить", callback_data="user:plans")],
        [InlineKeyboardButton(text="◀️ Главное меню",      callback_data="user:back")],
    ])
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()

# ══════════════════════════════════════════════
# НАСТРОЙКИ
# ══════════════════════════════════════════════

@user_router.callback_query(F.data == "user:settings")
async def cb_settings(call: CallbackQuery):
    s = await get_user_settings(call.from_user.id)
    def ico(v): return "🟢" if v else "🔴"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{ico(s['notify_delete'])} Удалённые сообщения",    callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(text=f"{ico(s['notify_edit'])} Редактирования",           callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(text=f"{ico(s['notify_self_destruct'])} Исчезающие медиа",callback_data="toggle:notify_self_destruct")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="user:back")],
    ])
    await call.message.edit_text(
        f"⚙️ <b>Настройки уведомлений</b>\n\n"
        f"{ico(s['notify_delete'])} Удалённые сообщения\n"
        f"{ico(s['notify_edit'])} Редактирования\n"
        f"{ico(s['notify_self_destruct'])} Исчезающие медиа\n\n"
        f"<i>Нажми на пункт чтобы включить/выключить</i>",
        reply_markup=kb)
    await call.answer()

@user_router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if not (await is_subscribed(call.from_user.id) or is_admin(call.from_user.id)):
        return await call.answer("❌ Нужна активная подписка!", show_alert=True)
    await toggle_user_setting(call.from_user.id, call.data.split(":", 1)[1])
    await cb_settings(call)

# ══════════════════════════════════════════════
# ПОМОЩЬ
# ══════════════════════════════════════════════

@user_router.callback_query(F.data == "user:help")
async def cb_help(call: CallbackQuery):
    await call.message.edit_text(
        "❓ <b>Как пользоваться ShadowWatch</b>\n\n"
        "<b>1️⃣ Добавить бота в чат:</b>\n"
        "Открой чат → Настройки → Автоматизация чатов → добавь бота → выдай права администратора\n\n"
        "<b>2️⃣ Что отслеживается:</b>\n"
        "🗑 Удалённые сообщения — получишь копию текста и медиа\n"
        "✏️ Редактирования — покажу что было и что стало\n"
        "💣 Исчезающие фото/видео — перехвачу до удаления\n\n"
        "<b>3️⃣ Команды:</b>\n"
        "/start — главное меню\n"
        "/sub — статус подписки\n"
        "/settings — настройки\n\n"
        "<b>⚠️ Важно:</b>\n"
        "Бот работает только в группах/чатах куда добавлен. В личных переписках 1 на 1 не работает.",
        reply_markup=main_back_kb())
    await call.answer()

@user_router.message(Command("sub"))
async def cmd_sub(msg: Message):
    uid = msg.from_user.id
    if is_admin(uid): text = "👑 Администратор — безлимитный доступ"
    else:
        sub = await get_subscription(uid)
        if not sub: text = "❌ Подписка не активна."
        else:
            exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
            text = (f"✅ Активна до {exp.strftime('%d.%m.%Y %H:%M')}" if exp > datetime.now()
                    else "⏰ Подписка истекла.")
    await msg.answer(text, reply_markup=main_back_kb())

@user_router.message(Command("settings"))
async def cmd_settings_msg(msg: Message):
    s = await get_user_settings(msg.from_user.id)
    def ico(v): return "🟢" if v else "🔴"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{ico(s['notify_delete'])} Удалённые",    callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(text=f"{ico(s['notify_edit'])} Редактирования", callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(text=f"{ico(s['notify_self_destruct'])} ViewOnce", callback_data="toggle:notify_self_destruct")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="user:back")],
    ])
    await msg.answer(
        f"⚙️ <b>Настройки уведомлений</b>\n\n"
        f"{ico(s['notify_delete'])} Удалённые сообщения\n"
        f"{ico(s['notify_edit'])} Редактирования\n"
        f"{ico(s['notify_self_destruct'])} Исчезающие медиа",
        reply_markup=kb)

# ══════════════════════════════════════════════
# ОБРАБОТЧИКИ ПОСТОЯННОЙ КЛАВИАТУРЫ
# ══════════════════════════════════════════════

@user_router.message(F.text == "💳 Тарифы")
async def kb_plans(msg: Message):
    uid = msg.from_user.id
    trial_ok = not await has_used_trial(uid) and not is_admin(uid)
    subscribed = await is_subscribed(uid)
    sub_info = ""
    if subscribed:
        sub = await get_subscription(uid)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        sub_info = f"\n\n✅ <b>Текущая подписка:</b> до {exp.strftime('%d.%m.%Y')} ({days_left} дн.)"
    trial_note = "\n<i>Пробный период уже использован</i>" if not trial_ok and not is_admin(uid) else ""
    await msg.answer(
        f"💳 <b>Тарифы ShadowWatch</b>{sub_info}\n\n"
        f"{'🎁 <b>Пробный период</b> · 7 дней бесплатно' + chr(10) if trial_ok else ''}"
        f"📅 <b>1 месяц</b> · 35 ⭐\n"
        f"📦 <b>3 месяца</b> · 89 ⭐ <i>(скидка 15%)</i>\n"
        f"👑 <b>1 год</b> · 299 ⭐ <i>(скидка 29%)</i>\n"
        f"{trial_note}\n\n"
        f"<i>⭐ Оплата через Telegram Stars — безопасно и мгновенно</i>",
        reply_markup=plans_keyboard(trial_ok)
    )

@user_router.message(F.text == "📋 Моя подписка")
async def kb_sub(msg: Message):
    uid = msg.from_user.id
    if is_admin(uid):
        text = "👑 <b>Администратор</b>\nБезлимитный доступ ко всем функциям"
    else:
        sub = await get_subscription(uid)
        if not sub:
            text = "❌ <b>Подписка не активна</b>\n\nОформи подписку чтобы получить доступ ко всем функциям."
        else:
            exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if exp > now:
                days_left = (exp - now).days
                text = f"✅ <b>Подписка активна</b>\n\n📅 Истекает: <b>{exp.strftime('%d.%m.%Y %H:%M')}</b>\n⏳ Осталось: <b>{days_left} дн.</b>"
            else:
                text = f"⏰ <b>Подписка истекла</b>\n\nДата: {exp.strftime('%d.%m.%Y %H:%M')}\nОформи новую подписку."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Продлить / Купить", callback_data="user:plans")],
    ])
    await msg.answer(text, reply_markup=kb)

@user_router.message(F.text == "⚙️ Настройки")
async def kb_settings(msg: Message):
    s = await get_user_settings(msg.from_user.id)
    def ico(v): return "🟢" if v else "🔴"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{ico(s['notify_delete'])} Удалённые сообщения",     callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(text=f"{ico(s['notify_edit'])} Редактирования",            callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(text=f"{ico(s['notify_self_destruct'])} Исчезающие медиа", callback_data="toggle:notify_self_destruct")],
    ])
    await msg.answer(
        f"⚙️ <b>Настройки уведомлений</b>\n\n"
        f"{ico(s['notify_delete'])} Удалённые сообщения\n"
        f"{ico(s['notify_edit'])} Редактирования\n"
        f"{ico(s['notify_self_destruct'])} Исчезающие медиа\n\n"
        f"<i>Нажми на пункт чтобы включить/выключить</i>",
        reply_markup=kb)

@user_router.message(F.text == "❓ Помощь")
async def kb_help(msg: Message):
    await msg.answer(
        "❓ <b>Как пользоваться ShadowWatch</b>\n\n"
        "1️⃣ <b>Подключи бота:</b>\n"
        "Настройки профиля → Автоматизация чатов → добавь @ShadowSMSq_BOT\n\n"
        "2️⃣ <b>Что отслеживается:</b>\n"
        "🗑 Удалённые сообщения — получишь копию текста и медиа\n"
        "✏️ Редактирования — покажу что было и что стало\n"
        "💣 Исчезающие фото/видео — перехвачу до удаления\n\n"
        "3️⃣ <b>Команды:</b>\n"
        "/start — главное меню\n"
        "/settings — настройки уведомлений\n\n"
        "💬 <b>Поддержка:</b> напиши администратору",
        reply_markup=main_back_kb())

# ══════════════════════════════════════════════
# ПОДКЛЮЧЕНИЕ ЧЕРЕЗ BUSINESS (Secretary Mode)
# ══════════════════════════════════════════════

@user_router.business_connection()
async def on_business_connection(bc: BusinessConnection, bot: Bot):
    uid = bc.user.id
    await upsert_user(uid, bc.user.username, bc.user.first_name)

    if not bc.is_enabled:
        # Бот отключён от аккаунта
        try:
            await bot.send_message(uid,
                "😔 <b>ShadowWatch отключён</b>\n\n"
                "Ты отключил бота от своего аккаунта.\n"
                "Чтобы снова включить — добавь бота в Автоматизацию чатов.\n\n"
                "🤖 @ShadowSMSq_BOT",
                reply_markup=reply_keyboard()
            )
        except: pass
        return

    # Активируем пробный период если не использован
    trial_activated = False
    if not await has_used_trial(uid) and not is_admin(uid) and not await is_subscribed(uid):
        await mark_trial_used(uid)
        await grant_subscription(uid, 7, 0)
        trial_activated = True

    sub = await get_subscription(uid)
    exp_str = ""
    if sub:
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        exp_str = exp.strftime("%d.%m.%Y")

    if trial_activated:
        text = (
            f"👁 <b>ShadowWatch подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}! 🎉\n\n"
            f"🎁 <b>Пробный период активирован — 7 дней бесплатно!</b>\n"
            f"📅 Действует до: <b>{exp_str}</b>\n\n"
            f"Теперь я слежу за твоими чатами:\n"
            f"🗑 Удалённые сообщения — сразу пришлю копию\n"
            f"✏️ Редактирования — покажу что изменили\n"
            f"💣 Исчезающие медиа — перехвачу до удаления\n\n"
            f"<i>Используй меню ниже для управления 👇</i>\n\n"
            f"🤖 @ShadowSMSq_BOT"
        )
    elif await is_subscribed(uid) or is_admin(uid):
        text = (
            f"👁 <b>ShadowWatch подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}! ✅\n\n"
            f"Бот успешно подключён к твоему аккаунту.\n"
            f"Все уведомления активны и работают.\n\n"
            f"🗑 Удалённые сообщения\n"
            f"✏️ Редактирования\n"
            f"💣 Исчезающие медиа\n\n"
            f"<i>Используй меню ниже для управления 👇</i>\n\n"
            f"🤖 @ShadowSMSq_BOT"
        )
    else:
        text = (
            f"👁 <b>ShadowWatch подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}! 👋\n\n"
            f"Бот подключён, но для работы нужна подписка.\n\n"
            f"💳 Оформи подписку чтобы начать получать уведомления:\n"
            f"📅 1 месяц · 35 ⭐\n"
            f"📦 3 месяца · 89 ⭐\n"
            f"👑 1 год · 299 ⭐\n\n"
            f"🤖 @ShadowSMSq_BOT"
        )

    try:
        await bot.send_message(uid, text, reply_markup=reply_keyboard())
        await bot.send_message(uid, "Меню:", reply_markup=main_keyboard())
    except Exception as e:
        logger.warning(f"business_connection notify {uid}: {e}")

    # Уведомляем админа
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"🔗 <b>Новое Business подключение!</b>\n\n"
                f"👤 {user_link(uid, bc.user.first_name, bc.user.username)}\n"
                f"{'🎁 Активирован пробный период' if trial_activated else '✅ Уже подписан'}",
                parse_mode="HTML")
        except: pass

def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Все пользователи",  callback_data="adm:users")],
        [InlineKeyboardButton(text="⭐ Активные подписки", callback_data="adm:subs")],
        [InlineKeyboardButton(text="✅ Выдать подписку",   callback_data="adm:grant")],
        [InlineKeyboardButton(text="❌ Отозвать подписку", callback_data="adm:revoke")],
        [InlineKeyboardButton(text="📊 Статистика",        callback_data="adm:stats")],
    ])

@admin_router.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return await msg.answer("⛔ Нет доступа.")
    await state.clear()
    await msg.answer("👁 <b>ShadowWatch — Панель администратора</b>\n\nВыбери действие:",
                     reply_markup=admin_keyboard())

@admin_router.callback_query(F.data == "adm:back")
async def adm_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("👁 <b>ShadowWatch — Панель администратора</b>\n\nВыбери действие:",
                                  reply_markup=admin_keyboard())
    await call.answer()

@admin_router.callback_query(F.data == "adm:users")
async def adm_users(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users = await get_all_users()
    if not users: return await call.message.edit_text("Пользователей нет.", reply_markup=back_kb_adm())
    lines = ["👥 <b>Пользователи</b> (последние 50):\n"]
    for u in users[:50]:
        uname = f"@{u['username']}" if u['username'] else "—"
        lines.append(f"• <code>{u['user_id']}</code> | {u['first_name'] or '—'} | {uname}")
    await call.message.edit_text("\n".join(lines), reply_markup=back_kb_adm())
    await call.answer()

@admin_router.callback_query(F.data == "adm:subs")
async def adm_subs(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    subs = await get_all_subscriptions()
    now = datetime.now()
    active = [s for s in subs if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]
    if not active: return await call.message.edit_text("Активных подписок нет.", reply_markup=back_kb_adm())
    lines = ["⭐ <b>Активные подписки</b>:\n"]
    for s in active:
        exp = datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - now).days
        uname = f"@{s['username']}" if s.get("username") else "—"
        lines.append(f"• <code>{s['user_id']}</code> | {s.get('first_name') or '—'} | {uname}\n"
                     f"  До: <b>{exp.strftime('%d.%m.%Y %H:%M')}</b> ({days_left} дн.)")
    await call.message.edit_text("\n".join(lines), reply_markup=back_kb_adm())
    await call.answer()

@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users = await get_all_users()
    subs  = await get_all_subscriptions()
    now   = datetime.now()
    active = [s for s in subs if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]
    await call.message.edit_text(
        f"📊 <b>Статистика ShadowWatch</b>\n\n"
        f"👥 Всего пользователей: <b>{len(users)}</b>\n"
        f"⭐ Активных подписок: <b>{len(active)}</b>\n"
        f"📋 Всего выдано: <b>{len(subs)}</b>",
        reply_markup=back_kb_adm())
    await call.answer()

@admin_router.callback_query(F.data == "adm:grant")
async def adm_grant_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_user_id)
    await call.message.edit_text("✅ <b>Выдача подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:\n<i>Отмена: /admin</i>")
    await call.answer()

@admin_router.message(AdminStates.waiting_user_id)
async def adm_grant_get_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❗ Введи числовой ID.")
    await state.update_data(target_user_id=uid)
    await state.set_state(AdminStates.waiting_days)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="7 дней",   callback_data="days:7"),
         InlineKeyboardButton(text="1 месяц",  callback_data="days:30"),
         InlineKeyboardButton(text="3 месяца", callback_data="days:90")],
        [InlineKeyboardButton(text="1 год",    callback_data="days:365"),
         InlineKeyboardButton(text="♾ Навсегда", callback_data="days:9999")],
    ])
    await msg.answer(f"👤 ID: <code>{uid}</code>\n\nВыбери срок:", reply_markup=kb)

@admin_router.callback_query(F.data.startswith("days:"))
async def adm_grant_days_btn(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    days = int(call.data.split(":")[1])
    data = await state.get_data()
    uid  = data.get("target_user_id")
    if not uid: await state.clear(); return await call.answer("Сессия истекла.", show_alert=True)
    expires = await grant_subscription(uid, days, call.from_user.id)
    await state.clear()
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await call.message.edit_text(
        f"✅ <b>Подписка выдана!</b>\n\n👤 ID: <code>{uid}</code>\n"
        f"⏳ Срок: <b>{days} дн.</b>\n📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>",
        reply_markup=back_kb_adm())
    await call.answer("✅ Готово!")
    try:
        await call.bot.send_message(uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Используй /start 👁")
    except: pass

@admin_router.message(AdminStates.waiting_days)
async def adm_grant_days_text(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        days = int(msg.text.strip())
        if days <= 0: raise ValueError
    except ValueError: return await msg.answer("❗ Введи положительное число.")
    data = await state.get_data()
    uid  = data.get("target_user_id")
    expires = await grant_subscription(uid, days, msg.from_user.id)
    await state.clear()
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await msg.answer(f"✅ Подписка выдана <code>{uid}</code> на {days} дн. до {exp_dt.strftime('%d.%m.%Y %H:%M')}",
                     reply_markup=back_kb_adm())
    try:
        await msg.bot.send_message(uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\nИспользуй /start 👁")
    except: pass

@admin_router.callback_query(F.data == "adm:revoke")
async def adm_revoke_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_revoke_id)
    await call.message.edit_text("❌ <b>Отзыв подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:")
    await call.answer()

@admin_router.message(AdminStates.waiting_revoke_id)
async def adm_revoke_do(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❗ Введи числовой ID.")
    sub = await get_subscription(uid)
    if not sub:
        await state.clear(); return await msg.answer(f"У <code>{uid}</code> нет подписки.")
    await revoke_subscription(uid)
    await state.clear()
    await msg.answer(f"❌ Подписка <code>{uid}</code> отозвана.", reply_markup=back_kb_adm())
    try: await msg.bot.send_message(uid, "⚠️ Твоя подписка <b>ShadowWatch</b> отозвана администратором.")
    except: pass

@admin_router.message(Command("grant"))
async def cmd_grant(msg: Message):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    if len(parts) != 3: return await msg.answer("Использование: /grant <user_id> <days>")
    try: uid, days = int(parts[1]), int(parts[2])
    except ValueError: return await msg.answer("Пример: /grant 123456789 30")
    expires = await grant_subscription(uid, days, msg.from_user.id)
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await msg.answer(f"✅ Подписка выдана <code>{uid}</code> до {exp_dt.strftime('%d.%m.%Y %H:%M')}")
    try:
        await msg.bot.send_message(uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n⏳ {days} дн. | До: {exp_dt.strftime('%d.%m.%Y %H:%M')}\n\nИспользуй /start 👁")
    except: pass

@admin_router.message(Command("revoke"))
async def cmd_revoke(msg: Message):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    if len(parts) != 2: return await msg.answer("Использование: /revoke <user_id>")
    try: uid = int(parts[1])
    except ValueError: return await msg.answer("❗ Неверный user_id.")
    await revoke_subscription(uid)
    await msg.answer(f"❌ Подписка <code>{uid}</code> отозвана.")

# ══════════════════════════════════════════════
# СОБЫТИЯ (удаления / редактирования / медиа)
# ══════════════════════════════════════════════

class DeletedMessageMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Update, data: Dict[str, Any]) -> Any:
        if hasattr(event, "deleted_messages") and event.deleted_messages:
            await self._process(event.deleted_messages, data["bot"])
        return await handler(event, data)

    async def _process(self, upd, bot: Bot):
        for item in getattr(upd, "messages", []) or []:
            chat_id = getattr(getattr(item, "chat", None), "id", None)
            msg_id  = getattr(item, "message_id", None)
            if not chat_id or not msg_id: continue
            cached = await get_cached_message(chat_id, msg_id)
            if not cached: continue
            await _notify_deleted(bot, cached)
            await delete_cached_message(chat_id, msg_id)

async def _notify_deleted(bot: Bot, cached: dict):
    sender_id  = cached.get("user_id")
    first_name = cached.get("first_name") or "Пользователь"
    sender = user_link(sender_id, first_name, cached.get("username")) if sender_id else "Неизвестный"
    text_preview = (cached.get("text") or "")[:300]
    media_type = cached.get("media_type")
    file_id    = cached.get("file_id")
    emoji = MEDIA_EMOJI.get(media_type, "") if media_type else ""
    now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
    chat_label = ""
    notify_text = (
        f"🗑 <b>Удалённое сообщение</b>\n"
        f""
        f"📅 <b>{now_str}</b>\n"
        f"👤 <b>Автор:</b> {sender}\n"
    )
    if text_preview:
        notify_text += f"\n💬 <b>Текст сообщения:</b>\n<blockquote>{text_preview}</blockquote>\n"
    if media_type:
        notify_text += f"\n{emoji} <b>Медиа:</b> {media_type}\n"
    notify_text += f"\n🤖 @ShadowSMSq_BOT"
    for admin_id in ADMIN_IDS:
        if await should_notify(admin_id, "notify_delete"):
            await notify_user(bot, admin_id, text=notify_text, parse_mode="HTML")
            if file_id and media_type:
                await send_media(bot, admin_id, media_type, file_id, "📎 Медиа удалённого сообщения")

@event_router.edited_message()
async def handle_edited(msg: Message, bot: Bot):
    if not msg.from_user: return
    if msg.from_user.is_bot: return
    u = msg.from_user
    cached   = await get_cached_message(msg.chat.id, msg.message_id)
    old_text = cached.get("text") if cached else None
    new_text = msg.text or msg.caption
    if old_text != new_text:
        def trim(t): return "<i>пусто</i>" if not t else (t[:300] + "…") if len(t) > 300 else t
        notify_text = (
            f"✏️ <b>Изменённое сообщение</b>\n"
            f""
            f"📅 <b>{datetime.now().strftime('%d.%m.%Y в %H:%M:%S')}</b>\n"
            f"👤 <b>Автор:</b> {user_link(u.id, u.first_name, u.username)}\n"
            f"💬 <b>Чат:</b> {msg.chat.title or 'личный чат'}\n\n"
            f"📝 <b>Было:</b>\n<blockquote>{trim(old_text)}</blockquote>\n\n"
            f"📝 <b>Стало:</b>\n<blockquote>{trim(new_text)}</blockquote>\n"
            f"\n🤖 @ShadowSMSq_BOT"
        )
        for admin_id in ADMIN_IDS:
            if await should_notify(admin_id, "notify_edit"):
                await notify_user(bot, admin_id, text=notify_text, parse_mode="HTML")
    media_type, file_id = extract_media(msg)
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                         new_text, media_type, file_id)

@event_router.message()
async def cache_incoming(msg: Message, bot: Bot):
    if not msg.from_user: return
    # Игнорируем сообщения самого бота
    if msg.from_user.is_bot: return
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    media_type, file_id = extract_media(msg)
    is_self_destruct = False
    if media_type in ("photo", "video"):
        raw = msg.model_dump()
        media_raw = raw.get("photo", {}) if media_type == "photo" else raw.get("video", {})
        if isinstance(media_raw, list): media_raw = media_raw[-1] if media_raw else {}
        if media_raw.get("self_destruct_type") or raw.get("has_protected_content"):
            is_self_destruct = True
    text_content = msg.text or msg.caption
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                         text_content, media_type, file_id)
    if is_self_destruct and media_type and file_id:
        caption = (
            f"💣 <b>Исчезающее медиа перехвачено!</b>\n"
            f""
            f"📅 <b>{datetime.now().strftime('%d.%m.%Y в %H:%M:%S')}</b>\n"
            f"👤 <b>От:</b> {user_link(u.id, u.first_name, u.username)}\n"
            f"💬 <b>Чат:</b> {msg.chat.title or 'личный чат'}\n"
            f"{MEDIA_EMOJI.get(media_type, '📎')} <b>Тип:</b> {media_type}\n"
            f"\n🤖 @ShadowSMSq_BOT"
        )
        for admin_id in ADMIN_IDS:
            if await should_notify(admin_id, "notify_self_destruct"):
                await notify_user(bot, admin_id, text=caption, parse_mode="HTML")
                await send_media(bot, admin_id, media_type, file_id)

# ══════════════════════════════════════════════
# BUSINESS API — сообщения через бизнес-аккаунт
# ══════════════════════════════════════════════

@event_router.business_message()
async def cache_business_message(msg: Message, bot: Bot):
    """Кэшируем сообщения из бизнес-подключений"""
    if not msg.from_user: return
    if msg.from_user.is_bot: return
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    media_type, file_id = extract_media(msg)
    text_content = msg.text or msg.caption
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                         text_content, media_type, file_id)

@event_router.edited_business_message()
async def handle_edited_business(msg: Message, bot: Bot):
    """Редактирования в бизнес-чатах"""
    if not msg.from_user: return
    if msg.from_user.is_bot: return
    u = msg.from_user
    cached   = await get_cached_message(msg.chat.id, msg.message_id)
    old_text = cached.get("text") if cached else None
    new_text = msg.text or msg.caption
    if old_text != new_text:
        def trim(t): return "<i>пусто</i>" if not t else (t[:300] + "…") if len(t) > 300 else t
        notify_text = (
            f"✏️ <b>Изменённое сообщение</b>\n"
            f""
            f"📅 <b>{datetime.now().strftime('%d.%m.%Y в %H:%M:%S')}</b>\n"
            f"👤 <b>Автор:</b> {user_link(u.id, u.first_name, u.username)}\n"
            f"💬 <b>Чат:</b> {msg.chat.title or 'личный чат'}\n\n"
            f"📝 <b>Было:</b>\n<blockquote>{trim(old_text)}</blockquote>\n\n"
            f"📝 <b>Стало:</b>\n<blockquote>{trim(new_text)}</blockquote>\n"
            f"\n🤖 @ShadowSMSq_BOT"
        )
        for admin_id in ADMIN_IDS:
            if await should_notify(admin_id, "notify_edit"):
                await notify_user(bot, admin_id, text=notify_text, parse_mode="HTML")
    media_type, file_id = extract_media(msg)
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                         new_text, media_type, file_id)

@event_router.deleted_business_messages()
async def handle_deleted_business(event, bot: Bot):
    """Удалённые сообщения из бизнес-чатов"""
    chat_id = getattr(getattr(event, "chat", None), "id", None)
    if not chat_id: return
    for msg_id in getattr(event, "message_ids", []):
        cached = await get_cached_message(chat_id, msg_id)
        if not cached: continue
        await _notify_deleted(bot, cached)
        await delete_cached_message(chat_id, msg_id)

# ══════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════

async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher()
    dp.update.middleware(DeletedMessageMiddleware())
    dp.include_router(admin_router)
    dp.include_router(payment_router)
    dp.include_router(user_router)
    dp.include_router(event_router)
    await init_db()
    logger.info("✅ БД инициализирована")
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("🚀 ShadowWatch запущен")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
