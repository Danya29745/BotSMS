#!/usr/bin/env python3
"""
👁 ShadowWatch Bot — единый файл для деплоя на BotHost
"""

import asyncio
import logging
import os
import sqlite3
import io
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
    InlineKeyboardButton, ContentType, Update
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# КОНФИГ
# ══════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан! Укажи его в переменных окружения.")

_admin_ids_raw = os.getenv("ADMIN_IDS", "7965055989")
ADMIN_IDS = [int(x.strip()) for x in _admin_ids_raw.split(",") if x.strip()]

DB_PATH = os.getenv("DB_PATH", "shadowwatch.db")
MESSAGE_CACHE_TTL = int(os.getenv("MESSAGE_CACHE_TTL", "86400"))

BOT_NAME = "ShadowWatch"
BOT_NICK = "@shadowwatchbot"

# ══════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════

def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db_sync():
    conn = _get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            registered  TEXT DEFAULT (datetime('now')),
            last_seen   TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id     INTEGER PRIMARY KEY,
            expires_at  TEXT NOT NULL,
            granted_by  INTEGER,
            granted_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS message_cache (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            message_id  INTEGER NOT NULL,
            user_id     INTEGER,
            username    TEXT,
            first_name  TEXT,
            text        TEXT,
            media_type  TEXT,
            file_id     TEXT,
            created_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(chat_id, message_id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id              INTEGER PRIMARY KEY,
            notify_delete        INTEGER DEFAULT 1,
            notify_edit          INTEGER DEFAULT 1,
            notify_self_destruct INTEGER DEFAULT 1
        )
    """)
    conn.commit()
    conn.close()


async def init_db():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init_db_sync)


def _run(fn):
    return asyncio.get_event_loop().run_in_executor(None, fn)


async def upsert_user(user_id, username=None, first_name=None):
    def _f():
        conn = _get_conn()
        conn.execute("""
            INSERT INTO users (user_id, username, first_name, last_seen)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_seen=datetime('now')
        """, (user_id, username, first_name))
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
        conn.execute("""
            INSERT INTO subscriptions (user_id, expires_at, granted_by)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                expires_at=?, granted_by=?, granted_at=datetime('now')
        """, (user_id, expires, granted_by, expires, granted_by))
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
        rows = conn.execute("""
            SELECT s.*, u.username, u.first_name FROM subscriptions s
            LEFT JOIN users u ON u.user_id=s.user_id ORDER BY s.expires_at DESC
        """).fetchall()
        conn.close(); return [dict(r) for r in rows]
    return await _run(_f)


async def cache_message(chat_id, message_id, user_id, username, first_name,
                         text=None, media_type=None, file_id=None):
    def _f():
        conn = _get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO message_cache
            (chat_id, message_id, user_id, username, first_name, text, media_type, file_id)
            VALUES (?,?,?,?,?,?,?,?)
        """, (chat_id, message_id, user_id, username, first_name, text, media_type, file_id))
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
        conn.execute("DELETE FROM message_cache WHERE chat_id=? AND message_id=?",
                     (chat_id, message_id))
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
    allowed = {"notify_delete", "notify_edit", "notify_self_destruct"}
    if field not in allowed: return
    def _f():
        conn = _get_conn()
        conn.execute(f"""
            INSERT INTO user_settings (user_id, {field}) VALUES (?, 1)
            ON CONFLICT(user_id) DO UPDATE SET {field} = 1 - {field}
        """, (user_id,))
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

async def notify_user(bot: Bot, user_id, **kwargs):
    try: await bot.send_message(user_id, **kwargs)
    except Exception as e: logger.warning(f"Не удалось отправить {user_id}: {e}")

async def send_media(bot: Bot, user_id, media_type, file_id, caption=None):
    method = getattr(bot, MEDIA_SEND.get(media_type, ""), None)
    if not method: return
    kwargs = {"chat_id": user_id}
    if media_type in ("video_note", "sticker"): kwargs[media_type] = file_id
    else:
        kwargs[media_type] = file_id
        if caption: kwargs["caption"] = caption
    try: await method(**kwargs)
    except Exception as e: logger.warning(f"Не удалось переслать медиа {user_id}: {e}")

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

# ══════════════════════════════════════════════
# РОУТЕРЫ
# ══════════════════════════════════════════════

admin_router = Router()
user_router  = Router()
event_router = Router()

# ══════════════════════════════════════════════
# FSM СОСТОЯНИЯ
# ══════════════════════════════════════════════

class AdminStates(StatesGroup):
    waiting_user_id  = State()
    waiting_days     = State()
    waiting_revoke_id = State()

# ══════════════════════════════════════════════
# АДМИН ПАНЕЛЬ
# ══════════════════════════════════════════════

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
    if not is_admin(msg.from_user.id):
        return await msg.answer("⛔ Нет доступа.")
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
    if not users:
        return await call.message.edit_text("Пользователей пока нет.", reply_markup=back_kb_adm())
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
    if not active:
        return await call.message.edit_text("Активных подписок нет.", reply_markup=back_kb_adm())
    lines = ["⭐ <b>Активные подписки</b>:\n"]
    for s in active:
        exp = datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - now).days
        uname = f"@{s['username']}" if s.get("username") else "—"
        lines.append(f"• <code>{s['user_id']}</code> | {s.get('first_name') or '—'} | {uname}\n"
                     f"  До: <b>{exp.strftime('%d.%m.%Y %H:%M')}</b> (осталось {days_left} дн.)")
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
        reply_markup=back_kb_adm()
    )
    await call.answer()

# ── Выдача подписки ──
@admin_router.callback_query(F.data == "adm:grant")
async def adm_grant_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_user_id)
    await call.message.edit_text("✅ <b>Выдача подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:\n<i>(Отмена: /admin)</i>")
    await call.answer()

@admin_router.message(AdminStates.waiting_user_id)
async def adm_grant_get_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        return await msg.answer("❗ Введи числовой ID.")
    await state.update_data(target_user_id=uid)
    await state.set_state(AdminStates.waiting_days)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 день",  callback_data="days:1"),
         InlineKeyboardButton(text="3 дня",   callback_data="days:3"),
         InlineKeyboardButton(text="7 дней",  callback_data="days:7")],
        [InlineKeyboardButton(text="14 дней", callback_data="days:14"),
         InlineKeyboardButton(text="30 дней", callback_data="days:30"),
         InlineKeyboardButton(text="90 дней", callback_data="days:90")],
        [InlineKeyboardButton(text="365 дней",       callback_data="days:365"),
         InlineKeyboardButton(text="♾ Навсегда",     callback_data="days:9999")],
    ])
    await msg.answer(f"👤 ID: <code>{uid}</code>\n\nВыбери срок:", reply_markup=kb)

@admin_router.callback_query(F.data.startswith("days:"))
async def adm_grant_days_btn(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    days = int(call.data.split(":")[1])
    data = await state.get_data()
    uid  = data.get("target_user_id")
    if not uid:
        await state.clear(); return await call.answer("Сессия истекла.", show_alert=True)
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
            f"Используй /start.")
    except: pass

@admin_router.message(AdminStates.waiting_days)
async def adm_grant_days_text(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        days = int(msg.text.strip())
        if days <= 0: raise ValueError
    except ValueError:
        return await msg.answer("❗ Введи положительное число дней.")
    data = await state.get_data()
    uid  = data.get("target_user_id")
    expires = await grant_subscription(uid, days, msg.from_user.id)
    await state.clear()
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await msg.answer(
        f"✅ <b>Подписка выдана!</b>\n\n👤 ID: <code>{uid}</code>\n"
        f"⏳ Срок: <b>{days} дн.</b>\n📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>",
        reply_markup=back_kb_adm())
    try:
        await msg.bot.send_message(uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Используй /start.")
    except: pass

# ── Отзыв подписки ──
@admin_router.callback_query(F.data == "adm:revoke")
async def adm_revoke_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_revoke_id)
    await call.message.edit_text("❌ <b>Отзыв подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:")
    await call.answer()

@admin_router.message(AdminStates.waiting_revoke_id)
async def adm_revoke_do(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        return await msg.answer("❗ Введи числовой ID.")
    sub = await get_subscription(uid)
    if not sub:
        await state.clear()
        return await msg.answer(f"У <code>{uid}</code> нет подписки.")
    await revoke_subscription(uid)
    await state.clear()
    await msg.answer(f"❌ Подписка <code>{uid}</code> отозвана.", reply_markup=back_kb_adm())
    try: await msg.bot.send_message(uid, "⚠️ Твоя подписка <b>ShadowWatch</b> отозвана администратором.")
    except: pass

# ── Быстрые команды ──
@admin_router.message(Command("grant"))
async def cmd_grant(msg: Message):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    if len(parts) != 3: return await msg.answer("Использование: /grant <user_id> <days>")
    try: uid, days = int(parts[1]), int(parts[2])
    except ValueError: return await msg.answer("❗ Пример: /grant 123456789 30")
    expires = await grant_subscription(uid, days, msg.from_user.id)
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await msg.answer(f"✅ Подписка выдана <code>{uid}</code>\nДо: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>")
    try:
        await msg.bot.send_message(uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n"
            f"⏳ {days} дн. | До: {exp_dt.strftime('%d.%m.%Y %H:%M')}\n\nИспользуй /start.")
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
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ══════════════════════════════════════════════

@user_router.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    subscribed = await is_subscribed(u.id)
    if is_admin(u.id):
        status = "👑 Администратор"
    elif subscribed:
        sub = await get_subscription(u.id)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        status = f"✅ Подписка активна (осталось {(exp - datetime.now()).days} дн.)"
    else:
        status = "❌ Нет подписки"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки уведомлений", callback_data="user:settings")],
        [InlineKeyboardButton(text="📋 Моя подписка",           callback_data="user:sub")],
        [InlineKeyboardButton(text="❓ Как пользоваться",       callback_data="user:help")],
    ])
    await msg.answer(
        f"👁 <b>ShadowWatch</b>\n\nПривет, {u.first_name}!\n\nСтатус: {status}\n\n"
        f"Я слежу за сообщениями в чатах и уведомляю тебя о:\n"
        f"• 🗑 Удалённых сообщениях\n• ✏️ Отредактированных сообщениях\n• 💣 Самоуничтожающихся медиа\n\n"
        f"<i>Добавь меня в чат через «Автоматизация» → я начну мониторинг</i>",
        reply_markup=kb)

@user_router.callback_query(F.data == "user:back")
async def cb_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    u = call.from_user
    subscribed = await is_subscribed(u.id) or is_admin(u.id)
    status = "👑 Администратор" if is_admin(u.id) else ("✅ Подписка активна" if subscribed else "❌ Нет подписки")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки уведомлений", callback_data="user:settings")],
        [InlineKeyboardButton(text="📋 Моя подписка",           callback_data="user:sub")],
        [InlineKeyboardButton(text="❓ Как пользоваться",       callback_data="user:help")],
    ])
    await call.message.edit_text(f"👁 <b>ShadowWatch</b>\n\nСтатус: {status}", reply_markup=kb)
    await call.answer()

@user_router.callback_query(F.data == "user:help")
async def cb_help(call: CallbackQuery):
    await call.message.edit_text(
        "❓ <b>Как пользоваться ShadowWatch</b>\n\n"
        "<b>1. Добавить бота в чат:</b>\nНастройки чата → Автоматизация чатов → добавь бота\n\n"
        "<b>2. Что отслеживается:</b>\n"
        "• 🗑 Удалённые сообщения — бот присылает копию\n"
        "• ✏️ Редактирования — показывает «до» и «после»\n"
        "• 💣 ViewOnce медиа — перехватывает до удаления\n\n"
        "<b>3. Команды:</b>\n/start /sub /settings",
        reply_markup=main_back_kb())
    await call.answer()

@user_router.callback_query(F.data == "user:sub")
async def cb_sub(call: CallbackQuery):
    uid = call.from_user.id
    if is_admin(uid):
        text = "👑 <b>Администратор</b> — безлимитный доступ"
    else:
        sub = await get_subscription(uid)
        if not sub:
            text = "❌ <b>Подписка не активна</b>\n\nОбратись к администратору."
        else:
            exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if exp > now:
                text = (f"✅ <b>Подписка активна</b>\n\n📅 До: <b>{exp.strftime('%d.%m.%Y %H:%M')}</b>\n"
                        f"⏳ Осталось: <b>{(exp-now).days} дн.</b>")
            else:
                text = f"⏰ <b>Подписка истекла</b>\n\nДата: {exp.strftime('%d.%m.%Y %H:%M')}"
    await call.message.edit_text(text, reply_markup=main_back_kb())
    await call.answer()

@user_router.callback_query(F.data == "user:settings")
async def cb_settings(call: CallbackQuery):
    s = await get_user_settings(call.from_user.id)
    def ico(v): return "🟢" if v else "🔴"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{ico(s['notify_delete'])} Удалённые",       callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(text=f"{ico(s['notify_edit'])} Редактирования",    callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(text=f"{ico(s['notify_self_destruct'])} ViewOnce", callback_data="toggle:notify_self_destruct")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="user:back")],
    ])
    await call.message.edit_text(
        f"⚙️ <b>Настройки уведомлений</b>\n\n"
        f"{ico(s['notify_delete'])} Удалённые сообщения\n"
        f"{ico(s['notify_edit'])} Редактирования\n"
        f"{ico(s['notify_self_destruct'])} Самоуничтожающиеся медиа",
        reply_markup=kb)
    await call.answer()

@user_router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if not (await is_subscribed(call.from_user.id) or is_admin(call.from_user.id)):
        return await call.answer("❌ Нужна активная подписка!", show_alert=True)
    await toggle_user_setting(call.from_user.id, call.data.split(":", 1)[1])
    await cb_settings(call)

@user_router.message(Command("sub"))
async def cmd_sub(msg: Message):
    uid = msg.from_user.id
    if is_admin(uid):
        text = "👑 Администратор — безлимитный доступ"
    else:
        sub = await get_subscription(uid)
        if not sub: text = "❌ Подписка не активна."
        else:
            exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
            text = (f"✅ Активна до {exp.strftime('%d.%m.%Y %H:%M')}" if exp > datetime.now()
                    else "⏰ Подписка истекла.")
    await msg.answer(text, reply_markup=main_back_kb())

@user_router.message(Command("settings"))
async def cmd_settings(msg: Message):
    s = await get_user_settings(msg.from_user.id)
    def ico(v): return "🟢" if v else "🔴"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{ico(s['notify_delete'])} Удалённые",       callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(text=f"{ico(s['notify_edit'])} Редактирования",    callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(text=f"{ico(s['notify_self_destruct'])} ViewOnce", callback_data="toggle:notify_self_destruct")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="user:back")],
    ])
    await msg.answer(
        f"⚙️ <b>Настройки уведомлений</b>\n\n"
        f"{ico(s['notify_delete'])} Удалённые сообщения\n"
        f"{ico(s['notify_edit'])} Редактирования\n"
        f"{ico(s['notify_self_destruct'])} Самоуничтожающиеся медиа",
        reply_markup=kb)

# ══════════════════════════════════════════════
# СОБЫТИЯ — УДАЛЕНИЯ / РЕДАКТИРОВАНИЯ / МЕДИА
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
    notify_text = (f"🗑 <b>Сообщение удалено!</b>\n\n👤 Автор: {sender}\n"
                   f"🕒 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    if text_preview: notify_text += f"\n\n📝 <b>Текст:</b>\n{text_preview}"
    if media_type:   notify_text += f"\n{emoji} Медиа: <b>{media_type}</b>"
    for admin_id in ADMIN_IDS:
        if await should_notify(admin_id, "notify_delete"):
            await notify_user(bot, admin_id, text=notify_text, parse_mode="HTML")
            if file_id and media_type:
                await send_media(bot, admin_id, media_type, file_id, "📎 Медиа удалённого сообщения")

@event_router.edited_message()
async def handle_edited(msg: Message, bot: Bot):
    if msg.chat.type not in ("group", "supergroup", "channel"): return
    if not msg.from_user: return
    u = msg.from_user
    cached   = await get_cached_message(msg.chat.id, msg.message_id)
    old_text = cached.get("text") if cached else None
    new_text = msg.text or msg.caption
    if old_text != new_text:
        def trim(t): return ("<i>пусто</i>" if not t else (t[:300] + "…") if len(t) > 300 else t)
        notify_text = (
            f"✏️ <b>Сообщение отредактировано!</b>\n\n"
            f"👤 Автор: {user_link(u.id, u.first_name, u.username)}\n"
            f"💬 Чат: <b>{msg.chat.title or msg.chat.id}</b>\n"
            f"🕒 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
            f"📝 <b>Было:</b>\n{trim(old_text)}\n\n📝 <b>Стало:</b>\n{trim(new_text)}"
        )
        for admin_id in ADMIN_IDS:
            if await should_notify(admin_id, "notify_edit"):
                await notify_user(bot, admin_id, text=notify_text, parse_mode="HTML")
    media_type, file_id = extract_media(msg)
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                         new_text, media_type, file_id)

@event_router.message()
async def cache_incoming(msg: Message, bot: Bot):
    if msg.chat.type not in ("group", "supergroup", "channel"): return
    if not msg.from_user: return
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    media_type, file_id = extract_media(msg)
    # Проверка self-destruct
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
        chat_name = msg.chat.title or str(msg.chat.id)
        caption = (f"💣 <b>Самоуничтожающееся медиа</b>\n\n"
                   f"👤 От: {user_link(u.id, u.first_name, u.username)}\n"
                   f"💬 Чат: <b>{chat_name}</b>\n"
                   f"🕒 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                   f"Тип: {MEDIA_EMOJI.get(media_type, '📎')} {media_type}")
        for admin_id in ADMIN_IDS:
            if await should_notify(admin_id, "notify_self_destruct"):
                await notify_user(bot, admin_id, text=caption, parse_mode="HTML")
                await send_media(bot, admin_id, media_type, file_id)



# ══════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════

async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher()
    dp.update.middleware(DeletedMessageMiddleware())
    dp.include_router(admin_router)
    dp.include_router(user_router)
    dp.include_router(event_router)
    await init_db()
    logger.info("✅ База данных инициализирована")
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("🚀 ShadowWatch запущен")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
