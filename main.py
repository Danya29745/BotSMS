#!/usr/bin/env python3
"""
👁 ShadowSMSq Bot — v3
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery,
    ReplyKeyboardMarkup, KeyboardButton, BusinessConnection,
    FSInputFile, InputMediaPhoto,
)
from aiogram.utils.media_group import MediaGroupBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# КОНФИГ
# ══════════════════════════════════════════════

BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
START_PHOTO_URL  = os.getenv("START_PHOTO_URL", "")   # URL или Telegram file_id картинки /start
_data_dir     = os.getenv("DATA_DIR", os.getenv("DB_PATH_DIR", "/app/data"))
DB_PATH       = os.getenv("DB_PATH", os.path.join(_data_dir, "shadowwatch.db"))
ADMIN_IDS     = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip()]
BOT_USERNAME  = "ShadowSMSq_bot"
BOT_NAME      = "ShadowSMSq"
# Папка для временного хранения медиафайлов (скачивание с таймером)
MEDIA_DIR     = Path(os.getenv("MEDIA_DIR", "/app/data/media"))

PLANS = {
    "month": {"label": "1 месяц",  "days": 30,  "stars": 35,  "desc": "1 месяц"},
    "three": {"label": "3 месяца", "days": 90,  "stars": 89,  "desc": "3 месяца"},
    "year":  {"label": "1 год",    "days": 365, "stars": 299, "desc": "1 год"},
}

# ══════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _init_db_sync():
    c = _conn()
    cur = c.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INTEGER PRIMARY KEY,
        username   TEXT,
        first_name TEXT,
        registered TEXT DEFAULT (datetime('now')),
        last_seen  TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS subscriptions (
        user_id    INTEGER PRIMARY KEY,
        expires_at TEXT NOT NULL,
        granted_by INTEGER,
        granted_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS trial_used (
        user_id INTEGER PRIMARY KEY,
        used_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS message_cache (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id     INTEGER NOT NULL,
        message_id  INTEGER NOT NULL,
        owner_id    INTEGER,
        user_id     INTEGER,
        username    TEXT,
        first_name  TEXT,
        text        TEXT,
        media_type  TEXT,
        file_id     TEXT,
        is_view_once INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(chat_id, message_id)
    );
    CREATE TABLE IF NOT EXISTS user_settings (
        user_id              INTEGER PRIMARY KEY,
        notify_delete        INTEGER DEFAULT 1,
        notify_edit          INTEGER DEFAULT 1,
        notify_self_destruct INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS business_connections (
        connection_id TEXT PRIMARY KEY,
        owner_id      INTEGER NOT NULL,
        connected_at  TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS targets (
        target_user_id  INTEGER PRIMARY KEY,
        set_by          INTEGER NOT NULL,
        set_at          TEXT DEFAULT (datetime('now')),
        notify_messages INTEGER DEFAULT 1,
        notify_deleted  INTEGER DEFAULT 1,
        notify_edited   INTEGER DEFAULT 1,
        notify_viewonce INTEGER DEFAULT 1
    );
    """)
    # Миграция targets
    for col, default in [
        ("notify_messages", 1), ("notify_deleted", 1),
        ("notify_edited", 1),   ("notify_viewonce", 1)
    ]:
        try:
            c.execute(f"ALTER TABLE targets ADD COLUMN {col} INTEGER DEFAULT {default}")
            c.commit()
        except Exception:
            pass
    c.commit()
    c.close()

async def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    await asyncio.get_event_loop().run_in_executor(None, _init_db_sync)

def _run(fn):
    return asyncio.get_event_loop().run_in_executor(None, fn)

# ── Business connections ──

_biz_owners: dict = {}

async def save_biz_connection(connection_id: str, owner_id: int):
    _biz_owners[connection_id] = owner_id
    def _f():
        c = _conn()
        c.execute("""INSERT INTO business_connections (connection_id, owner_id)
            VALUES (?, ?) ON CONFLICT(connection_id) DO UPDATE SET owner_id=excluded.owner_id""",
            (connection_id, owner_id))
        c.commit(); c.close()
    await _run(_f)

async def remove_biz_connection(connection_id: str):
    _biz_owners.pop(connection_id, None)
    def _f():
        c = _conn()
        c.execute("DELETE FROM business_connections WHERE connection_id=?", (connection_id,))
        c.commit(); c.close()
    await _run(_f)

async def restore_biz_connections():
    def _f():
        c = _conn()
        rows = c.execute("SELECT connection_id, owner_id FROM business_connections").fetchall()
        c.close()
        return [(r["connection_id"], r["owner_id"]) for r in rows]
    pairs = await _run(_f)
    for conn_id, owner_id in pairs:
        _biz_owners[conn_id] = owner_id
    logger.info(f"Восстановлено {len(pairs)} business-подключений из БД")

def get_biz_owner(bc_id: str | None) -> int | None:
    if not bc_id: return None
    return _biz_owners.get(bc_id)

async def resolve_biz_owner(bc_id: str | None, bot: Bot) -> int | None:
    if not bc_id: return None
    owner_id = _biz_owners.get(bc_id)
    if owner_id: return owner_id
    try:
        bc = await bot.get_business_connection(bc_id)
        if bc and bc.user:
            owner_id = bc.user.id
            await upsert_user(owner_id, bc.user.username, bc.user.first_name)
            await save_biz_connection(bc_id, owner_id)
            return owner_id
    except Exception as ex:
        logger.warning(f"resolve_biz_owner: {bc_id}: {ex}")
    return None

# ── Targets ──

_targets: set = set()

async def add_target(target_uid: int, set_by: int):
    _targets.add(target_uid)
    def _f():
        c = _conn()
        c.execute("""INSERT INTO targets
            (target_user_id, set_by, notify_messages, notify_deleted, notify_edited, notify_viewonce)
            VALUES (?, ?, 1, 1, 1, 1)
            ON CONFLICT(target_user_id) DO UPDATE SET
            set_by=excluded.set_by, set_at=datetime('now')""",
            (target_uid, set_by))
        c.commit(); c.close()
    await _run(_f)

async def get_target(target_uid: int) -> dict | None:
    def _f():
        c = _conn()
        row = c.execute("""SELECT t.*, u.username, u.first_name FROM targets t
            LEFT JOIN users u ON u.user_id=t.target_user_id
            WHERE t.target_user_id=?""", (target_uid,)).fetchone()
        c.close()
        return dict(row) if row else None
    return await _run(_f)

async def toggle_target_setting(target_uid: int, field: str):
    if field not in {"notify_messages","notify_deleted","notify_edited","notify_viewonce"}: return
    def _f():
        c = _conn()
        c.execute(f"UPDATE targets SET {field}=1-{field} WHERE target_user_id=?", (target_uid,))
        c.commit(); c.close()
    await _run(_f)

async def get_target_settings(target_uid: int) -> dict:
    t = await get_target(target_uid)
    if not t: return {"notify_messages":1,"notify_deleted":1,"notify_edited":1,"notify_viewonce":1}
    return t

async def remove_target(target_uid: int):
    _targets.discard(target_uid)
    def _f():
        c = _conn()
        c.execute("DELETE FROM targets WHERE target_user_id=?", (target_uid,))
        c.commit(); c.close()
    await _run(_f)

async def restore_targets():
    def _f():
        c = _conn()
        rows = c.execute("SELECT target_user_id FROM targets").fetchall()
        c.close()
        return [r["target_user_id"] for r in rows]
    uids = await _run(_f)
    for uid in uids:
        _targets.add(uid)
    logger.info(f"Восстановлено {len(uids)} targets из БД")

async def get_all_targets():
    def _f():
        c = _conn()
        rows = c.execute("""SELECT t.*, u.username, u.first_name FROM targets t
            LEFT JOIN users u ON u.user_id=t.target_user_id
            ORDER BY t.set_at DESC""").fetchall()
        c.close()
        return [dict(r) for r in rows]
    return await _run(_f)

def is_target(uid: int) -> bool:
    return uid in _targets

# ── Пользователи и подписки ──

async def upsert_user(uid, username=None, first_name=None):
    def _f():
        c = _conn()
        c.execute("""INSERT INTO users (user_id, username, first_name, last_seen)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_seen=datetime('now')""", (uid, username, first_name))
        c.commit(); c.close()
    await _run(_f)

async def get_all_users():
    def _f():
        c = _conn()
        rows = c.execute("SELECT * FROM users ORDER BY registered DESC").fetchall()
        c.close()
        return [dict(r) for r in rows]
    return await _run(_f)

async def grant_subscription(uid, days, granted_by):
    def _f():
        c = _conn()
        exp = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""INSERT INTO subscriptions (user_id, expires_at, granted_by)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
            expires_at=?, granted_by=?, granted_at=datetime('now')""",
            (uid, exp, granted_by, exp, granted_by))
        c.commit(); c.close()
        return exp
    return await _run(_f)

async def revoke_subscription(uid):
    def _f():
        c = _conn()
        c.execute("DELETE FROM subscriptions WHERE user_id=?", (uid,))
        c.commit(); c.close()
    await _run(_f)

async def is_subscribed(uid) -> bool:
    def _f():
        c = _conn()
        row = c.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (uid,)).fetchone()
        c.close()
        if not row: return False
        return datetime.strptime(row["expires_at"], "%Y-%m-%d %H:%M:%S") > datetime.now()
    return await _run(_f)

async def get_subscription(uid):
    def _f():
        c = _conn()
        row = c.execute("SELECT * FROM subscriptions WHERE user_id=?", (uid,)).fetchone()
        c.close()
        return dict(row) if row else None
    return await _run(_f)

async def get_all_subscriptions():
    def _f():
        c = _conn()
        rows = c.execute("""SELECT s.*, u.username, u.first_name FROM subscriptions s
            LEFT JOIN users u ON u.user_id=s.user_id
            ORDER BY s.expires_at DESC""").fetchall()
        c.close()
        return [dict(r) for r in rows]
    return await _run(_f)

async def has_used_trial(uid) -> bool:
    def _f():
        c = _conn()
        row = c.execute("SELECT 1 FROM trial_used WHERE user_id=?", (uid,)).fetchone()
        c.close()
        return row is not None
    return await _run(_f)

async def mark_trial_used(uid):
    def _f():
        c = _conn()
        c.execute("INSERT OR IGNORE INTO trial_used (user_id) VALUES (?)", (uid,))
        c.commit(); c.close()
    await _run(_f)

async def cache_message(chat_id, message_id, user_id, username, first_name,
                        text=None, media_type=None, file_id=None,
                        owner_id=None, is_view_once=False):
    def _f():
        c = _conn()
        c.execute("""INSERT OR REPLACE INTO message_cache
            (chat_id, message_id, owner_id, user_id, username, first_name,
             text, media_type, file_id, is_view_once)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (chat_id, message_id, owner_id, user_id, username, first_name,
             text, media_type, file_id, int(is_view_once)))
        c.commit(); c.close()
    await _run(_f)

async def get_cached_message(chat_id, message_id):
    def _f():
        c = _conn()
        row = c.execute("SELECT * FROM message_cache WHERE chat_id=? AND message_id=?",
                        (chat_id, message_id)).fetchone()
        c.close()
        return dict(row) if row else None
    return await _run(_f)

async def delete_cached_message(chat_id, message_id):
    def _f():
        c = _conn()
        c.execute("DELETE FROM message_cache WHERE chat_id=? AND message_id=?",
                  (chat_id, message_id))
        c.commit(); c.close()
    await _run(_f)

async def get_user_settings(uid) -> dict:
    def _f():
        c = _conn()
        c.execute("INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (uid,))
        c.commit()
        row = c.execute("SELECT * FROM user_settings WHERE user_id=?", (uid,)).fetchone()
        c.close()
        return dict(row)
    return await _run(_f)

async def toggle_user_setting(uid, field):
    if field not in {"notify_delete","notify_edit","notify_self_destruct"}: return
    def _f():
        c = _conn()
        c.execute(f"""INSERT INTO user_settings (user_id, {field}) VALUES (?, 1)
            ON CONFLICT(user_id) DO UPDATE SET {field}=1-{field}""", (uid,))
        c.commit(); c.close()
    await _run(_f)

async def should_notify(uid, setting) -> bool:
    if is_admin(uid): return True
    if not await is_subscribed(uid): return False
    s = await get_user_settings(uid)
    return bool(s.get(setting, 1))

# ══════════════════════════════════════════════
# ХЕЛПЕРЫ
# ══════════════════════════════════════════════

def is_admin(uid): return uid in ADMIN_IDS

def user_link(uid, first_name, username=None):
    name = first_name or "Пользователь"
    uname = f" (@{username})" if username else ""
    return f'<a href="tg://user?id={uid}">{name}</a>{uname}'

def trim(t, n=400):
    if not t: return "<i>пусто</i>"
    return (t[:n] + "…") if len(t) > n else t

def extract_media(msg: Message):
    if msg.photo:      return "фото",          msg.photo[-1].file_id
    if msg.video:      return "видео",          msg.video.file_id
    if msg.video_note: return "видеосообщение", msg.video_note.file_id
    if msg.voice:      return "голосовое",      msg.voice.file_id
    if msg.audio:      return "аудио",          msg.audio.file_id
    if msg.document:   return "документ",       msg.document.file_id
    if msg.sticker:    return "стикер",         msg.sticker.file_id
    if msg.animation:  return "анимация",       msg.animation.file_id
    return None, None

def is_view_once_msg(msg: Message) -> bool:
    if getattr(msg, "has_media_spoiler", False): return True
    if msg.photo and getattr(msg.photo[-1], "has_media_spoiler", False): return True
    if msg.video and getattr(msg.video, "has_media_spoiler", False): return True
    if msg.video_note and getattr(msg.video_note, "has_media_spoiler", False): return True
    return False

MEDIA_EMOJI = {
    "фото": "🖼", "видео": "🎬", "видеосообщение": "⭕",
    "голосовое": "🎤", "аудио": "🎵", "документ": "📄",
    "стикер": "🎭", "анимация": "🎞",
}

async def notify_admins(bot: Bot, text: str, **kwargs):
    for admin_id in ADMIN_IDS:
        try: await bot.send_message(admin_id, text, parse_mode="HTML", **kwargs)
        except Exception as ex: logger.warning(f"notify admin {admin_id}: {ex}")

async def safe_edit(call: CallbackQuery, text: str, **kwargs):
    try:
        await call.message.edit_text(text, parse_mode="HTML", **kwargs)
    except Exception:
        try: await call.message.delete()
        except: pass
        try: await call.message.answer(text, parse_mode="HTML", **kwargs)
        except: pass

# ══════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Главное меню"), KeyboardButton(text="💳 Тарифы")],
            [KeyboardButton(text="📋 Подписка"),      KeyboardButton(text="⚙️ Настройки")],
            [KeyboardButton(text="❓ Помощь")],
        ],
        resize_keyboard=True, persistent=True
    )

def start_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡️ Подключить", url="tg://settings/edit")],
    ])

def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Тарифы",      callback_data="u:plans"),
         InlineKeyboardButton(text="📋 Подписка",    callback_data="u:sub")],
        [InlineKeyboardButton(text="⚙️ Настройки",  callback_data="u:settings"),
         InlineKeyboardButton(text="❓ Помощь",      callback_data="u:help")],
        [InlineKeyboardButton(text="⚡️ Подключить", url="tg://settings/edit")],
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="u:main")]
    ])

def plans_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 1 месяц · 35 ⭐",        callback_data="plan:month")],
        [InlineKeyboardButton(text="📦 3 месяца · 89 ⭐  −15%", callback_data="plan:three")],
        [InlineKeyboardButton(text="👑 1 год · 299 ⭐  −29%",   callback_data="plan:year")],
        [InlineKeyboardButton(text="🏠 Главное меню",           callback_data="u:main")],
    ])

def renew_kb():
    """Кнопка продления подписки — отправляется при истечении"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Продлить подписку", callback_data="u:plans")],
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm:users")],
        [InlineKeyboardButton(text="⭐ Подписки",      callback_data="adm:subs")],
        [InlineKeyboardButton(text="✅ Выдать",        callback_data="adm:grant"),
         InlineKeyboardButton(text="❌ Отозвать",      callback_data="adm:revoke")],
        [InlineKeyboardButton(text="🎯 Таргеты",       callback_data="adm:targets")],
        [InlineKeyboardButton(text="📊 Статистика",    callback_data="adm:stats")],
    ])

def adm_back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
    ])

def targets_list_kb(targets: list) -> InlineKeyboardMarkup:
    rows = []
    for t in targets:
        name  = t.get("first_name") or "—"
        uname = f" @{t['username']}" if t.get("username") else ""
        rows.append([InlineKeyboardButton(
            text=f"🎯 {name}{uname}",
            callback_data=f"tgt:view:{t['target_user_id']}"
        )])
    rows.append([InlineKeyboardButton(text="➕ Добавить таргет", callback_data="tgt:add")])
    rows.append([InlineKeyboardButton(text="◀️ Назад",           callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def target_detail_kb(t: dict) -> InlineKeyboardMarkup:
    uid = t["target_user_id"]
    def icon(val): return "✅" if val else "❌"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{icon(t.get('notify_messages',1))} Сообщения",
            callback_data=f"tgt:toggle:{uid}:notify_messages"
        )],
        [InlineKeyboardButton(
            text=f"{icon(t.get('notify_deleted',1))} Удалённые",
            callback_data=f"tgt:toggle:{uid}:notify_deleted"
        )],
        [InlineKeyboardButton(
            text=f"{icon(t.get('notify_edited',1))} Редактирования",
            callback_data=f"tgt:toggle:{uid}:notify_edited"
        )],
        [InlineKeyboardButton(
            text=f"{icon(t.get('notify_viewonce',1))} Исчезающие медиа",
            callback_data=f"tgt:toggle:{uid}:notify_viewonce"
        )],
        [InlineKeyboardButton(text="🗑 Удалить таргет", callback_data=f"tgt:del:{uid}")],
        [InlineKeyboardButton(text="◀️ К списку",       callback_data="adm:targets")],
    ])

# ══════════════════════════════════════════════
# ТЕКСТЫ
# ══════════════════════════════════════════════

START_PHOTO_URL = "https://i.imgur.com/placeholder.jpg"  # заменить на реальный file_id после первой отправки

async def start_text(uid: int, first_name: str) -> str:
    is_trial = False
    if is_admin(uid):
        status_line = "👑 Администратор — безлимитный доступ"
    elif await is_subscribed(uid):
        sub = await get_subscription(uid)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        # Определяем, это тестовый период (цена 0) или платная подписка
        is_trial = sub.get("price", 1) == 0
        exp_str = exp.strftime("%d.%m.%Y")
        if is_trial:
            status_line = f"🎁 Тестовый период активен · осталось {days_left} дн."
        else:
            status_line = f"✅ Подписка активна до {exp_str}"
    else:
        status_line = "❌ Подписка не активна"

    trial_notice = (
        "\n⚠️ <i>Вы используете <b>тестовый период</b> — 7 дней бесплатно.\n"
        "После окончания потребуется оформить подписку.</i>\n"
    ) if is_trial else ""

    return (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        f"Добро пожаловать в бота — я слежу за важным, пока ты не заметил 🕵️\n\n"
        f"<b>Возможности бота:</b>\n"
        f"🗑 Моментально пришлёт уведомление, если ваш собеседник <b>удалит</b> сообщение\n"
        f"✏️ Покажет что было <b>изменено</b> в сообщении\n"
        f"💣 Сохраняет <b>исчезающие медиа</b> (фото/видео с таймером)\n"
        f"📥 <b>Скачивает файлы с таймером</b> — фото, видео, голосовые, кружки\n\n"
        f"<b>Как подключить:</b>\n"
        f"1. Нажмите кнопку <b>«⚡️ Подключить»</b> ниже\n"
        f"2. Выберите <b>«Автоматизация чатов»</b>\n"
        f"3. Введите в поле: <code>@{BOT_USERNAME}</code>\n\n"
        f"<b>Статус:</b> {status_line}"
        f"{trial_notice}"
    )

HELP_TEXT = (
    f"📖 <b>Инструкция по подключению</b>\n\n"
    f"1️⃣ Заранее скопируй имя бота:\n"
    f"<code>@{BOT_USERNAME}</code>\n\n"
    f"2️⃣ Нажми кнопку <b>«⚡️ Подключить»</b> ниже —\n"
    f"откроются настройки Telegram\n\n"
    f"3️⃣ Найди раздел <b>Автоматизация чатов</b>\n"
    f"и вставь скопированное имя бота\n\n"
    f"4️⃣ Готово! Бот начнёт работать сразу ✅\n\n"
    f"──────────────────────\n"
    f"<b>Что умеет бот:</b>\n\n"
    f"🗑 <b>Удалённые сообщения</b> — мгновенно получаешь копию с именем автора\n\n"
    f"✏️ <b>Редактирования</b> — видишь что было написано до изменения\n\n"
    f"💣 <b>Исчезающие медиа</b> — фото и видео «один раз» перехватываются автоматически\n\n"
    f"📥 <b>Скачивание файлов с таймером</b> — ответь на любое сообщение с фото/видео/голосовым/кружком, и бот пришлёт тебе этот файл\n\n"
    f"🎯 <b>Слежка за контактом</b> — все сообщения выбранного человека зеркалируются тебе\n\n"
    f"<i>ℹ️ Telegram Premium не нужен — работает у всех пользователей</i>"
)

# ══════════════════════════════════════════════
# УВЕДОМЛЕНИЯ
# ══════════════════════════════════════════════

async def _send_deleted_notify(bot: Bot, cached: dict, owner_id: int = None):
    author_uid = cached.get("user_id")
    fname      = cached.get("first_name") or "Неизвестно"
    uname      = cached.get("username")
    text       = cached.get("text")
    mtype      = cached.get("media_type")
    fid        = cached.get("file_id")
    is_tgt     = is_target(author_uid) if author_uid else False

    recipients = []
    if is_tgt:
        recipients = ADMIN_IDS[:]
    elif owner_id:
        recipients = [owner_id]
    elif cached.get("owner_id"):
        recipients = [cached["owner_id"]]
    else:
        logger.warning(f"_send_deleted_notify: owner_id не найден")
        return

    now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
    sender  = user_link(author_uid, fname, uname) if author_uid else fname

    caption = (
        f"{'🎯 TARGET · ' if is_tgt else ''}🗑 <b>Удалённое сообщение</b>\n\n"
        f"📅 <b>{now_str}</b>\n"
        f"👤 <b>Автор:</b> {sender}\n"
        + (f"\n💬 <b>Текст:</b>\n{trim(text)}\n" if text else "")
        + (f"\n{MEDIA_EMOJI.get(mtype,'📎')} <b>Медиа:</b> {mtype}\n" if mtype else "")
        + f"\n🤖 @{BOT_USERNAME}"
    )

    no_sub_notice = (
        f"👁 <b>Сообщение было удалено</b>\n\n"
        f"📅 {now_str}\n"
        f"👤 Автор: {sender}\n\n"
        f"🔒 <b>Для просмотра содержимого нужна подписка.</b>\n\n"
        f"💳 Оформи подписку: /start → Тарифы\n"
        f"🤖 @{BOT_USERNAME}"
    )

    async def _deliver(to: int):
        try:
            if fid and mtype:
                send_fn = {
                    "фото":           bot.send_photo,
                    "видео":          bot.send_video,
                    "видеосообщение": bot.send_video_note,
                    "голосовое":      bot.send_voice,
                    "аудио":          bot.send_audio,
                    "документ":       bot.send_document,
                }.get(mtype)
                if send_fn:
                    if mtype == "видеосообщение":
                        await send_fn(to, fid)
                        await bot.send_message(to, caption, parse_mode="HTML")
                    else:
                        await send_fn(to, fid, caption=caption, parse_mode="HTML")
                else:
                    await bot.send_message(to, caption, parse_mode="HTML")
            else:
                await bot.send_message(to, caption, parse_mode="HTML")
        except Exception as ex:
            logger.warning(f"deleted notify {to}: {ex}")

    for r in recipients:
        if is_tgt:
            t_settings = await get_target_settings(author_uid)
            if t_settings.get("notify_deleted", 1):
                await _deliver(r)
        else:
            if is_admin(r):
                await _deliver(r)
            elif await is_subscribed(r):
                s = await get_user_settings(r)
                if s.get("notify_delete", 1):
                    await _deliver(r)
            else:
                try:
                    await bot.send_message(r, no_sub_notice, parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="💳 Купить подписку", callback_data="u:plans")]
                        ]))
                except Exception as ex:
                    logger.warning(f"no_sub notice {r}: {ex}")


async def _send_edited_notify(bot: Bot, uid: int, notify_text: str, is_tgt: bool = False):
    if is_tgt:
        for admin_id in ADMIN_IDS:
            try: await bot.send_message(admin_id, notify_text, parse_mode="HTML")
            except Exception as ex: logger.warning(f"target edit notify {admin_id}: {ex}")
    elif is_admin(uid):
        try: await bot.send_message(uid, notify_text, parse_mode="HTML")
        except: pass
    elif await is_subscribed(uid):
        s = await get_user_settings(uid)
        if s.get("notify_edit", 1):
            try: await bot.send_message(uid, notify_text, parse_mode="HTML")
            except: pass
    else:
        now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
        no_sub = (
            f"✏️ <b>Сообщение было изменено</b>\n\n"
            f"📅 {now_str}\n\n"
            f"🔒 <b>Для просмотра содержимого нужна подписка.</b>\n\n"
            f"💳 Оформи подписку: /start → Тарифы\n"
            f"🤖 @{BOT_USERNAME}"
        )
        try:
            await bot.send_message(uid, no_sub, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Купить подписку", callback_data="u:plans")]
                ]))
        except: pass


async def _send_view_once_notify(bot: Bot, msg: Message, owner_id: int, mtype: str, fid: str):
    u = msg.from_user
    now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
    caption = (
        f"💣 <b>Исчезающее медиа перехвачено!</b>\n\n"
        f"📅 <b>{now_str}</b>\n"
        f"👤 <b>Отправитель:</b> {user_link(u.id, u.first_name, u.username)}\n"
        f"{MEDIA_EMOJI.get(mtype,'📎')} <b>Тип:</b> {mtype}\n\n"
        f"🤖 @{BOT_USERNAME}"
    )
    if is_target(u.id):
        t_settings = await get_target_settings(u.id)
        if not t_settings.get("notify_viewonce", 1): return
        recipients = ADMIN_IDS[:]
    elif is_admin(owner_id):
        recipients = [owner_id]
    elif await is_subscribed(owner_id):
        s = await get_user_settings(owner_id)
        if not s.get("notify_self_destruct", 1): return
        recipients = [owner_id]
    else:
        try:
            await bot.send_message(owner_id,
                f"💣 <b>Тебе отправили исчезающее медиа</b>\n\n"
                f"🔒 <b>Для просмотра нужна подписка.</b>\n\n"
                f"💳 /start → Тарифы\n🤖 @{BOT_USERNAME}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Купить подписку", callback_data="u:plans")]
                ]))
        except: pass
        return

    for r in recipients:
        try:
            send_fn = {
                "фото":          bot.send_photo,
                "видео":         bot.send_video,
                "видеосообщение": bot.send_video_note,
                "голосовое":     bot.send_voice,
            }.get(mtype)
            if send_fn:
                if mtype == "видеосообщение":
                    await send_fn(r, fid)
                    await bot.send_message(r, caption, parse_mode="HTML")
                else:
                    await send_fn(r, fid, caption=caption, parse_mode="HTML")
            else:
                await bot.send_message(r, caption, parse_mode="HTML")
        except Exception as ex:
            logger.warning(f"view_once notify {r}: {ex}")


async def _mirror_to_admins(bot: Bot, msg: Message):
    if not msg.from_user: return
    if not is_target(msg.from_user.id): return

    t_settings = await get_target_settings(msg.from_user.id)
    if not t_settings.get("notify_messages", 1): return

    u = msg.from_user
    now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")

    bc_id = getattr(msg, "business_connection_id", None)
    if msg.chat.type == "private":
        if bc_id:
            recipient = f"в чат: {msg.chat.title or msg.chat.first_name or str(msg.chat.id)}"
        else:
            recipient = f"боту @{BOT_USERNAME}"
    else:
        recipient = f"в группу «{msg.chat.title or str(msg.chat.id)}»"

    mtype, fid = extract_media(msg)
    text = msg.text or msg.caption

    header = (
        f"🎯 <b>TARGET · {user_link(u.id, u.first_name, u.username)}</b>\n"
        f"📅 {now_str}\n"
        f"📨 Кому: <b>{recipient}</b>\n"
        f"─────────────────────\n"
    )

    for admin_id in ADMIN_IDS:
        try:
            if fid and mtype:
                send_fn = {
                    "фото":           bot.send_photo,
                    "видео":          bot.send_video,
                    "видеосообщение": bot.send_video_note,
                    "голосовое":      bot.send_voice,
                    "аудио":          bot.send_audio,
                    "документ":       bot.send_document,
                    "стикер":         bot.send_sticker,
                    "анимация":       bot.send_animation,
                }.get(mtype)
                if send_fn:
                    if mtype in ("видеосообщение", "стикер"):
                        await send_fn(admin_id, fid)
                        await bot.send_message(admin_id, header, parse_mode="HTML")
                    else:
                        cap = header + (f"\n{trim(text)}" if text else "")
                        await send_fn(admin_id, fid, caption=cap, parse_mode="HTML")
                else:
                    await bot.send_message(admin_id,
                        header + (trim(text) if text else ""), parse_mode="HTML")
            else:
                await bot.send_message(admin_id,
                    header + (trim(text) if text else "<i>пусто</i>"), parse_mode="HTML")
        except Exception as ex:
            logger.warning(f"mirror to admin {admin_id}: {ex}")

# ══════════════════════════════════════════════
# СКАЧИВАНИЕ ФАЙЛОВ С ТАЙМЕРОМ (reply)
# Пользователь отвечает на любое сообщение с медиа —
# бот скачивает файл и пересылает владельцу аккаунта.
# ══════════════════════════════════════════════

async def _handle_reply_download(bot: Bot, msg: Message, owner_id: int):
    """
    Если сообщение является ответом на медиа — скачиваем файл и отправляем владельцу.
    Работает для: фото, видео, видеосообщение (кружок), голосовое.
    Файл скачивается во временную папку и сразу удаляется после отправки.
    """
    if not msg.reply_to_message:
        return False

    reply = msg.reply_to_message
    now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
    sender_name = reply.from_user.first_name if reply.from_user else "Неизвестно"
    sender_username = reply.from_user.username if reply.from_user else None
    sender_link = user_link(reply.from_user.id, sender_name, sender_username) if reply.from_user else sender_name

    # Проверяем подписку
    if not is_admin(owner_id) and not await is_subscribed(owner_id):
        await bot.send_message(owner_id,
            f"🔒 <b>Скачивание файлов доступно только по подписке</b>\n\n"
            f"💳 Оформи подписку: /start → Тарифы",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Купить подписку", callback_data="u:plans")]
            ]))
        return True

    file_path = None
    try:
        if reply.photo:
            # Берём фото в максимальном качестве
            photo = reply.photo[-1]
            fl = await bot.get_file(photo.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.jpg"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанное фото</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_photo(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")

        elif reply.video:
            fl = await bot.get_file(reply.video.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.mp4"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанное видео</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_video(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")

        elif reply.video_note:
            fl = await bot.get_file(reply.video_note.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.mp4"
            await bot.download_file(fl.file_path, file_path)
            await bot.send_video_note(owner_id, FSInputFile(file_path))
            await bot.send_message(owner_id,
                f"📥 <b>Скачанный кружок ⬆️</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}",
                parse_mode="HTML")

        elif reply.voice:
            fl = await bot.get_file(reply.voice.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.ogg"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанное голосовое</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_voice(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")

        elif reply.audio:
            fl = await bot.get_file(reply.audio.file_id)
            ext = "mp3"
            if reply.audio.mime_type:
                ext = reply.audio.mime_type.split("/")[-1]
            file_path = MEDIA_DIR / f"{uuid4()}.{ext}"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанное аудио</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_audio(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")

        elif reply.document:
            fl = await bot.get_file(reply.document.file_id)
            ext = "bin"
            if reply.document.mime_type:
                ext = reply.document.mime_type.split("/")[-1]
            file_path = MEDIA_DIR / f"{uuid4()}.{ext}"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанный документ</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_document(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")

        else:
            return False  # не медиа — не обрабатываем

        return True

    except Exception as ex:
        logger.warning(f"reply_download {owner_id}: {ex}")
        return False
    finally:
        # Удаляем временный файл
        if file_path and Path(file_path).exists():
            try: Path(file_path).unlink()
            except: pass

# ══════════════════════════════════════════════
# РОУТЕРЫ
# ══════════════════════════════════════════════

user_router    = Router()
admin_router   = Router()
event_router   = Router()
payment_router = Router()

class AdminStates(StatesGroup):
    waiting_user_id    = State()
    waiting_days       = State()
    waiting_revoke     = State()
    waiting_target_id  = State()

# ══════════════════════════════════════════════
# СОБЫТИЯ — кэширование и отслеживание
# ══════════════════════════════════════════════

async def _do_cache(msg: Message, owner_id: int = None):
    if not msg.from_user or msg.from_user.is_bot: return
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    mtype, fid = extract_media(msg)
    view_once = is_view_once_msg(msg)
    await cache_message(
        msg.chat.id, msg.message_id,
        u.id, u.username, u.first_name,
        msg.text or msg.caption, mtype, fid,
        owner_id=owner_id, is_view_once=view_once
    )

@event_router.message()
async def on_message(msg: Message, bot: Bot):
    if getattr(msg, "business_connection_id", None):
        return
    owner_id = ADMIN_IDS[0] if (msg.from_user and is_target(msg.from_user.id) and ADMIN_IDS) else None
    await _do_cache(msg, owner_id=owner_id)
    await _mirror_to_admins(bot, msg)

@event_router.edited_message()
async def on_edit(msg: Message, bot: Bot, owner_id: int = None):
    if owner_id is None and getattr(msg, "business_connection_id", None):
        return
    if not msg.from_user or msg.from_user.is_bot: return
    u = msg.from_user
    cached   = await get_cached_message(msg.chat.id, msg.message_id)
    old_text = cached.get("text") if cached else None
    new_text = msg.text or msg.caption
    is_tgt   = is_target(u.id)
    notify_to = owner_id or (cached.get("owner_id") if cached else None)

    if old_text != new_text:
        now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
        notify_text = (
            f"{'🎯 TARGET · ' if is_tgt else ''}✏️ <b>Изменённое сообщение</b>\n\n"
            f"📅 <b>{now_str}</b>\n"
            f"👤 <b>Автор:</b> {user_link(u.id, u.first_name, u.username)}\n"
            f"💬 <b>Чат:</b> {msg.chat.title or 'личный чат'}\n\n"
            f"📝 <b>Было:</b>\n{trim(old_text)}\n\n"
            f"📝 <b>Стало:</b>\n{trim(new_text)}\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
        if is_tgt:
            t_settings = await get_target_settings(u.id)
            if t_settings.get("notify_edited", 1):
                await _send_edited_notify(bot, u.id, notify_text, is_tgt=True)
        elif notify_to:
            await _send_edited_notify(bot, notify_to, notify_text, is_tgt=False)

    mtype, fid = extract_media(msg)
    effective_owner = notify_to or (ADMIN_IDS[0] if is_tgt and ADMIN_IDS else None)
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                        new_text, mtype, fid, owner_id=effective_owner)

# ── Business API ──

@event_router.business_message()
async def on_biz_message(msg: Message, bot: Bot):
    bc_id    = getattr(msg, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)

    # Исчезающие медиа — перехватываем
    if is_view_once_msg(msg) and owner_id:
        mtype, fid = extract_media(msg)
        if fid and mtype:
            await _send_view_once_notify(bot, msg, owner_id, mtype, fid)

    # Скачивание файлов через reply (ответ на медиа)
    if msg.reply_to_message and owner_id:
        downloaded = await _handle_reply_download(bot, msg, owner_id)
        if downloaded:
            return  # reply обработан — дальше не кэшируем как обычное сообщение

    await _do_cache(msg, owner_id=owner_id)
    await _mirror_to_admins(bot, msg)

@event_router.edited_business_message()
async def on_biz_edit(msg: Message, bot: Bot):
    bc_id    = getattr(msg, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)
    await on_edit(msg, bot, owner_id=owner_id)

@event_router.deleted_business_messages()
async def on_biz_deleted(event, bot: Bot):
    chat_id = getattr(getattr(event, "chat", None), "id", None)
    if not chat_id: return
    bc_id    = getattr(event, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)
    for mid in getattr(event, "message_ids", []):
        cached = await get_cached_message(chat_id, mid)
        if not cached: continue
        effective_owner = owner_id or cached.get("owner_id")
        await _send_deleted_notify(bot, cached, owner_id=effective_owner)
        await delete_cached_message(chat_id, mid)

@event_router.business_connection()
async def on_biz_connect(bc: BusinessConnection, bot: Bot):
    uid = bc.user.id
    await upsert_user(uid, bc.user.username, bc.user.first_name)

    if hasattr(bc, "id") and bc.id:
        if bc.is_enabled:
            await save_biz_connection(bc.id, uid)
        else:
            await remove_biz_connection(bc.id)

    if not bc.is_enabled:
        try:
            await bot.send_message(uid,
                f"👁 <b>{BOT_NAME} отключён</b>\n\n"
                f"Вы отключили бота от своего аккаунта.\n"
                f"Чтобы снова включить — нажмите кнопку ниже 👇",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡️ Подключить снова", url="tg://settings/edit")]
                ]))
        except: pass
        return

    # Автоматически выдаём пробный период 7 дней при первом подключении
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
            f"🎉 <b>Бот успешно подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎁 <b>Тестовый период активирован!</b>\n"
            f"⏳ Срок: <b>7 дней бесплатно</b>\n"
            f"📅 Действует до: <b>{exp_str}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Теперь бот следит за вашими чатами:\n"
            f"🗑 Удалённые сообщения\n"
            f"✏️ Редактирования\n"
            f"💣 Исчезающие медиа\n"
            f"📥 Скачивание файлов с таймером\n\n"
            f"<i>По истечении пробного периода потребуется продление подписки.</i>\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Тарифы", callback_data="u:plans")],
            [InlineKeyboardButton(text="🏠 Меню",   callback_data="u:main")],
        ])
    elif await is_subscribed(uid) or is_admin(uid):
        text = (
            f"✅ <b>{BOT_NAME} подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}!\n\n"
            f"Бот снова следит за вашими чатами.\n\n"
            f"🗑 Удалённые · ✏️ Редактирования · 💣 Медиа · 📥 Файлы\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Меню", callback_data="u:main")],
        ])
    else:
        text = (
            f"👁 <b>{BOT_NAME} подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}!\n\n"
            f"Для работы нужна подписка.\n\n"
            f"📅 1 месяц · 35 ⭐\n"
            f"📦 3 месяца · 89 ⭐\n"
            f"👑 1 год · 299 ⭐\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Тарифы", callback_data="u:plans")],
        ])

    try:
        await bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
    except Exception as ex:
        logger.warning(f"biz connect notify {uid}: {ex}")

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"🔗 <b>Новое подключение!</b>\n\n"
                f"👤 {user_link(uid, bc.user.first_name, bc.user.username)}\n"
                f"{'🎁 Выдан тестовый период 7 дней' if trial_activated else '✅ Подписан'}",
                parse_mode="HTML")
        except: pass

# ══════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ══════════════════════════════════════════════

# Хранилище file_id картинки /start (кэшируем после первой отправки)
_start_photo_file_id: str | None = None

@user_router.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)

    # Автоматически выдаём пробный период 7 дней при первом /start
    trial_just_activated = False
    if not is_admin(u.id) and not await is_subscribed(u.id) and not await has_used_trial(u.id):
        await mark_trial_used(u.id)
        await grant_subscription(u.id, 7, 0)
        trial_just_activated = True

    text = await start_text(u.id, u.first_name)

    global _start_photo_file_id
    photo_path = Path(__file__).parent / "start_image.jpg"

    # Определяем источник фото: кэш → файл на диске → URL/file_id из .env
    photo_source = None
    use_cached   = False
    if _start_photo_file_id:
        photo_source = _start_photo_file_id
        use_cached   = True
    elif photo_path.exists():
        photo_source = FSInputFile(photo_path)
    elif START_PHOTO_URL:
        photo_source = START_PHOTO_URL

    try:
        if photo_source is not None:
            sent = await msg.answer_photo(
                photo=photo_source,
                caption=text,
                reply_markup=start_kb(),
                parse_mode="HTML"
            )
            # Кэшируем Telegram file_id после первой успешной загрузки
            if not use_cached and sent.photo:
                _start_photo_file_id = sent.photo[-1].file_id
        else:
            # Ни файла, ни URL — только текст
            await msg.answer(text, reply_markup=start_kb(), parse_mode="HTML")
    except Exception as ex:
        logger.warning(f"start photo send error: {ex}")
        await msg.answer(text, reply_markup=start_kb(), parse_mode="HTML")

    # Если только что выдали пробник — дополнительное уведомление
    if trial_just_activated:
        sub = await get_subscription(u.id)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        exp_str = exp.strftime("%d.%m.%Y")
        await msg.answer(
            f"🎁 <b>Вам активирован тестовый период!</b>\n\n"
            f"⏳ Срок: <b>7 дней бесплатно</b>\n"
            f"📅 Действует до: <b>{exp_str}</b>\n\n"
            f"<i>По истечении потребуется продление подписки.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Тарифы", callback_data="u:plans")]
            ])
        )

@user_router.message(F.text == "🏠 Главное меню")
@user_router.callback_query(F.data == "u:main")
async def cb_main(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    if state: await state.clear()
    u = event.from_user
    text = await start_text(u.id, u.first_name)
    if is_call:
        await safe_edit(event, text, reply_markup=main_kb())
        await event.answer()
    else:
        await event.answer(text, reply_markup=main_kb())

# ── Тарифы ──

@user_router.callback_query(F.data == "u:plans")
@user_router.message(F.text == "💳 Тарифы")
async def show_plans(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    subscribed = await is_subscribed(uid)
    sub_info = ""
    if subscribed:
        sub = await get_subscription(uid)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        sub_info = f"\n\n✅ <b>Подписка активна</b> · до {exp.strftime('%d.%m.%Y')} ({days_left} дн.)"
    text = (
        f"💳 <b>Тарифы {BOT_NAME}</b>{sub_info}\n\n"
        f"📅 <b>1 месяц</b> · 35 ⭐\n"
        f"📦 <b>3 месяца</b> · 89 ⭐  <i>скидка 15%</i>\n"
        f"👑 <b>1 год</b> · 299 ⭐  <i>скидка 29%</i>\n\n"
        f"<i>🔒 Оплата через Telegram Stars — мгновенно и безопасно</i>"
    )
    if is_call:
        await safe_edit(event, text, reply_markup=plans_kb())
        await event.answer()
    else:
        await event.answer(text, reply_markup=plans_kb())

# ── Подписка ──

@user_router.callback_query(F.data == "u:sub")
@user_router.message(F.text == "📋 Подписка")
async def show_sub(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    if is_admin(uid):
        text = "👑 <b>Администратор</b>\nБезлимитный доступ ко всем функциям."
    else:
        sub = await get_subscription(uid)
        if not sub:
            text = (
                f"❌ <b>Подписка не активна</b>\n\n"
                f"Оформите подписку чтобы начать использовать {BOT_NAME}."
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
                    f"Дата истечения: {exp.strftime('%d.%m.%Y %H:%M')}\n"
                    f"Оформите новую подписку для продолжения."
                )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Купить / Продлить", callback_data="u:plans")],
        [InlineKeyboardButton(text="🏠 Главное меню",      callback_data="u:main")],
    ])
    if is_call:
        await safe_edit(event, text, reply_markup=kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb)

# ── Настройки ──

@user_router.callback_query(F.data == "u:settings")
@user_router.message(F.text == "⚙️ Настройки")
async def show_settings(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    s = await get_user_settings(uid)
    def ico(v): return "🟢" if v else "🔴"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{ico(s['notify_delete'])} Удалённые сообщения",
            callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(
            text=f"{ico(s['notify_edit'])} Редактирования",
            callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(
            text=f"{ico(s['notify_self_destruct'])} Исчезающие медиа",
            callback_data="toggle:notify_self_destruct")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="u:main")],
    ])
    text = (
        f"⚙️ <b>Настройки уведомлений</b>\n\n"
        f"{ico(s['notify_delete'])} Удалённые сообщения\n"
        f"{ico(s['notify_edit'])} Редактирования\n"
        f"{ico(s['notify_self_destruct'])} Исчезающие медиа\n\n"
        f"<i>Нажми на пункт чтобы включить / выключить</i>"
    )
    if is_call:
        await safe_edit(event, text, reply_markup=kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb)

@user_router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if not (await is_subscribed(call.from_user.id) or is_admin(call.from_user.id)):
        return await call.answer("❌ Нужна активная подписка!", show_alert=True)
    await toggle_user_setting(call.from_user.id, call.data.split(":", 1)[1])
    await show_settings(call)

# ── Помощь ──

@user_router.callback_query(F.data == "u:help")
@user_router.message(F.text == "❓ Помощь")
async def show_help(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡️ Подключить", url="tg://settings/edit")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="u:main")],
    ])
    if is_call:
        await safe_edit(event, HELP_TEXT, reply_markup=kb)
        await event.answer()
    else:
        await event.answer(HELP_TEXT, reply_markup=kb)

# ── Покупка ──

@user_router.callback_query(F.data.startswith("plan:"))
async def cb_plan(call: CallbackQuery, bot: Bot):
    plan_key = call.data.split(":")[1]
    uid  = call.from_user.id
    plan = PLANS.get(plan_key)
    if not plan: return await call.answer("Неизвестный тариф", show_alert=True)

    plan_icons     = {"month": "📅", "three": "📦", "year": "👑"}
    plan_discounts = {"month": "", "three": "  🔥 скидка 15%", "year": "  🔥 скидка 29%"}

    try: await call.message.delete()
    except: pass

    await bot.send_message(uid,
        f"⭐ <b>Оформление подписки</b>\n\n"
        f"{plan_icons.get(plan_key,'')} <b>Тариф:</b> {plan['label']}{plan_discounts.get(plan_key,'')}\n"
        f"💳 <b>Стоимость:</b> {plan['stars']} ⭐ Stars\n\n"
        f"<b>Что входит:</b>\n"
        f"🗑 Удалённые сообщения\n"
        f"✏️ Редактирования\n"
        f"💣 Исчезающие медиа\n"
        f"📥 Скачивание файлов с таймером\n\n"
        f"🔒 Оплата защищена Telegram",
        parse_mode="HTML"
    )
    await bot.send_invoice(
        chat_id=uid,
        title=f"👁 {BOT_NAME} · {plan['label']}",
        description=f"Доступ ко всем функциям {BOT_NAME} · {plan['desc']}",
        payload=f"sub_{plan_key}_{uid}",
        currency="XTR",
        prices=[LabeledPrice(label=f"{BOT_NAME} · {plan['label']}", amount=plan["stars"])],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⭐ Оплатить {plan['stars']} Stars", pay=True)],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data="u:plans")],
        ])
    )
    await call.answer()

@payment_router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)

@payment_router.message(F.successful_payment)
async def on_payment(msg: Message, bot: Bot):
    payload  = msg.successful_payment.invoice_payload
    parts    = payload.split("_")
    if len(parts) < 2: return
    plan_key = parts[1]
    uid      = msg.from_user.id
    plan     = PLANS.get(plan_key)
    if not plan: return
    expires  = await grant_subscription(uid, plan["days"], 0)
    exp_dt   = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    stars    = msg.successful_payment.total_amount
    await msg.answer(
        f"🎉 <b>Оплата прошла успешно!</b>\n\n"
        f"👑 <b>Тариф:</b> {plan['label']}\n"
        f"⭐ <b>Списано:</b> {stars} Stars\n"
        f"📅 <b>Подписка до:</b> {exp_dt.strftime('%d.%m.%Y %H:%M')}\n\n"
        f"✅ Все функции активированы!\n"
        f"🗑 · ✏️ · 💣 · 📥\n\n"
        f"<i>👁 {BOT_NAME} уже следит за вашими чатами</i>",
        reply_markup=back_kb()
    )
    await notify_admins(bot,
        f"💳 <b>Новая оплата!</b>\n\n"
        f"👤 {user_link(uid, msg.from_user.first_name, msg.from_user.username)}\n"
        f"📦 Тариф: {plan['label']}\n"
        f"⭐ Stars: {stars}\n"
        f"📅 До: {exp_dt.strftime('%d.%m.%Y %H:%M')}")

@user_router.message(Command("settings"))
async def cmd_settings(msg: Message):
    await show_settings(msg)

@user_router.message(Command("sub"))
async def cmd_sub(msg: Message):
    await show_sub(msg)

# ══════════════════════════════════════════════
# ПРОВЕРКА ИСТЁКШИХ ПОДПИСОК (фоновая задача)
# Каждый час проверяет у кого подписка истекла сегодня
# и отправляет уведомление с кнопкой "Продлить"
# ══════════════════════════════════════════════

async def check_expired_subscriptions(bot: Bot):
    while True:
        try:
            await asyncio.sleep(3600)  # раз в час
            def _f():
                c = _conn()
                # Подписки которые истекли за последние 2 часа
                threshold = (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
                now_str   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rows = c.execute(
                    "SELECT user_id, expires_at FROM subscriptions WHERE expires_at <= ? AND expires_at >= ?",
                    (now_str, threshold)
                ).fetchall()
                c.close()
                return [dict(r) for r in rows]
            expired = await _run(_f)
            for s in expired:
                uid = s["user_id"]
                if is_admin(uid): continue
                try:
                    await bot.send_message(uid,
                        f"⏰ <b>Ваша подписка {BOT_NAME} истекла</b>\n\n"
                        f"Чтобы продолжить пользоваться ботом — продлите подписку.\n\n"
                        f"📅 1 месяц · 35 ⭐\n"
                        f"📦 3 месяца · 89 ⭐\n"
                        f"👑 1 год · 299 ⭐",
                        parse_mode="HTML",
                        reply_markup=renew_kb()
                    )
                except Exception as ex:
                    logger.warning(f"expired notify {uid}: {ex}")
        except Exception as ex:
            logger.warning(f"check_expired_subscriptions error: {ex}")

# ══════════════════════════════════════════════
# АДМИН ПАНЕЛЬ
# ══════════════════════════════════════════════

@admin_router.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return await msg.answer("⛔ Нет доступа.")
    await state.clear()
    await msg.answer(
        f"👁 <b>{BOT_NAME} · Панель администратора</b>\n\nВыбери действие:",
        reply_markup=admin_kb())

@admin_router.callback_query(F.data == "adm:back")
async def adm_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call,
        f"👁 <b>{BOT_NAME} · Панель администратора</b>\n\nВыбери действие:",
        reply_markup=admin_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users     = await get_all_users()
    subs      = await get_all_subscriptions()
    now       = datetime.now()
    active    = [s for s in subs if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]
    biz_count = len(_biz_owners)
    tgt_count = len(_targets)
    await safe_edit(call,
        f"📊 <b>Статистика {BOT_NAME}</b>\n\n"
        f"👥 Всего пользователей: <b>{len(users)}</b>\n"
        f"⭐ Активных подписок: <b>{len(active)}</b>\n"
        f"📋 Всего выдано: <b>{len(subs)}</b>\n"
        f"🔗 Подключений: <b>{biz_count}</b>\n"
        f"🎯 Активных таргетов: <b>{tgt_count}</b>",
        reply_markup=adm_back_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:users")
async def adm_users(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users = await get_all_users()
    if not users:
        return await safe_edit(call, "Пользователей нет.", reply_markup=adm_back_kb())
    lines = [f"👥 <b>Пользователи</b> (последние 50):\n"]
    for u in users[:50]:
        uname = f"@{u['username']}" if u['username'] else "—"
        tgt   = " 🎯" if is_target(u["user_id"]) else ""
        lines.append(f"• <code>{u['user_id']}</code> | {u['first_name'] or '—'} | {uname}{tgt}")
    await safe_edit(call, "\n".join(lines), reply_markup=adm_back_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:subs")
async def adm_subs(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    subs = await get_all_subscriptions()
    now  = datetime.now()
    active = [s for s in subs if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]
    if not active:
        return await safe_edit(call, "Активных подписок нет.", reply_markup=adm_back_kb())
    lines = [f"⭐ <b>Активные подписки</b>:\n"]
    for s in active:
        exp = datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - now).days
        uname = f"@{s['username']}" if s.get("username") else "—"
        lines.append(
            f"• <code>{s['user_id']}</code> | {s.get('first_name') or '—'} | {uname}\n"
            f"  До: <b>{exp.strftime('%d.%m.%Y')}</b> ({days_left} дн.)"
        )
    await safe_edit(call, "\n".join(lines), reply_markup=adm_back_kb())
    await call.answer()

# ── Таргеты ──

@admin_router.callback_query(F.data == "adm:targets")
async def adm_targets(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    targets = await get_all_targets()
    text = (
        f"🎯 <b>Таргеты</b> ({len(targets)})\n\nВыбери таргет для настройки:"
        if targets else
        "🎯 <b>Таргеты</b>\n\nСписок пуст. Нажми кнопку ниже чтобы добавить."
    )
    await safe_edit(call, text, reply_markup=targets_list_kb(targets))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:view:"))
async def tgt_view(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    uid = int(call.data.split(":")[2])
    t = await get_target(uid)
    if not t:
        await call.answer("Таргет не найден", show_alert=True)
        return await adm_targets(call)
    name  = t.get("first_name") or "—"
    uname = f" (@{t['username']})" if t.get("username") else ""
    text  = (
        f"🎯 <b>Таргет: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит от этого пользователя:\n\n"
        f"✅ включено · ❌ выключено"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:toggle:"))
async def tgt_toggle(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    parts = call.data.split(":")
    uid   = int(parts[2])
    field = parts[3]
    await toggle_target_setting(uid, field)
    t = await get_target(uid)
    if not t: return await call.answer("Ошибка", show_alert=True)
    name  = t.get("first_name") or "—"
    uname = f" (@{t['username']})" if t.get("username") else ""
    text  = (
        f"🎯 <b>Таргет: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит от этого пользователя:\n\n"
        f"✅ включено · ❌ выключено"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t))
    await call.answer("✅ Сохранено")

@admin_router.callback_query(F.data.startswith("tgt:del:"))
async def tgt_delete(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    uid = int(call.data.split(":")[2])
    await remove_target(uid)
    await call.answer("✅ Таргет удалён", show_alert=True)
    targets = await get_all_targets()
    text = (
        f"🎯 <b>Таргеты</b> ({len(targets)})\n\nВыбери таргет для настройки:"
        if targets else
        "🎯 <b>Таргеты</b>\n\nСписок пуст."
    )
    await safe_edit(call, text, reply_markup=targets_list_kb(targets))

@admin_router.callback_query(F.data == "tgt:add")
async def tgt_add_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_target_id)
    users = await get_all_users()
    if users:
        rows = []
        for u in users[:20]:
            name    = u.get("first_name") or "—"
            uname   = f" @{u['username']}" if u.get("username") else ""
            already = "🎯 " if is_target(u["user_id"]) else ""
            rows.append([InlineKeyboardButton(
                text=f"{already}{name}{uname} [{u['user_id']}]",
                callback_data=f"tgt:pick:{u['user_id']}"
            )])
        rows.append([InlineKeyboardButton(text="✏️ Ввести ID вручную", callback_data="tgt:manual")])
        rows.append([InlineKeyboardButton(text="◀️ Назад",             callback_data="adm:targets")])
        await safe_edit(call,
            "🎯 <b>Выбери пользователя из списка:</b>\n\n🎯 = уже таргет",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    else:
        await safe_edit(call,
            "🎯 <b>Добавить таргет</b>\n\nПользователей пока нет.\nВведи Telegram ID вручную:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:targets")]
            ]))
    await call.answer()

@admin_router.callback_query(F.data == "tgt:manual")
async def tgt_manual(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_target_id)
    await safe_edit(call,
        "🎯 <b>Добавить таргет</b>\n\nВведи Telegram ID пользователя:\n<i>Отмена: /admin</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:targets")]
        ]))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:pick:"))
async def tgt_pick(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    uid = int(call.data.split(":")[2])
    await state.clear()
    await add_target(uid, call.from_user.id)
    t = await get_target(uid)
    name  = t.get("first_name") or "—" if t else "—"
    uname = f" (@{t['username']})" if t and t.get("username") else ""
    text  = (
        f"🎯 <b>Таргет добавлен: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит:"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t) if t else adm_back_kb())
    await call.answer("✅ Таргет добавлен!")

@admin_router.message(AdminStates.waiting_target_id)
async def tgt_add_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        return await msg.answer("❗ Введи числовой ID.")
    await state.clear()
    await add_target(uid, msg.from_user.id)
    t = await get_target(uid)
    name  = t.get("first_name") or f"ID {uid}" if t else f"ID {uid}"
    uname = f" (@{t['username']})" if t and t.get("username") else ""
    await msg.answer(
        f"🎯 <b>Таргет добавлен: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настройки: /admin → Таргеты",
        parse_mode="HTML"
    )

@admin_router.message(Command("target"))
async def cmd_target(msg: Message):
    if not is_admin(msg.from_user.id): return await msg.answer("⛔ Нет доступа.")
    parts = msg.text.strip().split()
    if len(parts) < 2:
        return await msg.answer("🎯 Используй панель: /admin → Таргеты")
    try:
        target_uid = int(parts[1])
    except ValueError:
        return await msg.answer("❗ ID должен быть числом.")
    await add_target(target_uid, msg.from_user.id)
    await msg.answer(
        f"🎯 <b>Таргет добавлен!</b>\n🆔 <code>{target_uid}</code>\n\n"
        f"Настройки: /admin → Таргеты",
        parse_mode="HTML"
    )

# ── Выдача / отзыв подписки ──

@admin_router.callback_query(F.data == "adm:grant")
async def adm_grant_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_user_id)
    await safe_edit(call,
        "✅ <b>Выдача подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:\n<i>Отмена: /admin</i>")
    await call.answer()

@admin_router.message(AdminStates.waiting_user_id)
async def adm_grant_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❗ Введи числовой ID.")
    await state.update_data(target_user_id=uid)
    await state.set_state(AdminStates.waiting_days)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="7 дней",     callback_data="days:7"),
         InlineKeyboardButton(text="1 месяц",    callback_data="days:30"),
         InlineKeyboardButton(text="3 месяца",   callback_data="days:90")],
        [InlineKeyboardButton(text="1 год",      callback_data="days:365"),
         InlineKeyboardButton(text="♾ Навсегда", callback_data="days:9999")],
    ])
    await msg.answer(f"👤 ID: <code>{uid}</code>\n\nВыбери срок:", reply_markup=kb)

@admin_router.callback_query(F.data.startswith("days:"))
async def adm_grant_days(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    days = int(call.data.split(":")[1])
    data = await state.get_data()
    uid  = data.get("target_user_id")
    if not uid:
        await state.clear()
        return await call.answer("Сессия истекла.", show_alert=True)
    expires = await grant_subscription(uid, days, call.from_user.id)
    await state.clear()
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await safe_edit(call,
        f"✅ <b>Подписка выдана!</b>\n\n"
        f"👤 ID: <code>{uid}</code>\n"
        f"⏳ Срок: <b>{days} дн.</b>\n"
        f"📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>",
        reply_markup=adm_back_kb())
    await call.answer("✅ Готово!")
    try:
        await call.bot.send_message(uid,
            f"🎉 <b>Вам выдана подписка {BOT_NAME}!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n"
            f"📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"<i>Используй /start 👁</i>",
            parse_mode="HTML")
    except: pass

@admin_router.message(AdminStates.waiting_days)
async def adm_grant_days_text(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        days = int(msg.text.strip())
        if days <= 0: raise ValueError
    except ValueError:
        return await msg.answer("❗ Введи положительное число.")
    data = await state.get_data()
    uid  = data.get("target_user_id")
    expires = await grant_subscription(uid, days, msg.from_user.id)
    await state.clear()
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await msg.answer(
        f"✅ <b>Подписка выдана!</b>\n\n"
        f"👤 ID: <code>{uid}</code>\n"
        f"⏳ {days} дн. · до {exp_dt.strftime('%d.%m.%Y %H:%M')}",
        reply_markup=adm_back_kb())
    try:
        await msg.bot.send_message(uid,
            f"🎉 <b>Вам выдана подписка {BOT_NAME}!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n"
            f"📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>",
            parse_mode="HTML")
    except: pass

@admin_router.callback_query(F.data == "adm:revoke")
async def adm_revoke_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_revoke)
    await safe_edit(call,
        "❌ <b>Отзыв подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:\n<i>Отмена: /admin</i>")
    await call.answer()

@admin_router.message(AdminStates.waiting_revoke)
async def adm_revoke_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❗ Введи числовой ID.")
    await revoke_subscription(uid)
    await state.clear()
    await msg.answer(
        f"✅ Подписка пользователя <code>{uid}</code> отозвана.",
        reply_markup=adm_back_kb())

# ══════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════

async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher()
    dp.include_routers(admin_router, user_router, event_router, payment_router)
    await init_db()
    await restore_biz_connections()
    await restore_targets()
    # Запускаем фоновую задачу проверки истёкших подписок
    asyncio.create_task(check_expired_subscriptions(bot))
    logger.info(f"{BOT_NAME} запущен")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types() + [
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages"
    ])

if __name__ == "__main__":
    asyncio.run(main())
