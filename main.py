#!/usr/bin/env python3
"""
👁 ShadowSMSq Bot — v3
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

# Множество (chat_id, message_id) сообщений отправленных самим ботом — игнорируем их удаление
_bot_message_ids: set[tuple] = set()


# Московское время (UTC+3) для отображения в уведомлениях
_MSK = timezone(timedelta(hours=3))

def _now_str() -> str:
    """Текущее время по Москве (UTC+3) в формате для уведомлений."""
    return datetime.now(_MSK).strftime("%d.%m.%Y в %H:%M:%S")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery,
    BusinessConnection, BotCommand,
    FSInputFile, InputMediaPhoto, URLInputFile,
    MessageReactionUpdated,
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
    "month": {"label": "1 месяц",  "days": 30,  "stars": 100, "desc": "1 месяц"},
    "three": {"label": "3 месяца", "days": 90,  "stars": 300, "desc": "3 месяца"},
    "year":  {"label": "1 год",    "days": 365, "stars": 700, "desc": "1 год"},
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
        is_outgoing  INTEGER DEFAULT 0,
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
    CREATE TABLE IF NOT EXISTS kv_store (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
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
    CREATE TABLE IF NOT EXISTS pending_notifications (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     INTEGER NOT NULL,
        event_type  TEXT NOT NULL,
        caption     TEXT,
        media_type  TEXT,
        file_id     TEXT,
        created_at  TEXT DEFAULT (datetime('now'))
    );
    """)
    # Миграция users: добавляем ever_connected
    try:
        c.execute("ALTER TABLE users ADD COLUMN ever_connected INTEGER DEFAULT 0")
        c.commit()
    except Exception:
        pass
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
    # Миграция: добавляем is_outgoing в message_cache если ещё нет
    try:
        c.execute("ALTER TABLE message_cache ADD COLUMN is_outgoing INTEGER DEFAULT 0")
        c.commit()
    except Exception:
        pass
    c.commit()
    c.close()

async def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    await asyncio.get_event_loop().run_in_executor(None, _init_db_sync)

def _kv_set(key: str, value: str):
    c = _conn()
    c.execute("INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, value))
    c.commit()
    c.close()

def _kv_get(key: str):
    c = _conn()
    row = c.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
    c.close()
    return row["value"] if row else None

def _load_demo_video_cache():
    """Загружает сохранённые file_id демо-видео из БД в память при старте."""
    for key in ("deleted", "edited", "media"):
        fid = _kv_get(f"demo_video:{key}")
        if fid:
            _demo_video_file_ids[key] = fid

def _run(fn):
    return asyncio.get_event_loop().run_in_executor(None, fn)

# ── Business connections ──

_biz_owners: dict = {}

def has_biz_connection(uid: int) -> bool:
    """Проверяет, подключил ли пользователь бота в автоматизацию чатов."""
    return uid in _biz_owners.values()

async def save_biz_connection(connection_id: str, owner_id: int):
    _biz_owners[connection_id] = owner_id
    def _f():
        c = _conn()
        c.execute("""INSERT INTO business_connections (connection_id, owner_id)
            VALUES (?, ?) ON CONFLICT(connection_id) DO UPDATE SET owner_id=excluded.owner_id""",
            (connection_id, owner_id))
        c.execute("UPDATE users SET ever_connected=1 WHERE user_id=?", (owner_id,))
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

async def save_pending_notification(user_id: int, event_type: str, caption: str, media_type: str = None, file_id: str = None):
    def _f():
        c = _conn()
        c.execute(
            "INSERT INTO pending_notifications (user_id, event_type, caption, media_type, file_id) VALUES (?, ?, ?, ?, ?)",
            (user_id, event_type, caption, media_type, file_id)
        )
        c.commit(); c.close()
    await _run(_f)

async def get_pending_notifications(user_id: int) -> list:
    def _f():
        c = _conn()
        rows = c.execute(
            "SELECT * FROM pending_notifications WHERE user_id=? ORDER BY created_at ASC",
            (user_id,)
        ).fetchall()
        c.close()
        return [dict(r) for r in rows]
    return await _run(_f)

async def clear_pending_notifications(user_id: int):
    def _f():
        c = _conn()
        c.execute("DELETE FROM pending_notifications WHERE user_id=?", (user_id,))
        c.commit(); c.close()
    await _run(_f)

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

async def get_all_trial_used() -> set:
    def _f():
        c = _conn()
        rows = c.execute("SELECT user_id FROM trial_used").fetchall()
        c.close()
        return {r["user_id"] for r in rows}
    return await _run(_f)

async def cache_message(chat_id, message_id, user_id, username, first_name,
                        text=None, media_type=None, file_id=None,
                        owner_id=None, is_view_once=False, is_outgoing=False):
    def _f():
        c = _conn()
        c.execute("""INSERT OR REPLACE INTO message_cache
            (chat_id, message_id, owner_id, user_id, username, first_name,
             text, media_type, file_id, is_view_once, is_outgoing)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (chat_id, message_id, owner_id, user_id, username, first_name,
             text, media_type, file_id, int(is_view_once), int(is_outgoing)))
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
def _is_admin_sync(uid): return uid in ADMIN_IDS

def user_link(uid, first_name, username=None):
    name = first_name or "Пользователь"
    uname = f" (@{username})" if username else ""
    return f'<a href="tg://user?id={uid}">{name}</a>{uname}'

def trim(t, n=None):
    if not t: return "<i>пусто</i>"
    return t

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
    """
    Определяет исчезающие медиа (таймер 1x) через Business API.
    Логирует все поля для диагностики.
    """
    has_any_media = msg.photo or msg.video or msg.video_note or msg.voice

    if has_any_media:
        # Собираем все возможные флаги для лога
        ttl = None
        spoiler = getattr(msg, "has_media_spoiler", None)
        protect = getattr(msg, "protect_content", None)
        if msg.photo:
            ttl     = getattr(msg.photo[-1], "ttl_seconds", None)
            spoiler = spoiler or getattr(msg.photo[-1], "has_media_spoiler", None)
        elif msg.video:
            ttl     = getattr(msg.video, "ttl_seconds", None)
            spoiler = spoiler or getattr(msg.video, "has_media_spoiler", None)
        elif msg.video_note:
            ttl     = getattr(msg.video_note, "ttl_seconds", None)
        elif msg.voice:
            ttl     = getattr(msg.voice, "ttl_seconds", None)

        logger.info(
            f"[VIEW_ONCE_CHECK] ttl={ttl} spoiler={spoiler} "
            f"protect={protect} "
            f"msg_protect={getattr(msg, 'protect_content', None)}"
        )

        if ttl:           return True
        if spoiler:       return True
        if protect:       return True

    return False

MEDIA_EMOJI = {
    "фото": "<tg-emoji emoji-id=\"5375074927252621134\">🖼</tg-emoji>", "видео": "<tg-emoji emoji-id=\"5375464961822695044\">🎬</tg-emoji>", "видеосообщение": "⭕",
    "голосовое": "<tg-emoji emoji-id=\"5260652149469094137\">🎤</tg-emoji>", "аудио": "<tg-emoji emoji-id=\"5188621441926438751\">🎵</tg-emoji>", "документ": "<tg-emoji emoji-id=\"5257965810634202885\">📄</tg-emoji>",
    "стикер": "<tg-emoji emoji-id=\"5359441070201513074\">🎭</tg-emoji>", "анимация": "🎞",
}

async def notify_admins(bot: Bot, text: str, **kwargs):
    for admin_id in ADMIN_IDS:
        try: await bot.send_message(admin_id, text, parse_mode="HTML", **kwargs)
        except Exception as ex: logger.warning(f"notify admin {admin_id}: {ex}")

async def safe_edit(call: CallbackQuery, text: str, **kwargs):
    """Редактирует сообщение. Если это фото — редактирует caption."""
    msg = call.message
    try:
        if msg.photo or msg.document or msg.video:
            await msg.edit_caption(caption=text, parse_mode="HTML", **kwargs)
        else:
            await msg.edit_text(text, parse_mode="HTML", **kwargs)
    except Exception:
        try:
            # Помечаем сообщение бота как is_outgoing чтобы on_biz_deleted его проигнорировал
            await cache_message(msg.chat.id, msg.message_id, call.from_user.id,
                                None, None, owner_id=call.from_user.id, is_outgoing=True)
            await msg.delete()
        except: pass
        try:
            sent = await msg.answer(text, parse_mode="HTML", **kwargs)
            if sent:
                _bot_message_ids.add((sent.chat.id, sent.message_id))
                await cache_message(sent.chat.id, sent.message_id, call.from_user.id,
                                    None, None, owner_id=call.from_user.id, is_outgoing=True)
        except: pass

# Кэши file_id для фото разделов (взрыв-анимация)
_section_photo_cache: dict[str, str | None] = {
    "help":      None,
    "main":      None,
    "plans":     None,
    "settings":  None,
    "setup":     None,
    "expired":   None,
}
_SECTION_PHOTO_FILES = {
    "help":     "help_image.jpg",
    "main":     "cabinet_image.jpg",
    "plans":    "plans_image.jpg",
    "settings": "notifications_image.jpg",
    "setup":    "start_image.jpg",
    "expired":  "expired_image.jpg",
}

async def send_with_explosion(call: CallbackQuery, section: str, text: str, kb, bot: Bot = None):
    """Удаляет старое сообщение и отправляет новое с фото (эффект взрыва)."""
    msg = call.message
    photo_path = Path(__file__).parent / _SECTION_PHOTO_FILES.get(section, "start_image.jpg")

    cached_fid = _section_photo_cache.get(section)
    photo_source = None
    use_cached = False
    if cached_fid:
        photo_source = cached_fid
        use_cached = True
    elif photo_path.exists():
        photo_source = FSInputFile(photo_path)

    try:
        await cache_message(msg.chat.id, msg.message_id, call.from_user.id,
                            None, None, owner_id=call.from_user.id, is_outgoing=True)
        await msg.delete()
    except Exception:
        pass

    try:
        if photo_source is not None:
            sent = await msg.answer_photo(
                photo=photo_source,
                caption=text,
                reply_markup=kb,
                parse_mode="HTML"
            )
            if not use_cached and sent.photo:
                _section_photo_cache[section] = sent.photo[-1].file_id
            _bot_message_ids.add((sent.chat.id, sent.message_id))
        else:
            sent = await msg.answer(text, reply_markup=kb, parse_mode="HTML")
            _bot_message_ids.add((sent.chat.id, sent.message_id))
        await cache_message(sent.chat.id, sent.message_id, call.from_user.id,
                            None, None, owner_id=call.from_user.id, is_outgoing=True)
    except Exception as ex:
        logger.warning(f"send_with_explosion [{section}] error: {ex}")
        try:
            await msg.answer(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass

# ══════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⚡️ Подключить бота"), KeyboardButton(text="💳 Тарифы")],
            [KeyboardButton(text="👤 Личный кабинет"), KeyboardButton(text="🔔 Уведомления")],
            [KeyboardButton(text="❓ Инструкция")],
        ],
        resize_keyboard=True, persistent=True
    )

def start_kb(uid: int = None):
    buttons = [
        [InlineKeyboardButton(text="⚡️ Перейти в Автоматизацию", url="tg://settings/edit")],
        [InlineKeyboardButton(text="❓ Как работает бот", callback_data="u:help")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Тарифы",        callback_data="u:plans"),
         InlineKeyboardButton(text="🔔 Уведомления",  callback_data="u:settings")],
        [InlineKeyboardButton(text="❓ Как работает бот", callback_data="u:help")],
        [InlineKeyboardButton(text="◀️ Назад",       callback_data="u:back_start")],
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]
    ])

def plans_kb():
    m = PLANS["month"]["stars"]
    t = PLANS["three"]["stars"]
    y = PLANS["year"]["stars"]
    t_pct = round((1 - t / (m * 3)) * 100)
    y_pct = round((1 - y / (m * 12)) * 100)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💎 1 месяц · {m} ⭐",   callback_data="plan:month")],
        [InlineKeyboardButton(text=f"💎 3 месяца · {t} ⭐", callback_data="plan:three")],
        [InlineKeyboardButton(text=f"💎 1 год · {y} ⭐",    callback_data="plan:year")],
        [InlineKeyboardButton(text="🏠 Главное меню",                   callback_data="u:main")],
    ])

def renew_kb():
    """Кнопка продления подписки — отправляется при истечении"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Продлить подписку", callback_data="u:plans")],
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm:users")],
        [InlineKeyboardButton(text="⭐ Подписки",      callback_data="adm:subs")],
        [InlineKeyboardButton(text="✅ Выдать",        callback_data="adm:grant"),
         InlineKeyboardButton(text="❌ Отозвать",      callback_data="adm:revoke")],
        [InlineKeyboardButton(text="🔗 Подключения",   callback_data="adm:connections")],
        [InlineKeyboardButton(text="🎯 Таргеты",       callback_data="adm:targets")],
        [InlineKeyboardButton(text="📊 Статистика",    callback_data="adm:stats")],
        [InlineKeyboardButton(text="💰 Изменить цены", callback_data="adm:prices")],
        [InlineKeyboardButton(text="📢 Рассылка",      callback_data="adm:broadcast")],
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

START_PHOTO_URL   = os.getenv("START_PHOTO_URL", "")    # file_id или URL для /start

async def start_text(uid: int, first_name: str) -> str:
    return (
        f"<b>Добро пожаловать в ShadowSMSq! <tg-emoji emoji-id=\"5424892643760937442\">👁</tg-emoji></b>\n\n"
        f"<b>Возможности бота:</b>\n"
        f"• <i>Моментально пришлёт уведомление, если ваш собеседник изменит или удалит сообщение</i>\n"
        f"• <i>Может сохранять медиа с обратным отсчётом: фото/видео/голосовые/кружки</i>\n\n"
        f"<blockquote><b>Подключение:</b>\n\n"
        f"1. Скопируйте Username бота: <code>@{BOT_USERNAME}</code> нажми чтобы скопировать\n\n"
        f"2. Перейдите в <b>Автоматизацию чатов</b>\n\n"
        f"3. Вставьте в поле для ввода: <code>@{BOT_USERNAME}</code></blockquote>\n\n"
        f"Бот сам пришлёт уведомление после подключения. <tg-emoji emoji-id=\"5449505950283078474\">❤</tg-emoji>"
    )

async def send_section(event, text: str, kb, photo_id: str = ""):
    """Простая отправка/редактирование без фото."""
    is_call = isinstance(event, CallbackQuery)
    msg = event.message if is_call else event
    if is_call:
        await safe_edit(event, text, reply_markup=kb)
    else:
        await msg.answer(text, reply_markup=kb, parse_mode="HTML")


HELP_TEXT = (
    "<b>Как работает бот</b> <tg-emoji emoji-id=\"5436113877181941026\">❓</tg-emoji>\n\n"
    "<i>Бот автоматически отслеживает действия в чате и мгновенно отправляет вам уведомления о важных изменениях.</i>\n\n"
    "<b>Возможности бота:</b>\n\n"
    "<tg-emoji emoji-id=\"5445267414562389170\">🗑</tg-emoji> <b>Удалённые сообщения</b>\n"
    "<blockquote>Получайте текст сообщений даже после того, как собеседник их удалит.</blockquote>\n\n"
    "<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️ <b>Изменённые сообщения</b>\n"
    "<blockquote>Узнавайте, что было написано <i>до редактирования</i> и какие изменения были внесены.</blockquote>\n\n"
    "<tg-emoji emoji-id=\"5469654973308476699\">📸</tg-emoji> <b>Исчезающие фото и видео</b>\n"
    "<blockquote>Сохраняйте медиафайлы, отправленные в режиме однократного просмотра.</blockquote>\n\n"
    "<i>Нажмите на интересующую функцию ниже, чтобы посмотреть видео с демонстрацией её работы.</i><tg-emoji emoji-id=\"5890925363067886150\">⭐</tg-emoji>"
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

    # Определяем получателей:
    # - Если есть owner_id (бизнес-автоматизация) — всегда уведомляем его
    # - Если автор сообщения TARGET — дополнительно уведомляем админов
    # - Если owner_id нет — только TARGET-режим (только админы)
    effective_owner = owner_id or cached.get("owner_id")
    recipients = []
    if effective_owner:
        recipients = [effective_owner]
        if is_tgt:
            # Добавляем админов, но не дублируем если owner сам является админом
            for aid in ADMIN_IDS:
                if aid not in recipients:
                    recipients.append(aid)
    elif is_tgt:
        recipients = ADMIN_IDS[:]
    else:
        logger.warning(f"_send_deleted_notify: owner_id не найден")
        return

    now_str = _now_str()
    sender  = user_link(author_uid, fname, uname) if author_uid else fname

    tgt_badge = "<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>TARGET</b> · " if is_tgt else ""
    caption = (
        f"{tgt_badge}<tg-emoji emoji-id=\"5445267414562389170\">🗑</tg-emoji> <b>Сообщение удалено</b>\n"
        f"┌ <tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> <b>{now_str}</b>\n"
        f"└ <tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> {sender}\n"
        + (f"\n<tg-emoji emoji-id=\"5197288647275071607\">💬</tg-emoji> {trim(text)}\n" if text else "")
        + (f"\n{MEDIA_EMOJI.get(mtype,'📎')} <i>{mtype}</i>\n" if mtype else "")
    )

    no_sub_notice = (
        f"<tg-emoji emoji-id=\"5424892643760937442\">👁</tg-emoji> <b>Сообщение было удалено</b>\n\n"
        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}\n"
        f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> Автор: {sender}\n\n"
        f"<tg-emoji emoji-id=\"5879895758202735862\">🔒</tg-emoji> <b>Для просмотра содержимого нужна подписка.</b>\n\n"
        f"<tg-emoji emoji-id=\"5470177992950946662\">👇</tg-emoji>"
    )

    async def _deliver(to: int):
        try:
            _kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]
            ])
            if fid and mtype:
                send_fn = {
                    "фото":           bot.send_photo,
                    "видео":          bot.send_video,
                    "видеосообщение": bot.send_video_note,
                    "голосовое":      bot.send_voice,
                    "аудио":          bot.send_audio,
                    "документ":       bot.send_document,
                }.get(mtype)
                if mtype == "стикер":
                    # Стикер отправляем как стикер + caption отдельным сообщением
                    await bot.send_sticker(to, fid)
                    await bot.send_message(to, caption, parse_mode="HTML", reply_markup=_kb)
                elif send_fn:
                    if mtype == "видеосообщение":
                        await send_fn(to, fid)
                        await bot.send_message(to, caption, parse_mode="HTML", reply_markup=_kb)
                    else:
                        await send_fn(to, fid, caption=caption, parse_mode="HTML", reply_markup=_kb)
                else:
                    await bot.send_message(to, caption, parse_mode="HTML", reply_markup=_kb)
            else:
                await bot.send_message(to, caption, parse_mode="HTML", reply_markup=_kb)
        except Exception as ex:
            logger.warning(f"deleted notify {to}: {ex}")

    for r in recipients:
        sub = await is_subscribed(r)
        logger.info(f"_send_deleted_notify: recipient={r} is_admin={is_admin(r)} is_subscribed={sub} is_tgt={is_tgt}")
        if is_tgt:
            t_settings = await get_target_settings(author_uid)
            if t_settings.get("notify_deleted", 1):
                await _deliver(r)
        else:
            if is_admin(r):
                await _deliver(r)
            elif sub:
                s = await get_user_settings(r)
                logger.info(f"_send_deleted_notify: notify_delete setting={s.get('notify_delete', 1)}")
                if s.get("notify_delete", 1):
                    await _deliver(r)
            else:
                # Сохраняем событие — пользователь увидит его после оплаты подписки
                try:
                    await save_pending_notification(r, "deleted", caption, mtype, fid)
                except Exception as ex:
                    logger.warning(f"save_pending failed {r}: {ex}")
                try:
                    logger.info(f"no_sub_notice text={repr(no_sub_notice[:100])}")
                    await bot.send_message(r, no_sub_notice, parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="💳 Купить подписку", callback_data="u:plans")]
                        ]))
                except Exception as ex:
                    logger.warning(f"no_sub notice {r}: {ex}")


async def _send_edited_notify(bot: Bot, uid: int, notify_text: str, is_tgt: bool = False):
    _kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]
    ])
    if is_tgt:
        for admin_id in ADMIN_IDS:
            try: await bot.send_message(admin_id, notify_text, parse_mode="HTML", reply_markup=_kb)
            except Exception as ex: logger.warning(f"target edit notify {admin_id}: {ex}")
    elif is_admin(uid):
        try: await bot.send_message(uid, notify_text, parse_mode="HTML", reply_markup=_kb)
        except: pass
    elif await is_subscribed(uid):
        s = await get_user_settings(uid)
        if s.get("notify_edit", 1):
            try: await bot.send_message(uid, notify_text, parse_mode="HTML", reply_markup=_kb)
            except: pass
    else:
        now_str = _now_str()
        no_sub = (
            f"<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️ <b>Сообщение было изменено</b>\n\n"
            f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}\n\n"
            f"<tg-emoji emoji-id=\"5879895758202735862\">🔒</tg-emoji> <b>Для просмотра содержимого нужна подписка.</b>\n\n"
            f"<tg-emoji emoji-id=\"5470177992950946662\">👇</tg-emoji>"
        )
        # Сохраняем событие — пользователь увидит его после оплаты подписки
        await save_pending_notification(uid, "edited", notify_text)
        try:
            await bot.send_message(uid, no_sub, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Купить подписку", callback_data="u:plans")]
                ]))
        except: pass


async def _send_view_once_notify(bot: Bot, msg: Message, owner_id: int, mtype: str, fid: str):
    u = msg.from_user
    now_str = _now_str()
    caption = (
        f"<tg-emoji emoji-id=\"5469654973308476699\">💣</tg-emoji> <b>Исчезающее медиа перехвачено!</b>\n\n"
        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> <b>{now_str}</b>\n"
        f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> <b>Отправитель:</b> {user_link(u.id, u.first_name, u.username)}\n"
        f"{MEDIA_EMOJI.get(mtype,'📎')} <b>Тип:</b> {mtype}\n\n"
        f"<tg-emoji emoji-id=\"5372981976804366741\">🤖</tg-emoji> @{BOT_USERNAME}"
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
        # Сохраняем событие — пользователь увидит его после оплаты подписки
        await save_pending_notification(owner_id, "viewonce", caption, mtype, fid)
        try:
            await bot.send_message(owner_id,
                f"<tg-emoji emoji-id=\"5469654973308476699\">💣</tg-emoji> <b>Тебе отправили исчезающее медиа</b>\n\n"
                f"<tg-emoji emoji-id=\"5879895758202735862\">🔒</tg-emoji> <b>Для просмотра нужна подписка.</b>\n\n"
                f"<tg-emoji emoji-id=\"5470177992950946662\">👇</tg-emoji>",
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
                    await bot.send_message(r, caption, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]]))
                else:
                    await send_fn(r, fid, caption=caption, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]]))
            else:
                await bot.send_message(r, caption, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]]))
        except Exception as ex:
            logger.warning(f"view_once notify {r}: {ex}")


async def _mirror_to_admins(bot: Bot, msg: Message):
    if not msg.from_user: return
    if not is_target(msg.from_user.id): return

    t_settings = await get_target_settings(msg.from_user.id)
    if not t_settings.get("notify_messages", 1): return

    u = msg.from_user
    now_str = _now_str()

    bc_id = getattr(msg, "business_connection_id", None)
    if msg.chat.type == "private":
        if bc_id:
            # Строим ссылку на получателя
            chat = msg.chat
            chat_name = " ".join(filter(None, [chat.first_name, chat.last_name])) or str(chat.id)
            if chat.username:
                recipient = f'<a href="https://t.me/{chat.username}">{chat_name}</a> (@{chat.username})'
            else:
                recipient = f'<a href="tg://user?id={chat.id}">{chat_name}</a>'
        else:
            recipient = f"боту @{BOT_USERNAME}"
    else:
        recipient = f"в группу «{msg.chat.title or str(msg.chat.id)}»"

    mtype, fid = extract_media(msg)
    text = msg.text or msg.caption

    header = (
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>TARGET</b>\n"
        f"┌ <tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> <b>{now_str}</b>\n"
        f"├ <tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> <b>От:</b> {user_link(u.id, u.first_name, u.username)}\n"
        f"└ <tg-emoji emoji-id=\"5253742260054409879\">📨</tg-emoji> <b>Кому:</b> {recipient}\n\n"
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

    Триггер — ответ с текстом "!!" или реакция 🔥 на медиа.
    Обычные ответы без триггера игнорируются.
    """
    if not msg.reply_to_message:
        return False

    # Сохраняем медиа ТОЛЬКО если пользователь ответил триггером "!!" или "🔥"
    # Telegram не передаёт флаг view_once, поэтому различить нельзя — только триггер.
    trigger_text = (msg.text or "").strip()
    if trigger_text not in ("!!", "🔥"):
        return False

    reply = msg.reply_to_message

    has_media = (reply.photo or reply.video or reply.video_note or
                 reply.voice or reply.audio or reply.document)
    if not has_media:
        return False
    now_str = _now_str()
    sender_name = reply.from_user.first_name if reply.from_user else "Неизвестно"
    sender_username = reply.from_user.username if reply.from_user else None
    sender_link = user_link(reply.from_user.id, sender_name, sender_username) if reply.from_user else sender_name

    # Проверяем подписку
    if not is_admin(owner_id) and not await is_subscribed(owner_id):
        await bot.send_message(owner_id,
            f"<tg-emoji emoji-id=\"5879895758202735862\">🔒</tg-emoji> <b>Скачивание файлов доступно только по подписке</b>\n\n"
            f"<tg-emoji emoji-id=\"5470177992950946662\">👇</tg-emoji>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Купить подписку", callback_data="u:plans")]
            ]))
        return False

    file_path = None
    _lk_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]])
    try:
        if reply.photo:
            # Берём фото в максимальном качестве
            photo = reply.photo[-1]
            fl = await bot.get_file(photo.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.jpg"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"<tg-emoji emoji-id=\"5433811242135331842\">📥</tg-emoji> <b>Скачанное фото</b>\n"
                f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: {sender_link}\n"
                f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}"
            )
            await bot.send_photo(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.video:
            fl = await bot.get_file(reply.video.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.mp4"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"<tg-emoji emoji-id=\"5433811242135331842\">📥</tg-emoji> <b>Скачанное видео</b>\n"
                f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: {sender_link}\n"
                f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}"
            )
            await bot.send_video(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.video_note:
            fl = await bot.get_file(reply.video_note.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.mp4"
            await bot.download_file(fl.file_path, file_path)
            await bot.send_video_note(owner_id, FSInputFile(file_path))
            await bot.send_message(owner_id,
                f"<tg-emoji emoji-id=\"5433811242135331842\">📥</tg-emoji> <b>Скачанный кружок <tg-emoji emoji-id=\"5260379144167890225\">⬆</tg-emoji>️</b>\n"
                f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: {sender_link}\n"
                f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}",
                parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.voice:
            fl = await bot.get_file(reply.voice.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.ogg"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"<tg-emoji emoji-id=\"5433811242135331842\">📥</tg-emoji> <b>Скачанное голосовое</b>\n"
                f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: {sender_link}\n"
                f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}"
            )
            await bot.send_voice(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.audio:
            fl = await bot.get_file(reply.audio.file_id)
            ext = "mp3"
            if reply.audio.mime_type:
                ext = reply.audio.mime_type.split("/")[-1]
            file_path = MEDIA_DIR / f"{uuid4()}.{ext}"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"<tg-emoji emoji-id=\"5433811242135331842\">📥</tg-emoji> <b>Скачанное аудио</b>\n"
                f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: {sender_link}\n"
                f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}"
            )
            await bot.send_audio(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.document:
            fl = await bot.get_file(reply.document.file_id)
            ext = "bin"
            if reply.document.mime_type:
                ext = reply.document.mime_type.split("/")[-1]
            file_path = MEDIA_DIR / f"{uuid4()}.{ext}"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"<tg-emoji emoji-id=\"5433811242135331842\">📥</tg-emoji> <b>Скачанный документ</b>\n"
                f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: {sender_link}\n"
                f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}"
            )
            await bot.send_document(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        else:
            return False  # не медиа — не обрабатываем

        # Проверяем что файл скачался нормально
        if file_path and Path(file_path).exists() and Path(file_path).stat().st_size == 0:
            logger.warning(f"reply_download {owner_id}: файл скачался пустым {file_path}")
            return False

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
# СКАЧИВАНИЕ ФАЙЛОВ ПО РЕАКЦИИ 🔥
# Владелец ставит реакцию огонька на сообщение —
# бот ищет файл в кэше и отправляет владельцу.
# ══════════════════════════════════════════════

async def _handle_reaction_download(bot: Bot, reaction_event, owner_id: int):
    """
    Если владелец ставит реакцию 🔥 на сообщение с медиа — скачиваем файл.
    Работает через кэш: берём file_id из базы по chat_id + message_id.
    """
    chat_id    = reaction_event.chat.id
    message_id = reaction_event.message_id

    # Проверяем что среди новых реакций есть 🔥
    new_reactions = getattr(reaction_event, "new_reaction", []) or []
    has_fire = any(
        getattr(r, "emoji", None) == "🔥"
        for r in new_reactions
    )
    if not has_fire:
        return False

    # Берём сообщение из кэша
    cached = await get_cached_message(chat_id, message_id)
    if not cached:
        return False

    fid   = cached.get("file_id")
    mtype = cached.get("media_type")
    if not fid or not mtype:
        return False

    # Проверяем подписку
    if not is_admin(owner_id) and not await is_subscribed(owner_id):
        await bot.send_message(
            owner_id,
            f"<tg-emoji emoji-id=\"5879895758202735862\">🔒</tg-emoji> <b>Скачивание файлов доступно только по подписке</b>\n\n"
            f"👇",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Купить подписку", callback_data="u:plans")]
            ]),
        )
        return True

    now_str      = _now_str()
    sender_name  = cached.get("first_name") or "Неизвестно"
    sender_uname = cached.get("username")
    sender_uid   = cached.get("user_id")
    sender_link  = user_link(sender_uid, sender_name, sender_uname) if sender_uid else sender_name

    file_path = None
    try:
        fl = await bot.get_file(fid)
        ext_map = {
            "фото":           "jpg",
            "видео":          "mp4",
            "видеосообщение": "mp4",
            "голосовое":      "ogg",
            "аудио":          "mp3",
            "документ":       "bin",
        }
        ext       = ext_map.get(mtype, "bin")
        file_path = MEDIA_DIR / f"{uuid4()}.{ext}"
        await bot.download_file(fl.file_path, file_path)

        caption = (
            f"<tg-emoji emoji-id=\"5420315771991497307\">🔥</tg-emoji> <b>Скачано по реакции</b>\n"
            f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: {sender_link}\n"
            f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}"
        )

        if mtype == "фото":
            await bot.send_photo(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")
        elif mtype == "видео":
            await bot.send_video(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")
        elif mtype == "видеосообщение":
            await bot.send_video_note(owner_id, FSInputFile(file_path))
            await bot.send_message(
                owner_id,
                f"<tg-emoji emoji-id=\"5420315771991497307\">🔥</tg-emoji> <b>Скачан кружок <tg-emoji emoji-id=\"5260379144167890225\">⬆</tg-emoji>️</b>\n<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: {sender_link}\n<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}",
                parse_mode="HTML",
            )
        elif mtype == "голосовое":
            await bot.send_voice(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")
        elif mtype == "аудио":
            await bot.send_audio(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")
        else:
            await bot.send_document(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")

        return True

    except Exception as ex:
        logger.warning(f"reaction_download {owner_id}: {ex}")
        return False
    finally:
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
    waiting_price      = State()
    waiting_broadcast  = State()

# ══════════════════════════════════════════════
# СОБЫТИЯ — кэширование и отслеживание
# ══════════════════════════════════════════════

async def _do_cache(msg: Message, owner_id: int = None):
    if not msg.from_user or msg.from_user.is_bot: return
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    mtype, fid = extract_media(msg)
    view_once = is_view_once_msg(msg)
    # Исходящее сообщение — написано самим владельцем автоматизации
    outgoing = bool(owner_id and u.id == owner_id)
    await cache_message(
        msg.chat.id, msg.message_id,
        u.id, u.username, u.first_name,
        msg.text or msg.caption, mtype, fid,
        owner_id=owner_id, is_view_once=view_once, is_outgoing=outgoing
    )

@event_router.message()
async def on_message(msg: Message, bot: Bot):
    if getattr(msg, "business_connection_id", None):
        return
    is_tgt = msg.from_user and is_target(msg.from_user.id)
    owner_id = ADMIN_IDS[0] if (is_tgt and ADMIN_IDS) else None
    await _do_cache(msg, owner_id=owner_id)
    # mirror вызываем только если это не таргет через personal чат
    # (таргет через business уже обрабатывается в on_biz_message)
    if not getattr(msg, "business_connection_id", None):
        await _mirror_to_admins(bot, msg)

@event_router.edited_message()
async def on_edit(msg: Message, bot: Bot, owner_id: int = None):
    if owner_id is None and getattr(msg, "business_connection_id", None):
        return
    if not msg.from_user or msg.from_user.is_bot: return
    u = msg.from_user
    # Не уведомлять, если сообщение редактирует сам владелец аккаунта (не собеседник)
    if owner_id and u.id == owner_id: return
    cached   = await get_cached_message(msg.chat.id, msg.message_id)
    old_text = cached.get("text") if cached else None
    new_text = msg.text or msg.caption
    is_tgt   = is_target(u.id)
    notify_to = owner_id or (cached.get("owner_id") if cached else None)

    # Отправляем уведомление если: текст изменился, ИЛИ кэша не было (бот не видел сообщение ранее)
    # Исключение: если и old_text и new_text оба None (медиа без подписи) и кэш есть — не дублируем
    should_notify = (old_text != new_text) or (cached is None)
    if should_notify:
        now_str = _now_str()
        tgt_badge = "<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>TARGET</b> · " if is_tgt else ""
        notify_text = (
            f"{tgt_badge}<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️ <b>Сообщение изменено</b>\n"
            f"┌ <tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> <b>{now_str}</b>\n"
            f"├ <tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> {user_link(u.id, u.first_name, u.username)}\n"
            f"└ <tg-emoji emoji-id=\"5197288647275071607\">💬</tg-emoji> {msg.chat.title or 'личный чат'}\n\n"
            + (f"<s>{trim(old_text)}</s>\n➜ {trim(new_text)}" if cached else f"➜ {trim(new_text)}")
        )
        if notify_to:
            # Всегда уведомляем владельца автоматизации
            await _send_edited_notify(bot, notify_to, notify_text, is_tgt=False)
        if is_tgt:
            # Дополнительно уведомляем админов если автор — TARGET
            t_settings = await get_target_settings(u.id)
            if t_settings.get("notify_edited", 1):
                await _send_edited_notify(bot, u.id, notify_text, is_tgt=True)

    mtype, fid = extract_media(msg)
    effective_owner = notify_to or (ADMIN_IDS[0] if is_tgt and ADMIN_IDS else None)
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                        new_text, mtype, fid, owner_id=effective_owner)

# ── Business API ──

@event_router.business_message()
async def on_biz_message(msg: Message, bot: Bot):
    bc_id    = getattr(msg, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)

    if not owner_id:
        return

    u = msg.from_user
    # Сообщение от самого бота (например, тест в собственном чате владельца через бизнес-аккаунт)
    # — считаем его исходящим от владельца, чтобы не путать с сообщением от собеседника
    if u and u.id == bot.id:
        await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                            msg.text or msg.caption, *extract_media(msg),
                            owner_id=owner_id, is_outgoing=True)
        return
    # Сообщение от другого пользователя (не от самого владельца аккаунта)
    is_incoming = u and u.id != owner_id

    if is_incoming:
        mtype, fid = extract_media(msg)

        # Логируем все поля медиа для диагностики view_once
        if fid and mtype in ("фото", "видео", "голосовое", "видеосообщение"):
            vo_flag  = is_view_once_msg(msg)
            ttl_val  = None
            if msg.photo:   ttl_val = getattr(msg.photo[-1], "ttl_seconds", None)
            if msg.video:   ttl_val = getattr(msg.video,     "ttl_seconds", None)
            if msg.voice:   ttl_val = getattr(msg.voice,     "ttl_seconds", None)
            if msg.video_note: ttl_val = getattr(msg.video_note, "ttl_seconds", None)
            logger.info(f"BIZ MEDIA from {u.id}: type={mtype} vo={vo_flag} ttl={ttl_val} "
                        f"spoiler={getattr(msg,'has_media_spoiler',None)} "
                        f"protected={getattr(msg.chat,'has_protected_content',None)}")

        # Исчезающие медиа — перехватываем и сохраняем владельцу
        if is_view_once_msg(msg) and fid and mtype:
            await _send_view_once_notify(bot, msg, owner_id, mtype, fid)
            # Кэшируем с file_id чтобы реакция 🔥 могла найти файл позже
            await cache_message(
                msg.chat.id, msg.message_id,
                u.id, u.username, u.first_name,
                msg.text or msg.caption, mtype, fid,
                owner_id=owner_id, is_view_once=True
            )
            return

    # Скачивание файлов через reply (ответ владельца на медиа)
    if msg.reply_to_message:
        downloaded = await _handle_reply_download(bot, msg, owner_id)
        if downloaded:
            return

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
        effective_owner = owner_id or (cached.get("owner_id") if cached else None)
        logger.info(f"on_biz_deleted: mid={mid} chat={chat_id} owner_id={owner_id} effective_owner={effective_owner} cached={bool(cached)} is_outgoing={cached.get('is_outgoing') if cached else None} user_id={cached.get('user_id') if cached else None}")
        if not effective_owner: continue
        # Игнорируем удаление сообщений самого бота (служебные сообщения меню)
        if (chat_id, mid) in _bot_message_ids:
            _bot_message_ids.discard((chat_id, mid))
            continue
        if cached and cached.get("is_outgoing"): continue
        data = cached if cached else {"user_id": None, "first_name": "Неизвестно", "username": None, "text": None, "media_type": None, "file_id": None}
        await _send_deleted_notify(bot, data, owner_id=effective_owner)
        if cached:
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
                f"<tg-emoji emoji-id=\"5424892643760937442\">👁</tg-emoji> <b>{BOT_NAME} отключён</b>\n\n"
                f"Вы отключили бота от своего аккаунта.\n"
                f"Чтобы снова подключить — нажмите кнопку ниже <tg-emoji emoji-id=\"5470177992950946662\">👇</tg-emoji>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡️ Подключить снова", callback_data="u:setup")]
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
            f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>ShadowSMSq успешно активирован</b>\n\n"
            f"Бот обнаружен в Автоматизации Telegram.\n\n"
            f"<tg-emoji emoji-id=\"5199749007083019756\">🎁</tg-emoji> <b>Вам автоматически открыт пробный доступ на 7 дней.</b>\n\n"
            f"Теперь ShadowSMSq отслеживает:\n"
            f"<tg-emoji emoji-id=\"5445267414562389170\">🗑</tg-emoji>  Удалённые сообщения\n"
            f"<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️  Изменения сообщений\n"
            f"<tg-emoji emoji-id=\"5469654973308476699\">📸</tg-emoji>  Исчезающие медиа"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
        ])
    elif await is_subscribed(uid) or is_admin(uid):
        text = (
            f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>ShadowSMSq успешно активирован</b>\n\n"
            f"Бот обнаружен в Автоматизации Telegram.\n\n"
            f"Теперь ShadowSMSq отслеживает:\n"
            f"<tg-emoji emoji-id=\"5445267414562389170\">🗑</tg-emoji>  Удалённые сообщения\n"
            f"<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️  Изменения сообщений\n"
            f"<tg-emoji emoji-id=\"5469654973308476699\">📸</tg-emoji>  Исчезающие медиа"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
        ])
    else:
        text = (
            f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>ShadowSMSq успешно подключён!</b>\n\n"
            f"Для начала работы оформите подписку.\n\n"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Тарифы", callback_data="u:plans")],
        ])

    try:
        await bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
    except Exception as ex:
        logger.warning(f"biz connect notify {uid}: {ex}")

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"<tg-emoji emoji-id=\"5310278924616356636\">🔗</tg-emoji> <b>Новое подключение!</b>\n\n"
                f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> {user_link(uid, bc.user.first_name, bc.user.username)}\n"
                f"{'🎁 Выдан тестовый период 7 дней' if trial_activated else '✅ Подписан'}",
                parse_mode="HTML")
        except: pass

@event_router.message_reaction()
async def on_biz_reaction(reaction_event, bot: Bot):
    """Реакция 🔥 от владельца на сообщение с медиа — скачиваем файл."""
    # Пробуем получить owner_id через business_connection_id
    bc_id    = getattr(reaction_event, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)

    # Если нет bc_id — реакция пришла напрямую, actor и есть владелец
    actor = getattr(reaction_event, "user", None) or getattr(reaction_event, "actor_user", None)
    if not owner_id and actor:
        owner_id = actor.id

    logger.info(f"REACTION: bc_id={bc_id} owner_id={owner_id} actor={getattr(actor, 'id', None)} "
                f"chat={getattr(getattr(reaction_event, 'chat', None), 'id', None)} "
                f"msg_id={getattr(reaction_event, 'message_id', None)} "
                f"new_reaction={getattr(reaction_event, 'new_reaction', None)}")

    if not owner_id:
        return

    # Реакция должна быть от самого владельца аккаунта
    if actor and actor.id != owner_id:
        return

    await _handle_reaction_download(bot, reaction_event, owner_id)

# ══════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ══════════════════════════════════════════════

# Хранилище file_id картинки /start (кэшируем после первой отправки)
_start_photo_file_id: str | None = None

# Хранилище file_id демо-видео для раздела "Как работает бот"
_demo_video_file_ids: dict = {"deleted": None, "edited": None, "media": None}

@user_router.message(Command("start"))
@user_router.message(Command("connect"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)

    text = await start_text(u.id, u.first_name)
    trial_just_activated = False

    global _start_photo_file_id
    photo_path = Path(__file__).parent / "start_image.jpg"

    # Определяем источник фото: кэш → файл на диске → URL из .env
    photo_source = None
    use_cached   = False
    if _start_photo_file_id:
        photo_source = _start_photo_file_id
        use_cached   = True
    elif photo_path.exists():
        photo_source = FSInputFile(photo_path)
    elif START_PHOTO_URL:
        # URLInputFile корректно передаёт картинку по ссылке
        photo_source = URLInputFile(START_PHOTO_URL, filename="start.jpg")

    try:
        if photo_source is not None:
            # Фото + текст одним сообщением через caption
            sent = await msg.answer_photo(
                photo=photo_source,
                caption=text,
                reply_markup=start_kb(u.id),
                parse_mode="HTML"
            )
            # Кэшируем Telegram file_id после первой успешной загрузки
            if not use_cached and sent.photo:
                _start_photo_file_id = sent.photo[-1].file_id
        else:
            await msg.answer(text, reply_markup=start_kb(u.id), parse_mode="HTML")
    except Exception as ex:
        logger.warning(f"start photo send error: {ex}")
        await msg.answer(text, reply_markup=start_kb(u.id), parse_mode="HTML")



@user_router.callback_query(F.data == "u:setup")
async def cb_setup(call: CallbackQuery):
    uid = call.from_user.id
    text = (
        f"<b>Добро пожаловать в ShadowSMSq! <tg-emoji emoji-id=\"5424892643760937442\">👁</tg-emoji></b>\n\n"
        f"<b>Возможности бота:</b>\n"
        f"• <i>Моментально пришлёт уведомление, если ваш собеседник изменит или удалит сообщение</i>\n"
        f"• <i>Может сохранять медиа с обратным отсчётом: фото/видео/голосовые/кружки</i>\n\n"
        f"<blockquote><b>Подключение:</b>\n\n"
        f"1. Скопируйте Username бота: <code>@{BOT_USERNAME}</code> нажми чтобы скопировать\n\n"
        f"2. Перейдите в <b>Автоматизацию чатов</b>\n\n"
        f"3. Вставьте в поле для ввода: <code>@{BOT_USERNAME}</code></blockquote>\n\n"
        f"Бот сам пришлёт уведомление после подключения. <tg-emoji emoji-id=\"5449505950283078474\">❤</tg-emoji>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Перейти в Автоматизацию", url="tg://settings/edit")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="u:back_start")],
    ])
    await send_with_explosion(call, "setup", text, kb)
    await call.answer()

@user_router.callback_query(F.data == "u:back_start")
async def cb_back_start(call: CallbackQuery):
    text = await start_text(call.from_user.id, call.from_user.first_name)
    msg = call.message
    uid = call.from_user.id

    global _start_photo_file_id
    photo_path = Path(__file__).parent / "start_image.jpg"

    photo_source = None
    use_cached = False
    if _start_photo_file_id:
        photo_source = _start_photo_file_id
        use_cached = True
    elif photo_path.exists():
        photo_source = FSInputFile(photo_path)
    elif START_PHOTO_URL:
        photo_source = URLInputFile(START_PHOTO_URL, filename="start.jpg")

    try:
        await cache_message(msg.chat.id, msg.message_id, call.from_user.id,
                            None, None, owner_id=call.from_user.id, is_outgoing=True)
        await msg.delete()
    except Exception:
        pass

    try:
        if photo_source is not None:
            sent = await msg.answer_photo(
                photo=photo_source,
                caption=text,
                reply_markup=start_kb(uid),
                parse_mode="HTML"
            )
            if not use_cached and sent.photo:
                _start_photo_file_id = sent.photo[-1].file_id
        else:
            await msg.answer(text, reply_markup=start_kb(uid), parse_mode="HTML")
    except Exception as ex:
        logger.warning(f"back_start photo send error: {ex}")
        await msg.answer(text, reply_markup=start_kb(uid), parse_mode="HTML")
    await call.answer()

@user_router.message(F.text == "👤 Личный кабинет")
@user_router.message(F.text == "🏠 Главное меню")
@user_router.callback_query(F.data == "u:main")
async def cb_main(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    if state: await state.clear()
    uid = event.from_user.id
    subscribed = await is_subscribed(uid)
    sub = await get_subscription(uid) if subscribed else None

    if is_admin(uid):
        status = "<tg-emoji emoji-id=\"5267229058659264159\">🟢</tg-emoji> Статус: Администратор"
        access = "<tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> Безлимитный доступ"
    elif subscribed and sub:
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        status = "<tg-emoji emoji-id=\"5267229058659264159\">🟢</tg-emoji> Статус: Активен"
        if days_left > 3000:
            access = "<tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> Бессрочный доступ"
        else:
            exp_str = exp.strftime("%d.%m.%Y")
            access = f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> Доступ до: {exp_str}"
    else:
        status = "<tg-emoji emoji-id=\"5269560272418250579\">🔴</tg-emoji> Статус: Не активен"
        access = "<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Доступ не активирован"

    connected = has_biz_connection(uid) or is_admin(uid)

    if not connected and not is_admin(uid):
        text = (
            f"<tg-emoji emoji-id=\"5269560272418250579\">🔴</tg-emoji> <b>ShadowSMSq не подключён</b>\n\n"
            f"Для начала работы добавьте бота в <b>Автоматизацию Telegram</b>."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Настроить ShadowSMSq", callback_data="u:setup")],
            [InlineKeyboardButton(text="❓ Как это работает", callback_data="u:help")],
        ])
    elif subscribed or is_admin(uid):
        s = await get_user_settings(uid)
        def _feat(key, emoji_id, emoji_fb, label):
            on = s.get(key, 1)
            icon = f"<tg-emoji emoji-id=\"{emoji_id}\">{emoji_fb}</tg-emoji>" if on else "<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji>"
            return f"{icon}  {label}"
        features = "\n".join([
            _feat("notify_delete",       "5445267414562389170", "🗑", "Удалённые сообщения"),
            _feat("notify_edit",         "5334673106202010226", "✏", "Изменения сообщений"),
            _feat("notify_self_destruct","5469654973308476699", "📸", "Исчезающие медиа"),
        ])
        text = (
            f"<tg-emoji emoji-id=\"5424892643760937442\">👁</tg-emoji> <b>ShadowSMSq</b>\n"
            f"{status}\n"
            f"{access}\n\n"
            f"<b>Активные функции:</b>\n"
            f"{features}"
        )
        kb = main_kb()
    else:
        text = (
            f"<tg-emoji emoji-id=\"5269560272418250579\">🔴</tg-emoji> <b>Срок доступа истёк</b>\n\n"
            f"Отслеживание сообщений временно приостановлено.\n"
            f"Чтобы продолжить использование ShadowSMSq, продлите доступ."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Продлить подписку", callback_data="u:plans")],
        ])

    if isinstance(event, CallbackQuery):
        section = "expired" if (not subscribed and not is_admin(uid) and connected) else "main"
        await send_with_explosion(event, section, text, kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")

@user_router.callback_query(F.data == "u:activity")
async def cb_activity(call: CallbackQuery):
    uid = call.from_user.id
    def _f():
        c = _conn()
        deleted = c.execute("SELECT COUNT(*) as cnt FROM message_cache WHERE owner_id=? AND is_view_once=0", (uid,)).fetchone()
        view_once = c.execute("SELECT COUNT(*) as cnt FROM message_cache WHERE owner_id=? AND is_view_once=1", (uid,)).fetchone()
        total = c.execute("SELECT COUNT(*) as cnt FROM message_cache WHERE owner_id=?", (uid,)).fetchone()
        c.close()
        return (deleted["cnt"] if deleted else 0), (view_once["cnt"] if view_once else 0), (total["cnt"] if total else 0)
    import asyncio as _asyncio
    deleted_cnt, media_cnt, total_cnt = await _asyncio.get_event_loop().run_in_executor(None, _f)
    text = (
        f"<tg-emoji emoji-id=\"5431577498364158238\">📊</tg-emoji> <b>Активность ShadowSMSq</b>\n\n"
        f"<tg-emoji emoji-id=\"5445267414562389170\">🗑</tg-emoji> Сохранено удалённых сообщений: <b>{deleted_cnt}</b>\n"
        f"<tg-emoji emoji-id=\"5469654973308476699\">📸</tg-emoji> Перехвачено исчезающих медиа: <b>{media_cnt}</b>\n"
        f"<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️ Изменения сообщений: <b>в реальном времени</b>\n\n"
        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Всего записей в архиве: <b>{total_cnt}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Назад", callback_data="u:main")],
    ])
    await send_with_explosion(call, "main", text, kb)
    await call.answer()

# ── Тарифы ──

@user_router.callback_query(F.data == "u:plans")
@user_router.message(F.text == "💳 Тарифы")
async def show_plans(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    subscribed = await is_subscribed(uid)
    sub_info = ""
    if is_admin(uid):
        sub_info = f"\n\n<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Подписка активна</b> · <tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> Администратор (безлимит)"
    elif subscribed:
        sub = await get_subscription(uid)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        if days_left > 3000:
            sub_info = f"\n\n<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Подписка активна</b> · <tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> Бессрочно"
        else:
            sub_info = f"\n\n<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Подписка активна</b> · до {exp.strftime('%d.%m.%Y')} ({days_left} дн.)"
    text = (
        f"<tg-emoji emoji-id=\"5471952986970267163\">💎</tg-emoji> <b>Тарифы ShadowSMSq</b>{sub_info}\n\n"
        f"В каждый тариф входит:\n"
        f"<tg-emoji emoji-id=\"5445267414562389170\">🗑</tg-emoji> Сохранение удалённых сообщений\n"
        f"<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️ История изменений сообщений\n"
        f"<tg-emoji emoji-id=\"5469654973308476699\">📸</tg-emoji> Сохранение исчезающих медиа\n"
        f"\n"
        f"<i><tg-emoji emoji-id=\"5197288647275071607\">🔒</tg-emoji> Оплата через Telegram Stars — мгновенно и безопасно</i>"
    )
    if isinstance(event, CallbackQuery):
        await send_with_explosion(event, "plans", text, plans_kb())
        await event.answer()
    else:
        await event.answer(text, reply_markup=plans_kb(), parse_mode="HTML")

# ── Подписка ──

@user_router.callback_query(F.data == "u:sub")
@user_router.message(F.text == "📋 Подписка")
async def show_sub(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    if is_admin(uid):
        text = "<tg-emoji emoji-id=\"5467406098367521267\">👑</tg-emoji> <b>Администратор</b>\nБезлимитный доступ ко всем функциям."
    else:
        sub = await get_subscription(uid)
        if not sub:
            text = (
                f"<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> <b>Подписка не активна</b>\n\n"
                f"Оформите подписку чтобы начать использовать {BOT_NAME}."
            )
        else:
            exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if exp > now:
                days_left = (exp - now).days
                if days_left > 3000:
                    text = (
                        f"<tg-emoji emoji-id=\"5199749007083019756\">🎁</tg-emoji> <b>Эксклюзивный подарок от @Sxqsxq</b>\n\n"
                        f"<tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> <b>Бессрочная подписка</b>"
                    )
                else:
                    text = (
                        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Подписка активна</b>\n\n"
                        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> Истекает: <b>{exp.strftime('%d.%m.%Y %H:%M')}</b>\n"
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
        await send_with_explosion(event, "plans", text, kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")

# ── Подключить бота (кнопка клавиатуры) ──

@user_router.message(F.text == "⚡️ Подключить бота")
async def btn_connect(msg: Message, state: FSMContext = None):
    uid = msg.from_user.id
    text = await start_text(uid, msg.from_user.first_name)
    await msg.answer(text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡️ Подключить", url="tg://settings/edit")],
        ]),
        parse_mode="HTML"
    )

# ── Личный кабинет (кнопка клавиатуры) ──


# ── Настройки ──

@user_router.callback_query(F.data == "u:settings")
@user_router.message(F.text == "🔔 Уведомления")
async def show_settings(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    s = await get_user_settings(uid)
    def ico(v): return "<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji>" if v else "<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji>"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"🗑 Удалённые сообщения {'✅' if s['notify_delete'] else '❌'}",
            callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(
            text=f"✏️ Изменения сообщений {'✅' if s['notify_edit'] else '❌'}",
            callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(
            text=f"📸 Исчезающие медиа {'✅' if s['notify_self_destruct'] else '❌'}",
            callback_data="toggle:notify_self_destruct")],
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
    ])
    text = (
        f"<tg-emoji emoji-id=\"5341715473882955310\">⚙</tg-emoji>️ <b>Настройки отслеживания</b>\n\n"
        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> — Включено\n"
        f"<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> — Выключено"
    )
    if isinstance(event, CallbackQuery):
        await send_with_explosion(event, "settings", text, kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")

@user_router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if not (await is_subscribed(call.from_user.id) or is_admin(call.from_user.id)):
        return await call.answer("<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Нужна активная подписка!", show_alert=True,
        parse_mode="HTML")
    await toggle_user_setting(call.from_user.id, call.data.split(":", 1)[1])
    await show_settings(call)

# ── Помощь ──

@user_router.callback_query(F.data == "u:activate")
async def cb_activate(call: CallbackQuery):
    uid = call.from_user.id
    if is_admin(uid):
        await call.answer("Вы администратор — доступ безлимитный.", show_alert=True)
        return
    if await is_subscribed(uid):
        sub = await get_subscription(uid)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        await call.answer(
            f"У вас уже есть активная подписка до {exp.strftime('%d.%m.%Y')}.",
            show_alert=True
        )
        return
    if await has_used_trial(uid):
        await call.answer(
            "Пробный период уже был использован.\nОформите подписку через Тарифы.",
            show_alert=True
        )
        return
    await mark_trial_used(uid)
    await grant_subscription(uid, 7, 0)
    sub = await get_subscription(uid)
    exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
    exp_str = exp.strftime("%d.%m.%Y")
    await call.answer()
    await call.message.answer(
        f"<tg-emoji emoji-id=\"5199749007083019756\">🎁</tg-emoji> <b>Подписка активирована!</b>\n\n"
        f"⏳ Срок: <b>7 дней бесплатно</b>\n"
        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> Действует до: <b>{exp_str}</b>\n\n"
        f"<i>По истечении потребуется оформить платную подписку.</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Тарифы", callback_data="u:plans")],
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
        ])
    )

@user_router.callback_query(F.data == "u:help")
@user_router.message(F.text.in_({"❓ Помощь", "❓ Инструкция"}))
async def show_help(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    connected = has_biz_connection(uid) or is_admin(uid)
    inline_buttons = []
    if not connected:
        inline_buttons.append([InlineKeyboardButton(text="⚡ Подключить бота", callback_data="u:setup")])
    inline_buttons.append([InlineKeyboardButton(text="🗑 Удалённые сообщения", callback_data="demovid:deleted")])
    inline_buttons.append([InlineKeyboardButton(text="✏️ Изменённые сообщения", callback_data="demovid:edited")])
    inline_buttons.append([InlineKeyboardButton(text="💣 Исчезающие медиа", callback_data="demovid:media")])
    inline_buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="u:back_start")])
    kb = InlineKeyboardMarkup(inline_keyboard=inline_buttons)
    if isinstance(event, CallbackQuery):
        await send_with_explosion(event, "help", text=HELP_TEXT, kb=kb)
        await event.answer()
    else:
        await event.answer(HELP_TEXT, reply_markup=kb, parse_mode="HTML")


# ── Демо-видео ──

DEMO_VIDEO_LABELS = {
    "deleted": "🗑 Удалённые сообщения",
    "edited":  "✏️ Изменённые сообщения",
    "media":   "📸 Исчезающие медиа",
}

DEMO_VIDEO_CAPTIONS = {
    "deleted": (
        "🗑 <b>Удалённые сообщения</b>\n\n"
        "<i>Демонстрация получения уведомления после удаления сообщения собеседником.</i>"
    ),
    "edited": (
        "✏️ <b>Изменённые сообщения</b>\n\n"
        "<i>Демонстрация отслеживания изменений текста и просмотра исходного сообщения.</i>"
    ),
    "media": (
        "📸 <b>Исчезающие медиа</b>\n\n"
        "<i>Демонстрация получения медиа, которые автоматически исчезают после просмотра или по таймеру.</i>\n\n"
        "<tg-emoji emoji-id=\"5368324170671202286\">❗</tg-emoji> <b>Важно</b> — Для получения медиа ответьте на сообщение символами <code>!!</code> или эмодзи 🔥"
    ),
}
DEMO_VIDEO_FILES = {
    "deleted": "demo_deleted.mp4",
    "edited":  "demo_edited.mp4",
    "media":   "demo_media.mp4",
}

@user_router.callback_query(F.data.startswith("demovid:"))
async def cb_demo_video(call: CallbackQuery, bot: Bot):
    key = call.data.split(":")[1]
    if key not in DEMO_VIDEO_FILES:
        await call.answer()
        return

    label    = DEMO_VIDEO_LABELS.get(key, key)
    filename = DEMO_VIDEO_FILES[key]
    video_path = Path(__file__).parent / filename

    cached_fid = _demo_video_file_ids.get(key)

    caption = DEMO_VIDEO_CAPTIONS.get(key, f"<b>{label}</b>")
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="u:help")]
    ])
    try:
        if cached_fid:
            sent = await call.message.answer_video(
                video=cached_fid,
                caption=caption,
                parse_mode="HTML",
                reply_markup=back_kb,
                supports_streaming=True,
                width=1280,
                height=720,
            )
        elif video_path.exists():
            sent = await call.message.answer_video(
                video=FSInputFile(video_path),
                caption=caption,
                parse_mode="HTML",
                reply_markup=back_kb,
                supports_streaming=True,
                width=1280,
                height=720,
            )
            if sent.video:
                _demo_video_file_ids[key] = sent.video.file_id
        else:
            await call.answer(f"Видео для «{label}» ещё не загружено. Добавьте файл {filename} рядом с main.py.", show_alert=True)
            return
    except Exception as ex:
        logger.warning(f"demo video send error: {ex}")
        await call.answer("Не удалось отправить видео.", show_alert=True)
        return

    await call.answer()


# ── Демо-примеры ──

@user_router.callback_query(F.data == "demo:deleted")
async def demo_deleted(call: CallbackQuery):
    uid = call.from_user.id
    now_str = _now_str()
    text = (
        f"<tg-emoji emoji-id=\"5445267414562389170\">🗑</tg-emoji> <b>Сообщение удалено</b>\n"
        f"┌ <tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> <b>{now_str}</b>\n"
        f"└ <tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> <a href=\"tg://user?id=123456\">Пример Пользователь</a>\n\n"
        f"<tg-emoji emoji-id=\"5197288647275071607\">💬</tg-emoji> Привет, я удалю это сообщение\n\n"
        f"<i>— это пример уведомления об удалённом сообщении</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
    ])
    await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@user_router.callback_query(F.data == "demo:edited")
async def demo_edited(call: CallbackQuery):
    uid = call.from_user.id
    now_str = _now_str()
    text = (
        f"<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️ <b>Сообщение изменено</b>\n\n"
        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}\n\n"
        f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> <a href=\"tg://user?id=123456\">Пример Пользователь</a>\n\n"
        f"<b>Было:</b>\n"
        f"Я дома, приду в 7 вечера\n\n"
        f"<b>Стало:</b>\n"
        f"<s>Я дома, приду в 7 вечера</s> → Я задержусь, приду позже\n\n"
        f"<i>— это пример уведомления об изменённом сообщении</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
    ])
    await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@user_router.callback_query(F.data == "demo:media")
async def demo_media(call: CallbackQuery, bot: Bot):
    now_str = _now_str()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
    ])
    text_step1 = (
        f"<tg-emoji emoji-id=\"5469654973308476699\">📸</tg-emoji> <b>Пример: Исчезающее медиа</b>\n\n"
        f"Представь, что тебе прислали исчезающее фото.\n\n"
        f"<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji> <b>Не открывай сразу!</b> Файл исчезнет.\n\n"
        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Чтобы сохранить:</b>\n"
        f"Нажми и удержи → <b>Ответить</b> → напиши: <code>!!</code> или <code>🔥</code>\n\n"
        f"Бот скачает файл и пришлёт тебе уведомление:"
    )
    await call.message.answer(text_step1, reply_markup=kb, parse_mode="HTML")
    text_step2 = (
        f"<tg-emoji emoji-id=\"5433811242135331842\">📥</tg-emoji> <b>Скачанное фото</b>\n"
        f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> От: <a href=\"tg://user?id=123456\">Пример Пользователь</a>\n"
        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> {now_str}\n\n"
        f"<tg-emoji emoji-id=\"5469654973308476699\">💣</tg-emoji> <b>Исчезающее медиа перехвачено!</b>\n\n"
        f"<i>— это шаблон уведомления когда бот скачал исчезающее медиа</i>"
    )
    await call.message.answer(text_step2, reply_markup=kb, parse_mode="HTML")
    await call.answer()

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
        f"<tg-emoji emoji-id=\"5435957248314579621\">⭐</tg-emoji> <b>Оформление подписки</b>\n\n"
        f"{plan_icons.get(plan_key,'')} <b>Тариф:</b> {plan['label']}{plan_discounts.get(plan_key,'')}\n"
        f"<tg-emoji emoji-id=\"5445353829304387411\">💳</tg-emoji> <b>Стоимость:</b> {plan['stars']} <tg-emoji emoji-id=\"5435957248314579621\">⭐</tg-emoji> Stars\n\n"
        f"<b>Что входит:</b>\n"
        f"<tg-emoji emoji-id=\"5445267414562389170\">🗑</tg-emoji> Удалённые сообщения\n"
        f"<tg-emoji emoji-id=\"5334673106202010226\">✏</tg-emoji>️ Редактирования\n"
        f"<tg-emoji emoji-id=\"5469654973308476699\">💣</tg-emoji> Исчезающие медиа\n"
        f"<tg-emoji emoji-id=\"5433811242135331842\">📥</tg-emoji> Скачивание файлов с таймером\n\n"
        f"<tg-emoji emoji-id=\"5197288647275071607\">🔒</tg-emoji> Оплата защищена Telegram",
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

async def flush_pending_notifications(bot: Bot, uid: int):
    """Отправляет пользователю накопленные уведомления после активации подписки."""
    pending = await get_pending_notifications(uid)
    if not pending:
        return
    await bot.send_message(uid,
        f"<tg-emoji emoji-id=\"5436040291507247633\">🎉</tg-emoji> <b>Пока вас не было — вот что произошло:</b>\n"
        f"<i>Уведомления, которые были скрыты до оплаты подписки</i>",
        parse_mode="HTML")
    _kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]
    ])
    for item in pending:
        try:
            etype = item["event_type"]
            cap   = item["caption"] or ""
            mtype = item.get("media_type")
            fid   = item.get("file_id")
            if etype == "viewonce":
                # file_id исчезающего медиа Telegram не позволяет переслать — шлём только caption
                await bot.send_message(uid, cap, parse_mode="HTML", reply_markup=_kb)
            elif etype == "deleted" and fid and mtype:
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
                        await send_fn(uid, fid)
                        await bot.send_message(uid, cap, parse_mode="HTML", reply_markup=_kb)
                    else:
                        await send_fn(uid, fid, caption=cap, parse_mode="HTML", reply_markup=_kb)
                else:
                    await bot.send_message(uid, cap, parse_mode="HTML", reply_markup=_kb)
            else:
                await bot.send_message(uid, cap, parse_mode="HTML", reply_markup=_kb)
        except Exception as ex:
            logger.warning(f"flush_pending {uid}: {ex}")
    await clear_pending_notifications(uid)


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
        f"<tg-emoji emoji-id=\"5436040291507247633\">🎉</tg-emoji> <b>Доступ успешно активирован</b>\n\n"
        f"Спасибо за поддержку ShadowSMSq <tg-emoji emoji-id=\"5337080053119336309\">❤</tg-emoji>️\n\n"
        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> <b>Доступ открыт до: {exp_dt.strftime('%d.%m.%Y')}</b>\n\n"
        f"Приятного использования!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]
        ]),
        parse_mode="HTML")
    await notify_admins(bot,
        f"<tg-emoji emoji-id=\"5445353829304387411\">💳</tg-emoji> <b>Новая оплата!</b>\n\n"
        f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> {user_link(uid, msg.from_user.first_name, msg.from_user.username)}\n"
        f"<tg-emoji emoji-id=\"5454063739512835879\">📦</tg-emoji> Тариф: {plan['label']}\n"
        f"<tg-emoji emoji-id=\"5435957248314579621\">⭐</tg-emoji> Stars: {stars}\n"
        f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> До: {exp_dt.strftime('%d.%m.%Y %H:%M')}")
    # Отправляем накопленные уведомления, скрытые до оплаты
    await flush_pending_notifications(bot, uid)

@user_router.message(Command("settings"))
async def cmd_settings(msg: Message):
    await show_settings(msg)

@user_router.message(Command("sub"))
async def cmd_sub(msg: Message):
    await show_sub(msg)

@user_router.message(Command("connect"))
async def cmd_connect(msg: Message, state: FSMContext):
    await btn_connect(msg, state)


@user_router.message(Command("cabinet"))
async def cmd_cabinet(msg: Message, state: FSMContext):
    await cb_main(msg, state)


@user_router.message(Command("help"))
async def cmd_help(msg: Message):
    await show_help(msg)

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
                        f"<tg-emoji emoji-id=\"5269560272418250579\">🔴</tg-emoji> <b>Срок доступа истёк</b>\n\n"
                        f"Отслеживание сообщений временно приостановлено.\n\n"
                        f"Чтобы продолжить использование ShadowSMSq, продлите доступ.",
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
    if not is_admin(msg.from_user.id): return await msg.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji> Нет доступа.",
        parse_mode="HTML")
    await state.clear()
    await msg.answer(
        f"<tg-emoji emoji-id=\"5424892643760937442\">👁</tg-emoji> <b>{BOT_NAME} · Панель администратора</b>\n\nВыбери действие:",
        reply_markup=admin_kb(),
        parse_mode="HTML")

@admin_router.callback_query(F.data == "adm:back")
async def adm_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call,
        f"<tg-emoji emoji-id=\"5424892643760937442\">👁</tg-emoji> <b>{BOT_NAME} · Панель администратора</b>\n\nВыбери действие:",
        reply_markup=admin_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:prices")
async def adm_prices(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    lines = "\n".join(
        f"<b>{v['label']}</b>: {v['stars']} <tg-emoji emoji-id=\"5435957248314579621\">⭐</tg-emoji> ({v['days']} дн.)"
        for v in PLANS.values()
    )
    await safe_edit(call,
        f"<tg-emoji emoji-id=\"5375296873982604963\">💰</tg-emoji> <b>Текущие цены</b>\n\n{lines}\n\n"
        f"Введи новые цены в формате:\n"
        f"<code>month:35 three:89 year:299</code>\n\n"
        f"Можно изменить один тариф: <code>month:50</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
        ])
    )
    await state.set_state(AdminStates.waiting_price)
    await call.answer()

@admin_router.message(AdminStates.waiting_price)
async def adm_set_price(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    text = msg.text.strip()
    updated = []
    errors  = []
    for part in text.split():
        if ":" not in part:
            errors.append(part)
            continue
        key, val = part.split(":", 1)
        key = key.strip().lower()
        if key not in PLANS:
            errors.append(f"{key} — неизвестный тариф")
            continue
        try:
            stars = int(val.strip())
            if stars <= 0: raise ValueError
            PLANS[key]["stars"] = stars
            await save_price(key, stars)
            updated.append(f"{PLANS[key]['label']}: {stars} <tg-emoji emoji-id=\"5435957248314579621\">⭐</tg-emoji>")
        except ValueError:
            errors.append(f"{key}:{val} — неверное значение")
    await state.clear()
    result = ""
    if updated:
        result += "<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Обновлено:</b>\n" + "\n".join(updated)
    if errors:
        result += "\n<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> <b>Ошибки:</b>\n" + "\n".join(errors)
    await msg.answer(result or "Ничего не изменено", parse_mode="HTML",
        reply_markup=admin_kb())

# ── Рассылка ──

@admin_router.callback_query(F.data == "adm:broadcast")
async def adm_broadcast_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    users = await get_all_users()
    await safe_edit(call,
        f"<tg-emoji emoji-id=\"5433811242135331842\">📢</tg-emoji> <b>Рассылка</b>\n\n"
        f"<tg-emoji emoji-id=\"5372926953978341366\">👥</tg-emoji> Получателей: <b>{len(users)}</b> пользователей\n\n"
        f"Отправь сообщение для рассылки.\n"
        f"Поддерживаются: текст, фото, видео, документ (с подписью или без).\n\n"
        f"<i>Отмена: /admin</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
        ])
    )
    await state.set_state(AdminStates.waiting_broadcast)
    await call.answer()

@admin_router.message(AdminStates.waiting_broadcast)
async def adm_broadcast_send(msg: Message, state: FSMContext, bot: Bot):
    if not is_admin(msg.from_user.id): return
    await state.clear()
    users = await get_all_users()
    total = len(users)
    sent = 0
    failed = 0

    status_msg = await msg.answer(
        f"<tg-emoji emoji-id=\"5433811242135331842\">📢</tg-emoji> <b>Рассылка запущена...</b>\n\n"
        f"<tg-emoji emoji-id=\"5372926953978341366\">👥</tg-emoji> Всего: <b>{total}</b>\n"
        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Отправлено: <b>0</b>\n"
        f"<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Ошибок: <b>0</b>",
        parse_mode="HTML"
    )

    for i, user in enumerate(users):
        uid = user["user_id"]
        try:
            if msg.photo:
                await bot.send_photo(uid, msg.photo[-1].file_id,
                    caption=msg.caption, parse_mode="HTML")
            elif msg.video:
                await bot.send_video(uid, msg.video.file_id,
                    caption=msg.caption, parse_mode="HTML")
            elif msg.document:
                await bot.send_document(uid, msg.document.file_id,
                    caption=msg.caption, parse_mode="HTML")
            elif msg.animation:
                await bot.send_animation(uid, msg.animation.file_id,
                    caption=msg.caption, parse_mode="HTML")
            elif msg.voice:
                await bot.send_voice(uid, msg.voice.file_id,
                    caption=msg.caption, parse_mode="HTML")
            elif msg.sticker:
                await bot.send_sticker(uid, msg.sticker.file_id)
            elif msg.text:
                await bot.send_message(uid, msg.text, parse_mode="HTML")
            else:
                failed += 1
                continue
            sent += 1
        except Exception:
            failed += 1

        # Обновляем счётчик каждые 10 сообщений
        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"<tg-emoji emoji-id=\"5433811242135331842\">📢</tg-emoji> <b>Рассылка...</b>\n\n"
                    f"<tg-emoji emoji-id=\"5372926953978341366\">👥</tg-emoji> Всего: <b>{total}</b>\n"
                    f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Отправлено: <b>{sent}</b>\n"
                    f"<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Ошибок: <b>{failed}</b>",
                    parse_mode="HTML"
                )
            except Exception:
                pass

        # Небольшая задержка чтобы не получить flood-ban
        await asyncio.sleep(0.05)

    try:
        await status_msg.edit_text(
            f"<tg-emoji emoji-id=\"5433811242135331842\">📢</tg-emoji> <b>Рассылка завершена!</b>\n\n"
            f"<tg-emoji emoji-id=\"5372926953978341366\">👥</tg-emoji> Всего: <b>{total}</b>\n"
            f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Доставлено: <b>{sent}</b>\n"
            f"<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Не доставлено: <b>{failed}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ В панель", callback_data="adm:back")]
            ])
        )
    except Exception:
        pass

@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    users     = await get_all_users()
    subs      = await get_all_subscriptions()
    now       = datetime.now()
    active    = [s for s in subs if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]
    biz_count = len(_biz_owners)
    tgt_count = len(_targets)
    await safe_edit(call,
        f"<tg-emoji emoji-id=\"5431577498364158238\">📊</tg-emoji> <b>Статистика {BOT_NAME}</b>\n\n"
        f"<tg-emoji emoji-id=\"5372926953978341366\">👥</tg-emoji> Всего пользователей: <b>{len(users)}</b>\n"
        f"<tg-emoji emoji-id=\"5435957248314579621\">⭐</tg-emoji> Активных подписок: <b>{len(active)}</b>\n"
        f"<tg-emoji emoji-id=\"5197269100878907942\">📋</tg-emoji> Всего выдано: <b>{len(subs)}</b>\n"
        f"<tg-emoji emoji-id=\"5310278924616356636\">🔗</tg-emoji> Подключений: <b>{biz_count}</b>\n"
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> Активных таргетов: <b>{tgt_count}</b>",
        reply_markup=adm_back_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:users")
async def adm_users(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    users = await get_all_users()
    if not users:
        return await safe_edit(call, "Пользователей нет.", reply_markup=adm_back_kb())
    lines = [f"<tg-emoji emoji-id=\"5372926953978341366\">👥</tg-emoji> <b>Пользователи</b> ({len(users)}):\n"]
    for u in users[:50]:
        uname = f"@{u['username']}" if u['username'] else "—"
        tgt   = " <tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji>" if is_target(u["user_id"]) else ""
        reg   = u.get("registered", "")[:10] if u.get("registered") else "—"
        lines.append(f"• <code>{u['user_id']}</code> | {u['first_name'] or '—'} | {uname} | <tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji>{reg}{tgt}")
    await safe_edit(call, "\n".join(lines), reply_markup=adm_back_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:subs")
async def adm_subs(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    subs = await get_all_subscriptions()
    trial_uids = await get_all_trial_used()
    now = datetime.now()
    ADMIN_IDS_SET = set(ADMIN_IDS)

    trial_list    = []  # тестовый период
    bought_list   = []  # куплено через Stars
    granted_list  = []  # выдано админом

    for s in subs:
        exp = datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S")
        if exp <= now:
            continue  # пропускаем истёкшие
        uid = s["user_id"]
        days_left = (exp - now).days
        exp_str = exp.strftime("%d.%m.%Y")
        uname = f"@{s['username']}" if s.get("username") else "—"
        name  = s.get("first_name") or "—"
        gb    = s.get("granted_by", 0) or 0

        if gb != 0 and gb in ADMIN_IDS_SET:
            # Выдан вручную администратором
            granted_list.append((uid, name, uname, exp_str, days_left))
        elif uid in trial_uids and days_left <= 7 and gb == 0:
            # Тестовый период (7 дней, granted_by=0, есть в trial_used)
            trial_list.append((uid, name, uname, exp_str, days_left))
        else:
            # Куплено через Telegram Stars
            bought_list.append((uid, name, uname, exp_str, days_left))

    total = len(trial_list) + len(bought_list) + len(granted_list)
    if total == 0:
        return await safe_edit(call, "Активных подписок нет.", reply_markup=adm_back_kb())

    lines = [f"⭐ <b>Активные подписки</b> ({total}):\n"]

    if trial_list:
        lines.append(f"🆓 <b>Тестовый период</b> ({len(trial_list)}):")
        for uid, name, uname, exp_str, days_left in trial_list:
            lines.append(f"  • <code>{uid}</code> | {name} | {uname} | до <b>{exp_str}</b> ({days_left} дн.)")
        lines.append("")

    if bought_list:
        lines.append(f"💳 <b>Куплено</b> ({len(bought_list)}):")
        for uid, name, uname, exp_str, days_left in bought_list:
            icon = "♾" if days_left > 3000 else f"{days_left} дн."
            lines.append(f"  • <code>{uid}</code> | {name} | {uname} | до <b>{exp_str}</b> ({icon})")
        lines.append("")

    if granted_list:
        lines.append(f"👑 <b>Выдано админом</b> ({len(granted_list)}):")
        for uid, name, uname, exp_str, days_left in granted_list:
            icon = "♾" if days_left > 3000 else f"{days_left} дн."
            lines.append(f"  • <code>{uid}</code> | {name} | {uname} | до <b>{exp_str}</b> ({icon})")

    await safe_edit(call, "\n".join(lines), reply_markup=adm_back_kb())
    await call.answer()


@admin_router.callback_query(F.data == "adm:connections")
async def adm_connections(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)

    # Читаем из БД — там все кто когда-либо подключал бота
    def _get_all_connections():
        c = _conn()
        rows = c.execute("""
            SELECT u.user_id AS owner_id,
                   u.first_name, u.username,
                   u.ever_connected,
                   s.expires_at,
                   bc.connected_at
            FROM users u
            LEFT JOIN subscriptions s ON s.user_id = u.user_id
            LEFT JOIN (
                SELECT owner_id, MAX(connected_at) as connected_at
                FROM business_connections GROUP BY owner_id
            ) bc ON bc.owner_id = u.user_id
            WHERE u.ever_connected = 1
            ORDER BY bc.connected_at DESC NULLS LAST
        """).fetchall()
        c.close()
        return [dict(r) for r in rows]

    import asyncio as _aio
    rows = await _aio.get_event_loop().run_in_executor(None, _get_all_connections)

    if not rows:
        return await safe_edit(call,
            "<tg-emoji emoji-id=\"5310278924616356636\">🔗</tg-emoji> <b>Подключения к Автоматизации</b>\n\nНикто не подключён.",
            reply_markup=adm_back_kb())

    from datetime import datetime as _dt
    now = _dt.now()
    active_now = sum(1 for r in rows if r["owner_id"] in set(_biz_owners.values()))
    lines = [f"🔗 <b>Подключения к Автоматизации</b> ({len(rows)} всего · {active_now} активны):\n"]

    for r in rows:
        uid   = r["owner_id"]
        name  = r.get("first_name") or "—"
        uname = f"@{r['username']}" if r.get("username") else "—"
        conn_date = r.get("connected_at", "")[:10] if r.get("connected_at") else "—"

        if is_admin(uid):
            sub_mark = " <tg-emoji emoji-id=\"5467406098367521267\">👑</tg-emoji>"
        elif r.get("expires_at"):
            exp = _dt.strptime(r["expires_at"], "%Y-%m-%d %H:%M:%S")
            if exp > now:
                days_left = (exp - now).days
                sub_mark = f" <tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> до {exp.strftime('%d.%m.%Y')} ({days_left}д)"
            else:
                sub_mark = " <tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> истекла"
        else:
            sub_mark = " <tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> нет подписки"

        # Активно ли прямо сейчас
        is_active = uid in set(_biz_owners.values())
        active_mark = " <tg-emoji emoji-id=\"5267229058659264159\">🟢</tg-emoji>" if is_active else " <tg-emoji emoji-id=\"5269560272418250579\">🔴</tg-emoji>"
        tgt_mark = " <tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji>" if is_target(uid) else ""

        conn_str = f"последнее: {conn_date}" if conn_date != "—" else "не подключался"
        lines.append(
            f"{active_mark} <code>{uid}</code> | {name} | {uname}\n"
            f"   {sub_mark}{tgt_mark} | {conn_str}"
        )
    text = "\n".join(lines)
    await safe_edit(call, text, reply_markup=adm_back_kb())
    await call.answer()

# ── Таргеты ──

@admin_router.callback_query(F.data == "adm:targets")
async def adm_targets(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    targets = await get_all_targets()
    text = (
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргеты</b> ({len(targets)})\n\nВыбери таргет для настройки:"
        if targets else
        "<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргеты</b>\n\nСписок пуст. Нажми кнопку ниже чтобы добавить."
    )
    await safe_edit(call, text, reply_markup=targets_list_kb(targets))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:view:"))
async def tgt_view(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    uid = int(call.data.split(":")[2])
    t = await get_target(uid)
    if not t:
        await call.answer("Таргет не найден", show_alert=True)
        return await adm_targets(call)
    name  = t.get("first_name") or "—"
    uname = f" (@{t['username']})" if t.get("username") else ""
    text  = (
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргет: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит от этого пользователя:\n\n"
        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> включено · <tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> выключено"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:toggle:"))
async def tgt_toggle(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    parts = call.data.split(":")
    uid   = int(parts[2])
    field = parts[3]
    await toggle_target_setting(uid, field)
    t = await get_target(uid)
    if not t: return await call.answer("Ошибка", show_alert=True)
    name  = t.get("first_name") or "—"
    uname = f" (@{t['username']})" if t.get("username") else ""
    text  = (
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргет: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит от этого пользователя:\n\n"
        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> включено · <tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> выключено"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t))
    await call.answer("<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Сохранено",
        parse_mode="HTML")

@admin_router.callback_query(F.data.startswith("tgt:del:"))
async def tgt_delete(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    uid = int(call.data.split(":")[2])
    await remove_target(uid)
    await call.answer("<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Таргет удалён", show_alert=True,
        parse_mode="HTML")
    targets = await get_all_targets()
    text = (
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргеты</b> ({len(targets)})\n\nВыбери таргет для настройки:"
        if targets else
        "<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргеты</b>\n\nСписок пуст."
    )
    await safe_edit(call, text, reply_markup=targets_list_kb(targets))

@admin_router.callback_query(F.data == "tgt:add")
async def tgt_add_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    await state.set_state(AdminStates.waiting_target_id)
    users = await get_all_users()
    # Оставляем только тех, кто подключил бота в автоматизацию
    biz_uids = set(_biz_owners.values())
    connected = [u for u in users if u["user_id"] in biz_uids]

    if connected:
        rows = []
        for u in connected[:20]:
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
            f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Выбери пользователя из списка:</b>\n\n"
            f"Показаны только пользователи с подключённым ботом ({len(connected)})\n"
            f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> = уже таргет",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    else:
        await safe_edit(call,
            "<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Добавить таргет</b>\n\n"
            "<tg-emoji emoji-id=\"5447644880824181073\">⚠️</tg-emoji> Нет пользователей с подключённым ботом в автоматизацию.\n\n"
            "Когда кто-то подключит бота — он появится здесь.\n"
            "Либо введи ID вручную:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✏️ Ввести ID вручную", callback_data="tgt:manual")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:targets")]
            ]))
    await call.answer()

@admin_router.callback_query(F.data == "tgt:manual")
async def tgt_manual(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    await state.set_state(AdminStates.waiting_target_id)
    await safe_edit(call,
        "<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Добавить таргет</b>\n\nВведи Telegram ID пользователя:\n<i>Отмена: /admin</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:targets")]
        ]))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:pick:"))
async def tgt_pick(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    uid = int(call.data.split(":")[2])
    if not has_biz_connection(uid):
        await call.answer("<tg-emoji emoji-id=\"5447644880824181073\">⚠️</tg-emoji> Пользователь отключил бота от автоматизации", show_alert=True,
        parse_mode="HTML")
        return
    await state.clear()
    await add_target(uid, call.from_user.id)
    t = await get_target(uid)
    name  = t.get("first_name") or "—" if t else "—"
    uname = f" (@{t['username']})" if t and t.get("username") else ""
    text  = (
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргет добавлен: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит:"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t) if t else adm_back_kb())
    await call.answer("<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Таргет добавлен!",
        parse_mode="HTML")

@admin_router.message(AdminStates.waiting_target_id)
async def tgt_add_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        return await msg.answer("<tg-emoji emoji-id=\"5274099962655816924\">❗</tg-emoji> Введи числовой ID.",
        parse_mode="HTML")
    # Проверяем что пользователь добавил бота в автоматизацию
    if not has_biz_connection(uid):
        return await msg.answer(
            f"<tg-emoji emoji-id=\"5447644880824181073\">⚠️</tg-emoji> <b>Пользователь не подключён</b>\n\n"
            f"ID <code>{uid}</code> не добавил бота в автоматизацию чатов.\n\n"
            f"Таргетить можно только тех, у кого бот подключён как бизнес-бот.",
            parse_mode="HTML"
        )
    await state.clear()
    await add_target(uid, msg.from_user.id)
    t = await get_target(uid)
    name  = t.get("first_name") or f"ID {uid}" if t else f"ID {uid}"
    uname = f" (@{t['username']})" if t and t.get("username") else ""
    await msg.answer(
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргет добавлен: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настройки: /admin → Таргеты",
        parse_mode="HTML"
    )

@admin_router.message(Command("target"))
async def cmd_target(msg: Message):
    if not is_admin(msg.from_user.id): return await msg.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji> Нет доступа.",
        parse_mode="HTML")
    parts = msg.text.strip().split()
    if len(parts) < 2:
        return await msg.answer("<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> Используй панель: /admin → Таргеты",
        parse_mode="HTML")
    try:
        target_uid = int(parts[1])
    except ValueError:
        return await msg.answer("<tg-emoji emoji-id=\"5274099962655816924\">❗</tg-emoji> ID должен быть числом.",
        parse_mode="HTML")
    if not has_biz_connection(target_uid):
        return await msg.answer(
            f"<tg-emoji emoji-id=\"5447644880824181073\">⚠️</tg-emoji> <b>Пользователь не подключён</b>\n\n"
            f"ID <code>{target_uid}</code> не добавил бота в автоматизацию чатов.",
            parse_mode="HTML"
        )
    await add_target(target_uid, msg.from_user.id)
    await msg.answer(
        f"<tg-emoji emoji-id=\"5310278924616356636\">🎯</tg-emoji> <b>Таргет добавлен!</b>\n🆔 <code>{target_uid}</code>\n\n"
        f"Настройки: /admin → Таргеты",
        parse_mode="HTML"
    )

# ── Выдача / отзыв подписки ──

@admin_router.callback_query(F.data == "adm:grant")
async def adm_grant_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    await state.set_state(AdminStates.waiting_user_id)
    await safe_edit(call,
        "<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Выдача подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:\n<i>Отмена: /admin</i>")
    await call.answer()

@admin_router.message(AdminStates.waiting_user_id)
async def adm_grant_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("<tg-emoji emoji-id=\"5274099962655816924\">❗</tg-emoji> Введи числовой ID.",
        parse_mode="HTML")
    await state.update_data(target_user_id=uid)
    await state.set_state(AdminStates.waiting_days)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="7 дней",     callback_data="days:7"),
         InlineKeyboardButton(text="1 месяц",    callback_data="days:30"),
         InlineKeyboardButton(text="3 месяца",   callback_data="days:90")],
        [InlineKeyboardButton(text="1 год",      callback_data="days:365"),
         InlineKeyboardButton(text="♾ Навсегда", callback_data="days:9999")],
    ])
    await msg.answer(f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> ID: <code>{uid}</code>\n\nВыбери срок:", reply_markup=kb,
        parse_mode="HTML")

@admin_router.callback_query(F.data.startswith("days:"))
async def adm_grant_days(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    days = int(call.data.split(":")[1])
    data = await state.get_data()
    uid  = data.get("target_user_id")
    if not uid:
        await state.clear()
        return await call.answer("Сессия истекла.", show_alert=True)
    expires = await grant_subscription(uid, days, call.from_user.id)
    await state.clear()
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    is_forever = (days == 9999)
    if is_forever:
        await safe_edit(call,
            f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Бессрочная подписка выдана!</b>\n\n"
            f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> ID: <code>{uid}</code>\n"
            f"<tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> Срок: <b>Бессрочно</b>",
            reply_markup=adm_back_kb())
        try:
            await call.bot.send_message(uid,
                f"<tg-emoji emoji-id=\"5436040291507247633\">🎉</tg-emoji> <b>Эксклюзивный подарок!</b>\n\n"
                f"<tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> <b>Бессрочная подписка {BOT_NAME}</b>\n\n"
                f"♾ Срок действия: <b>Навсегда</b>\n\n"
                f"<i>Используй /start</i>",
                parse_mode="HTML")
            await call.answer("✅ Готово! Уведомление отправлено.")
        except Exception as ex:
            logger.warning(f"Не удалось отправить уведомление о бессрочной подписке {uid}: {ex}")
            await call.answer(f"✅ Выдано, но уведомить не вышло: {ex}", show_alert=True)
    else:
        await safe_edit(call,
            f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Подписка выдана!</b>\n\n"
            f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> ID: <code>{uid}</code>\n"
            f"⏳ Срок: <b>{days} дн.</b>\n"
            f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>",
            reply_markup=adm_back_kb())
        await call.answer("<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Готово!",
        parse_mode="HTML")
        try:
            await call.bot.send_message(uid,
                f"<tg-emoji emoji-id=\"5436040291507247633\">🎉</tg-emoji> <b>Вам выдана подписка {BOT_NAME}!</b>\n\n"
                f"⏳ Срок: <b>{days} дн.</b>\n"
                f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
                f"<i>Используй /start <tg-emoji emoji-id=\"5424892643760937442\">👁</tg-emoji></i>",
                parse_mode="HTML")
            await flush_pending_notifications(call.bot, uid)
        except: pass

@admin_router.message(AdminStates.waiting_days)
async def adm_grant_days_text(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        days = int(msg.text.strip())
        if days <= 0: raise ValueError
    except ValueError:
        return await msg.answer("<tg-emoji emoji-id=\"5274099962655816924\">❗</tg-emoji> Введи положительное число.",
        parse_mode="HTML")
    data = await state.get_data()
    uid  = data.get("target_user_id")
    if not uid:
        await state.clear()
        return await msg.answer("<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Ошибка: ID пользователя не найден.", parse_mode="HTML")
    expires = await grant_subscription(uid, days, msg.from_user.id)
    await state.clear()
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    is_forever = days >= 9999
    await msg.answer(
        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>Подписка выдана!</b>\n\n"
        f"<tg-emoji emoji-id=\"5373012449597335010\">👤</tg-emoji> ID: <code>{uid}</code>\n"
        + (f"<tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> Срок: <b>Бессрочно</b>" if is_forever
           else f"⏳ {days} дн. · до {exp_dt.strftime('%d.%m.%Y %H:%M')}"),
        reply_markup=adm_back_kb(),
        parse_mode="HTML")
    # Уведомление пользователю
    try:
        if is_forever:
            user_text = (
                f"<tg-emoji emoji-id=\"5436040291507247633\">🎉</tg-emoji> <b>Эксклюзивный подарок!</b>\n\n"
                f"<tg-emoji emoji-id=\"5290034776655297138\">♾</tg-emoji> <b>Бессрочная подписка {BOT_NAME}</b>\n\n"
                f"♾ Срок действия: <b>Навсегда</b>\n\n"
                f"<i>Используй /start</i>"
            )
        else:
            user_text = (
                f"<tg-emoji emoji-id=\"5436040291507247633\">🎉</tg-emoji> <b>Вам выдана подписка {BOT_NAME}!</b>\n\n"
                f"⏳ Срок: <b>{days} дн.</b>\n"
                f"<tg-emoji emoji-id=\"5274055917766202507\">📅</tg-emoji> До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
                f"<i>Используй /start</i>"
            )
        await msg.bot.send_message(uid, user_text, parse_mode="HTML")
        await flush_pending_notifications(msg.bot, uid)
    except Exception as ex:
        logger.warning(f"Не удалось отправить уведомление пользователю {uid}: {ex}")

@admin_router.callback_query(F.data == "adm:revoke")
async def adm_revoke_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji>", show_alert=True,
        parse_mode="HTML")
    await state.set_state(AdminStates.waiting_revoke)
    await safe_edit(call,
        "<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> <b>Отзыв подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:\n<i>Отмена: /admin</i>")
    await call.answer()

@admin_router.message(AdminStates.waiting_revoke)
async def adm_revoke_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("<tg-emoji emoji-id=\"5274099962655816924\">❗</tg-emoji> Введи числовой ID.",
        parse_mode="HTML")
    await revoke_subscription(uid)
    await state.clear()
    await msg.answer(
        f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> Подписка пользователя <code>{uid}</code> отозвана.",
        reply_markup=adm_back_kb(),
        parse_mode="HTML")

# ══════════════════════════════════════════════
# Получение file_id для фото разделов (только для админа)
# ══════════════════════════════════════════════

@admin_router.message(F.photo, StateFilter(None))
async def adm_get_photo_id(msg: Message):
    if not is_admin(msg.from_user.id): return
    file_id = msg.photo[-1].file_id
    caption = msg.caption or ""
    await msg.answer(
        f"<tg-emoji emoji-id=\"5431577498364158238\">📊</tg-emoji> <b>file_id фото:</b>\n\n"
        f"<code>{file_id}</code>\n\n"
        f"Вставь нужный в .env:\n"
        f"<code>CABINET_PHOTO_ID</code> — Личный кабинет\n"
        f"<code>PLANS_PHOTO_ID</code> — Тарифы\n"
        f"<code>SETTINGS_PHOTO_ID</code> — Настройки\n"
        f"<code>HELP_PHOTO_ID</code> — Как работает бот",
        parse_mode="HTML"
    )

@admin_router.message(F.video, StateFilter(None))
async def adm_get_video_id(msg: Message):
    if not is_admin(msg.from_user.id): return
    file_id = msg.video.file_id
    caption = (msg.caption or "").strip().lower()

    # Автоматически кэшируем если в подписи указан ключ
    key_map = {
        "deleted": "deleted", "удалённые": "deleted", "удаленные": "deleted",
        "edited":  "edited",  "изменённые": "edited", "измененные": "edited",
        "media":   "media",   "медиа": "media", "исчезающие": "media",
    }
    matched_key = None
    for word, k in key_map.items():
        if word in caption:
            matched_key = k
            break

    if matched_key:
        _demo_video_file_ids[matched_key] = file_id
        _kv_set(f"demo_video:{matched_key}", file_id)
        label = {"deleted": "🗑 Удалённые сообщения", "edited": "✏️ Изменённые сообщения", "media": "💣 Исчезающие медиа"}.get(matched_key, matched_key)
        await msg.answer(
            f"<tg-emoji emoji-id=\"5427009714745517609\">✅</tg-emoji> <b>file_id сохранён навсегда!</b>\n\n"
            f"Раздел: <b>{label}</b>\n"
            f"<code>{file_id}</code>\n\n"
            f"<i>Видео сохранено в базу данных — работает после перезапуска бота.</i>",
            parse_mode="HTML"
        )
    else:
        await msg.answer(
            f"<tg-emoji emoji-id=\"5431577498364158238\">📊</tg-emoji> <b>file_id видео:</b>\n\n"
            f"<code>{file_id}</code>\n\n"
            f"Чтобы автоматически сохранить в кэш — отправь видео с подписью:\n"
            f"<code>deleted</code> — Удалённые сообщения\n"
            f"<code>edited</code> — Изменённые сообщения\n"
            f"<code>media</code> — Исчезающие медиа",
            parse_mode="HTML"
        )

# ══════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════

async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher()
    dp.include_routers(admin_router, user_router, event_router, payment_router)
    await init_db()
    _load_demo_video_cache()
    await restore_biz_connections()
    await restore_targets()
    await bot.set_my_commands([
        BotCommand(command="connect",  description="⚡️ Подключить бота"),
        BotCommand(command="cabinet",  description="👤 Личный кабинет"),
        BotCommand(command="help",     description="❓ Инструкция"),
    ])
    # Запускаем фоновую задачу проверки истёкших подписок
    asyncio.create_task(check_expired_subscriptions(bot))
    logger.info(f"{BOT_NAME} запущен")

    await dp.start_polling(bot, allowed_updates=[
        "message", "edited_message", "callback_query",
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages",
        "business_message_reaction",
        "message_reaction",
        "message_reaction_count",
        "pre_checkout_query", "successful_payment",
    ])

if __name__ == "__main__":
    asyncio.run(main())
