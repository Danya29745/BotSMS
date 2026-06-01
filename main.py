#!/usr/bin/env python3
"""
👁 ShadowSMSq Bot
"""

import asyncio, logging, os, sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4
from typing import Any, Awaitable, Callable, Dict

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
    Update, LabeledPrice, PreCheckoutQuery,
    BusinessConnection
)
from aiogram.types import FSInputFile

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# КОНФИГ
# ══════════════════════════════════════════════

BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
_data_dir    = os.getenv("DATA_DIR", os.getenv("DB_PATH_DIR", "/app/data"))
DB_PATH      = os.getenv("DB_PATH", os.path.join(_data_dir, "shadowwatch.db"))
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip()]
BOT_USERNAME = "ShadowSMSq_BOT"
BOT_NAME     = "ShadowSMSq"
MSG_CACHE_TTL = int(os.getenv("MESSAGE_CACHE_TTL", "86400"))
MEDIA_DIR     = Path(_data_dir) / "media"  # папка для хранения медиафайлов

PLANS = {
    "month": {"label": "1 месяц",   "days": 30,  "stars": 35,  "desc": "1 месяц"},
    "three": {"label": "3 месяца",  "days": 90,  "stars": 89,  "desc": "3 месяца"},
    "year":  {"label": "1 год",     "days": 365, "stars": 299, "desc": "1 год"},
}

# ══════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _init_db_sync():
    c = _conn(); cur = c.cursor()
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
        local_path  TEXT,
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
    # Миграция message_cache — добавляем local_path если нет
    try:
        c.execute("ALTER TABLE message_cache ADD COLUMN local_path TEXT")
        c.commit()
    except Exception:
        pass
    # Миграция targets — добавляем колонки если их нет (для старых БД)
    for col, default in [
        ("notify_messages", 1), ("notify_deleted", 1),
        ("notify_edited", 1), ("notify_viewonce", 1)
    ]:
        try:
            c.execute(f"ALTER TABLE targets ADD COLUMN {col} INTEGER DEFAULT {default}")
            c.commit()
        except Exception:
            pass
    c.commit(); c.close()

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

# ── Кэш сообщений ──

async def cache_message(chat_id, message_id, user_id, username, first_name,
                        text=None, media_type=None, file_id=None,
                        owner_id=None, is_view_once=False, local_path=None):
    def _f():
        c = _conn()
        c.execute("""INSERT OR REPLACE INTO message_cache
            (chat_id, message_id, owner_id, user_id, username, first_name,
             text, media_type, file_id, is_view_once, local_path)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (chat_id, message_id, owner_id, user_id, username, first_name,
             text, media_type, file_id, int(is_view_once), local_path))
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

# ── Настройки пользователя ──

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
    if msg.photo:      return "фото",           msg.photo[-1].file_id
    if msg.video:      return "видео",           msg.video.file_id
    if msg.video_note: return "видеосообщение",  msg.video_note.file_id
    if msg.voice:      return "голосовое",       msg.voice.file_id
    if msg.audio:      return "аудио",           msg.audio.file_id
    if msg.document:   return "документ",        msg.document.file_id
    if msg.sticker:    return "стикер",          msg.sticker.file_id
    if msg.animation:  return "анимация",        msg.animation.file_id
    return None, None

def is_view_once_msg(msg: Message) -> bool:
    if getattr(msg, "has_media_spoiler", False): return True
    if msg.photo and getattr(msg.photo[-1], "has_media_spoiler", False): return True
    if msg.video and getattr(msg.video, "has_media_spoiler", False): return True
    if msg.video_note and getattr(msg.video_note, "has_media_spoiler", False): return True
    return False

async def download_media(bot: Bot, msg: Message) -> tuple[str | None, str | None, str | None]:
    """
    Скачивает медиафайл из сообщения на диск.
    Возвращает (media_type, file_id, local_path) или (None, None, None).
    Сохраняет файл в MEDIA_DIR с уникальным именем.
    """
    ext_map = {
        "фото":          ".jpg",
        "видео":         ".mp4",
        "видеосообщение":".mp4",
        "голосовое":     ".ogg",
        "аудио":         ".mp3",
        "стикер":        ".webp",
        "анимация":      ".mp4",
    }
    mtype, fid = extract_media(msg)
    if not fid or not mtype:
        return None, None, None

    # Документ — берём расширение из mime_type
    if mtype == "документ" and msg.document:
        mime = getattr(msg.document, "mime_type", "") or ""
        ext = "." + mime.split("/")[-1] if "/" in mime else ".bin"
    else:
        ext = ext_map.get(mtype, ".bin")

    filename = f"{uuid4().hex}{ext}"
    local_path = MEDIA_DIR / filename

    try:
        fl = await bot.get_file(fid)
        await bot.download_file(fl.file_path, local_path)
        logger.info(f"Скачан файл: {local_path} ({mtype})")
        return mtype, fid, str(local_path)
    except Exception as ex:
        logger.warning(f"Ошибка скачивания медиа {fid}: {ex}")
        return mtype, fid, None  # fallback — file_id без локального файла


async def send_media_from_disk(bot: Bot, chat_id: int, local_path: str,
                                mtype: str, caption: str = None):
    """Отправляет медиа из локального файла."""
    path = Path(local_path)
    if not path.exists():
        logger.warning(f"Файл не найден на диске: {local_path}")
        return False
    try:
        f = FSInputFile(path)
        if mtype == "фото":
            await bot.send_photo(chat_id, f, caption=caption, parse_mode="HTML")
        elif mtype == "видео":
            await bot.send_video(chat_id, f, caption=caption, parse_mode="HTML")
        elif mtype == "видеосообщение":
            await bot.send_video_note(chat_id, f)
            if caption:
                await bot.send_message(chat_id, caption, parse_mode="HTML")
        elif mtype == "голосовое":
            await bot.send_voice(chat_id, f, caption=caption, parse_mode="HTML")
        elif mtype == "аудио":
            await bot.send_audio(chat_id, f, caption=caption, parse_mode="HTML")
        elif mtype == "документ":
            await bot.send_document(chat_id, f, caption=caption, parse_mode="HTML")
        elif mtype == "стикер":
            await bot.send_sticker(chat_id, f)
            if caption:
                await bot.send_message(chat_id, caption, parse_mode="HTML")
        elif mtype == "анимация":
            await bot.send_animation(chat_id, f, caption=caption, parse_mode="HTML")
        else:
            await bot.send_document(chat_id, f, caption=caption, parse_mode="HTML")
        return True
    except Exception as ex:
        logger.warning(f"Ошибка отправки из файла {local_path}: {ex}")
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

# ── Единственная постоянная клавиатура (3 кнопки снизу) ──
def main_reply_kb():
    from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💡 Для чего бот?")],
            [KeyboardButton(text="🔌 Как подключить?")],
            [KeyboardButton(text="👤 Личный кабинет")],
        ],
        resize_keyboard=True,
        persistent=True,
        input_field_placeholder="Выбери раздел 👇"
    )

# ── Главное приветственное меню — 2 инлайн-кнопки ──
def welcome_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🟢 Подключить",
            url="tg://settings/edit"
        )],
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="u:help")],
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="u:main")]
    ])

def plans_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 1 месяц  ·  35 ⭐",       callback_data="plan:month")],
        [InlineKeyboardButton(text="📦 3 месяца  ·  89 ⭐  🔥",  callback_data="plan:three")],
        [InlineKeyboardButton(text="👑 1 год  ·  299 ⭐  💎",    callback_data="plan:year")],
        [InlineKeyboardButton(text="◀️ Назад",                   callback_data="u:main")],
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

WELCOME_TEXT = (
    "👁 <b>ShadowSMSq</b>\n\n"
    "Бот показывает тебе то, что другие пытаются скрыть:\n\n"
    "🗑 <b>Удалённые сообщения</b> — увидишь даже если их стёрли\n"
    "✏️ <b>Редактирования</b> — узнаешь что было до правки\n"
    "💣 <b>Исчезающие медиа</b> — фото и видео сохраняются\n\n"
    "Чтобы начать — нажми кнопку <b>Подключить</b> ниже 👇\n\n"
    "1️⃣ Скопируй имя бота: <code>@ShadowSMSq_BOT</code>\n"
    "2️⃣ Нажми <b>🟢 Подключить</b>\n"
    "3️⃣ Найди раздел <b>Чат-боты</b> → вставь имя бота\n"
    "4️⃣ Бот пришлёт уведомление об успешном подключении ✅"
)

ABOUT_TEXT = (
    "👁 <b>Для чего нужен ShadowSMSq?</b>\n\n"
    "🗑 <b>Удалённые сообщения</b>\n"
    "Кто-то удалил сообщение? Бот уже сохранил его и пришлёт тебе с именем автора и временем удаления.\n\n"
    "✏️ <b>Редактирования</b>\n"
    "Кто-то изменил текст? Ты увидишь что было написано до правки — ничего не скроется.\n\n"
    "💣 <b>Исчезающие медиа</b>\n"
    "Фото и видео «просмотреть один раз» бот перехватывает и сохраняет до того как они исчезают.\n\n"
    "⚡️ <b>Работает в фоне</b>\n"
    "Бот подключается через автоматизацию чатов и работает незаметно — никто не узнает."
)

CONNECT_TEXT = (
    "🔌 <b>Как подключить бота?</b>\n\n"
    "1️⃣ Скопируй имя бота:\n"
    "   <code>@ShadowSMSq_BOT</code>\n\n"
    "2️⃣ Нажми кнопку <b>🟢 Подключить</b> ниже\n\n"
    "3️⃣ Откроется Telegram → найди раздел <b>Чат-боты</b>\n\n"
    "4️⃣ Вставь имя бота и нажми <b>Добавить</b>\n\n"
    "5️⃣ Бот пришлёт сообщение:\n"
    "   ✅ <i>«ShadowSMSq подключён! Пробный период 7 дней активирован»</i>\n\n"
    "❓ Если не получается — напиши в поддержку"
)

HELP_TEXT = CONNECT_TEXT  # совместимость

async def start_text(uid: int, first_name: str) -> str:
    return WELCOME_TEXT

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
    notify_to  = owner_id or cached.get("owner_id") or None
    is_tgt     = is_target(author_uid) if author_uid else False

    recipients = []
    if is_tgt:
        recipients = ADMIN_IDS[:]
    elif notify_to:
        recipients = [notify_to]
    else:
        logger.warning(f"_send_deleted_notify: owner_id не найден, cached={cached}")
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

    local_path = cached.get("local_path") if cached else None

    async def _deliver(to: int):
        try:
            if mtype:
                # Сначала пробуем отправить из локального файла (надёжнее)
                if local_path and Path(local_path).exists():
                    sent = await send_media_from_disk(bot, to, local_path, mtype, caption)
                    if sent: return
                # Fallback — по file_id
                if fid:
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
                        return
            # Если медиа нет или не удалось — просто текст
            await bot.send_message(to, caption, parse_mode="HTML")
        except Exception as ex:
            logger.warning(f"deleted notify {to}: {ex}")

    for r in recipients:
        if is_tgt:
            t_settings = await get_target_settings(author_uid)
            if t_settings.get("notify_deleted", 1):
                await _deliver(r)
        else:
            # Проверяем подписку — если нет, шлём уведомление о необходимости подписки
            if is_admin(r):
                await _deliver(r)
            elif await is_subscribed(r):
                s = await get_user_settings(r)
                if s.get("notify_delete", 1):
                    await _deliver(r)
            else:
                # Бот в автоматизации, но подписки нет — шлём заглушку
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


async def _send_view_once_notify(bot: Bot, msg: Message, owner_id: int,
                                 mtype: str, fid: str, local_path: str = None):
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
            # Приоритет — локальный файл (view_once особенно важен)
            if local_path and Path(local_path).exists():
                sent = await send_media_from_disk(bot, r, local_path, mtype, caption)
                if sent: continue
            # Fallback — file_id
            send_fn = {"фото": bot.send_photo, "видео": bot.send_video}.get(mtype)
            if send_fn and fid:
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

    # Берём cached для local_path
    cached = await get_cached_message(msg.chat.id, msg.message_id)
    local_path = cached.get("local_path") if cached else None

    for admin_id in ADMIN_IDS:
        try:
            if mtype:
                cap = header + (f"\n{trim(text)}" if text else "")
                # Сначала локальный файл
                if local_path and Path(local_path).exists():
                    sent = await send_media_from_disk(bot, admin_id, local_path, mtype, cap)
                    if sent: continue
                # Fallback file_id
                if fid:
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
                            await send_fn(admin_id, fid, caption=cap, parse_mode="HTML")
                        continue
            await bot.send_message(admin_id,
                header + (trim(text) if text else "<i>пусто</i>"), parse_mode="HTML")
        except Exception as ex:
            logger.warning(f"mirror to admin {admin_id}: {ex}")

# ══════════════════════════════════════════════
# РОУТЕРЫ
# ══════════════════════════════════════════════

user_router    = Router()
admin_router   = Router()
event_router   = Router()
payment_router = Router()

class AdminStates(StatesGroup):
    waiting_user_id   = State()
    waiting_days      = State()
    waiting_revoke    = State()
    waiting_target_id = State()

# ══════════════════════════════════════════════
# СОБЫТИЯ — кэширование и отслеживание
# ══════════════════════════════════════════════

async def _do_cache(msg: Message, owner_id: int = None, bot: Bot = None):
    if not msg.from_user or msg.from_user.is_bot: return
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    view_once = is_view_once_msg(msg)

    local_path = None
    mtype, fid = extract_media(msg)

    # Скачиваем медиа на диск для Business API сообщений
    # Это позволяет отправить файл при удалении даже если file_id устарел
    if bot and mtype and fid:
        dl_mtype, dl_fid, local_path = await download_media(bot, msg)
        if dl_mtype: mtype = dl_mtype
        if dl_fid:   fid   = dl_fid

    await cache_message(
        msg.chat.id, msg.message_id,
        u.id, u.username, u.first_name,
        msg.text or msg.caption, mtype, fid,
        owner_id=owner_id, is_view_once=view_once,
        local_path=local_path
    )

@event_router.message()
async def on_message(msg: Message, bot: Bot):
    # Пропускаем business — обрабатываются в on_biz_message
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

    # Сначала кэшируем с загрузкой на диск
    await _do_cache(msg, owner_id=owner_id, bot=bot)

    # Исчезающие медиа — уведомляем отдельно
    if is_view_once_msg(msg) and owner_id:
        cached = await get_cached_message(msg.chat.id, msg.message_id)
        mtype = cached.get("media_type") if cached else None
        local_path = cached.get("local_path") if cached else None
        fid = cached.get("file_id") if cached else None
        if mtype:
            await _send_view_once_notify(bot, msg, owner_id, mtype, fid, local_path)

    # Зеркалирование таргетов только через бизнес (без дублей)
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
                f"Ты отключил бота от своего аккаунта.\n"
                f"Чтобы снова включить — нажми кнопку ниже 👇",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡️ Подключить снова",
                                         url="https://t.me/settings/business/chatbots")]
                ]))
        except: pass
        return

    # Автоматически выдаём пробный период при первом подключении
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
            f"🎉 <b>{BOT_NAME} подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}!\n\n"
            f"🎁 <b>Пробный период активирован — 7 дней бесплатно!</b>\n"
            f"📅 Действует до: <b>{exp_str}</b>\n\n"
            f"Теперь бот следит за твоими чатами:\n"
            f"🗑 Удалённые сообщения\n"
            f"✏️ Редактирования\n"
            f"💣 Исчезающие медиа\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
    elif await is_subscribed(uid) or is_admin(uid):
        text = (
            f"✅ <b>{BOT_NAME} подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}!\n\n"
            f"Бот снова следит за твоими чатами.\n\n"
            f"🗑 Удалённые · ✏️ Редактирования · 💣 Медиа\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
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

    try:
        await bot.send_message(uid, text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Тарифы", callback_data="u:plans")],
                [InlineKeyboardButton(text="🏠 Меню",   callback_data="u:main")],
            ]))
    except Exception as ex:
        logger.warning(f"biz connect notify {uid}: {ex}")

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"🔗 <b>Новое подключение!</b>\n\n"
                f"👤 {user_link(uid, bc.user.first_name, bc.user.username)}\n"
                f"{'🎁 Выдан пробный период 7 дней' if trial_activated else '✅ Подписан'}",
                parse_mode="HTML")
        except: pass

# ══════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ══════════════════════════════════════════════

async def send_welcome(target, state: FSMContext = None):
    """Универсальная отправка главного экрана с reply-клавиатурой"""
    is_call = isinstance(target, CallbackQuery)
    if state: await state.clear()
    kb_reply = main_reply_kb()
    kb_inline = welcome_kb()
    if is_call:
        await target.message.answer(WELCOME_TEXT, reply_markup=kb_reply)
        await safe_edit(target, WELCOME_TEXT, reply_markup=kb_inline)
        await target.answer()
    else:
        await target.answer(WELCOME_TEXT, reply_markup=kb_reply)
        await target.answer(WELCOME_TEXT, reply_markup=kb_inline)

@user_router.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    # Автовыдача пробного периода при первом /start
    if not is_admin(u.id) and not await is_subscribed(u.id) and not await has_used_trial(u.id):
        await mark_trial_used(u.id)
        await grant_subscription(u.id, 7, 0)
        trial_msg = (
            f"🎁 <b>Пробный период активирован!</b>\n"
            f"⏳ 7 дней бесплатного доступа ко всем функциям\n\n"
        )
    else:
        trial_msg = ""
    await msg.answer(
        trial_msg + WELCOME_TEXT,
        reply_markup=main_reply_kb()
    )
    await msg.answer(
        "👇 <b>Быстрые действия:</b>",
        reply_markup=welcome_kb()
    )

# Любая команда "/" — показываем приветствие
@user_router.message(F.text.startswith("/"))
async def any_command(msg: Message, state: FSMContext):
    if msg.text.startswith("/admin"): return  # пропускаем — обработает admin_router
    await state.clear()
    await msg.answer(WELCOME_TEXT, reply_markup=main_reply_kb())
    await msg.answer(WELCOME_TEXT, reply_markup=welcome_kb())

@user_router.callback_query(F.data == "u:main")
async def cb_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call, WELCOME_TEXT, reply_markup=welcome_kb())
    await call.answer()

# ── Для чего бот ──

@user_router.message(F.text == "💡 Для чего бот?")
async def show_about(msg: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Подключить", url="tg://settings/edit")],
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:sub")],
    ])
    await msg.answer(ABOUT_TEXT, reply_markup=kb)

# ── Как подключить ──

@user_router.message(F.text == "🔌 Как подключить?")
@user_router.callback_query(F.data == "u:help")
async def show_connect(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Подключить", url="tg://settings/edit")],
        [InlineKeyboardButton(text="◀️ Назад",      callback_data="u:main")],
    ])
    if is_call:
        await safe_edit(event, CONNECT_TEXT, reply_markup=kb)
        await event.answer()
    else:
        await event.answer(CONNECT_TEXT, reply_markup=kb)

# ── Личный кабинет ──

@user_router.message(F.text == "👤 Личный кабинет")
@user_router.callback_query(F.data == "u:sub")
async def show_cabinet(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id

    if is_admin(uid):
        text = (
            f"👑 <b>Администратор</b>\n\n"
            f"Безлимитный доступ ко всем функциям.\n"
            f"Подписка не требуется."
        )
        kb = adm_back_kb() if is_call else None
    else:
        sub = await get_subscription(uid)
        if not sub:
            status = "❌ Нет активной подписки"
            exp_line = ""
            days_line = ""
        else:
            exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if exp > now:
                days_left = (exp - now).days
                status = "✅ Подписка активна"
                exp_line = f"\n📅 Действует до: <b>{exp.strftime('%d.%m.%Y')}</b>"
                days_line = f"\n⏳ Осталось: <b>{days_left} дн.</b>"
            else:
                status = "⏰ Подписка истекла"
                exp_line = f"\n📅 Истекла: <b>{exp.strftime('%d.%m.%Y')}</b>"
                days_line = ""

        trial_used = await has_used_trial(uid)
        text = (
            f"👤 <b>Личный кабинет</b>\n\n"
            f"🔐 Статус: <b>{status}</b>{exp_line}{days_line}\n\n"
            f"🎁 Пробный период: <b>{'использован' if trial_used else 'доступен (7 дней бесплатно)'}</b>\n\n"
            f"💳 <b>Тарифы:</b>\n"
            f"📅 1 месяц  ·  35 ⭐\n"
            f"📦 3 месяца  ·  89 ⭐  🔥\n"
            f"👑 1 год  ·  299 ⭐  💎"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Продлить подписку", callback_data="u:plans")],
            [InlineKeyboardButton(text="⚙️ Настройки уведомлений", callback_data="u:settings")],
        ])

    if is_call:
        await safe_edit(event, text, reply_markup=kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb)

# ── Настройки ──

@user_router.callback_query(F.data == "u:settings")
async def show_settings(call: CallbackQuery):
    uid = call.from_user.id
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
        [InlineKeyboardButton(text="◀️ Назад", callback_data="u:sub")],
    ])
    text = (
        f"⚙️ <b>Настройки уведомлений</b>\n\n"
        f"{ico(s['notify_delete'])} Удалённые сообщения\n"
        f"{ico(s['notify_edit'])} Редактирования\n"
        f"{ico(s['notify_self_destruct'])} Исчезающие медиа\n\n"
        f"<i>Нажми чтобы включить или выключить</i>"
    )
    await safe_edit(call, text, reply_markup=kb)
    await call.answer()

@user_router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if not (await is_subscribed(call.from_user.id) or is_admin(call.from_user.id)):
        return await call.answer("❌ Нужна активная подписка!", show_alert=True)
    await toggle_user_setting(call.from_user.id, call.data.split(":", 1)[1])
    await show_settings(call)

# ── Тарифы ──

@user_router.callback_query(F.data == "u:plans")
async def show_plans(call: CallbackQuery):
    uid = call.from_user.id
    sub = await get_subscription(uid)
    sub_info = ""
    if sub:
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        if exp > datetime.now():
            days_left = (exp - datetime.now()).days
            sub_info = f"\n\n✅ Подписка активна · до {exp.strftime('%d.%m.%Y')} ({days_left} дн.)"
    text = (
        f"💳 <b>Тарифы {BOT_NAME}</b>{sub_info}\n\n"
        f"📅 <b>1 месяц</b>  ·  35 ⭐\n"
        f"📦 <b>3 месяца</b>  ·  89 ⭐  🔥 скидка 15%\n"
        f"👑 <b>1 год</b>  ·  299 ⭐  💎 скидка 29%\n\n"
        f"🔒 Оплата через Telegram Stars — мгновенно и анонимно"
    )
    await safe_edit(call, text, reply_markup=plans_kb())
    await call.answer()

# ── Покупка ──

@user_router.callback_query(F.data.startswith("plan:"))
async def cb_plan(call: CallbackQuery, bot: Bot):
    plan_key = call.data.split(":")[1]
    uid  = call.from_user.id
    plan = PLANS.get(plan_key)
    if not plan: return await call.answer("Неизвестный тариф", show_alert=True)

    plan_icons    = {"month": "📅", "three": "📦", "year": "👑"}
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
        f"💣 Исчезающие медиа\n\n"
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
        f"🗑 · ✏️ · 💣\n\n"
        f"<i>👁 {BOT_NAME} уже следит за твоими чатами</i>",
        reply_markup=back_kb()
    )
    await notify_admins(bot,
        f"💳 <b>Новая оплата!</b>\n\n"
        f"👤 {user_link(uid, msg.from_user.first_name, msg.from_user.username)}\n"
        f"📦 Тариф: {plan['label']}\n"
        f"⭐ Stars: {stars}\n"
        f"📅 До: {exp_dt.strftime('%d.%m.%Y %H:%M')}")


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
            f"🎉 <b>Тебе выдана подписка {BOT_NAME}!</b>\n\n"
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
            f"🎉 <b>Тебе выдана подписка {BOT_NAME}!</b>\n\n"
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
    logger.info(f"{BOT_NAME} запущен")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types() + [
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages"
    ])

if __name__ == "__main__":
    asyncio.run(main())
