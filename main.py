#!/usr/bin/env python3
"""
👁 ShadowWatch Bot — исправленная версия
"""

import asyncio, logging, os, sqlite3
from datetime import datetime, timedelta
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
    ReplyKeyboardMarkup, KeyboardButton, BusinessConnection
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# КОНФИГ
# ══════════════════════════════════════════════

BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
DB_PATH      = os.getenv("DB_PATH", "shadowwatch.db")
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip()]
BOT_USERNAME = "ShadowSMSq_BOT"
MSG_CACHE_TTL = int(os.getenv("MESSAGE_CACHE_TTL", "86400"))

# ── Анимированные эмодзи ──
_AE = {
    "👁":  "5368324170671202286",
    "🗑":  "5463006522278981341",
    "✏️": "5469835553556640139",
    "💣":  "5411093492302864150",
    "🎁":  "5373058572649699086",
    "⭐":  "5361800486327202251",
    "👑":  "5361315553439728262",
    "💳":  "5471952986970267163",
    "✅":  "5368324170671202286",
    "❌":  "5447644880824181073",
    "🔔":  "5373141891321699086",
    "🎉":  "5373058572649699086",
    "🔥":  "5364580842178811152",
    "📅":  "5471931082790226428",
    "📦":  "5471968242696816270",
    "🔒":  "5373141891321699086",
}
def e(emoji: str) -> str:
    eid = _AE.get(emoji)
    return f"<tg-emoji emoji-id='{eid}'>{emoji}</tg-emoji>" if eid else emoji

# ── Тарифы ──
PLANS = {
    "trial": {"label": "Пробный период", "days": 7,   "stars": 0,   "desc": "7 дней бесплатно"},
    "month": {"label": "1 месяц",        "days": 30,  "stars": 35,  "desc": "1 месяц"},
    "three": {"label": "3 месяца",       "days": 90,  "stars": 89,  "desc": "3 месяца"},
    "year":  {"label": "1 год",          "days": 365, "stars": 299, "desc": "1 год"},
}

# ══════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════

def _conn():
    c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row; return c

def _init_db_sync():
    c = _conn(); cur = c.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
        registered TEXT DEFAULT (datetime('now')), last_seen TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS subscriptions (
        user_id INTEGER PRIMARY KEY, expires_at TEXT NOT NULL,
        granted_by INTEGER, granted_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS trial_used (
        user_id INTEGER PRIMARY KEY, used_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS message_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
        owner_id INTEGER,
        user_id INTEGER, username TEXT, first_name TEXT,
        text TEXT, media_type TEXT, file_id TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(chat_id, message_id)
    );
    CREATE TABLE IF NOT EXISTS user_settings (
        user_id INTEGER PRIMARY KEY,
        notify_delete INTEGER DEFAULT 1,
        notify_edit INTEGER DEFAULT 1,
        notify_self_destruct INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS business_connections (
        connection_id TEXT PRIMARY KEY,
        owner_id INTEGER NOT NULL,
        connected_at TEXT DEFAULT (datetime('now'))
    );
    """)
    c.commit(); c.close()

async def init_db():
    await asyncio.get_event_loop().run_in_executor(None, _init_db_sync)

def _run(fn):
    return asyncio.get_event_loop().run_in_executor(None, fn)

# ── ИСПРАВЛЕНИЕ #1: biz_owners теперь хранится в БД и кэшируется в памяти ──

# Кэш в памяти (восстанавливается из БД при старте)
_biz_owners: dict = {}

async def save_biz_connection(connection_id: str, owner_id: int):
    """Сохранить business_connection_id -> owner_id в БД и в кэш памяти"""
    _biz_owners[connection_id] = owner_id
    def _f():
        c = _conn()
        c.execute("""INSERT INTO business_connections (connection_id, owner_id)
            VALUES (?, ?)
            ON CONFLICT(connection_id) DO UPDATE SET owner_id=excluded.owner_id""",
            (connection_id, owner_id))
        c.commit(); c.close()
    await _run(_f)

async def remove_biz_connection(connection_id: str):
    """Удалить business подключение (при отключении)"""
    _biz_owners.pop(connection_id, None)
    def _f():
        c = _conn()
        c.execute("DELETE FROM business_connections WHERE connection_id=?", (connection_id,))
        c.commit(); c.close()
    await _run(_f)

async def restore_biz_connections():
    """Загрузить все сохранённые подключения из БД в память при старте бота"""
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
    """Получить owner_id по business_connection_id (только из кэша памяти)"""
    if not bc_id:
        return None
    return _biz_owners.get(bc_id)

async def resolve_biz_owner(bc_id: str | None, bot: Bot) -> int | None:
    """
    Получить owner_id: сначала из кэша памяти,
    если нет — запросить у Telegram API и сохранить в БД+память.
    Это нужно после перезапуска бота, когда _biz_owners пуст,
    а пользователи ещё не переподключились.
    """
    if not bc_id:
        return None
    # 1. Проверяем кэш памяти
    owner_id = _biz_owners.get(bc_id)
    if owner_id:
        return owner_id
    # 2. Спрашиваем Telegram API
    try:
        bc = await bot.get_business_connection(bc_id)
        if bc and bc.user:
            owner_id = bc.user.id
            await upsert_user(owner_id, bc.user.username, bc.user.first_name)
            await save_biz_connection(bc_id, owner_id)
            logger.info(f"resolve_biz_owner: восстановлено {bc_id} -> {owner_id} через API")
            return owner_id
    except Exception as ex:
        logger.warning(f"resolve_biz_owner: не удалось получить connection {bc_id}: {ex}")
    return None

# ── Остальные функции БД ──

async def upsert_user(uid, username=None, first_name=None):
    def _f():
        c = _conn()
        c.execute("""INSERT INTO users (user_id,username,first_name,last_seen) VALUES (?,?,?,datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,
            first_name=excluded.first_name,last_seen=datetime('now')""", (uid,username,first_name))
        c.commit(); c.close()
    await _run(_f)

async def get_all_users():
    def _f():
        c = _conn(); rows = c.execute("SELECT * FROM users ORDER BY registered DESC").fetchall()
        c.close(); return [dict(r) for r in rows]
    return await _run(_f)

async def grant_subscription(uid, days, granted_by):
    def _f():
        c = _conn()
        exp = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""INSERT INTO subscriptions (user_id,expires_at,granted_by) VALUES (?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET expires_at=?,granted_by=?,granted_at=datetime('now')""",
            (uid,exp,granted_by,exp,granted_by))
        c.commit(); c.close(); return exp
    return await _run(_f)

async def revoke_subscription(uid):
    def _f():
        c = _conn(); c.execute("DELETE FROM subscriptions WHERE user_id=?", (uid,)); c.commit(); c.close()
    await _run(_f)

async def is_subscribed(uid) -> bool:
    def _f():
        c = _conn(); row = c.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (uid,)).fetchone()
        c.close()
        if not row: return False
        return datetime.strptime(row["expires_at"], "%Y-%m-%d %H:%M:%S") > datetime.now()
    return await _run(_f)

async def get_subscription(uid):
    def _f():
        c = _conn(); row = c.execute("SELECT * FROM subscriptions WHERE user_id=?", (uid,)).fetchone()
        c.close(); return dict(row) if row else None
    return await _run(_f)

async def get_all_subscriptions():
    def _f():
        c = _conn()
        rows = c.execute("""SELECT s.*,u.username,u.first_name FROM subscriptions s
            LEFT JOIN users u ON u.user_id=s.user_id ORDER BY s.expires_at DESC""").fetchall()
        c.close(); return [dict(r) for r in rows]
    return await _run(_f)

async def has_used_trial(uid) -> bool:
    def _f():
        c = _conn(); row = c.execute("SELECT 1 FROM trial_used WHERE user_id=?", (uid,)).fetchone()
        c.close(); return row is not None
    return await _run(_f)

async def mark_trial_used(uid):
    def _f():
        c = _conn(); c.execute("INSERT OR IGNORE INTO trial_used (user_id) VALUES (?)", (uid,))
        c.commit(); c.close()
    await _run(_f)

async def cache_message(chat_id, message_id, user_id, username, first_name,
                        text=None, media_type=None, file_id=None, owner_id=None):
    def _f():
        c = _conn()
        c.execute("""INSERT OR REPLACE INTO message_cache
            (chat_id,message_id,owner_id,user_id,username,first_name,text,media_type,file_id)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (chat_id,message_id,owner_id,user_id,username,first_name,text,media_type,file_id))
        c.commit(); c.close()
    await _run(_f)

async def get_cached_message(chat_id, message_id):
    def _f():
        c = _conn()
        row = c.execute("SELECT * FROM message_cache WHERE chat_id=? AND message_id=?",
                        (chat_id,message_id)).fetchone()
        c.close(); return dict(row) if row else None
    return await _run(_f)

async def delete_cached_message(chat_id, message_id):
    def _f():
        c = _conn()
        c.execute("DELETE FROM message_cache WHERE chat_id=? AND message_id=?", (chat_id,message_id))
        c.commit(); c.close()
    await _run(_f)

async def get_user_settings(uid) -> dict:
    def _f():
        c = _conn()
        c.execute("INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (uid,)); c.commit()
        row = c.execute("SELECT * FROM user_settings WHERE user_id=?", (uid,)).fetchone()
        c.close(); return dict(row)
    return await _run(_f)

async def toggle_user_setting(uid, field):
    if field not in {"notify_delete","notify_edit","notify_self_destruct"}: return
    def _f():
        c = _conn()
        c.execute(f"""INSERT INTO user_settings (user_id,{field}) VALUES (?,1)
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
    if msg.photo:           return "фото",       msg.photo[-1].file_id
    if msg.video:           return "видео",       msg.video.file_id
    if msg.video_note:      return "видеосообщение", msg.video_note.file_id
    if msg.voice:           return "голосовое",   msg.voice.file_id
    if msg.audio:           return "аудио",       msg.audio.file_id
    if msg.document:        return "документ",    msg.document.file_id
    if msg.sticker:         return "стикер",      msg.sticker.file_id
    if msg.animation:       return "анимация",    msg.animation.file_id
    return None, None

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
        await call.message.edit_text(text, **kwargs)
    except Exception:
        try: await call.message.delete()
        except: pass
        await call.message.answer(text, **kwargs)

# ══════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💳 Тарифы"),     KeyboardButton(text="📋 Подписка")],
            [KeyboardButton(text="⚙️ Настройки"),  KeyboardButton(text="❓ Помощь")],
        ],
        resize_keyboard=True, persistent=True
    )

def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Тарифы",       callback_data="u:plans"),
         InlineKeyboardButton(text="📋 Подписка",      callback_data="u:sub")],
        [InlineKeyboardButton(text="⚙️ Настройки",    callback_data="u:settings"),
         InlineKeyboardButton(text="❓ Помощь",        callback_data="u:help")],
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="u:main")]
    ])

def plans_kb(trial_ok: bool):
    rows = []
    if trial_ok:
        rows.append([InlineKeyboardButton(text="🎁 Пробный период · 7 дней БЕСПЛАТНО", callback_data="plan:trial")])
    rows.append([InlineKeyboardButton(text="📅 1 месяц · 35 ⭐",           callback_data="plan:month")])
    rows.append([InlineKeyboardButton(text="📦 3 месяца · 89 ⭐  −15%",    callback_data="plan:three")])
    rows.append([InlineKeyboardButton(text="👑 1 год · 299 ⭐  −29%",      callback_data="plan:year")])
    rows.append([InlineKeyboardButton(text="◀️ Назад",                     callback_data="u:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Пользователи",  callback_data="adm:users")],
        [InlineKeyboardButton(text="⭐ Подписки",       callback_data="adm:subs")],
        [InlineKeyboardButton(text="✅ Выдать",         callback_data="adm:grant"),
         InlineKeyboardButton(text="❌ Отозвать",       callback_data="adm:revoke")],
        [InlineKeyboardButton(text="📊 Статистика",     callback_data="adm:stats")],
    ])

def adm_back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
    ])

# ══════════════════════════════════════════════
# ТЕКСТЫ
# ══════════════════════════════════════════════

async def start_text(uid: int, first_name: str) -> str:
    if is_admin(uid):
        status = f"👑 Администратор — безлимитный доступ"
    elif await is_subscribed(uid):
        sub = await get_subscription(uid)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        status = f"✅ Подписка активна · осталось {days_left} дн."
    else:
        status = f"❌ Подписка не активна"
    return (
        f"👁 <b>ShadowWatch</b>\n\n"
        f"Привет, {first_name}! 👋\n\n"
        f"<b>Статус:</b> {status}\n\n"
        f"🗑 Удалённые сообщения\n"
        f"✏️ Редактирования\n"
        f"💣 Исчезающие медиа\n\n"
        f"<i>Выбери действие 👇</i>"
    )

# ══════════════════════════════════════════════
# ИСПРАВЛЕНИЕ #2: Убираем DeleteMiddleware полностью
# (дублировала обработку deleted_business_messages с роутером)
# Middleware теперь ничего не делает с удалёнными — только пропускает дальше
# ══════════════════════════════════════════════

# (DeleteMiddleware удалена — была причиной двойных уведомлений)

# ══════════════════════════════════════════════
# ОТПРАВКА УВЕДОМЛЕНИЙ
# ══════════════════════════════════════════════

async def _send_deleted_notify(bot: Bot, cached: dict, owner_id: int = None):
    """Отправляет уведомление об удалённом сообщении владельцу аккаунта"""
    author_uid = cached.get("user_id")
    fname      = cached.get("first_name") or "Неизвестно"
    uname      = cached.get("username")
    text       = cached.get("text")
    mtype      = cached.get("media_type")
    fid        = cached.get("file_id")
    # owner_id — кому шлём уведомление
    notify_to  = owner_id or cached.get("owner_id") or None
    if not notify_to:
        logger.warning(f"_send_deleted_notify: owner_id не найден для cached={cached}")
        return
    now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
    sender  = user_link(author_uid, fname, uname) if author_uid else fname

    caption = (
        f"🗑 <b>Удалённое сообщение</b>\n\n"
        f"📅 <b>{now_str}</b>\n"
        f"👤 <b>Автор:</b> {sender}\n"
        + (f"\n💬 <b>Текст:</b>\n{trim(text)}\n" if text else "")
        + (f"\n{MEDIA_EMOJI.get(mtype,'📎')} <b>Медиа:</b> {mtype}\n" if mtype else "")
        + f"\n🤖 @{BOT_USERNAME}"
    )

    if not await should_notify(notify_to, "notify_delete"): return
    try:
        if fid and mtype:
            send = {
                "фото":           bot.send_photo,
                "видео":          bot.send_video,
                "видеосообщение": bot.send_video_note,
                "голосовое":      bot.send_voice,
                "аудио":          bot.send_audio,
                "документ":       bot.send_document,
            }.get(mtype)
            if send:
                if mtype == "видеосообщение":
                    await send(notify_to, fid)
                    await bot.send_message(notify_to, caption, parse_mode="HTML")
                else:
                    await send(notify_to, fid, caption=caption, parse_mode="HTML")
            else:
                await bot.send_message(notify_to, caption, parse_mode="HTML")
        else:
            await bot.send_message(notify_to, caption, parse_mode="HTML")
    except Exception as ex:
        logger.warning(f"deleted notify {notify_to}: {ex}")

# ══════════════════════════════════════════════
# РОУТЕРЫ
# ══════════════════════════════════════════════

user_router    = Router()
admin_router   = Router()
event_router   = Router()
payment_router = Router()

class AdminStates(StatesGroup):
    waiting_user_id = State()
    waiting_days    = State()
    waiting_revoke  = State()

# ══════════════════════════════════════════════
# СОБЫТИЯ — кэширование и отслеживание
# ══════════════════════════════════════════════

async def _do_cache(msg: Message, owner_id: int = None):
    if not msg.from_user or msg.from_user.is_bot: return
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    mtype, fid = extract_media(msg)
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                        msg.text or msg.caption, mtype, fid, owner_id=owner_id)

@event_router.message()
async def on_message(msg: Message, bot: Bot):
    await _do_cache(msg)

@event_router.edited_message()
async def on_edit(msg: Message, bot: Bot, owner_id: int = None):
    if not msg.from_user or msg.from_user.is_bot: return
    u = msg.from_user
    cached   = await get_cached_message(msg.chat.id, msg.message_id)
    old_text = cached.get("text") if cached else None
    new_text = msg.text or msg.caption
    notify_to = owner_id or (cached.get("owner_id") if cached else None)
    if old_text != new_text and notify_to:
        now_str = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
        notify  = (
            f"✏️ <b>Изменённое сообщение</b>\n\n"
            f"📅 <b>{now_str}</b>\n"
            f"👤 <b>Автор:</b> {user_link(u.id, u.first_name, u.username)}\n"
            f"💬 <b>Чат:</b> {msg.chat.title or 'личный чат'}\n\n"
            f"📝 <b>Было:</b>\n{trim(old_text)}\n\n"
            f"📝 <b>Стало:</b>\n{trim(new_text)}\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
        if await should_notify(notify_to, "notify_edit"):
            try: await bot.send_message(notify_to, notify, parse_mode="HTML")
            except Exception as ex: logger.warning(f"edit notify {notify_to}: {ex}")
    mtype, fid = extract_media(msg)
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                        new_text, mtype, fid, owner_id=notify_to)

# ── Business API ──

@event_router.business_message()
async def on_biz_message(msg: Message, bot: Bot):
    bc_id    = getattr(msg, "business_connection_id", None)
    # resolve_biz_owner — обращается к Telegram API если нет в кэше (после перезапуска)
    owner_id = await resolve_biz_owner(bc_id, bot)
    await _do_cache(msg, owner_id=owner_id)

@event_router.edited_business_message()
async def on_biz_edit(msg: Message, bot: Bot):
    bc_id    = getattr(msg, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)
    await on_edit(msg, bot, owner_id=owner_id)

# Обработка удалений ТОЛЬКО ЗДЕСЬ (не в middleware)
@event_router.deleted_business_messages()
async def on_biz_deleted(event, bot: Bot):
    chat_id = getattr(getattr(event, "chat", None), "id", None)
    if not chat_id: return
    bc_id    = getattr(event, "business_connection_id", None)
    # resolve_biz_owner автоматически восстанавливает связь через API если её нет в кэше
    owner_id = await resolve_biz_owner(bc_id, bot)
    for mid in getattr(event, "message_ids", []):
        cached = await get_cached_message(chat_id, mid)
        if not cached: continue
        # owner_id из resolve (из БД/API) приоритетнее owner_id из кэша сообщения
        effective_owner = owner_id or cached.get("owner_id")
        await _send_deleted_notify(bot, cached, owner_id=effective_owner)
        await delete_cached_message(chat_id, mid)

@event_router.business_connection()
async def on_biz_connect(bc: BusinessConnection, bot: Bot):
    uid = bc.user.id
    await upsert_user(uid, bc.user.username, bc.user.first_name)

    # ИСПРАВЛЕНИЕ #1: Сохраняем подключение в БД (переживает перезапуск)
    if hasattr(bc, "id") and bc.id:
        if bc.is_enabled:
            await save_biz_connection(bc.id, uid)
            logger.info(f"Business подключение сохранено: {bc.id} -> {uid}")
        else:
            await remove_biz_connection(bc.id)
            logger.info(f"Business подключение удалено: {bc.id}")

    if not bc.is_enabled:
        try:
            await bot.send_message(uid, "👁 ShadowWatch", reply_markup=reply_kb())
            await bot.send_message(uid,
                f"👁 <b>ShadowWatch отключён</b>\n\n"
                f"Ты отключил бота от своего аккаунта.\n"
                f"Чтобы снова включить — добавь бота в Автоматизацию чатов:\n\n"
                f"<code>{BOT_USERNAME}</code>\n\n"
                f"🤖 @{BOT_USERNAME}",
                parse_mode="HTML")
        except: pass
        return

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
            f"🗑 Удалённые сообщения\n"
            f"✏️ Редактирования\n"
            f"💣 Исчезающие медиа\n\n"
            f"<i>Используй меню ниже 👇</i>\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
    elif await is_subscribed(uid) or is_admin(uid):
        text = (
            f"👁 <b>ShadowWatch подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}! ✅\n\n"
            f"Бот успешно подключён к твоему аккаунту.\n\n"
            f"🗑 Удалённые сообщения\n"
            f"✏️ Редактирования\n"
            f"💣 Исчезающие медиа\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
    else:
        text = (
            f"👁 <b>ShadowWatch подключён!</b>\n\n"
            f"Привет, {bc.user.first_name}! 👋\n\n"
            f"Для работы нужна подписка.\n\n"
            f"💳 Оформи подписку:\n"
            f"📅 1 месяц · 35 ⭐\n"
            f"📦 3 месяца · 89 ⭐\n"
            f"👑 1 год · 299 ⭐\n\n"
            f"🤖 @{BOT_USERNAME}"
        )
    try:
        await bot.send_message(uid, "👁 ShadowWatch", reply_markup=reply_kb())
        await bot.send_message(uid, text, parse_mode="HTML", reply_markup=main_kb())
    except Exception as ex: logger.warning(f"biz connect notify {uid}: {ex}")

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"🔗 <b>Новое Business подключение!</b>\n\n"
                f"👤 {user_link(uid, bc.user.first_name, bc.user.username)}\n"
                f"{'🎁 Активирован пробный период' if trial_activated else '✅ Подписан'}",
                parse_mode="HTML")
        except: pass

# ══════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ══════════════════════════════════════════════

@user_router.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)
    text = await start_text(u.id, u.first_name)
    await msg.answer("👁", reply_markup=reply_kb())
    await msg.answer(text, reply_markup=main_kb())

@user_router.callback_query(F.data == "u:main")
async def cb_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    u = call.from_user
    text = await start_text(u.id, u.first_name)
    await safe_edit(call, text, reply_markup=main_kb())
    await call.answer()

# ── Тарифы ──

@user_router.callback_query(F.data == "u:plans")
@user_router.message(F.text == "💳 Тарифы")
async def show_plans(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    trial_ok   = not await has_used_trial(uid) and not is_admin(uid)
    subscribed = await is_subscribed(uid)
    sub_info   = ""
    if subscribed:
        sub = await get_subscription(uid)
        exp = datetime.strptime(sub["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - datetime.now()).days
        sub_info = f"\n\n✅ <b>Подписка активна</b> · до {exp.strftime('%d.%m.%Y')} ({days_left} дн.)"
    trial_note = "\n<i>Пробный период уже использован</i>" if not trial_ok and not is_admin(uid) else ""
    text = (
        f"👑 <b>Тарифы ShadowWatch</b>{sub_info}\n\n"
        + (f"🎁 <b>Пробный период</b> · 7 дней бесплатно\n\n" if trial_ok else "")
        + f"📅 <b>1 месяц</b> · 35 ⭐\n"
        + f"📦 <b>3 месяца</b> · 89 ⭐  <i>скидка 15%</i>\n"
        + f"👑 <b>1 год</b> · 299 ⭐  <i>скидка 29%</i>"
        + trial_note
        + f"\n\n<i>🔒 Оплата через Telegram Stars — мгновенно и безопасно</i>"
    )
    if is_call:
        await safe_edit(event, text, reply_markup=plans_kb(trial_ok))
        await event.answer()
    else:
        await event.answer(text, reply_markup=plans_kb(trial_ok))

# ── Подписка ──

@user_router.callback_query(F.data == "u:sub")
@user_router.message(F.text == "📋 Подписка")
async def show_sub(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    if is_admin(uid):
        text = f"👑 <b>Администратор</b>\nБезлимитный доступ ко всем функциям"
    else:
        sub = await get_subscription(uid)
        if not sub:
            text = f"❌ <b>Подписка не активна</b>\n\nОформи подписку чтобы начать использовать ShadowWatch."
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
                    f"Оформи новую подписку для продолжения."
                )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Купить / Продлить", callback_data="u:plans")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="u:main")],
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
        [InlineKeyboardButton(text=f"{ico(s['notify_delete'])} Удалённые сообщения",     callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(text=f"{ico(s['notify_edit'])} Редактирования",            callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(text=f"{ico(s['notify_self_destruct'])} Исчезающие медиа", callback_data="toggle:notify_self_destruct")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="u:main")],
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
    await toggle_user_setting(call.from_user.id, call.data.split(":",1)[1])
    await show_settings(call)

# ── Помощь ──
# ИСПРАВЛЕНИЕ #3: Убрано упоминание о платной Telegram Business подписке

HELP_TEXT = (
    f"👁 <b>Как подключить ShadowWatch</b>\n\n"
    f"<b>Шаг 1.</b> Открой настройки профиля:\n"
    f"📱 <b>iOS:</b> Профиль → Изменить профиль\n"
    f"📱 <b>Android:</b> Настройки → Аккаунт\n\n"
    f"<b>Шаг 2.</b> Найди <b>«Автоматизация чатов»</b> — прокрути вниз\n\n"
    f"<b>Шаг 3.</b> Нажми <b>«Добавить бота»</b> и введи:\n"
    f"<code>{BOT_USERNAME}</code>\n"
    f"<i>(нажми чтобы скопировать)</i>\n\n"
    f"<b>Шаг 4.</b> Нажми <b>Добавить</b> — готово! ✅\n\n"
    f"<b>Что отслеживается:</b>\n"
    f"🗑 Удалённые сообщения — пришлю копию\n"
    f"✏️ Редактирования — было и стало\n"
    f"💣 Исчезающие медиа — перехват\n\n"
    f"<i>ℹ️ Автоматизация чатов доступна всем пользователям Telegram — подписка Telegram Premium не требуется</i>"
)

@user_router.callback_query(F.data == "u:help")
@user_router.message(F.text == "❓ Помощь")
async def show_help(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="u:main")]
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

    if plan_key == "trial":
        if await has_used_trial(uid):
            return await call.answer("❌ Пробный период уже использован!", show_alert=True)
        await mark_trial_used(uid)
        expires = await grant_subscription(uid, plan["days"], 0)
        exp_dt  = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
        await safe_edit(call,
            f"🎁 <b>Пробный период активирован!</b>\n\n"
            f"⏳ Срок: <b>7 дней</b>\n"
            f"📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"✅ Все функции доступны!\n\n"
            f"<i>👁 ShadowWatch уже следит за твоими чатами</i>",
            reply_markup=back_kb())
        await call.answer("✅ Активировано!")
        await notify_admins(bot,
            f"🎁 Новый пробный период\n"
            f"👤 {user_link(uid, call.from_user.first_name, call.from_user.username)}")
        return

    plan_icons = {"month": "📅", "three": "📦", "year": "👑"}
    plan_discounts = {"month": "", "three": f"  🔥 скидка 15%", "year": f"  🔥 скидка 29%"}

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
        title=f"👁 ShadowWatch · {plan['label']}",
        description=f"Доступ ко всем функциям ShadowWatch · {plan['desc']}",
        payload=f"sub_{plan_key}_{uid}",
        currency="XTR",
        prices=[LabeledPrice(label=f"ShadowWatch · {plan['label']}", amount=plan["stars"])],
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
    payload   = msg.successful_payment.invoice_payload
    parts     = payload.split("_")
    if len(parts) < 2: return
    plan_key  = parts[1]
    uid       = msg.from_user.id
    plan      = PLANS.get(plan_key)
    if not plan: return
    expires   = await grant_subscription(uid, plan["days"], 0)
    exp_dt    = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    stars     = msg.successful_payment.total_amount
    await msg.answer(
        f"🎉 <b>Оплата прошла успешно!</b>\n\n"
        f"👑 <b>Тариф:</b> {plan['label']}\n"
        f"⭐ <b>Списано:</b> {stars} Stars\n"
        f"📅 <b>Подписка до:</b> {exp_dt.strftime('%d.%m.%Y %H:%M')}\n\n"
        f"✅ Все функции активированы!\n"
        f"🗑 · ✏️ · 💣\n\n"
        f"<i>👁 ShadowWatch уже следит за твоими чатами</i>",
        reply_markup=back_kb()
    )
    await notify_admins(bot,
        f"💳 <b>Новая оплата!</b>\n\n"
        f"👤 {user_link(uid, msg.from_user.first_name, msg.from_user.username)}\n"
        f"📦 Тариф: {plan['label']}\n"
        f"⭐ Stars: {stars}\n"
        f"📅 До: {exp_dt.strftime('%d.%m.%Y %H:%M')}")

# ── Команды ──

@user_router.message(Command("settings"))
async def cmd_settings(msg: Message):
    await show_settings(msg)

@user_router.message(Command("sub"))
async def cmd_sub(msg: Message):
    await show_sub(msg)

# ══════════════════════════════════════════════
# АДМИН ПАНЕЛЬ
# ══════════════════════════════════════════════

@admin_router.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return await msg.answer("⛔ Нет доступа.")
    await state.clear()
    await msg.answer(f"👁 <b>ShadowWatch · Панель администратора</b>\n\nВыбери действие:",
                     reply_markup=admin_kb())

@admin_router.callback_query(F.data == "adm:back")
async def adm_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call, f"👁 <b>ShadowWatch · Панель администратора</b>\n\nВыбери действие:",
                    reply_markup=admin_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users  = await get_all_users()
    subs   = await get_all_subscriptions()
    now    = datetime.now()
    active = [s for s in subs if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]
    biz_count = len(_biz_owners)
    await safe_edit(call,
        f"📊 <b>Статистика ShadowWatch</b>\n\n"
        f"👥 Всего пользователей: <b>{len(users)}</b>\n"
        f"⭐ Активных подписок: <b>{len(active)}</b>\n"
        f"📋 Всего выдано: <b>{len(subs)}</b>\n"
        f"🔗 Business подключений: <b>{biz_count}</b>",
        reply_markup=adm_back_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:users")
async def adm_users(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users = await get_all_users()
    if not users: return await safe_edit(call, "Пользователей нет.", reply_markup=adm_back_kb())
    lines = [f"👥 <b>Пользователи</b> (последние 50):\n"]
    for u in users[:50]:
        uname = f"@{u['username']}" if u['username'] else "—"
        lines.append(f"• <code>{u['user_id']}</code> | {u['first_name'] or '—'} | {uname}")
    await safe_edit(call, "\n".join(lines), reply_markup=adm_back_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:subs")
async def adm_subs(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    subs = await get_all_subscriptions()
    now  = datetime.now()
    active = [s for s in subs if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]
    if not active: return await safe_edit(call, "Активных подписок нет.", reply_markup=adm_back_kb())
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

@admin_router.callback_query(F.data == "adm:grant")
async def adm_grant_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_user_id)
    await safe_edit(call, "✅ <b>Выдача подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:\n<i>Отмена: /admin</i>")
    await call.answer()

@admin_router.message(AdminStates.waiting_user_id)
async def adm_grant_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❗ Введи числовой ID.")
    await state.update_data(target_user_id=uid)
    await state.set_state(AdminStates.waiting_days)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="7 дней",    callback_data="days:7"),
         InlineKeyboardButton(text="1 месяц",   callback_data="days:30"),
         InlineKeyboardButton(text="3 месяца",  callback_data="days:90")],
        [InlineKeyboardButton(text="1 год",     callback_data="days:365"),
         InlineKeyboardButton(text="♾ Навсегда", callback_data="days:9999")],
    ])
    await msg.answer(f"👤 ID: <code>{uid}</code>\n\nВыбери срок:", reply_markup=kb)

@admin_router.callback_query(F.data.startswith("days:"))
async def adm_grant_days(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    days = int(call.data.split(":")[1])
    data = await state.get_data()
    uid  = data.get("target_user_id")
    if not uid: await state.clear(); return await call.answer("Сессия истекла.", show_alert=True)
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
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n"
            f"📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"<i>Используй /start 👁</i>")
    except: pass

@admin_router.message(AdminStates.waiting_days)
async def adm_grant_days_text(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        days = int(msg.text.strip())
        if days <= 0: raise ValueError
    except ValueError: return await msg.answer("❗ Введи положительное число.")
    data = await state.get_data(); uid = data.get("target_user_id")
    expires = await grant_subscription(uid, days, msg.from_user.id)
    await state.clear()
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await msg.answer(
        f"✅ <b>Подписка выдана!</b>\n\n👤 ID: <code>{uid}</code>\n"
        f"⏳ {days} дн. · до {exp_dt.strftime('%d.%m.%Y %H:%M')}",
        reply_markup=adm_back_kb())
    try:
        await msg.bot.send_message(uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n📅 До: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>")
    except: pass

@admin_router.callback_query(F.data == "adm:revoke")
async def adm_revoke_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_revoke)
    await safe_edit(call, "❌ <b>Отзыв подписки</b>\n\nВведи <b>Telegram ID</b> пользователя:\n<i>Отмена: /admin</i>")
    await call.answer()

@admin_router.message(AdminStates.waiting_revoke)
async def adm_revoke_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❗ Введи числовой ID.")
    await revoke_subscription(uid)
    await state.clear()
    await msg.answer(f"✅ Подписка пользователя <code>{uid}</code> отозвана.", reply_markup=adm_back_kb())

# ══════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════

async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher()
    # ИСПРАВЛЕНИЕ #2: DeleteMiddleware убрана — не дублируем обработку удалений
    dp.include_routers(admin_router, user_router, event_router, payment_router)
    await init_db()
    # ИСПРАВЛЕНИЕ #1: Восстанавливаем business-подключения из БД при каждом старте
    await restore_biz_connections()
    logger.info("ShadowWatch запущен")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types() + [
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages"
    ])

if __name__ == "__main__":
    asyncio.run(main())
