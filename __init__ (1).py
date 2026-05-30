"""
SQLite база данных для ShadowWatch Bot
Таблицы: users, subscriptions, message_cache
"""

import sqlite3
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from config import DB_PATH


# ── Синхронные хелперы (выполняются в thread pool) ──────────

def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init():
    conn = _get_conn()
    c = conn.cursor()

    # Пользователи
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            registered  TEXT DEFAULT (datetime('now')),
            last_seen   TEXT DEFAULT (datetime('now'))
        )
    """)

    # Подписки
    c.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id     INTEGER PRIMARY KEY,
            expires_at  TEXT NOT NULL,
            granted_by  INTEGER,
            granted_at  TEXT DEFAULT (datetime('now'))
        )
    """)

    # Кэш сообщений (для отслеживания удалений/редактирований)
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

    # Настройки уведомлений пользователя
    c.execute("""
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id             INTEGER PRIMARY KEY,
            notify_delete       INTEGER DEFAULT 1,
            notify_edit         INTEGER DEFAULT 1,
            notify_self_destruct INTEGER DEFAULT 1
        )
    """)

    conn.commit()
    conn.close()


# ── Async обёртки ────────────────────────────────────────────

async def init_db():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _init)


# ── Пользователи ─────────────────────────────────────────────

async def upsert_user(user_id: int, username: str = None, first_name: str = None):
    def _f():
        conn = _get_conn()
        conn.execute("""
            INSERT INTO users (user_id, username, first_name, last_seen)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
                username   = excluded.username,
                first_name = excluded.first_name,
                last_seen  = datetime('now')
        """, (user_id, username, first_name))
        conn.commit()
        conn.close()
    await asyncio.get_event_loop().run_in_executor(None, _f)


async def get_all_users():
    def _f():
        conn = _get_conn()
        rows = conn.execute("SELECT * FROM users ORDER BY registered DESC").fetchall()
        conn.close()
        return [dict(r) for r in rows]
    return await asyncio.get_event_loop().run_in_executor(None, _f)


# ── Подписки ─────────────────────────────────────────────────

async def grant_subscription(user_id: int, days: int, granted_by: int):
    def _f():
        conn = _get_conn()
        expires = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""
            INSERT INTO subscriptions (user_id, expires_at, granted_by)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                expires_at = ?,
                granted_by = ?,
                granted_at = datetime('now')
        """, (user_id, expires, granted_by, expires, granted_by))
        conn.commit()
        conn.close()
        return expires
    return await asyncio.get_event_loop().run_in_executor(None, _f)


async def revoke_subscription(user_id: int):
    def _f():
        conn = _get_conn()
        conn.execute("DELETE FROM subscriptions WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    await asyncio.get_event_loop().run_in_executor(None, _f)


async def is_subscribed(user_id: int) -> bool:
    def _f():
        conn = _get_conn()
        row = conn.execute(
            "SELECT expires_at FROM subscriptions WHERE user_id = ?", (user_id,)
        ).fetchone()
        conn.close()
        if not row:
            return False
        return datetime.strptime(row["expires_at"], "%Y-%m-%d %H:%M:%S") > datetime.now()
    return await asyncio.get_event_loop().run_in_executor(None, _f)


async def get_subscription(user_id: int):
    def _f():
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM subscriptions WHERE user_id = ?", (user_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    return await asyncio.get_event_loop().run_in_executor(None, _f)


async def get_all_subscriptions():
    def _f():
        conn = _get_conn()
        rows = conn.execute("""
            SELECT s.*, u.username, u.first_name
            FROM subscriptions s
            LEFT JOIN users u ON u.user_id = s.user_id
            ORDER BY s.expires_at DESC
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    return await asyncio.get_event_loop().run_in_executor(None, _f)


# ── Кэш сообщений ────────────────────────────────────────────

async def cache_message(chat_id, message_id, user_id, username, first_name,
                         text=None, media_type=None, file_id=None):
    def _f():
        conn = _get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO message_cache
            (chat_id, message_id, user_id, username, first_name, text, media_type, file_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (chat_id, message_id, user_id, username, first_name, text, media_type, file_id))
        conn.commit()
        conn.close()
    await asyncio.get_event_loop().run_in_executor(None, _f)


async def get_cached_message(chat_id, message_id):
    def _f():
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM message_cache WHERE chat_id=? AND message_id=?",
            (chat_id, message_id)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    return await asyncio.get_event_loop().run_in_executor(None, _f)


async def delete_cached_message(chat_id, message_id):
    def _f():
        conn = _get_conn()
        conn.execute(
            "DELETE FROM message_cache WHERE chat_id=? AND message_id=?",
            (chat_id, message_id)
        )
        conn.commit()
        conn.close()
    await asyncio.get_event_loop().run_in_executor(None, _f)


async def cleanup_old_cache(ttl_seconds: int = 86400):
    """Удаляем записи старше TTL"""
    def _f():
        conn = _get_conn()
        conn.execute(
            "DELETE FROM message_cache WHERE created_at < datetime('now', ?)",
            (f"-{ttl_seconds} seconds",)
        )
        conn.commit()
        conn.close()
    await asyncio.get_event_loop().run_in_executor(None, _f)


# ── Настройки пользователя ───────────────────────────────────

async def get_user_settings(user_id: int) -> dict:
    def _f():
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM user_settings WHERE user_id=?", (user_id,)
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (user_id,)
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM user_settings WHERE user_id=?", (user_id,)
            ).fetchone()
        conn.close()
        return dict(row)
    return await asyncio.get_event_loop().run_in_executor(None, _f)


async def toggle_user_setting(user_id: int, field: str):
    allowed = {"notify_delete", "notify_edit", "notify_self_destruct"}
    if field not in allowed:
        return
    def _f():
        conn = _get_conn()
        conn.execute(f"""
            INSERT INTO user_settings (user_id, {field}) VALUES (?, 1)
            ON CONFLICT(user_id) DO UPDATE SET {field} = 1 - {field}
        """, (user_id,))
        conn.commit()
        conn.close()
    await asyncio.get_event_loop().run_in_executor(None, _f)
