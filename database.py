"""
База данных ShadowEye — SQLite через sqlite3
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager

DB_PATH = Path(__file__).parent.parent / "shadoweye.db"


class Database:
    def __init__(self, path: str = None):
        self.db_path = path or str(DB_PATH)
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id    INTEGER PRIMARY KEY,
                    username   TEXT    DEFAULT '',
                    first_name TEXT    DEFAULT '',
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS subscription_log (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    action     TEXT    NOT NULL,   -- 'grant' | 'revoke'
                    days       INTEGER,
                    expires_at TIMESTAMP,
                    by_admin   INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    # ── Users ──────────────────────────────────────────────────────────────────
    def upsert_user(self, user_id: int, username: str, first_name: str):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO users (user_id, username, first_name)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username   = excluded.username,
                    first_name = excluded.first_name,
                    updated_at = CURRENT_TIMESTAMP
            """, (user_id, username, first_name))

    def get_user(self, user_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if not row:
                return None
            result = dict(row)
            # Парсим datetime
            for field in ("expires_at", "created_at", "updated_at"):
                if result.get(field):
                    try:
                        result[field] = datetime.fromisoformat(result[field])
                    except Exception:
                        result[field] = None
            return result

    def get_all_users(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()
            result = []
            for row in rows:
                r = dict(row)
                for field in ("expires_at", "created_at", "updated_at"):
                    if r.get(field):
                        try:
                            r[field] = datetime.fromisoformat(r[field])
                        except Exception:
                            r[field] = None
                result.append(r)
            return result

    # ── Subscriptions ──────────────────────────────────────────────────────────
    def has_active_subscription(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if not user or not user["expires_at"]:
            return False
        return user["expires_at"] > datetime.utcnow()

    def grant_subscription(self, user_id: int, days: int, by_admin: int = None) -> datetime:
        user = self.get_user(user_id)
        now  = datetime.utcnow()

        # Если подписка ещё активна — продлеваем от текущей даты окончания
        if user and user["expires_at"] and user["expires_at"] > now:
            base = user["expires_at"]
        else:
            base = now

        expires = base + timedelta(days=days)

        # Создаём пользователя если не существует
        if not user:
            self.upsert_user(user_id, "", "")

        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET expires_at = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                (expires.isoformat(), user_id)
            )
            conn.execute(
                "INSERT INTO subscription_log (user_id, action, days, expires_at, by_admin) VALUES (?, 'grant', ?, ?, ?)",
                (user_id, days, expires.isoformat(), by_admin)
            )

        return expires

    def revoke_subscription(self, user_id: int, by_admin: int = None):
        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET expires_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                (user_id,)
            )
            conn.execute(
                "INSERT INTO subscription_log (user_id, action, by_admin) VALUES (?, 'revoke', ?)",
                (user_id, by_admin)
            )

    def get_subscription_log(self, user_id: int = None) -> list[dict]:
        with self._conn() as conn:
            if user_id:
                rows = conn.execute(
                    "SELECT * FROM subscription_log WHERE user_id = ? ORDER BY created_at DESC LIMIT 50",
                    (user_id,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM subscription_log ORDER BY created_at DESC LIMIT 100"
                ).fetchall()
            return [dict(r) for r in rows]

    # ── Stats ──────────────────────────────────────────────────────────────────
    def get_stats(self) -> dict:
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            now_iso = datetime.utcnow().isoformat()
            active = conn.execute(
                "SELECT COUNT(*) FROM users WHERE expires_at > ?", (now_iso,)
            ).fetchone()[0]
            today_start = datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat()
            today = conn.execute(
                "SELECT COUNT(*) FROM users WHERE created_at >= ?", (today_start,)
            ).fetchone()[0]
            return {
                "total":    total,
                "active":   active,
                "inactive": total - active,
                "today":    today,
            }
