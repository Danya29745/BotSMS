import os

# ── Токен бота ──────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан! Укажи его в переменных окружения.")

# ── Администратор ───────────────────────────────────────────
# Берём из env или используем дефолт
_admin_ids_raw = os.getenv("ADMIN_IDS", "7965055989")
ADMIN_IDS = [int(x.strip()) for x in _admin_ids_raw.split(",") if x.strip()]
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "@sxqsxq")

# ── База данных ─────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "shadowwatch.db")

# ── Параметры хранения сообщений (для отслеживания) ─────────
MESSAGE_CACHE_TTL = int(os.getenv("MESSAGE_CACHE_TTL", "86400"))

# ── Тексты ──────────────────────────────────────────────────
BOT_NAME = "ShadowWatch"
BOT_NICK = "@shadowwatchbot"
