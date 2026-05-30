#!/usr/bin/env python3
"""
ShadowWatch Bot (@shadowwatchbot)
Мониторинг: удалённые сообщения, редактирование, самоуничтожающиеся медиа
"""

import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from config import BOT_TOKEN
from database.db import init_db
from handlers import admin, user, events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    # Регистрируем роутеры
    dp.include_router(admin.router)
    dp.include_router(user.router)
    dp.include_router(events.router)

    await init_db()
    logger.info("✅ База данных инициализирована")

    # Удаляем вебхук и запускаем polling
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("🚀 ShadowWatch Bot запущен")

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
