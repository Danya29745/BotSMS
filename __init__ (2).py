"""
Ядро ShadowWatch:
— Кэширование входящих сообщений
— Уведомление об удалении
— Уведомление о редактировании
— Перехват самоуничтожающихся медиа (has_protected_content / ttl)
"""

import io
import logging
from datetime import datetime

from aiogram import Router, Bot, F
from aiogram.types import Message, ContentType
from aiogram.filters import IS_MEMBER

from config import ADMIN_IDS
from database.db import (
    cache_message, get_cached_message, delete_cached_message,
    is_subscribed, get_user_settings, upsert_user,
)

router = Router()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Вспомогательные функции
# ─────────────────────────────────────────────

def extract_media(msg: Message):
    """Возвращает (media_type, file_id) или (None, None)."""
    if msg.photo:
        return "photo", msg.photo[-1].file_id
    if msg.video:
        return "video", msg.video.file_id
    if msg.video_note:
        return "video_note", msg.video_note.file_id
    if msg.voice:
        return "voice", msg.voice.file_id
    if msg.audio:
        return "audio", msg.audio.file_id
    if msg.document:
        return "document", msg.document.file_id
    if msg.sticker:
        return "sticker", msg.sticker.file_id
    if msg.animation:
        return "animation", msg.animation.file_id
    return None, None


def user_link(user_id, first_name, username=None):
    name = first_name or "Пользователь"
    uname = f" (@{username})" if username else ""
    return f'<a href="tg://user?id={user_id}">{name}</a>{uname}'


MEDIA_EMOJI = {
    "photo": "🖼",
    "video": "🎬",
    "video_note": "📹",
    "voice": "🎙",
    "audio": "🎵",
    "document": "📄",
    "sticker": "🎭",
    "animation": "🎞",
}

MEDIA_SEND = {
    "photo": "send_photo",
    "video": "send_video",
    "video_note": "send_video_note",
    "voice": "send_voice",
    "audio": "send_audio",
    "document": "send_document",
    "sticker": "send_sticker",
    "animation": "send_animation",
}


async def get_subscribed_admins_in_chat(bot: Bot, chat_id: int):
    """Возвращаем admin_ids — они всегда получают уведомления."""
    return ADMIN_IDS


async def notify_subscriber(bot: Bot, user_id: int, **kwargs):
    """Пытаемся отправить уведомление пользователю."""
    try:
        await bot.send_message(user_id, **kwargs)
    except Exception as e:
        logger.warning(f"Не удалось отправить уведомление {user_id}: {e}")


async def send_media_to_user(bot: Bot, user_id: int, media_type: str,
                              file_id: str, caption: str = None):
    method_name = MEDIA_SEND.get(media_type)
    if not method_name:
        return
    method = getattr(bot, method_name)
    kwargs = {"chat_id": user_id}
    if media_type in ("video_note",):
        kwargs[media_type] = file_id
    elif media_type == "sticker":
        kwargs["sticker"] = file_id
    else:
        kwargs[media_type] = file_id
        if caption:
            kwargs["caption"] = caption
    try:
        await method(**kwargs)
    except Exception as e:
        logger.warning(f"Не удалось переслать медиа {user_id}: {e}")


async def should_notify(user_id: int, setting: str) -> bool:
    if user_id in ADMIN_IDS:
        return True
    if not await is_subscribed(user_id):
        return False
    s = await get_user_settings(user_id)
    return bool(s.get(setting, 1))


# ─────────────────────────────────────────────
# 1. Кэширование сообщений
# ─────────────────────────────────────────────

@router.message()
async def cache_incoming(msg: Message, bot: Bot):
    """Кэшируем все сообщения в группах/супергруппах."""
    if msg.chat.type not in ("group", "supergroup", "channel"):
        return
    if not msg.from_user:
        return

    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)

    # Самоуничтожающиеся медиа (has_protected_content + ttl)
    media_type, file_id = extract_media(msg)

    # Проверяем ttl (только в фото/видео с таймером)
    is_self_destruct = False
    if media_type in ("photo", "video"):
        obj = msg.photo[-1] if media_type == "photo" else msg.video
        # В aiogram 3.x ttl_seconds хранится в MessageAutoDeleteTimerChanged
        # Для ViewOnce-медиа используем has_media_spoiler или проверяем через raw
        raw = msg.model_dump()
        media_raw = raw.get(media_type if media_type != "photo" else "photo", {})
        if isinstance(media_raw, list):
            media_raw = media_raw[-1] if media_raw else {}
        if media_raw.get("self_destruct_type") or raw.get("has_protected_content"):
            is_self_destruct = True

    # Сохраняем в кэш
    text_content = msg.text or msg.caption
    await cache_message(
        chat_id=msg.chat.id,
        message_id=msg.message_id,
        user_id=u.id,
        username=u.username,
        first_name=u.first_name,
        text=text_content,
        media_type=media_type,
        file_id=file_id,
    )

    # Если самоуничтожающееся — немедленно уведомляем всех нужных
    if is_self_destruct and media_type and file_id:
        await handle_self_destruct(bot, msg, u, media_type, file_id)


async def handle_self_destruct(bot: Bot, msg: Message, user, media_type: str, file_id: str):
    """Перехватываем самоуничтожающееся медиа и пересылаем подписчикам."""
    chat_name = msg.chat.title or str(msg.chat.id)
    sender = user_link(user.id, user.first_name, user.username)
    emoji = MEDIA_EMOJI.get(media_type, "📎")

    caption = (
        f"💣 <b>Самоуничтожающееся медиа</b>\n\n"
        f"👤 От: {sender}\n"
        f"💬 Чат: <b>{chat_name}</b>\n"
        f"🕒 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
        f"Тип: {emoji} {media_type}"
    )

    for admin_id in ADMIN_IDS:
        if await should_notify(admin_id, "notify_self_destruct"):
            await bot.send_message(admin_id, caption)
            await send_media_to_user(bot, admin_id, media_type, file_id)


# ─────────────────────────────────────────────
# 2. Удалённые сообщения
# ─────────────────────────────────────────────

@router.message(F.content_type == ContentType.SERVICE)
async def handle_service(msg: Message):
    pass  # игнорируем служебные


# aiogram 3 не имеет нативного события deleted_message,
# поэтому используем update middleware / raw updates через специальный хук

from aiogram import BaseMiddleware
from aiogram.types import Update
from typing import Callable, Awaitable, Any, Dict


class DeletedMessageMiddleware(BaseMiddleware):
    """
    Перехватывает raw update типа message_deleted (Telegram Bot API 7.x+).
    В старом Bot API удалённые сообщения недоступны напрямую —
    обходим через кэш: если сообщение исчезло из истории, считаем удалённым.
    """

    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any],
    ) -> Any:
        # Проверяем поле deleted_messages (Bot API 7.x, aiogram 3.7+)
        if hasattr(event, "deleted_messages") and event.deleted_messages:
            await self.process_deleted(event.deleted_messages, data["bot"])
        return await handler(event, data)

    async def process_deleted(self, deleted_messages_update, bot: Bot):
        for item in getattr(deleted_messages_update, "messages", []) or []:
            chat_id = item.chat.id if hasattr(item, "chat") else None
            msg_id = item.message_id if hasattr(item, "message_id") else None
            if not chat_id or not msg_id:
                continue

            cached = await get_cached_message(chat_id, msg_id)
            if not cached:
                return

            await notify_deleted(bot, cached, chat_id)
            await delete_cached_message(chat_id, msg_id)


async def notify_deleted(bot: Bot, cached: dict, chat_id: int):
    """Уведомляем о удалённом сообщении."""
    sender_id = cached.get("user_id")
    first_name = cached.get("first_name") or "Пользователь"
    username = cached.get("username")
    sender = user_link(sender_id, first_name, username) if sender_id else "Неизвестный"

    text_preview = cached.get("text", "")
    if text_preview and len(text_preview) > 300:
        text_preview = text_preview[:300] + "…"

    media_type = cached.get("media_type")
    file_id = cached.get("file_id")
    emoji = MEDIA_EMOJI.get(media_type, "") if media_type else ""

    notify_text = (
        f"🗑 <b>Сообщение удалено!</b>\n\n"
        f"👤 Автор: {sender}\n"
        f"🕒 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
    )
    if text_preview:
        notify_text += f"\n📝 <b>Текст:</b>\n{text_preview}"
    if media_type:
        notify_text += f"\n{emoji} Медиа: <b>{media_type}</b>"

    for admin_id in ADMIN_IDS:
        if await should_notify(admin_id, "notify_delete"):
            await notify_subscriber(bot, admin_id, text=notify_text, parse_mode="HTML")
            if file_id and media_type:
                await send_media_to_user(bot, admin_id, media_type, file_id,
                                          caption="📎 Медиа удалённого сообщения")


# ─────────────────────────────────────────────
# 3. Редактированные сообщения
# ─────────────────────────────────────────────

@router.edited_message()
async def handle_edited(msg: Message, bot: Bot):
    """Перехватываем редактирования."""
    if msg.chat.type not in ("group", "supergroup", "channel"):
        return
    if not msg.from_user:
        return

    u = msg.from_user
    cached = await get_cached_message(msg.chat.id, msg.message_id)

    old_text = cached.get("text") if cached else None
    new_text = msg.text or msg.caption

    sender = user_link(u.id, u.first_name, u.username)
    chat_name = msg.chat.title or str(msg.chat.id)

    if old_text == new_text:
        # Только медиа изменилось — обновляем кэш и выходим
        media_type, file_id = extract_media(msg)
        await cache_message(
            msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
            new_text, media_type, file_id
        )
        return

    def trim(t):
        if not t:
            return "<i>пусто</i>"
        return (t[:300] + "…") if len(t) > 300 else t

    notify_text = (
        f"✏️ <b>Сообщение отредактировано!</b>\n\n"
        f"👤 Автор: {sender}\n"
        f"💬 Чат: <b>{chat_name}</b>\n"
        f"🕒 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        f"📝 <b>Было:</b>\n{trim(old_text)}\n\n"
        f"📝 <b>Стало:</b>\n{trim(new_text)}"
    )

    for admin_id in ADMIN_IDS:
        if await should_notify(admin_id, "notify_edit"):
            await notify_subscriber(bot, admin_id, text=notify_text, parse_mode="HTML")

    # Обновляем кэш
    media_type, file_id = extract_media(msg)
    await cache_message(
        msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
        new_text, media_type, file_id
    )


# Регистрируем middleware
router.update.middleware(DeletedMessageMiddleware())
