"""
Админ-панель ShadowWatch
Доступна только администраторам из config.ADMIN_IDS
"""

import logging
from datetime import datetime

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import ADMIN_IDS
from database.db import (
    get_all_users, get_all_subscriptions,
    grant_subscription, revoke_subscription, get_subscription
)

router = Router()
logger = logging.getLogger(__name__)


# ── Фильтр — только для админов ──────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


class AdminStates(StatesGroup):
    waiting_user_id = State()
    waiting_days = State()
    waiting_revoke_id = State()


# ── /admin — главная панель ──────────────────────────────────

@router.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        await msg.answer("⛔ У тебя нет доступа к этой команде.")
        return
    await state.clear()
    await msg.answer(admin_menu_text(), reply_markup=admin_keyboard())


def admin_menu_text() -> str:
    return (
        "👁 <b>ShadowWatch — Панель администратора</b>\n\n"
        "Выбери действие:"
    )


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Все пользователи", callback_data="adm:users")],
        [InlineKeyboardButton(text="⭐ Активные подписки", callback_data="adm:subs")],
        [InlineKeyboardButton(text="✅ Выдать подписку", callback_data="adm:grant")],
        [InlineKeyboardButton(text="❌ Отозвать подписку", callback_data="adm:revoke")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
    ])


# ── Все пользователи ─────────────────────────────────────────

@router.callback_query(F.data == "adm:users")
async def adm_users(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return await call.answer("⛔ Нет доступа", show_alert=True)

    users = await get_all_users()
    if not users:
        await call.message.edit_text("Пользователей пока нет.", reply_markup=back_kb())
        return

    lines = ["👥 <b>Пользователи</b> (последние 50):\n"]
    for u in users[:50]:
        uname = f"@{u['username']}" if u['username'] else "—"
        name = u['first_name'] or "Без имени"
        lines.append(f"• <code>{u['user_id']}</code> | {name} | {uname}")

    await call.message.edit_text("\n".join(lines), reply_markup=back_kb())
    await call.answer()


# ── Активные подписки ────────────────────────────────────────

@router.callback_query(F.data == "adm:subs")
async def adm_subs(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return await call.answer("⛔ Нет доступа", show_alert=True)

    subs = await get_all_subscriptions()
    now = datetime.now()

    active = [s for s in subs
              if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]

    if not active:
        await call.message.edit_text("Активных подписок нет.", reply_markup=back_kb())
        return

    lines = ["⭐ <b>Активные подписки</b>:\n"]
    for s in active:
        exp = datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S")
        days_left = (exp - now).days
        uname = f"@{s['username']}" if s.get('username') else "—"
        name = s.get('first_name') or "—"
        lines.append(
            f"• <code>{s['user_id']}</code> | {name} | {uname}\n"
            f"  До: <b>{exp.strftime('%d.%m.%Y %H:%M')}</b> (осталось {days_left} дн.)"
        )

    await call.message.edit_text("\n".join(lines), reply_markup=back_kb())
    await call.answer()


# ── Выдать подписку ──────────────────────────────────────────

@router.callback_query(F.data == "adm:grant")
async def adm_grant_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return await call.answer("⛔ Нет доступа", show_alert=True)

    await state.set_state(AdminStates.waiting_user_id)
    await call.message.edit_text(
        "✅ <b>Выдача подписки</b>\n\n"
        "Введи <b>Telegram ID</b> пользователя:\n"
        "<i>(Отмена: /admin)</i>",
        reply_markup=None
    )
    await call.answer()


@router.message(AdminStates.waiting_user_id)
async def adm_grant_get_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return

    try:
        uid = int(msg.text.strip())
    except ValueError:
        await msg.answer("❗ Введи числовой ID пользователя.")
        return

    await state.update_data(target_user_id=uid)
    await state.set_state(AdminStates.waiting_days)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1 день", callback_data="days:1"),
            InlineKeyboardButton(text="3 дня", callback_data="days:3"),
            InlineKeyboardButton(text="7 дней", callback_data="days:7"),
        ],
        [
            InlineKeyboardButton(text="14 дней", callback_data="days:14"),
            InlineKeyboardButton(text="30 дней", callback_data="days:30"),
            InlineKeyboardButton(text="90 дней", callback_data="days:90"),
        ],
        [
            InlineKeyboardButton(text="365 дней", callback_data="days:365"),
            InlineKeyboardButton(text="♾ Навсегда (9999)", callback_data="days:9999"),
        ],
    ])

    await msg.answer(
        f"👤 ID: <code>{uid}</code>\n\nВыбери срок подписки или введи число дней вручную:",
        reply_markup=kb
    )


@router.callback_query(F.data.startswith("days:"))
async def adm_grant_days_btn(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return await call.answer("⛔ Нет доступа", show_alert=True)

    days = int(call.data.split(":")[1])
    data = await state.get_data()
    uid = data.get("target_user_id")

    if not uid:
        await call.answer("Сессия истекла, начни заново.", show_alert=True)
        await state.clear()
        return

    expires = await grant_subscription(uid, days, call.from_user.id)
    await state.clear()

    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await call.message.edit_text(
        f"✅ <b>Подписка выдана!</b>\n\n"
        f"👤 ID: <code>{uid}</code>\n"
        f"⏳ Срок: <b>{days} дн.</b>\n"
        f"📅 Истекает: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>",
        reply_markup=back_kb()
    )
    await call.answer("✅ Подписка выдана!")

    # Уведомляем пользователя
    try:
        await call.bot.send_message(
            uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n"
            f"📅 Истекает: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Используй /start для активации функций."
        )
    except Exception:
        pass


@router.message(AdminStates.waiting_days)
async def adm_grant_days_text(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return

    try:
        days = int(msg.text.strip())
        if days <= 0:
            raise ValueError
    except ValueError:
        await msg.answer("❗ Введи положительное число дней.")
        return

    data = await state.get_data()
    uid = data.get("target_user_id")
    expires = await grant_subscription(uid, days, msg.from_user.id)
    await state.clear()

    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await msg.answer(
        f"✅ <b>Подписка выдана!</b>\n\n"
        f"👤 ID: <code>{uid}</code>\n"
        f"⏳ Срок: <b>{days} дн.</b>\n"
        f"📅 Истекает: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>",
        reply_markup=back_kb()
    )

    try:
        await msg.bot.send_message(
            uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n\n"
            f"⏳ Срок: <b>{days} дн.</b>\n"
            f"📅 Истекает: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Используй /start для активации функций."
        )
    except Exception:
        pass


# ── Отозвать подписку ────────────────────────────────────────

@router.callback_query(F.data == "adm:revoke")
async def adm_revoke_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return await call.answer("⛔ Нет доступа", show_alert=True)

    await state.set_state(AdminStates.waiting_revoke_id)
    await call.message.edit_text(
        "❌ <b>Отзыв подписки</b>\n\n"
        "Введи <b>Telegram ID</b> пользователя:",
        reply_markup=None
    )
    await call.answer()


@router.message(AdminStates.waiting_revoke_id)
async def adm_revoke_do(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return

    try:
        uid = int(msg.text.strip())
    except ValueError:
        await msg.answer("❗ Введи числовой ID.")
        return

    sub = await get_subscription(uid)
    if not sub:
        await msg.answer(f"У пользователя <code>{uid}</code> нет активной подписки.")
        await state.clear()
        return

    await revoke_subscription(uid)
    await state.clear()
    await msg.answer(
        f"❌ Подписка пользователя <code>{uid}</code> отозвана.",
        reply_markup=back_kb()
    )

    try:
        await msg.bot.send_message(uid, "⚠️ Твоя подписка <b>ShadowWatch</b> была отозвана администратором.")
    except Exception:
        pass


# ── Статистика ───────────────────────────────────────────────

@router.callback_query(F.data == "adm:stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return await call.answer("⛔ Нет доступа", show_alert=True)

    users = await get_all_users()
    subs = await get_all_subscriptions()
    now = datetime.now()
    active = [s for s in subs
              if datetime.strptime(s["expires_at"], "%Y-%m-%d %H:%M:%S") > now]

    text = (
        "📊 <b>Статистика ShadowWatch</b>\n\n"
        f"👥 Всего пользователей: <b>{len(users)}</b>\n"
        f"⭐ Активных подписок: <b>{len(active)}</b>\n"
        f"📋 Всего выдано подписок: <b>{len(subs)}</b>"
    )
    await call.message.edit_text(text, reply_markup=back_kb())
    await call.answer()


# ── Команды быстрого доступа ─────────────────────────────────

@router.message(Command("grant"))
async def cmd_grant(msg: Message):
    """Быстрая выдача: /grant <user_id> <days>"""
    if not is_admin(msg.from_user.id):
        return

    parts = msg.text.split()
    if len(parts) != 3:
        await msg.answer("Использование: /grant <user_id> <days>")
        return

    try:
        uid, days = int(parts[1]), int(parts[2])
    except ValueError:
        await msg.answer("❗ Неверный формат. Пример: /grant 123456789 30")
        return

    expires = await grant_subscription(uid, days, msg.from_user.id)
    exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S")
    await msg.answer(
        f"✅ Подписка выдана <code>{uid}</code>\n"
        f"Истекает: <b>{exp_dt.strftime('%d.%m.%Y %H:%M')}</b>"
    )
    try:
        await msg.bot.send_message(
            uid,
            f"🎉 <b>Тебе выдана подписка ShadowWatch!</b>\n"
            f"⏳ Срок: {days} дн. | До: {exp_dt.strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Используй /start для активации."
        )
    except Exception:
        pass


@router.message(Command("revoke"))
async def cmd_revoke(msg: Message):
    """Быстрый отзыв: /revoke <user_id>"""
    if not is_admin(msg.from_user.id):
        return

    parts = msg.text.split()
    if len(parts) != 2:
        await msg.answer("Использование: /revoke <user_id>")
        return

    try:
        uid = int(parts[1])
    except ValueError:
        await msg.answer("❗ Неверный user_id.")
        return

    await revoke_subscription(uid)
    await msg.answer(f"❌ Подписка <code>{uid}</code> отозвана.")


# ── Кнопка «Назад» ───────────────────────────────────────────

@router.callback_query(F.data == "adm:back")
async def adm_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text(admin_menu_text(), reply_markup=admin_keyboard())
    await call.answer()


def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
    ])
