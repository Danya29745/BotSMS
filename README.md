# 👁 ShadowWatch Bot

**Никнейм:** `@shadowwatchbot` *(зарегистрируй у @BotFather)*

Бот-шпион для Telegram: перехватывает удалённые сообщения, редактирования и самоуничтожающиеся медиа.

---

## 🚀 Быстрый старт

### 1. Установка

```bash
cd shadowwatch
pip install -r requirements.txt
```

### 2. Запуск

```bash
python bot.py
```

Или через переменную окружения (рекомендуется):

```bash
BOT_TOKEN=8766418607:AAF-... python bot.py
```

---

## 🛠 Настройка у @BotFather

1. `/mybots` → выбери бота → **Bot Settings**
2. Включи **Group Privacy = Disabled** — чтобы видел все сообщения в группах
3. Включи **Allow Groups** — чтобы добавлять в чаты

---

## 👑 Команды администратора

| Команда | Описание |
|---------|----------|
| `/admin` | Открыть панель администратора |
| `/grant <user_id> <days>` | Выдать подписку напрямую |
| `/revoke <user_id>` | Отозвать подписку |

**Пример:**
```
/grant 123456789 30     — выдать на 30 дней
/grant 123456789 9999   — навсегда
/revoke 123456789       — отозвать
```

---

## 📋 Команды пользователей

| Команда | Описание |
|---------|----------|
| `/start` | Главное меню |
| `/sub` | Статус подписки |
| `/settings` | Настройки уведомлений |
| `/help` | Справка |

---

## 📱 Как добавить бота в чат (инструкция для пользователей)

1. Зайди в нужный групповой чат
2. Настройки чата → **Автоматизация чатов** (Manage bots)
3. Найди `@shadowwatchbot` → добавь
4. Выдай боту права **администратора** (чтобы видел все сообщения)

---

## 🗂 Структура проекта

```
shadowwatch/
├── bot.py              # Точка входа
├── config.py           # Токен, ID админов
├── requirements.txt
├── database/
│   └── db.py           # SQLite + все операции
└── handlers/
    ├── admin.py        # Панель администратора
    ├── user.py         # /start, /sub, /settings
    └── events.py       # Удаления, редактирования, самоуничтожение
```

---

## ⚙️ Функционал

### 🗑 Удалённые сообщения
Бот кэширует все сообщения в чатах где он есть.
При удалении — мгновенно присылает копию с именем автора и временем.

### ✏️ Редактирования
При изменении сообщения — присылает **«было» / «стало»** с автором.

### 💣 Самоуничтожающиеся медиа
Перехватывает `ViewOnce`-медиа до того как они исчезнут, и пересылает.

---

## 🐳 Деплой на сервере (systemd)

Создай `/etc/systemd/system/shadowwatch.service`:

```ini
[Unit]
Description=ShadowWatch Telegram Bot
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/shadowwatch
ExecStart=/usr/bin/python3 bot.py
Environment=BOT_TOKEN=8766418607:AAF-C1h0kSU23aJfBP-OERryZFwjEOahc-M
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable shadowwatch
sudo systemctl start shadowwatch
sudo systemctl status shadowwatch
```

---

## 📌 Важные ограничения Telegram API

- Удалённые сообщения доступны только если бот был в чате **в момент отправки**
- Самоуничтожающиеся `ViewOnce`-медиа: бот успевает скачать до удаления только если обрабатывает сообщение достаточно быстро
- В личных чатах 1-на-1 бот **не** видит сообщения — только в группах
