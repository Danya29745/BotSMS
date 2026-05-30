#!/bin/bash
# ShadowWatch — быстрый деплой

echo "👁 ShadowWatch Bot — установка"
echo "================================"

# Проверяем python3
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 не найден. Установи: sudo apt install python3 python3-pip"
    exit 1
fi

# Устанавливаем зависимости
echo "📦 Устанавливаем зависимости..."
pip3 install -r requirements.txt

echo ""
echo "✅ Готово! Запуск:"
echo "   python3 bot.py"
echo ""
echo "👑 Твой ID для /admin: 7965055989"
echo "🔑 Токен уже прописан в config.py"
