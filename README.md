# UGC Leasing — бот заявок

Telegram-бот на aiogram 3 для сбора заявок от UGC-криэйторов (авто-тематика). Записывает заявки в Google Sheets и уведомляет админов.

## Развёртывание

### 1. Установка

```bash
cd "D:/мои файлы/бэзнес идеи/ugc/bot"
python -m venv venv
venv\Scripts\activate       # Windows
# source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

### 2. Google Sheets

1. Идёшь в [Google Cloud Console](https://console.cloud.google.com/) → создаёшь проект (или берёшь существующий)
2. **APIs & Services → Library** → включаешь **Google Sheets API**
3. **APIs & Services → Credentials → Create Credentials → Service Account** → создаёшь аккаунт
4. В созданном service account: **Keys → Add Key → JSON** → скачивается JSON-файл
5. Переименовываешь его в `credentials.json` и кладёшь рядом с `bot.py`
6. Открываешь JSON, копируешь поле `client_email` (что-то вроде `xxx@project.iam.gserviceaccount.com`)
7. Создаёшь новую Google Таблицу → жмёшь **Настройки доступа** → добавляешь `client_email` с правами **Редактор**
8. Копируешь ID таблицы из URL: `https://docs.google.com/spreadsheets/d/ИД_ТУТ/edit`

Лист `Applications` с шапкой бот создаст сам при первой заявке.

### 3. Переменные окружения

```bash
cp .env.example .env
```

Заполняешь `.env`:
- `BOT_TOKEN` — от [@BotFather](https://t.me/BotFather)
- `ADMIN_IDS` — свой Telegram ID (можно узнать у [@userinfobot](https://t.me/userinfobot)). Несколько — через запятую: `123,456,789`
- `SHEET_ID` — ID из шага 2.8
- `GOOGLE_CREDS_FILE` — можно оставить `credentials.json`

### 4. Запуск

```bash
python bot.py
```

Бот поднимется через long polling. Проверь: напиши ему `/start` в Telegram.

## Деплой на Render (опционально)

Тип сервиса: **Background Worker** (не Web Service — у нас polling, не webhook).

Переменные окружения в Render: `BOT_TOKEN`, `ADMIN_IDS`, `SHEET_ID`.
`credentials.json` положить через Render **Secret Files** (путь монтирования `/etc/secrets/credentials.json`) и в `.env`/переменных указать `GOOGLE_CREDS_FILE=/etc/secrets/credentials.json`.

Start Command: `python bot.py`

## Что внутри

- `bot.py` — точка входа, FSM на 3 шага, кнопки «🔄 Начать заново» и «❌ Отмена», антиспам (1 заявка на юзера в 10 минут), уведомления админам
- `sheets.py` — одна функция `append_application`, автосоздание листа с шапкой
- `bot.log` — ошибки и события (создаётся автоматически при запуске)

## Диалог

1. `/start` — приветствие, просим ссылки на примеры работ (YouTube, VK Video, Rutube, Telegram, Instagram)
2. Опыт и темы — одним сообщением
3. Контакт для связи — запись в Sheets + «Спасибо» + уведомление админам

На каждом шаге под полем ввода висят кнопки «🔄 Начать заново» и «❌ Отмена». Также работает `/cancel`.
