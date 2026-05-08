# Social link tracker

Мини-сервис для ссылок из шапок Instagram, TikTok и YouTube.

## Ссылки

- `/go/instagram` -> Telegram
- `/go/tiktok` -> Telegram
- `/go/youtube` -> Telegram
- `/go/ig-site` -> сайт с UTM
- `/go/tt-site` -> сайт с UTM
- `/go/yt-site` -> сайт с UTM

## Таблица

Клики пишутся в worksheet `Social_Clicks` в Google Sheet из `SHEET_ID`.

## Render

Build command:

```bash
pip install -r requirements-social-tracker.txt
```

Start command:

```bash
python social_tracker.py
```

Env vars:

```text
SHEET_ID=...
GOOGLE_CREDS_JSON=...
SOCIAL_TRACKER_ADMIN_TOKEN=...
TELEGRAM_CHANNEL_URL=https://t.me/LeasingStok
SITE_URL=https://xn--c1aeedbcapcxc2dyb.xn--p1ai/
```
