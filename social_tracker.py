from __future__ import annotations

import hashlib
import html
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


BASE_DIR = Path(__file__).resolve().parent
ENV_DIR = BASE_DIR / "env"
load_dotenv(ENV_DIR / ".env")

SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", str(ENV_DIR / "credentials.json")).strip()
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "").strip()
ADMIN_TOKEN = os.getenv("SOCIAL_TRACKER_ADMIN_TOKEN", os.getenv("ADMIN_WEB_TOKEN", "")).strip()
TELEGRAM_CHANNEL_URL = os.getenv("TELEGRAM_CHANNEL_URL", "https://t.me/LeasingStok").strip()
SITE_URL = os.getenv("SITE_URL", "https://xn--c1aeedbcapcxc2dyb.xn--p1ai/").strip()
PORT = int(os.getenv("PORT", "10000"))

WORKSHEET_NAME = "Social_Clicks"
HEADERS = [
    "timestamp",
    "date",
    "source",
    "source_label",
    "destination",
    "destination_label",
    "placement",
    "slug",
    "target_url",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "referrer",
    "user_agent",
    "ip_hash",
]


@dataclass(frozen=True)
class Route:
    slug: str
    source: str
    source_label: str
    destination: str
    destination_label: str
    placement: str = "bio"

    @property
    def target_url(self) -> str:
        if self.destination == "telegram":
            return TELEGRAM_CHANNEL_URL

        parsed = urlparse(SITE_URL)
        query = parse_qs(parsed.query)
        query["utm_source"] = [self.source]
        query["utm_medium"] = ["organic_social"]
        query["utm_campaign"] = ["bio"]
        query["utm_content"] = [self.destination]
        qs = urlencode(query, doseq=True)
        return parsed._replace(query=qs).geturl()


ROUTES = {
    "instagram": Route("instagram", "instagram", "Instagram", "telegram", "Telegram"),
    "ig": Route("ig", "instagram", "Instagram", "telegram", "Telegram"),
    "ig-tg": Route("ig-tg", "instagram", "Instagram", "telegram", "Telegram"),
    "tiktok": Route("tiktok", "tiktok", "TikTok", "telegram", "Telegram"),
    "tt": Route("tt", "tiktok", "TikTok", "telegram", "Telegram"),
    "tt-tg": Route("tt-tg", "tiktok", "TikTok", "telegram", "Telegram"),
    "youtube": Route("youtube", "youtube", "YouTube", "telegram", "Telegram"),
    "yt": Route("yt", "youtube", "YouTube", "telegram", "Telegram"),
    "yt-tg": Route("yt-tg", "youtube", "YouTube", "telegram", "Telegram"),
    "instagram-site": Route("instagram-site", "instagram", "Instagram", "site", "Сайт"),
    "ig-site": Route("ig-site", "instagram", "Instagram", "site", "Сайт"),
    "tiktok-site": Route("tiktok-site", "tiktok", "TikTok", "site", "Сайт"),
    "tt-site": Route("tt-site", "tiktok", "TikTok", "site", "Сайт"),
    "youtube-site": Route("youtube-site", "youtube", "YouTube", "site", "Сайт"),
    "yt-site": Route("yt-site", "youtube", "YouTube", "site", "Сайт"),
}

PUBLIC_LINKS = ["instagram", "tiktok", "youtube", "ig-site", "tt-site", "yt-site"]

_worksheet = None


def _credentials() -> Credentials:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if GOOGLE_CREDS_JSON:
        return Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), scopes=scopes)
    return Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)


def _get_worksheet():
    global _worksheet
    if _worksheet is not None:
        return _worksheet
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID is not configured")

    client = gspread.authorize(_credentials())
    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        ws = spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=len(HEADERS))

    existing_headers = ws.row_values(1)
    if existing_headers != HEADERS:
        ws.update("1:1", [HEADERS])
    _worksheet = ws
    return ws


def _client_ip(handler: BaseHTTPRequestHandler) -> str:
    forwarded = handler.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return handler.client_address[0] if handler.client_address else ""


def _ip_hash(handler: BaseHTTPRequestHandler) -> str:
    raw = "|".join([_client_ip(handler), handler.headers.get("user-agent", "")])
    if not raw.strip("|"):
        return ""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _append_click(route: Route, handler: BaseHTTPRequestHandler) -> None:
    now = datetime.now(timezone.utc)
    row = {
        "timestamp": now.isoformat(),
        "date": now.date().isoformat(),
        "source": route.source,
        "source_label": route.source_label,
        "destination": route.destination,
        "destination_label": route.destination_label,
        "placement": route.placement,
        "slug": route.slug,
        "target_url": route.target_url,
        "utm_source": route.source,
        "utm_medium": "organic_social",
        "utm_campaign": "bio",
        "utm_content": route.destination,
        "referrer": handler.headers.get("referer", ""),
        "user_agent": handler.headers.get("user-agent", ""),
        "ip_hash": _ip_hash(handler),
    }
    _get_worksheet().append_row([row.get(h, "") for h in HEADERS], value_input_option="RAW")


def _read_clicks() -> list[dict]:
    return _get_worksheet().get_all_records()


def _range_start(range_key: str) -> datetime | None:
    if range_key == "all":
        return None
    days = {"today": 1, "7d": 7, "30d": 30}.get(range_key, 7)
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return start - timedelta(days=days - 1)


def _filter_clicks(rows: list[dict], range_key: str) -> list[dict]:
    start = _range_start(range_key)
    if not start:
        return rows
    result = []
    for row in rows:
        try:
            ts = datetime.fromisoformat(str(row.get("timestamp", "")).replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts >= start:
            result.append(row)
    return result


def _summarize(rows: list[dict]) -> tuple[dict, list[dict]]:
    totals = {
        "clicks": len(rows),
        "telegram": sum(1 for r in rows if r.get("destination") == "telegram"),
        "site": sum(1 for r in rows if r.get("destination") == "site"),
        "unique": len({r.get("ip_hash") for r in rows if r.get("ip_hash")}),
    }
    by_source: dict[str, dict] = {}
    for source, label in [("instagram", "Instagram"), ("tiktok", "TikTok"), ("youtube", "YouTube")]:
        by_source[source] = {
            "source": source,
            "label": label,
            "telegram": 0,
            "site": 0,
            "total": 0,
            "unique": set(),
        }
    for row in rows:
        source = row.get("source") or "unknown"
        if source not in by_source:
            by_source[source] = {"source": source, "label": source, "telegram": 0, "site": 0, "total": 0, "unique": set()}
        by_source[source]["total"] += 1
        if row.get("destination") == "telegram":
            by_source[source]["telegram"] += 1
        if row.get("destination") == "site":
            by_source[source]["site"] += 1
        if row.get("ip_hash"):
            by_source[source]["unique"].add(row.get("ip_hash"))

    rows_by_source = []
    for item in by_source.values():
        item = dict(item)
        item["unique"] = len(item["unique"])
        rows_by_source.append(item)
    rows_by_source.sort(key=lambda x: x["total"], reverse=True)
    return totals, rows_by_source


def _html_page(title: str, body: str) -> bytes:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    body {{ margin:0; font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif; background:#f5f6f2; color:#18201d; }}
    main {{ max-width:1080px; margin:0 auto; padding:20px 12px 42px; }}
    h1 {{ margin:0 0 14px; font-size:clamp(26px,5vw,42px); line-height:1.05; }}
    h2 {{ margin:0; font-size:18px; }}
    a {{ color:#13795b; }}
    .muted {{ color:#68736f; }}
    .toolbar,.panel,.kpi {{ background:#fff; border:1px solid #dce3df; border-radius:8px; box-shadow:0 12px 32px rgba(24,32,29,.08); }}
    .toolbar {{ display:flex; flex-wrap:wrap; justify-content:space-between; gap:10px; padding:12px; margin:0 0 14px; }}
    .ranges a {{ display:inline-block; padding:8px 10px; margin:0 4px 4px 0; border:1px solid #dce3df; border-radius:8px; text-decoration:none; color:#18201d; }}
    .ranges a.active {{ background:#e4f3ed; border-color:#13795b; color:#13795b; font-weight:800; }}
    .kpis {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin:0 0 14px; }}
    .kpi {{ padding:14px; min-height:92px; }}
    .kpi span {{ display:block; color:#68736f; font-size:13px; }}
    .kpi b {{ display:block; margin-top:8px; font-size:32px; line-height:1; }}
    .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; }}
    .panel {{ padding:14px; margin-bottom:14px; overflow:auto; }}
    table {{ width:100%; border-collapse:collapse; min-width:520px; }}
    th,td {{ padding:10px 8px; border-bottom:1px solid #dce3df; text-align:left; vertical-align:top; }}
    th {{ color:#68736f; font-size:12px; text-transform:uppercase; }}
    code {{ display:block; padding:8px; background:#f0f5f3; border:1px solid #dce3df; border-radius:8px; overflow:auto; }}
    @media (max-width:760px) {{ .kpis,.grid {{ grid-template-columns:1fr; }} .toolbar {{ display:block; }} }}
  </style>
</head>
<body><main>{body}</main></body></html>""".encode("utf-8")


def _admin_html(base_url: str, token: str, range_key: str) -> bytes:
    rows = _filter_clicks(_read_clicks(), range_key)
    totals, by_source = _summarize(rows)
    ranges = [("today", "Сегодня"), ("7d", "7 дней"), ("30d", "30 дней"), ("all", "Всё")]
    range_links = " ".join(
        f'<a class="{"active" if key == range_key else ""}" href="/admin?token={html.escape(token)}&range={key}">{label}</a>'
        for key, label in ranges
    )
    source_rows = "".join(
        f"<tr><td><b>{html.escape(r['label'])}</b></td><td>{r['telegram']}</td><td>{r['site']}</td><td>{r['total']}</td><td>{r['unique']}</td></tr>"
        for r in by_source
    )
    link_rows = "".join(
        f"<tr><td>{html.escape(ROUTES[k].source_label)} → {html.escape(ROUTES[k].destination_label)}</td>"
        f"<td><code>{html.escape(base_url + '/go/' + k)}</code></td></tr>"
        for k in PUBLIC_LINKS
    )
    recent_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(r.get('timestamp', ''))[:16].replace('T', ' '))}</td>"
        f"<td>{html.escape(str(r.get('source_label') or r.get('source') or ''))}</td>"
        f"<td>{html.escape(str(r.get('destination_label') or r.get('destination') or ''))}</td>"
        f"<td class=\"muted\">{html.escape(str(r.get('referrer') or ''))}</td>"
        "</tr>"
        for r in rows[-30:][::-1]
    )
    body = f"""
      <h1>Переходы в LeasingStock</h1>
      <section class="toolbar">
        <div class="ranges">{range_links}</div>
        <div class="muted">Клики из шапок Instagram, TikTok и YouTube</div>
      </section>
      <section class="kpis">
        <div class="kpi"><span>Все клики</span><b>{totals['clicks']}</b></div>
        <div class="kpi"><span>Уникальные</span><b>{totals['unique']}</b></div>
        <div class="kpi"><span>В Telegram</span><b>{totals['telegram']}</b></div>
        <div class="kpi"><span>На сайт</span><b>{totals['site']}</b></div>
      </section>
      <section class="grid">
        <article class="panel"><h2>По источникам</h2><table><thead><tr><th>Источник</th><th>Telegram</th><th>Сайт</th><th>Всего</th><th>Уник.</th></tr></thead><tbody>{source_rows}</tbody></table></article>
        <article class="panel"><h2>Рабочие ссылки</h2><table><thead><tr><th>Куда</th><th>Ссылка</th></tr></thead><tbody>{link_rows}</tbody></table></article>
      </section>
      <section class="panel"><h2>Последние клики</h2><table><thead><tr><th>Время UTC</th><th>Источник</th><th>Куда</th><th>Referrer</th></tr></thead><tbody>{recent_rows or '<tr><td colspan="4" class="muted">Пока кликов нет.</td></tr>'}</tbody></table></section>
    """
    return _html_page("LeasingStock · переходы", body)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        print("%s - %s" % (self.address_string(), fmt % args))

    def _send(self, status: int, body: bytes, content_type: str = "text/html; charset=utf-8") -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("cache-control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, url: str) -> None:
        self.send_response(302)
        self.send_header("location", url)
        self.send_header("cache-control", "no-store")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.strip("/")
        query = parse_qs(parsed.query)
        base_url = f"https://{self.headers.get('host', '')}".rstrip("/")

        if path in {"", "links"}:
            items = "".join(
                f"<tr><td>{html.escape(ROUTES[k].source_label)} → {html.escape(ROUTES[k].destination_label)}</td>"
                f"<td><code>{html.escape(base_url + '/go/' + k)}</code></td></tr>"
                for k in PUBLIC_LINKS
            )
            body = f"<h1>LeasingStock links</h1><section class='panel'><table>{items}</table></section>"
            self._send(200, _html_page("LeasingStock links", body))
            return

        if path == "admin":
            token = query.get("token", [""])[0]
            if ADMIN_TOKEN and token != ADMIN_TOKEN:
                self._send(403, b"forbidden", "text/plain; charset=utf-8")
                return
            range_key = query.get("range", ["7d"])[0]
            if range_key not in {"today", "7d", "30d", "all"}:
                range_key = "7d"
            self._send(200, _admin_html(base_url, token, range_key))
            return

        slug = path.removeprefix("go/")
        route = ROUTES.get(slug)
        if not route:
            self._send(404, b"unknown link", "text/plain; charset=utf-8")
            return

        try:
            _append_click(route, self)
        except Exception as exc:
            print(f"click append failed: {exc}")
        self._redirect(route.target_url)


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"social tracker listening on :{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
