"""Rule-based генератор зацепок для follow-up.

Триггер только по СИЛЬНОМУ сигналу — длинное упоминание профессии или ссылка
на профильную платформу. Слабый сигнал (одно слово в проброс) → fallback.

Возвращаемая зацепка должна быть честной: либо мы видим конкретный факт в
заявке, либо отдаём универсальный текст без претензии «мы посмотрели».
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Hook:
    category: str           # для логов/аналитики
    text: str               # вставляется вместо плейсхолдера {зацепка}
    saw_examples: bool      # True если можно говорить «по примерам видно»


# Универсальный, не врущий текст. Никаких «мы посмотрели то что вы прислали».
FALLBACK = Hook(
    category="fallback",
    text="нам важно, чтобы человек умел живо снять короткий ролик — без студии, "
         "но с пониманием формата",
    saw_examples=False,
)


# Категории. Триггер срабатывает если найден хотя бы один сильный сигнал.
# Слабые упоминания (например слово «макияж» в проброс) не должны триггерить.
_CATEGORIES = [
    {
        "category": "auto",
        # авто-канал/ссылка с явным auto-словом, либо явная фраза «снимаю авто»
        "url_keywords": ["auto", "car", "авто", "drive2"],
        "text_patterns": [
            r"снима[юе][^.]{0,40}(авто|машин|тачк)",
            r"(автообзор|тест-драйв|автоблог)",
        ],
        "hook": "вы уже снимаете про авто — это наш формат, осталось показать, "
                "что вы попадаете в наш темп подачи",
    },
    {
        "category": "humor",
        "url_keywords": [],
        "text_patterns": [
            r"\b(стендап|комик|юмор|скетч)\b",
        ],
        "hook": "у вас юмористический формат — это плюс, в наших роликах нужна "
                "лёгкая подача, а не презентация",
    },
    {
        "category": "beauty_lifestyle",
        "url_keywords": [],
        "text_patterns": [
            r"\b(бьюти|beauty|лайфстайл|lifestyle|fashion|мод[нае])\b",
            r"снима[юе][^.]{0,40}(бьюти|макияж|стиль|лайфстайл)",
        ],
        "hook": "это не авто-тематика, но в бьюти/лайфстайле тоже важно быстро "
                "зацепить внимание и не выглядеть как реклама в лоб — нам это и нужно",
    },
    {
        "category": "smm_client",
        "url_keywords": [],
        "text_patterns": [
            r"\b(smm|смм|таргет|клиентск(ий|ие)|агентств)\b",
            r"снима(ю|ем)[^.]{0,40}клиент",
        ],
        "hook": "у вас опыт клиентских проектов — значит вы умеете быстро понять "
                "ТЗ и сдать в срок, для нас это критично",
    },
    {
        "category": "drive_portfolio",
        "url_keywords": ["drive.google", "yadi.sk", "disk.yandex", "dropbox", "icloud.com"],
        "text_patterns": [],
        "hook": "вы прислали портфолио ссылкой — посмотрим внимательнее, "
                "а пока хотим предложить тестовое",
    },
]


def _strong_match(text: str, patterns: list[str]) -> bool:
    """Текстовый паттерн считается сильным сигналом, только если матчится в
    тексте заявки (а не в случайной ссылке)."""
    if not text or not patterns:
        return False
    low = text.lower()
    return any(re.search(p, low) for p in patterns)


def _url_match(text: str, keywords: list[str]) -> bool:
    if not text or not keywords:
        return False
    low = text.lower()
    # ищем ключевое слово только если оно стоит после http:// или после точки
    # (т.е. в URL/домене), чтобы случайное «авто» в тексте не триггерило auto.
    return any(
        re.search(rf"https?://[^\s]*{re.escape(k)}", low) or re.search(rf"\.{re.escape(k)}\b", low)
        for k in keywords
    )


def build_hook(examples: str, experience: str) -> Hook:
    """Возвращает зацепку. Если ни одна категория не сработала — FALLBACK."""
    full = f"{examples}\n{experience}"
    for cat in _CATEGORIES:
        if _strong_match(full, cat["text_patterns"]) or _url_match(examples, cat["url_keywords"]):
            return Hook(
                category=cat["category"],
                text=cat["hook"],
                saw_examples=bool(examples and examples.strip()),
            )
    return FALLBACK
