"""
Копия логики заявок из main.py — для переноса в другую папку / отдельный процесс.
Тот же функционал: РП / VZP / обзвон / модерация приёма, /panel, /moderation, /sbor, /autopark.

Запуск: python second_bot.py
Токен: SECOND_BOT_TOKEN или DISCORD_TOKEN (отдельное приложение в Portal — по желанию).
Синк: SECOND_BOT_GUILD_ID или GUILD_ID.
Частые рестарты → Discord отвечает 429 на регистрацию slash-команд; клиент сам ждёт и повторяет.
В .env можно SYNC_SLASH_ON_START=0 — не вызывать sync при каждом запуске (после смены команд один раз включи 1).

В одном проекте достаточно main.py (там уже и заявки, и автопарк). second_bot — опционально.
"""

import asyncio
import json
import os
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()


def get_env_int(name: str, default: int = 0) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Переменная {name} должна содержать только цифры (сейчас: {value!r})."
        ) from exc


def get_env_float_hours(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip().replace(",", ".")
    if not raw:
        return default
    try:
        return max(0.25, min(168.0, float(raw)))
    except ValueError:
        return default


def get_env_int_set(name: str) -> set[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return set()
    result: set[int] = set()
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        if not value.isdigit():
            raise RuntimeError(
                f"Переменная {name} должна содержать только ID через запятую."
            )
        result.add(int(value))
    return result


def get_env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on", "y"):
        return True
    if raw in ("0", "false", "no", "off", "n"):
        return False
    return default


_TIME_HHMM_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


def parse_daily_role_ping_times(raw: str) -> list[tuple[int, int]]:
    """DAILY_ROLE_PING_TIMES: «09:00, 18:30» → список (час, минута), порядок как в строке, без дублей."""
    out: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        m = _TIME_HHMM_RE.match(part)
        if not m:
            raise RuntimeError(
                f"DAILY_ROLE_PING_TIMES: неверный фрагмент {part!r}. Нужен формат ЧЧ:ММ, несколько через запятую."
            )
        h, minute = int(m.group(1)), int(m.group(2))
        if h > 23 or minute > 59:
            raise RuntimeError(
                f"DAILY_ROLE_PING_TIMES: недопустимое время {part!r} (часы 0–23, минуты 0–59)."
            )
        key = (h, minute)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def next_daily_role_ping_fire(
    tz: ZoneInfo, times: list[tuple[int, int]]
) -> datetime:
    """Ближайший момент срабатывания из списка времён суток (timezone-aware)."""
    now = datetime.now(tz)
    today = now.date()
    best: Optional[datetime] = None
    for h, m in times:
        cand = datetime.combine(today, time(h, m, 0), tzinfo=tz)
        if cand > now and (best is None or cand < best):
            best = cand
    if best is not None:
        return best
    tomorrow = today + timedelta(days=1)
    for h, m in times:
        cand = datetime.combine(tomorrow, time(h, m, 0), tzinfo=tz)
        if best is None or cand < best:
            best = cand
    assert best is not None
    return best


def parse_terra_map_options(raw: str) -> list[tuple[str, list[str]]]:
    """TERRA_MAP_OPTIONS: подпись:файл или подпись:файл1+файл2 через | (до 25). До 10 файлов на пункт."""
    out: list[tuple[str, list[str]]] = []
    for block in raw.split("|"):
        block = block.strip()
        if not block or ":" not in block:
            continue
        label, files_raw = block.split(":", 1)
        label = label.strip()
        if not label:
            continue
        raw_files = files_raw.strip()
        if not raw_files:
            continue
        if "+" in raw_files:
            parts = [
                os.path.basename(p.strip())
                for p in raw_files.split("+")
                if p.strip()
            ]
        else:
            parts = [os.path.basename(raw_files)]
        parts = parts[:10]
        if parts:
            out.append((label, parts))
    return out[:25]


TOKEN = os.getenv("SECOND_BOT_TOKEN", "").strip() or os.getenv(
    "DISCORD_TOKEN", ""
).strip()
GUILD_ID = get_env_int("SECOND_BOT_GUILD_ID") or get_env_int("GUILD_ID")
SYNC_SLASH_ON_START = get_env_bool("SYNC_SLASH_ON_START", True)
# Заявки только в два канала: РП и VZP (общий канал не используется).
APPLICATIONS_RP_CHANNEL_ID = get_env_int("APPLICATIONS_RP_CHANNEL_ID")
APPLICATIONS_VZP_CHANNEL_ID = get_env_int("APPLICATIONS_VZP_CHANNEL_ID")
# После «Принять» в канале обзвона — роль заявителю по типу заявки (0 = не выдавать).
APPLICATION_RP_ACCEPT_ROLE_ID = get_env_int("APPLICATION_RP_ACCEPT_ROLE_ID")
APPLICATION_VZP_ACCEPT_ROLE_ID = get_env_int("APPLICATION_VZP_ACCEPT_ROLE_ID")
_TICKET_COUNTERS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "application_ticket_counters.json"
)
MOD_ROLE_ID = get_env_int("MOD_ROLE_ID")
PANEL_IMAGE_FILE = os.getenv("PANEL_IMAGE_FILE", "panel.gif").strip()
# Баннер /panel: только file (без set_image). В Discord большая картинка эмбеда всегда рисуется ПОД текстом;
# чтобы гиф был сверху как у GLOW — вложение + эмбед в одном сообщении (ширина превью и карточки может отличаться).
# Рекомендуемый размер гифки: широкая полоса (длиннее по горизонтали, чем 16:9):
PANEL_BANNER_RECOMMENDED_WIDTH = 1200
PANEL_BANNER_RECOMMENDED_HEIGHT = 500
# Вложение к сообщению в канале; сверх — Discord отклонит или нужно слать без файла.
PANEL_IMAGE_MAX_UPLOAD_BYTES = 25 * 1024 * 1024
MODERATION_ROLE_IDS = get_env_int_set("MODERATION_ROLE_IDS")


def role_ids_or_moderation(role_ids: set[int]) -> set[int]:
    """Пустой набор в .env → те же роли, что **MODERATION_ROLE_IDS** (один список на всё «модераторское»)."""
    return role_ids if role_ids else MODERATION_ROLE_IDS


PANEL_ROLE_IDS = get_env_int_set("PANEL_ROLE_IDS")
PANEL_CHANNEL_IDS = get_env_int_set("PANEL_CHANNEL_IDS")
ADMIN_ROLE_IDS = get_env_int_set("ADMIN_ROLE_IDS")


def resolve_panel_image_path() -> Optional[str]:
    """Файл баннера /panel: абсолютный путь из .env как есть; иначе папка со скриптом, затем cwd."""
    name = (PANEL_IMAGE_FILE or "").strip()
    if not name:
        return None
    if os.path.isabs(name):
        return name if os.path.isfile(name) else None
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for base in (script_dir, os.getcwd()):
        candidate = os.path.join(base, name)
        if os.path.isfile(candidate):
            return candidate
    return None

# Пустой *_GATHER_ROLE_IDS → те же роли, что MODERATION_ROLE_IDS (функция role_ids_or_moderation).
VZP_GATHER_ROLE_IDS = get_env_int_set("VZP_GATHER_ROLE_IDS")
VZP_GATHER_CHANNEL_ID = get_env_int("VZP_GATHER_CHANNEL_ID")
VZH_GATHER_ROLE_IDS = get_env_int_set("VZH_GATHER_ROLE_IDS")
VZH_GATHER_CHANNEL_ID = get_env_int("VZH_GATHER_CHANNEL_ID")
POSTAVKA_GATHER_ROLE_IDS = get_env_int_set("POSTAVKA_GATHER_ROLE_IDS")
POSTAVKA_GATHER_CHANNEL_ID = get_env_int("POSTAVKA_GATHER_CHANNEL_ID")
MP_GATHER_ROLE_IDS = get_env_int_set("MP_GATHER_ROLE_IDS")
MP_GATHER_CHANNEL_ID = get_env_int("MP_GATHER_CHANNEL_ID")
VZP_GATHER_PING_ROLE_ID = get_env_int("VZP_GATHER_PING_ROLE_ID")
VZH_GATHER_PING_ROLE_ID = get_env_int("VZH_GATHER_PING_ROLE_ID")
POSTAVKA_GATHER_PING_ROLE_ID = get_env_int("POSTAVKA_GATHER_PING_ROLE_ID")
MP_GATHER_PING_ROLE_ID = get_env_int("MP_GATHER_PING_ROLE_ID")

# Контракты: /kontrakt — панель; заявки с Участвовать / Пикнул / Отказ (ветка под отказом).
# Пустые KONTRAKT_*_ROLE_IDS → MODERATION_ROLE_IDS.
KONTRAKT_CHANNEL_ID = get_env_int("KONTRAKT_CHANNEL_ID")
KONTRAKT_POST_ROLE_IDS = get_env_int_set("KONTRAKT_POST_ROLE_IDS")
KONTRAKT_MANAGER_ROLE_IDS = get_env_int_set("KONTRAKT_MANAGER_ROLE_IDS")
# Через запятую в .env — @роль в сообщении при новом контракте (пусто = без пинга).
KONTRAKT_NEW_CONTRACT_PING_ROLE_IDS = get_env_int_set(
    "KONTRAKT_NEW_CONTRACT_PING_ROLE_IDS"
)
_KONTRAKT_RULES_DEFAULT = (
    "Здесь будут правила контрактов.\n\n"
    "Задайте текст в **KONTRAKT_RULES_TEXT** в .env (несколько строк через \\n)."
)
KONTRAKT_RULES_TEXT = (
    os.getenv("KONTRAKT_RULES_TEXT", "").strip() or _KONTRAKT_RULES_DEFAULT
)

# Тег роли в канале → ЛС всем с этой роли (текст как у автора; пишет бот).
# ROLE_MENTION_DM_CATEGORY_IDS — ID категорий: срабатывает в любом канале/ветке внутри категории.
# ROLE_MENTION_DM_CHANNEL_IDS — опционально: точечные каналы (или родитель ветки), если не хочешь всю категорию.
# Достаточно задать категории ИЛИ каналы (или оба — сработает любое совпадение).
# ROLE_MENTION_DM_TARGET_ROLE_IDS — какие роли при @ можно «разослать» (защита от случайных пингов).
# ROLE_MENTION_DM_ALLOWED_ROLE_IDS — пусто: писать может любой участник; иначе только эти роли (+ владелец / manage_guild).
ROLE_MENTION_DM_CATEGORY_IDS = get_env_int_set("ROLE_MENTION_DM_CATEGORY_IDS")
ROLE_MENTION_DM_CHANNEL_IDS = get_env_int_set("ROLE_MENTION_DM_CHANNEL_IDS")
ROLE_MENTION_DM_TARGET_ROLE_IDS = get_env_int_set("ROLE_MENTION_DM_TARGET_ROLE_IDS")
ROLE_MENTION_DM_ALLOWED_ROLE_IDS = get_env_int_set("ROLE_MENTION_DM_ALLOWED_ROLE_IDS")

TERRA_MAP_IMAGE_DIR = os.getenv("TERRA_MAP_IMAGE_DIR", "foto").strip() or "foto"
# Баннер сверху /karta_terry (gif/png/webm и т.д.). Пусто или не найден — тот же файл, что /panel.
TERRA_MAP_BANNER_FILE = os.getenv("TERRA_MAP_BANNER_FILE", "").strip()
TERRA_MAP_CHOICES: list[tuple[str, list[str]]] = parse_terra_map_options(
    os.getenv("TERRA_MAP_OPTIONS", "").strip()
)
TERRA_MAP_CHANNEL_IDS = get_env_int_set("TERRA_MAP_CHANNEL_IDS")
WAR_TIMER_CHANNEL_IDS = get_env_int_set("WAR_TIMER_CHANNEL_IDS")
WAR_TIMER_ROLE_IDS = get_env_int_set("WAR_TIMER_ROLE_IDS")
WAR_TIMER_PING_ROLE_ID = get_env_int("WAR_TIMER_PING_ROLE_ID")
STATS_CHANNEL_IDS = get_env_int_set("STATS_CHANNEL_IDS")
STATS_ROLE_IDS = get_env_int_set("STATS_ROLE_IDS")
STATS_PING_ROLE_ID = get_env_int("STATS_PING_ROLE_ID")
# /podarok — пустой PODAROK_POST_ROLE_IDS → MODERATION_ROLE_IDS.
PODAROK_POST_ROLE_IDS = get_env_int_set("PODAROK_POST_ROLE_IDS")
# В посте /podarok: пинг ролей «с доступом к каналу розыгрыша» (ID через запятую). Пусто = @everyone.
PODAROK_CHANNEL_ACCESS_ROLE_IDS = get_env_int_set("PODAROK_CHANNEL_ACCESS_ROLE_IDS")
# ЛС о новом розыгрыше: если **PODAROK_CHANNEL_ACCESS_ROLE_IDS** задан — всем с этими ролями; иначе — с этой одной роли (0 = без ЛС).
PODAROK_PING_ROLE_ID = get_env_int("PODAROK_PING_ROLE_ID")
# /sbormoney — только каналы из SBORMONEY_CHANNEL_IDS (список обязателен). Пустой SBORMONEY_POST_ROLE_IDS → MODERATION_ROLE_IDS. В посте @everyone.
SBORMONEY_POST_ROLE_IDS = get_env_int_set("SBORMONEY_POST_ROLE_IDS")
SBORMONEY_CHANNEL_IDS = get_env_int_set("SBORMONEY_CHANNEL_IDS")
_sb_money_mb = get_env_int("SBORMONEY_MAX_ATTACHMENT_MB", 10)
SBORMONEY_MAX_ATTACHMENT_BYTES = max(1, min(25, _sb_money_mb if _sb_money_mb > 0 else 10)) * (
    1024 * 1024
)
# Отчёты: ВЗХ (канал 1) и МП (канал 2). Пустой *_PANEL_ROLE_IDS → MODERATION_ROLE_IDS.
MATERIAL_REPORT_CHANNEL_IDS = get_env_int_set("MATERIAL_REPORT_CHANNEL_IDS")
MATERIAL_REPORT_PANEL_ROLE_IDS = get_env_int_set("MATERIAL_REPORT_PANEL_ROLE_IDS")
ACTIVITY_REPORT_CHANNEL_IDS = get_env_int_set("ACTIVITY_REPORT_CHANNEL_IDS")
ACTIVITY_REPORT_PANEL_ROLE_IDS = get_env_int_set("ACTIVITY_REPORT_PANEL_ROLE_IDS")
# Инактив / AFK: отдельные каналы; кто шлёт панель — INACTIV_PANEL_ROLE_IDS / AFK_PANEL_ROLE_IDS (пусто → MODERATION_ROLE_IDS).
INACTIV_CHANNEL_IDS = get_env_int_set("INACTIV_CHANNEL_IDS")
INACTIV_PANEL_ROLE_IDS = get_env_int_set("INACTIV_PANEL_ROLE_IDS")
AFK_CHANNEL_IDS = get_env_int_set("AFK_CHANNEL_IDS")
AFK_PANEL_ROLE_IDS = get_env_int_set("AFK_PANEL_ROLE_IDS")
# Пинг ролей при новой заявке (ID через запятую). Пусто = не пинговать.
# Поддерживаются оба имени: INACTIV_NOTIFY_ROLE_ID и INACTIV_NOTIFY_ROLE_IDS.
INACTIV_NOTIFY_ROLE_IDS = get_env_int_set("INACTIV_NOTIFY_ROLE_IDS") | get_env_int_set(
    "INACTIV_NOTIFY_ROLE_ID"
)
AFK_NOTIFY_ROLE_IDS = get_env_int_set("AFK_NOTIFY_ROLE_IDS") | get_env_int_set(
    "AFK_NOTIFY_ROLE_ID"
)
# Роль, выдаваемая при «Одобрить».
INACTIV_APPROVE_ROLE_ID = get_env_int("INACTIV_APPROVE_ROLE_ID")
# AFK: при «Одобрить» роль не выдаётся (только отметка в канале).

# /autopark — панель машин; бронь на AUTOPARK_BOOKING_MINUTES (по умолчанию 60); ЛС при взятии и снятии.
AUTOPARK_ROLE_IDS = get_env_int_set("AUTOPARK_ROLE_IDS")
AUTOPARK_CHANNEL_IDS = get_env_int_set("AUTOPARK_CHANNEL_IDS")
AUTOPARK_EDIT_USER_IDS = get_env_int_set("AUTOPARK_EDIT_USER_IDS")
# Роли, у кого есть «Изменить список» (ID ролей). Не путать с AUTOPARK_EDIT_USER_IDS — там user ID.
AUTOPARK_EDIT_ROLE_IDS = get_env_int_set("AUTOPARK_EDIT_ROLE_IDS")
_ap_bm = get_env_int("AUTOPARK_BOOKING_MINUTES", 60)
AUTOPARK_BOOKING_MINUTES = max(1, min(1440, _ap_bm if _ap_bm > 0 else 60))
AUTOPARK_BOOKING_SECONDS = AUTOPARK_BOOKING_MINUTES * 60
_ap_def = os.getenv("AUTOPARK_DEFAULT_CARS", "").strip()
AUTOPARK_DEFAULT_CAR_PARTS = [x.strip() for x in _ap_def.split("|") if x.strip()]
# JSON: { "guild_id": [ {"key","label","note","roles":[]} ] } рядом со скриптом, если путь не абсолютный.
AUTOPARK_LIST_FILE = os.getenv("AUTOPARK_LIST_FILE", "autopark_cars.json").strip() or "autopark_cars.json"
# Брони на панелях (message_id → state). После рестарта бота подхватывается, и свип снова отпускает машины.
AUTOPARK_PANELS_STATE_FILE = (
    os.getenv("AUTOPARK_PANELS_STATE_FILE", "autopark_panels.json").strip()
    or "autopark_panels.json"
)
# Розыгрыш (/podarok) и денежный сбор (/sbormoney): состояние панелей в JSON (переживает рестарт бота).
PODAROK_SBORMONEY_STATE_FILE = (
    os.getenv("PODAROK_SBORMONEY_STATE_FILE", "podarok_sbormoney_state.json").strip()
    or "podarok_sbormoney_state.json"
)
# Таймер att/deff, лог статистики ВЗП (/stats_panel), плашки /sbor и /kontrakt.
PANEL_EXTRA_STATE_FILE = (
    os.getenv("PANEL_EXTRA_STATE_FILE", "panel_extra_state.json").strip()
    or "panel_extra_state.json"
)

# Пост с <@&роль> в канале → удалить предыдущий пост бота → новый (по кругу). Пустой канал или роль = выкл.
# Режим 1: DAILY_ROLE_PING_TIMES=09:00,21:30 — каждый день в эти моменты (часовой пояс DAILY_ROLE_PING_TIMEZONE).
# Режим 2: если DAILY_ROLE_PING_TIMES пуст — через DAILY_ROLE_PING_INTERVAL_HOURS часов (как раньше).
DAILY_ROLE_PING_CHANNEL_ID = get_env_int("DAILY_ROLE_PING_CHANNEL_ID")
DAILY_ROLE_PING_ROLE_ID = get_env_int("DAILY_ROLE_PING_ROLE_ID")
_drp_iv = get_env_int("DAILY_ROLE_PING_INTERVAL_HOURS", 23)
DAILY_ROLE_PING_INTERVAL_HOURS = max(1, min(168, _drp_iv if _drp_iv > 0 else 23))
DAILY_ROLE_PING_MESSAGE = os.getenv("DAILY_ROLE_PING_MESSAGE", "").strip()
_drp_times_raw = os.getenv("DAILY_ROLE_PING_TIMES", "").strip()
DAILY_ROLE_PING_SCHEDULE = (
    parse_daily_role_ping_times(_drp_times_raw) if _drp_times_raw else []
)
_drp_tz_name = os.getenv("DAILY_ROLE_PING_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
try:
    DAILY_ROLE_PING_TZ: Optional[ZoneInfo] = (
        ZoneInfo(_drp_tz_name) if DAILY_ROLE_PING_SCHEDULE else None
    )
except ZoneInfoNotFoundError as exc:
    raise RuntimeError(
        f"DAILY_ROLE_PING_TIMEZONE: неизвестная зона {_drp_tz_name!r}."
    ) from exc

# Временные войсы: зайти в хаб → свой канал; панель в Text-in-Voice. 0 = выкл.
TEMP_VC_HUB_CHANNEL_ID = get_env_int("TEMP_VC_HUB_CHANNEL_ID")
TEMP_VC_CATEGORY_ID = get_env_int("TEMP_VC_CATEGORY_ID")

# Логи модерации (MOD_ACTION_LOG_CHANNEL_ID): только голос — серверный мик/наушники и перенос чужим;
# роли — выдача/снятие другим участником (по журналу аудита). Ник, бан, кик, выход сюда не пишутся.
# Вход на сервер — только в MEMBER_JOIN_LOG_CHANNEL_ID (если 0, вход не логируется).
# У бота на сервере должно быть право «Просматривать журнал аудита».
MOD_ACTION_LOG_CHANNEL_ID = get_env_int("MOD_ACTION_LOG_CHANNEL_ID")
MEMBER_JOIN_LOG_CHANNEL_ID = get_env_int("MEMBER_JOIN_LOG_CHANNEL_ID")

if not TOKEN:
    raise RuntimeError(
        "В .env нужен SECOND_BOT_TOKEN или DISCORD_TOKEN (токен бота заявок)."
    )

intents = discord.Intents.default()
intents.members = True  # role.members для /spam — в Portal включи Server Members Intent
intents.message_content = True  # ROLE_MENTION_DM_*: иначе в on_message часто пустые role_mentions
intents.voice_states = True  # временные голосовые комнаты
bot = commands.Bot(command_prefix="!", intents=intents)

APPLICATION_RP = "rp"
APPLICATION_VZP = "vzp"

APPLICATIONS_STATE = {
    APPLICATION_RP: True,
    APPLICATION_VZP: True,
}

GATHER_KIND_LABELS = {
    "vzp": "ВЗП",
    "vzh": "ВЗХ",
    "postavka": "Поставка",
    "mp": "МП",
}

# Слово в ЛС «Поставь реаку на …» — после «на» винительный падеж где нужно (Поставку).
GATHER_KIND_DM_REACTION_TAG = {
    "vzp": "взп",
    "vzh": "взх",
    "postavka": "Поставку",
    "mp": "МП",
}


def gather_roles_and_channel(kind_key: str) -> tuple[set[int], int]:
    if kind_key == "vzp":
        return VZP_GATHER_ROLE_IDS, VZP_GATHER_CHANNEL_ID
    if kind_key == "vzh":
        return VZH_GATHER_ROLE_IDS, VZH_GATHER_CHANNEL_ID
    if kind_key == "postavka":
        return POSTAVKA_GATHER_ROLE_IDS, POSTAVKA_GATHER_CHANNEL_ID
    if kind_key == "mp":
        return MP_GATHER_ROLE_IDS, MP_GATHER_CHANNEL_ID
    return set(), 0


def gather_ping_role_id(kind_key: str) -> int:
    if kind_key == "vzp":
        return VZP_GATHER_PING_ROLE_ID
    if kind_key == "vzh":
        return VZH_GATHER_PING_ROLE_ID
    if kind_key == "postavka":
        return POSTAVKA_GATHER_PING_ROLE_ID
    if kind_key == "mp":
        return MP_GATHER_PING_ROLE_ID
    return 0


def user_can_post_gather(interaction: discord.Interaction, kind_key: str) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    role_ids, channel_id = gather_roles_and_channel(kind_key)
    if channel_id != 0 and interaction.channel_id != channel_id:
        return False
    member = interaction.user
    if interaction.guild.owner_id == member.id:
        return True
    if member.guild_permissions.manage_guild:
        return True
    effective = role_ids_or_moderation(role_ids)
    if not effective:
        return False
    return any(role.id in effective for role in member.roles)


@dataclass
class GatherState:
    kind_key: str
    title: str
    max_main: int
    max_extra: int
    main_ids: list[int] = field(default_factory=list)
    extra_ids: list[int] = field(default_factory=list)
    status_open: bool = True
    creator_id: int = 0
    creator_tag: str = ""
    closes_at: Optional[datetime] = None  # UTC; после этого момента запись закрывается сама
    channel_id: int = 0
    # Если задано — «Время:» через <t:…> (относительное время в клиенте обновляется само)
    scheduled_at: Optional[datetime] = None


GATHER_MESSAGES: dict[int, GatherState] = {}


@dataclass
class PodarokState:
    """Розыгрыш в канале; состояние в **podarok_sbormoney_state.json** (и в памяти)."""

    prize: str
    max_participants: int
    winner_count: int
    ends_at: datetime  # UTC — до какого момента можно нажать «Участвовать»
    creator_id: int
    creator_tag: str
    channel_id: int
    participant_ids: list[int] = field(default_factory=list)
    finished: bool = False
    winner_ids: list[int] = field(default_factory=list)
    after_deadline_embed_done: bool = False


PODAROK_MESSAGES: dict[int, PodarokState] = {}


@dataclass
class SbormoneyState:
    """Сбор денег в канале; цель и «собрано» в **podarok_sbormoney_state.json** (и в памяти)."""

    na_chto: str
    summa_text: str
    goal_amount: int
    collected: int
    author_tag: str
    channel_id: int


SBORMONEY_MESSAGES: dict[int, SbormoneyState] = {}


def _podarok_parse_ends_at(raw: str) -> datetime:
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def podarok_state_to_dict(st: PodarokState) -> dict:
    return {
        "prize": st.prize,
        "max_participants": st.max_participants,
        "winner_count": st.winner_count,
        "ends_at": st.ends_at.isoformat(),
        "creator_id": st.creator_id,
        "creator_tag": st.creator_tag,
        "channel_id": st.channel_id,
        "participant_ids": list(st.participant_ids),
        "finished": st.finished,
        "winner_ids": list(st.winner_ids),
        "after_deadline_embed_done": st.after_deadline_embed_done,
    }


def podarok_post_content_and_mentions(
    guild: discord.Guild,
) -> tuple[str, discord.AllowedMentions]:
    """Пост /podarok: пинг ролей с доступом к каналу или @everyone, если список ролей пуст."""
    ids = PODAROK_CHANNEL_ACCESS_ROLE_IDS
    if not ids:
        return "@everyone", discord.AllowedMentions(everyone=True)
    roles: list[discord.Role] = []
    for rid in ids:
        r = guild.get_role(rid)
        if r:
            roles.append(r)
    if not roles:
        return "@everyone", discord.AllowedMentions(everyone=True)
    content = " ".join(r.mention for r in roles)
    return content, discord.AllowedMentions(everyone=False, roles=roles)


def podarok_state_from_dict(d: dict) -> PodarokState:
    return PodarokState(
        prize=str(d.get("prize", ""))[:2000],
        max_participants=int(d["max_participants"]),
        winner_count=int(d["winner_count"]),
        ends_at=_podarok_parse_ends_at(str(d["ends_at"])),
        creator_id=int(d["creator_id"]),
        creator_tag=str(d.get("creator_tag", ""))[:80],
        channel_id=int(d["channel_id"]),
        participant_ids=[int(x) for x in d.get("participant_ids", [])],
        finished=bool(d.get("finished", False)),
        winner_ids=[int(x) for x in d.get("winner_ids", [])],
        after_deadline_embed_done=bool(d.get("after_deadline_embed_done", False)),
    )


def sbormoney_state_to_dict(st: SbormoneyState) -> dict:
    return {
        "na_chto": st.na_chto,
        "summa_text": st.summa_text,
        "goal_amount": st.goal_amount,
        "collected": st.collected,
        "author_tag": st.author_tag,
        "channel_id": st.channel_id,
    }


def sbormoney_state_from_dict(d: dict) -> SbormoneyState:
    return SbormoneyState(
        na_chto=str(d.get("na_chto", ""))[:2000],
        summa_text=str(d.get("summa_text", ""))[:1024],
        goal_amount=int(d["goal_amount"]),
        collected=int(d.get("collected", 0)),
        author_tag=str(d.get("author_tag", ""))[:80],
        channel_id=int(d["channel_id"]),
    )


def persist_podarok_sbormoney() -> None:
    """Сохранить PODAROK_MESSAGES и SBORMONEY_MESSAGES на диск."""
    path = PODAROK_SBORMONEY_STATE_FILE
    try:
        payload = {
            "podarok": {
                str(mid): podarok_state_to_dict(st)
                for mid, st in PODAROK_MESSAGES.items()
            },
            "sbormoney": {
                str(mid): sbormoney_state_to_dict(st)
                for mid, st in SBORMONEY_MESSAGES.items()
            },
        }
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except OSError:
        pass


def load_podarok_sbormoney_state() -> None:
    """Загрузить состояние из JSON (при старте бота)."""
    path = PODAROK_SBORMONEY_STATE_FILE
    if not os.path.isfile(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(data, dict):
        return
    pod = data.get("podarok")
    sbor = data.get("sbormoney")
    PODAROK_MESSAGES.clear()
    if isinstance(pod, dict):
        for k, v in pod.items():
            try:
                mid = int(k)
                if isinstance(v, dict):
                    PODAROK_MESSAGES[mid] = podarok_state_from_dict(v)
            except (KeyError, TypeError, ValueError):
                continue
    SBORMONEY_MESSAGES.clear()
    if isinstance(sbor, dict):
        for k, v in sbor.items():
            try:
                mid = int(k)
                if isinstance(v, dict):
                    SBORMONEY_MESSAGES[mid] = sbormoney_state_from_dict(v)
            except (KeyError, TypeError, ValueError):
                continue


@dataclass
class ContractState:
    channel_id: int
    creator_id: int
    creator_tag: str
    title: str
    veksels: str
    time_slot: str
    razdel_100: str
    people_note: str
    max_participants: int
    participant_ids: list[int] = field(default_factory=list)
    status_open: bool = True


CONTRACT_MESSAGES: dict[int, ContractState] = {}


@dataclass
class AutoparkCar:
    """Запись в общем списке машин сервера (файл autopark_cars.json)."""

    key: str
    label: str
    note: str = ""
    access_role_ids: tuple[int, ...] = ()

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "label": self.label,
            "note": self.note,
            "roles": list(self.access_role_ids),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "AutoparkCar":
        roles_raw = d.get("roles") if "roles" in d else d.get("access_role_ids", [])
        rid: list[int] = []
        if isinstance(roles_raw, list):
            for x in roles_raw:
                try:
                    rid.append(int(x))
                except (TypeError, ValueError):
                    pass
        return cls(
            key=str(d["key"]).strip(),
            label=str(d.get("label") or d["key"]).strip(),
            note=str(d.get("note", "")).strip(),
            access_role_ids=tuple(rid),
        )


@dataclass
class AutoparkState:
    channel_id: int
    guild_id: int
    bookings: dict[str, tuple[int, float]] = field(default_factory=dict)


AUTOPARK_MESSAGES: dict[int, AutoparkState] = {}
_AUTOPARK_EXPIRE_TASKS: dict[tuple[int, str], asyncio.Task] = {}


def _autopark_panels_path() -> str:
    p = AUTOPARK_PANELS_STATE_FILE
    if os.path.isabs(p):
        return p
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), p)


def autopark_save_panels_state() -> None:
    try:
        payload = {
            "v": 1,
            "messages": {
                str(mid): {
                    "channel_id": st.channel_id,
                    "guild_id": st.guild_id,
                    "bookings": {ck: [t[0], t[1]] for ck, t in st.bookings.items()},
                }
                for mid, st in AUTOPARK_MESSAGES.items()
            },
        }
        with open(_autopark_panels_path(), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


def autopark_load_panels_state() -> bool:
    path = _autopark_panels_path()
    if not os.path.isfile(path):
        return False
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(data, dict) or data.get("v") != 1:
        return False
    msgs = data.get("messages")
    if not isinstance(msgs, dict):
        return False
    AUTOPARK_MESSAGES.clear()
    for mid_str, raw in msgs.items():
        try:
            mid = int(mid_str)
        except (TypeError, ValueError):
            continue
        if not isinstance(raw, dict):
            continue
        try:
            cid = int(raw["channel_id"])
            gid = int(raw["guild_id"])
        except (KeyError, TypeError, ValueError):
            continue
        bk_raw = raw.get("bookings") or {}
        bookings: dict[str, tuple[int, float]] = {}
        if isinstance(bk_raw, dict):
            for ck, pair in bk_raw.items():
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    continue
                try:
                    uid = int(pair[0])
                    exp = float(pair[1])
                except (TypeError, ValueError):
                    continue
                bookings[str(ck)] = (uid, exp)
        AUTOPARK_MESSAGES[mid] = AutoparkState(
            channel_id=cid, guild_id=gid, bookings=bookings
        )
    return bool(AUTOPARK_MESSAGES)


def autopark_restart_expire_workers() -> None:
    now = _autopark_now_ts()
    for mid, st in list(AUTOPARK_MESSAGES.items()):
        for car_key, (uid, exp) in list(st.bookings.items()):
            if now >= exp:
                continue
            if _AUTOPARK_EXPIRE_TASKS.get((mid, car_key)) is not None:
                t = _AUTOPARK_EXPIRE_TASKS[(mid, car_key)]
                if not t.done():
                    continue
            task = asyncio.create_task(
                autopark_expire_worker(mid, car_key, uid, exp)
            )
            _AUTOPARK_EXPIRE_TASKS[(mid, car_key)] = task


def _autopark_list_path() -> str:
    p = AUTOPARK_LIST_FILE
    if os.path.isabs(p):
        return p
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), p)


def _autopark_load_raw_file() -> dict:
    path = _autopark_list_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _autopark_write_raw_file(data: dict) -> None:
    path = _autopark_list_path()
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def autopark_default_cars_from_env() -> list[AutoparkCar]:
    out: list[AutoparkCar] = []
    for part in AUTOPARK_DEFAULT_CAR_PARTS:
        if ":" in part:
            key, _, label = part.partition(":")
            key, label = key.strip(), label.strip()
            if key and label:
                out.append(AutoparkCar(key=key, label=label))
        elif part:
            slug = re.sub(r"[^\w\-]+", "_", part, flags=re.UNICODE)[:48].strip("_")
            if not slug:
                slug = "car"
            out.append(AutoparkCar(key=slug, label=part))
    return out


def autopark_load_guild_cars(guild_id: int) -> list[AutoparkCar]:
    data = _autopark_load_raw_file()
    gid = str(guild_id)
    if gid not in data:
        return autopark_default_cars_from_env()
    raw = data.get(gid, [])
    cars: list[AutoparkCar] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and str(item.get("key", "")).strip():
                try:
                    cars.append(AutoparkCar.from_dict(item))
                except (KeyError, TypeError, ValueError):
                    pass
    return cars


def autopark_save_guild_cars(guild_id: int, cars: list[AutoparkCar]) -> None:
    data = _autopark_load_raw_file()
    data[str(guild_id)] = [c.to_dict() for c in cars]
    _autopark_write_raw_file(data)


def autopark_normalize_car_key(raw: str) -> str:
    s = raw.strip().upper().replace(" ", "_")
    s = re.sub(r"[^\w\-]", "", s, flags=re.UNICODE)
    return s[:80]


def autopark_parse_role_ids_field(raw: str) -> tuple[int, ...]:
    out: list[int] = []
    for chunk in re.split(r"[\s,;]+", raw.strip()):
        if chunk.isdigit():
            out.append(int(chunk))
    return tuple(out)


def autopark_car_by_key(guild_id: int, car_key: str) -> AutoparkCar | None:
    for c in autopark_load_guild_cars(guild_id):
        if c.key == car_key:
            return c
    return None


def autopark_car_label(guild_id: int, car_key: str) -> str:
    c = autopark_car_by_key(guild_id, car_key)
    return c.label if c is not None else car_key


def autopark_member_can_book(member: discord.Member, car: AutoparkCar) -> bool:
    if not car.access_role_ids:
        return True
    return any(r.id in car.access_role_ids for r in member.roles)


def _autopark_now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def autopark_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not AUTOPARK_CHANNEL_IDS:
        return True
    return interaction.channel_id in AUTOPARK_CHANNEL_IDS


def autopark_channel_restriction_message() -> str:
    if not AUTOPARK_CHANNEL_IDS:
        return ""
    parts = ", ".join(f"<#{cid}>" for cid in sorted(AUTOPARK_CHANNEL_IDS))
    return f"**Автопарк** можно отправить только в: {parts}."


def user_can_post_autopark(interaction: discord.Interaction) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    member = interaction.user
    if interaction.guild.owner_id == member.id:
        return True
    if member.guild_permissions.manage_guild:
        return True
    ap = role_ids_or_moderation(AUTOPARK_ROLE_IDS)
    if not ap:
        return False
    return any(r.id in ap for r in member.roles)


def user_can_edit_autopark_inventory(interaction: discord.Interaction) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    member = interaction.user
    if AUTOPARK_EDIT_USER_IDS and member.id in AUTOPARK_EDIT_USER_IDS:
        return True
    edit_roles = role_ids_or_moderation(AUTOPARK_EDIT_ROLE_IDS)
    if edit_roles and any(r.id in edit_roles for r in member.roles):
        return True
    return False


def autopark_cancel_expire_task(message_id: int, car_key: str) -> None:
    t = _AUTOPARK_EXPIRE_TASKS.pop((message_id, car_key), None)
    if t is not None and not t.done():
        t.cancel()


def autopark_active_booking(
    state: AutoparkState, car_key: str
) -> tuple[int, float] | None:
    tup = state.bookings.get(car_key)
    if tup is None:
        return None
    uid, exp = tup
    if _autopark_now_ts() >= exp:
        return None
    return tup


def autopark_free_cars(state: AutoparkState) -> list[AutoparkCar]:
    now = _autopark_now_ts()
    free: list[AutoparkCar] = []
    for car in autopark_load_guild_cars(state.guild_id):
        tup = state.bookings.get(car.key)
        if tup is None or now >= tup[1]:
            free.append(car)
    return free


def autopark_prune_removed_cars(state: AutoparkState, message_id: int) -> None:
    """Убирает брони по машинам, которых уже нет в списке (без ЛС — позиция удалена из парка)."""
    valid_keys = {c.key for c in autopark_load_guild_cars(state.guild_id)}
    changed = False
    for ck in list(state.bookings.keys()):
        if ck not in valid_keys:
            autopark_cancel_expire_task(message_id, ck)
            state.bookings.pop(ck, None)
            changed = True
    if changed:
        autopark_save_panels_state()


async def autopark_release_all_expired_for_message(
    message_id: int, guild: discord.Guild
) -> None:
    """Истёкшие брони: обновить панель, убрать из state, написать в ЛС (как по таймеру)."""
    state = AUTOPARK_MESSAGES.get(message_id)
    if state is None:
        return
    now = _autopark_now_ts()
    for car_key, (uid, exp) in list(state.bookings.items()):
        if now >= exp:
            await autopark_release_car(
                message_id, car_key, uid, guild=guild, auto=True
            )


async def autopark_expire_sweep_loop() -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = _autopark_now_ts()
            for mid, st in list(AUTOPARK_MESSAGES.items()):
                ch = await autopark_resolve_panel_channel(st.channel_id)
                if ch is None:
                    continue
                g = ch.guild
                if g is None:
                    continue
                had_expired = any(now >= t[1] for t in st.bookings.values())
                await autopark_release_all_expired_for_message(mid, g)
                pruned = False
                for ck in list(st.bookings.keys()):
                    tup = st.bookings.get(ck)
                    if tup is not None and now >= tup[1]:
                        st.bookings.pop(ck, None)
                        autopark_cancel_expire_task(mid, ck)
                        pruned = True
                if had_expired or pruned:
                    try:
                        msg = await ch.fetch_message(mid)
                        embed = build_autopark_embed(st, g)
                        await autopark_try_edit_panel(msg, embed, AutoparkView())
                    except discord.HTTPException:
                        pass
                    autopark_save_panels_state()
        except Exception:
            pass
        await asyncio.sleep(5)


async def autopark_try_edit_panel(
    message: discord.Message, embed: discord.Embed, view: discord.ui.View | None
) -> bool:
    try:
        await message.edit(embed=embed, view=view)
        return True
    except discord.NotFound:
        mid = message.id
        st = AUTOPARK_MESSAGES.pop(mid, None)
        if st is not None:
            for ck in list(st.bookings.keys()):
                autopark_cancel_expire_task(mid, ck)
        autopark_save_panels_state()
        return False
    except discord.HTTPException:
        return False


async def _autopark_ack_component(interaction: discord.Interaction) -> bool:
    try:
        await interaction.response.defer(thinking=True, ephemeral=True)
        return True
    except discord.NotFound:
        return False


async def autopark_resolve_panel_channel(
    channel_id: int,
) -> discord.TextChannel | discord.Thread | None:
    """Канал панели: кэш или fetch — иначе таймер/релиз не может отредактировать сообщение."""
    ch = bot.get_channel(channel_id)
    if isinstance(ch, (discord.TextChannel, discord.Thread)):
        return ch
    try:
        fetched = await bot.fetch_channel(channel_id)
        if isinstance(fetched, (discord.TextChannel, discord.Thread)):
            return fetched
    except discord.HTTPException:
        pass
    return None


def build_autopark_embed(state: AutoparkState, guild: discord.Guild) -> discord.Embed:
    now = _autopark_now_ts()
    free_lines: list[str] = []
    busy_lines: list[str] = []
    for car in autopark_load_guild_cars(state.guild_id):
        tup = state.bookings.get(car.key)
        if tup is None or now >= tup[1]:
            line = f"• **{car.label}**"
            if car.note:
                line += f"\n  _{car.note[:200]}_"
            free_lines.append(line)
        else:
            uid, exp = tup
            mem = guild.get_member(uid)
            tag = mem.mention if mem is not None else f"<@{uid}>"
            exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
            end_f = discord.utils.format_dt(exp_dt, style="F")
            line = f"• **{car.label}** — {tag} до {end_f}"
            if car.note:
                line += f"\n  _{car.note[:200]}_"
            busy_lines.append(line)
    free_val = "\n".join(free_lines) if free_lines else "—"
    busy_val = "\n".join(busy_lines) if busy_lines else "—"
    free_count = len(free_lines)
    busy_count = len(busy_lines)
    embed = discord.Embed(
        title="🚗 Автопарк",
        description="Актуальный статус автомобилей",
        color=discord.Color.green(),
    )
    embed.add_field(
        name=f"🟢 Свободные ({free_count})",
        value=free_val[:1024],
        inline=True,
    )
    embed.add_field(
        name=f"🔴 Занятые ({busy_count})",
        value=busy_val[:1024],
        inline=True,
    )
    ts = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")
    embed.set_footer(
        text=(
            f"Панель автопарка · бронь {AUTOPARK_BOOKING_MINUTES} мин · "
            f"обновляется при брони и после истечения · {ts}"
        )
    )
    return embed


async def autopark_refresh_all_panels_guild(guild_id: int) -> None:
    for mid, st in list(AUTOPARK_MESSAGES.items()):
        if st.guild_id == guild_id:
            await autopark_refresh_message(mid)


async def autopark_refresh_message(message_id: int) -> None:
    state = AUTOPARK_MESSAGES.get(message_id)
    if state is None:
        return
    ch = await autopark_resolve_panel_channel(state.channel_id)
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        st = AUTOPARK_MESSAGES.pop(message_id, None)
        if st is not None:
            for ck in list(st.bookings.keys()):
                autopark_cancel_expire_task(message_id, ck)
        autopark_save_panels_state()
        return
    try:
        msg = await ch.fetch_message(message_id)
    except discord.HTTPException:
        st = AUTOPARK_MESSAGES.pop(message_id, None)
        if st is not None:
            for ck in list(st.bookings.keys()):
                autopark_cancel_expire_task(message_id, ck)
        autopark_save_panels_state()
        return
    guild = ch.guild
    embed = build_autopark_embed(state, guild)
    await autopark_try_edit_panel(msg, embed, AutoparkView())


async def autopark_dm_user(guild: discord.Guild, user_id: int, content: str) -> None:
    member = guild.get_member(user_id)
    if member is not None and not member.bot:
        try:
            await member.send(content[:2000])
        except discord.HTTPException:
            pass
        return
    try:
        u = await bot.fetch_user(user_id)
        if not u.bot:
            await u.send(content[:2000])
    except discord.HTTPException:
        pass


async def autopark_release_car(
    message_id: int,
    car_key: str,
    user_id: int,
    *,
    guild: discord.Guild,
    auto: bool,
) -> None:
    state = AUTOPARK_MESSAGES.get(message_id)
    if state is None:
        return
    tup = state.bookings.get(car_key)
    if tup is None or tup[0] != user_id:
        return
    expired = _autopark_now_ts() >= tup[1]
    if not auto and expired:
        # Бронь уже истекла: клиент показывает «N секунд назад», но колонки эмбеда старые — синхронизируем.
        state.bookings.pop(car_key, None)
        autopark_cancel_expire_task(message_id, car_key)
        ch = await autopark_resolve_panel_channel(state.channel_id)
        if ch is not None:
            try:
                msg = await ch.fetch_message(message_id)
                embed = build_autopark_embed(state, guild)
                await autopark_try_edit_panel(msg, embed, AutoparkView())
            except discord.HTTPException:
                pass
        autopark_save_panels_state()
        return
    state.bookings.pop(car_key, None)
    autopark_cancel_expire_task(message_id, car_key)
    ch = await autopark_resolve_panel_channel(state.channel_id)
    if ch is not None:
        try:
            msg = await ch.fetch_message(message_id)
            embed = build_autopark_embed(state, guild)
            await autopark_try_edit_panel(msg, embed, AutoparkView())
        except discord.HTTPException:
            pass
    label = autopark_car_label(guild.id, car_key)
    if auto:
        text = (
            f"⏱ Время брони **{label}** истекло — машина **свободна** и снова в списке "
            f"«Свободные» на панели автопарка."
        )
    else:
        text = f"✅ Ты освободил **{label}**. Бронь снята."
    await autopark_dm_user(guild, user_id, text)
    autopark_save_panels_state()


async def autopark_expire_worker(
    message_id: int, car_key: str, user_id: int, expire_ts: float
) -> None:
    try:
        delay = expire_ts - _autopark_now_ts()
        if delay > 0:
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return
        state = AUTOPARK_MESSAGES.get(message_id)
        if state is None:
            return
        tup = state.bookings.get(car_key)
        if tup is None or tup[0] != user_id:
            return
        if abs(tup[1] - expire_ts) > 90.0:
            return
        ch = await autopark_resolve_panel_channel(state.channel_id)
        if ch is not None:
            guild = ch.guild
        else:
            guild = bot.get_guild(state.guild_id)
            if guild is None:
                try:
                    guild = await bot.fetch_guild(state.guild_id)
                except discord.HTTPException:
                    guild = None
        if guild is None:
            tup2 = state.bookings.get(car_key)
            if (
                tup2 is not None
                and tup2[0] == user_id
                and _autopark_now_ts() >= tup2[1]
            ):
                state.bookings.pop(car_key, None)
                autopark_cancel_expire_task(message_id, car_key)
                autopark_save_panels_state()
            return
        await autopark_release_car(
            message_id, car_key, user_id, guild=guild, auto=True
        )
    finally:
        _AUTOPARK_EXPIRE_TASKS.pop((message_id, car_key), None)


async def autopark_bootstrap_after_load() -> None:
    """После загрузки autopark_panels.json: снять истёкшие брони, обновить сообщения, заново повесить таймеры."""
    await bot.wait_until_ready()
    await asyncio.sleep(1.5)
    if not AUTOPARK_MESSAGES:
        return
    for mid, st in list(AUTOPARK_MESSAGES.items()):
        ch = await autopark_resolve_panel_channel(st.channel_id)
        if ch is None:
            continue
        g = ch.guild
        if g is None:
            continue
        now = _autopark_now_ts()
        had_expired = any(now >= t[1] for t in st.bookings.values())
        await autopark_release_all_expired_for_message(mid, g)
        pruned = False
        for ck in list(st.bookings.keys()):
            tup = st.bookings.get(ck)
            if tup is not None and now >= tup[1]:
                st.bookings.pop(ck, None)
                autopark_cancel_expire_task(mid, ck)
                pruned = True
        if had_expired or pruned:
            try:
                msg = await ch.fetch_message(mid)
                embed = build_autopark_embed(st, g)
                await autopark_try_edit_panel(msg, embed, AutoparkView())
            except discord.HTTPException:
                pass
    autopark_save_panels_state()
    autopark_restart_expire_workers()


async def autopark_take_car(
    interaction: discord.Interaction,
    message_id: int,
    car_key: str,
) -> None:
    guild = interaction.guild
    if guild is None:
        return
    state = AUTOPARK_MESSAGES.get(message_id)
    if state is None:
        await interaction.followup.send("Панель устарела.", ephemeral=True)
        return
    car = autopark_car_by_key(state.guild_id, car_key)
    if car is None:
        await interaction.followup.send("Этой машины нет в списке.", ephemeral=True)
        return
    if not isinstance(interaction.user, discord.Member):
        await interaction.followup.send("Ошибка участника.", ephemeral=True)
        return
    if not autopark_member_can_book(interaction.user, car):
        await interaction.followup.send(
            "У тебя нет роли для брони этой машины.", ephemeral=True
        )
        return
    if autopark_active_booking(state, car_key) is not None:
        await interaction.followup.send("Машина уже занята.", ephemeral=True)
        return
    uid = interaction.user.id
    now = _autopark_now_ts()
    exp = now + AUTOPARK_BOOKING_SECONDS
    state.bookings[car_key] = (uid, exp)
    autopark_cancel_expire_task(message_id, car_key)
    task = asyncio.create_task(
        autopark_expire_worker(message_id, car_key, uid, exp)
    )
    _AUTOPARK_EXPIRE_TASKS[(message_id, car_key)] = task
    ch = await autopark_resolve_panel_channel(state.channel_id)
    if ch is None:
        state.bookings.pop(car_key, None)
        autopark_cancel_expire_task(message_id, car_key)
        autopark_save_panels_state()
        await interaction.followup.send("Канал недоступен.", ephemeral=True)
        return
    try:
        panel_msg = await ch.fetch_message(message_id)
    except discord.HTTPException:
        state.bookings.pop(car_key, None)
        autopark_cancel_expire_task(message_id, car_key)
        autopark_save_panels_state()
        await interaction.followup.send("Сообщение панели не найдено.", ephemeral=True)
        return
    embed = build_autopark_embed(state, guild)
    if not await autopark_try_edit_panel(panel_msg, embed, AutoparkView()):
        state.bookings.pop(car_key, None)
        autopark_cancel_expire_task(message_id, car_key)
        autopark_save_panels_state()
        await interaction.followup.send("Не удалось обновить панель.", ephemeral=True)
        return
    exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
    abs_f = discord.utils.format_dt(exp_dt, style="F")
    rel_f = discord.utils.format_dt(exp_dt, style="R")
    dm_txt = (
        f"🚗 Ты занял(а) **{car.label}** на **{AUTOPARK_BOOKING_MINUTES} мин** "
        f"(до {abs_f}, {rel_f}). Освободить можно кнопкой **Освободить авто** на панели."
    )
    await autopark_dm_user(guild, uid, dm_txt)
    autopark_save_panels_state()
    await interaction.followup.send(f"Забронировано: **{car.label}**.", ephemeral=True)


class AutoparkPickSelect(discord.ui.Select):
    def __init__(self, message_id: int, cars: list[AutoparkCar]) -> None:
        opts = []
        for c in cars[:25]:
            desc = (c.note or c.key)[:100]
            opts.append(
                discord.SelectOption(
                    label=c.label[:100],
                    value=c.key[:100],
                    description=desc,
                )
            )
        super().__init__(
            placeholder="Выбери свободную машину",
            min_values=1,
            max_values=1,
            options=opts,
        )
        self._message_id = message_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await _autopark_ack_component(interaction):
            return
        if interaction.guild is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        car_key = self.values[0]
        await autopark_release_all_expired_for_message(
            self._message_id, interaction.guild
        )
        await autopark_take_car(interaction, self._message_id, car_key)


class AutoparkPickView(discord.ui.View):
    def __init__(self, message_id: int, cars: list[AutoparkCar]) -> None:
        super().__init__(timeout=300)
        self.add_item(AutoparkPickSelect(message_id, cars))


class AutoparkReleaseSelect(discord.ui.Select):
    def __init__(self, message_id: int, guild_id: int, car_keys: list[str]) -> None:
        opts = []
        for ck in car_keys[:25]:
            lbl = autopark_car_label(guild_id, ck)
            opts.append(
                discord.SelectOption(
                    label=lbl[:100],
                    value=ck[:100],
                    description=ck[:100],
                )
            )
        super().__init__(
            placeholder="Какую бронь снять?",
            min_values=1,
            max_values=1,
            options=opts,
        )
        self._message_id = message_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await _autopark_ack_component(interaction):
            return
        if interaction.guild is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        car_key = self.values[0]
        await autopark_release_all_expired_for_message(
            self._message_id, interaction.guild
        )
        uid = interaction.user.id
        await autopark_release_car(
            self._message_id, car_key, uid, guild=interaction.guild, auto=False
        )
        await interaction.followup.send("Машина освобождена.", ephemeral=True)


class AutoparkReleaseView(discord.ui.View):
    def __init__(self, message_id: int, guild_id: int, keys: list[str]) -> None:
        super().__init__(timeout=300)
        self.add_item(AutoparkReleaseSelect(message_id, guild_id, keys))


class AutoparkAddCarModal(discord.ui.Modal, title="Добавить авто в список"):
    def __init__(self, message_id: int) -> None:
        super().__init__()
        self._message_id = message_id
        self._key = discord.ui.TextInput(
            label="Ключ (уникальный ID)",
            required=True,
            max_length=80,
            placeholder="Например: PESTONA01",
        )
        self._label = discord.ui.TextInput(
            label="Как показывать в списке",
            required=True,
            max_length=120,
            placeholder="Pestona — PESTONA01",
        )
        self._note = discord.ui.TextInput(
            label="Текст под строкой (необязательно)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=400,
            placeholder="@роль …",
        )
        self._roles = discord.ui.TextInput(
            label="ID ролей доступа (через запятую)",
            required=False,
            max_length=400,
            placeholder="Пусто = доступно всем",
        )
        self.add_item(self._key)
        self.add_item(self._label)
        self.add_item(self._note)
        self.add_item(self._roles)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not user_can_edit_autopark_inventory(interaction):
            await interaction.response.send_message("Нет прав на редактирование.", ephemeral=True)
            return
        state = AUTOPARK_MESSAGES.get(self._message_id)
        if state is None:
            await interaction.response.send_message("Панель устарела.", ephemeral=True)
            return
        key = autopark_normalize_car_key(str(self._key.value))
        if not key:
            await interaction.response.send_message("Укажи непустой ключ.", ephemeral=True)
            return
        label = str(self._label.value).strip()
        if not label:
            await interaction.response.send_message("Укажи подпись для списка.", ephemeral=True)
            return
        note = str(self._note.value).strip()
        roles_t = autopark_parse_role_ids_field(str(self._roles.value))
        cars = list(autopark_load_guild_cars(state.guild_id))
        if any(c.key == key for c in cars):
            await interaction.response.send_message(
                f"Ключ **{key}** уже есть в списке.", ephemeral=True
            )
            return
        cars.append(
            AutoparkCar(
                key=key, label=label, note=note, access_role_ids=roles_t
            )
        )
        autopark_save_guild_cars(state.guild_id, cars)
        await interaction.response.defer(ephemeral=True)
        await autopark_refresh_all_panels_guild(state.guild_id)
        await interaction.followup.send(f"Добавлено: **{label}** (`{key}`).", ephemeral=True)


class AutoparkDeleteCarSelect(discord.ui.Select):
    def __init__(self, message_id: int, guild_id: int, cars: list[AutoparkCar]) -> None:
        opts = [
            discord.SelectOption(
                label=c.label[:100],
                value=c.key[:100],
                description=(c.key[:100]),
                emoji="🗑️",
            )
            for c in cars[:25]
        ]
        super().__init__(
            placeholder="Выбери авто, чтобы убрать из списка…",
            min_values=1,
            max_values=1,
            options=opts,
        )
        self._message_id = message_id
        self._guild_id = guild_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            return
        if not user_can_edit_autopark_inventory(interaction):
            await interaction.response.send_message("Нет прав.", ephemeral=True)
            return
        car_key = self.values[0]
        state = AUTOPARK_MESSAGES.get(self._message_id)
        if state is None:
            await interaction.response.send_message("Панель устарела.", ephemeral=True)
            return
        cars = [c for c in autopark_load_guild_cars(self._guild_id) if c.key != car_key]
        autopark_save_guild_cars(self._guild_id, cars)
        autopark_cancel_expire_task(self._message_id, car_key)
        state.bookings.pop(car_key, None)
        autopark_save_panels_state()
        await interaction.response.defer(ephemeral=True)
        await autopark_refresh_all_panels_guild(self._guild_id)
        await interaction.followup.send("Позиция удалена из списка.", ephemeral=True)


class AutoparkDeleteCarView(discord.ui.View):
    def __init__(self, message_id: int, guild_id: int, cars: list[AutoparkCar]) -> None:
        super().__init__(timeout=300)
        self.add_item(AutoparkDeleteCarSelect(message_id, guild_id, cars))


class AutoparkListEditorView(discord.ui.View):
    """Меню правки общего списка машин (файл `autopark_cars.json` по серверу)."""

    def __init__(self, message_id: int) -> None:
        super().__init__(timeout=900)
        self._message_id = message_id

    @discord.ui.button(
        label="Добавить авто",
        style=discord.ButtonStyle.success,
        row=0,
        emoji="➕",
    )
    async def btn_add(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_edit_autopark_inventory(interaction):
            await interaction.response.send_message("Нет прав.", ephemeral=True)
            return
        state = AUTOPARK_MESSAGES.get(self._message_id)
        if state is None:
            await interaction.response.send_message("Панель устарела.", ephemeral=True)
            return
        await interaction.response.send_modal(AutoparkAddCarModal(self._message_id))

    @discord.ui.button(
        label="Удалить из списка",
        style=discord.ButtonStyle.danger,
        row=0,
        emoji="➖",
    )
    async def btn_remove(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not user_can_edit_autopark_inventory(interaction):
            await interaction.response.send_message("Нет прав.", ephemeral=True)
            return
        state = AUTOPARK_MESSAGES.get(self._message_id)
        if state is None:
            await interaction.response.send_message("Панель устарела.", ephemeral=True)
            return
        cars = autopark_load_guild_cars(state.guild_id)
        if not cars:
            await interaction.response.send_message("Список машин пуст.", ephemeral=True)
            return
        view = AutoparkDeleteCarView(self._message_id, state.guild_id, cars)
        await interaction.response.send_message(
            "Выбери позицию для удаления:",
            view=view,
            ephemeral=True,
        )


class AutoparkView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Занять авто",
        style=discord.ButtonStyle.success,
        custom_id="autopark_take",
        row=0,
    )
    async def btn_take(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not await _autopark_ack_component(interaction):
            return
        if interaction.guild is None or interaction.message is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        mid = interaction.message.id
        state = AUTOPARK_MESSAGES.get(mid)
        if state is None:
            await interaction.followup.send(
                "Панель устарела — создай новую **/autopark**.", ephemeral=True
            )
            return
        autopark_prune_removed_cars(state, mid)
        await autopark_release_all_expired_for_message(mid, interaction.guild)
        free = autopark_free_cars(state)
        if not free:
            await interaction.followup.send(
                "Нет свободных машин в списке.", ephemeral=True
            )
            return
        if len(free) == 1:
            await autopark_take_car(interaction, mid, free[0].key)
            return
        view = AutoparkPickView(mid, free)
        await interaction.followup.send(
            "Выбери машину:", view=view, ephemeral=True
        )

    @discord.ui.button(
        label="Освободить авто",
        style=discord.ButtonStyle.danger,
        custom_id="autopark_release",
        row=0,
    )
    async def btn_release(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not await _autopark_ack_component(interaction):
            return
        if interaction.guild is None or interaction.message is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        mid = interaction.message.id
        state = AUTOPARK_MESSAGES.get(mid)
        if state is None:
            await interaction.followup.send("Панель устарела.", ephemeral=True)
            return
        autopark_prune_removed_cars(state, mid)
        await autopark_release_all_expired_for_message(mid, interaction.guild)
        uid = interaction.user.id
        now = _autopark_now_ts()
        mine = [
            ck
            for ck, (u, exp) in state.bookings.items()
            if u == uid and now < exp
        ]
        if not mine:
            await interaction.followup.send(
                "У тебя нет активной брони на этой панели.", ephemeral=True
            )
            return
        if len(mine) == 1:
            await autopark_release_car(mid, mine[0], uid, guild=interaction.guild, auto=False)
            await interaction.followup.send("Машина освобождена.", ephemeral=True)
            return
        view = AutoparkReleaseView(mid, interaction.guild.id, mine)
        await interaction.followup.send(
            "Выбери, какую бронь снять:", view=view, ephemeral=True
        )

    @discord.ui.button(
        label="Изменить список",
        style=discord.ButtonStyle.secondary,
        custom_id="autopark_edit",
        row=0,
        emoji="✏️",
    )
    async def btn_edit(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not await _autopark_ack_component(interaction):
            return
        if interaction.message is None:
            await interaction.followup.send("Ошибка.", ephemeral=True)
            return
        if not user_can_edit_autopark_inventory(interaction):
            await interaction.followup.send(
                embed=discord.Embed(
                    title="Нет доступа",
                    description=(
                        "Менять список могут:\n"
                        "• **user ID** в **AUTOPARK_EDIT_USER_IDS**, или\n"
                        "• роль из **MODERATION_ROLE_IDS** (если **AUTOPARK_EDIT_ROLE_IDS** пуст), "
                        "или узкий список **AUTOPARK_EDIT_ROLE_IDS**.\n\n"
                        "User ID: ПКМ по себе → «Копировать ID пользователя»."
                    ),
                    color=discord.Color.dark_red(),
                ),
                ephemeral=True,
            )
            return
        mid = interaction.message.id
        state = AUTOPARK_MESSAGES.get(mid)
        if state is None:
            await interaction.followup.send("Панель устарела.", ephemeral=True)
            return
        autopark_prune_removed_cars(state, mid)
        await autopark_release_all_expired_for_message(mid, interaction.guild)
        intro = discord.Embed(
            title="Редактирование списка (видно только тебе)",
            description=(
                "Список машин хранится **отдельно** от плашки (файл `autopark_cars.json`). "
                "Добавь авто или удали позицию — **все** панели автопарка на сервере обновятся."
            ),
            color=discord.Color.blurple(),
        )
        await interaction.followup.send(
            embed=intro,
            view=AutoparkListEditorView(mid),
            ephemeral=True,
        )


def kontrakt_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not KONTRAKT_CHANNEL_ID:
        return True
    return interaction.channel_id == KONTRAKT_CHANNEL_ID


def kontrakt_channel_restriction_message() -> str:
    if not KONTRAKT_CHANNEL_ID:
        return ""
    return f"**Контракт** можно отправить только в <#{KONTRAKT_CHANNEL_ID}>."


def user_can_post_kontrakt_panel(interaction: discord.Interaction) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    member = interaction.user
    if interaction.guild.owner_id == member.id:
        return True
    if member.guild_permissions.manage_guild:
        return True
    post = role_ids_or_moderation(KONTRAKT_POST_ROLE_IDS)
    if not post:
        return False
    return any(r.id in post for r in member.roles)


def user_can_manage_kontrakt_contract(
    interaction: discord.Interaction, state: ContractState
) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    member = interaction.user
    if interaction.guild.owner_id == member.id:
        return True
    if member.guild_permissions.manage_guild:
        return True
    mgr = role_ids_or_moderation(KONTRAKT_MANAGER_ROLE_IDS)
    if mgr and any(r.id in mgr for r in member.roles):
        return True
    return False


def _kontrakt_manage_forbidden_embed() -> discord.Embed:
    return discord.Embed(
        title="Нет доступа",
        description=(
            "**Пикнул** и **Отказ**: **MODERATION_ROLE_IDS** или **KONTRAKT_MANAGER_ROLE_IDS** (если задан)."
        ),
        color=discord.Color.dark_red(),
    )


_KONTRAKT_PEOPLE_RANGE = re.compile(
    r"(?i)(?:от\s*)?(\d+)\s*[-–]\s*(\d+)"
)


def parse_kontrakt_people_cap(raw: str) -> tuple[int, str]:
    """Верхняя граница набора из «От 2-6» и т.п.; подпись для футера."""
    s = raw.strip()
    note = (s[:100] + "…") if len(s) > 100 else (s or "—")
    if not s:
        return 6, "—"
    m = _KONTRAKT_PEOPLE_RANGE.search(s)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        cap = max(lo, hi)
        cap = min(max(cap, 1), 40)
        return cap, note
    m2 = re.search(r"\d+", s)
    if m2:
        cap = min(max(int(m2.group(0)), 1), 40)
        return cap, note
    return 6, note


def build_kontrakt_panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title="📋 Контракт",
        description=KONTRAKT_RULES_TEXT[:4000],
        color=discord.Color.dark_theme(),
    )
    return embed


def build_contract_listing_embed(
    state: ContractState, guild: discord.Guild
) -> discord.Embed:
    status = "Открыт" if state.status_open else "Закрыт"
    embed = discord.Embed(
        title="📋 Контракт",
        color=discord.Color.dark_theme(),
    )
    embed.add_field(
        name="**Название**",
        value=state.title[:1024] if state.title else "—",
        inline=False,
    )
    embed.add_field(
        name="**Вексели**",
        value=state.veksels[:1024] if state.veksels else "—",
        inline=False,
    )
    embed.add_field(
        name="**Время**",
        value=state.time_slot[:1024] if state.time_slot else "—",
        inline=False,
    )
    embed.add_field(
        name="**Раздел на 100%**",
        value=state.razdel_100[:256] if state.razdel_100 else "—",
        inline=False,
    )
    embed.add_field(
        name=f"**Участники ({len(state.participant_ids)}/{state.max_participants}):**",
        value=format_gather_participant_lines(state.participant_ids, guild),
        inline=False,
    )
    embed.add_field(name="**Статус**", value=status, inline=False)
    embed.set_footer(
        text=f"Автор: {state.creator_tag} · Набор: {state.people_note}"[:2048]
    )
    return embed


async def kontrakt_try_edit_panel(
    message: discord.Message, *, embed: discord.Embed, view: Optional[discord.ui.View]
) -> bool:
    try:
        await message.edit(embed=embed, view=view)
        return True
    except discord.NotFound:
        CONTRACT_MESSAGES.pop(message.id, None)
        persist_panel_extra_state()
        return False


@dataclass
class TempVoiceSession:
    guild_id: int
    voice_channel_id: int
    owner_id: int
    wait_room: bool = False


TEMP_VC_BY_VOICE: dict[int, TempVoiceSession] = {}


def user_can_manage_gather_panel(
    interaction: discord.Interaction, state: GatherState
) -> bool:
    """Создатель, модерация сервера или роли, которые могут постить этот тип /sbor."""
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    member = interaction.user
    if state.creator_id == member.id:
        return True
    if member.guild_permissions.manage_guild:
        return True
    if interaction.guild.owner_id == member.id:
        return True
    return user_can_post_gather(interaction, state.kind_key)


# Сдвиг по кнопкам «Атака» / «Деф»: отдельно или общий WAR_TIMER_BUMP_HOURS (часы, можно 1.5).
_war_atk_raw = os.getenv("WAR_TIMER_ATTACK_BUMP_HOURS", "").strip()
_war_def_raw = os.getenv("WAR_TIMER_DEFENSE_BUMP_HOURS", "").strip()
if _war_atk_raw or _war_def_raw:
    WAR_TIMER_ATTACK_BUMP_HOURS = get_env_float_hours(
        "WAR_TIMER_ATTACK_BUMP_HOURS", 3.0
    )
    WAR_TIMER_DEFENSE_BUMP_HOURS = get_env_float_hours(
        "WAR_TIMER_DEFENSE_BUMP_HOURS", 1.5
    )
else:
    _war_bump_h = get_env_int("WAR_TIMER_BUMP_HOURS", 4)
    _leg = float(max(1, min(168, _war_bump_h if _war_bump_h > 0 else 4)))
    WAR_TIMER_ATTACK_BUMP_HOURS = _leg
    WAR_TIMER_DEFENSE_BUMP_HOURS = _leg
try:
    MSK_TZ = ZoneInfo("Europe/Moscow")
except ZoneInfoNotFoundError:
    # Без пакета tzdata (часто Windows): МСК = UTC+3
    MSK_TZ = timezone(timedelta(hours=3))


@dataclass
class WarTimerState:
    channel_id: int
    attack_at: Optional[datetime] = None  # UTC
    defense_at: Optional[datetime] = None  # UTC


WAR_TIMER_MESSAGES: dict[int, WarTimerState] = {}

# Журнал записей статистики ВЗП (форма «+» с /stats_panel); обрезается при сохранении.
VZP_STATS_ENTRIES: list[dict] = []
_VZP_STATS_MAX_ENTRIES = 5000


def _dt_iso_opt(dt: Optional[datetime]) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _dt_parse_opt(raw: object) -> Optional[datetime]:
    if raw is None or raw == "":
        return None
    s = str(raw).strip()
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def war_timer_state_to_dict(st: WarTimerState) -> dict:
    return {
        "channel_id": st.channel_id,
        "attack_at": _dt_iso_opt(st.attack_at),
        "defense_at": _dt_iso_opt(st.defense_at),
    }


def war_timer_state_from_dict(d: dict) -> WarTimerState:
    return WarTimerState(
        channel_id=int(d["channel_id"]),
        attack_at=_dt_parse_opt(d.get("attack_at")),
        defense_at=_dt_parse_opt(d.get("defense_at")),
    )


def gather_state_to_dict(st: GatherState) -> dict:
    return {
        "kind_key": st.kind_key,
        "title": st.title,
        "max_main": st.max_main,
        "max_extra": st.max_extra,
        "main_ids": list(st.main_ids),
        "extra_ids": list(st.extra_ids),
        "status_open": st.status_open,
        "creator_id": st.creator_id,
        "creator_tag": st.creator_tag,
        "closes_at": _dt_iso_opt(st.closes_at),
        "channel_id": st.channel_id,
        "scheduled_at": _dt_iso_opt(st.scheduled_at),
    }


def gather_state_from_dict(d: dict) -> GatherState:
    return GatherState(
        kind_key=str(d.get("kind_key", "vzp"))[:32],
        title=str(d.get("title", ""))[:256],
        max_main=int(d["max_main"]),
        max_extra=int(d["max_extra"]),
        main_ids=[int(x) for x in d.get("main_ids", [])],
        extra_ids=[int(x) for x in d.get("extra_ids", [])],
        status_open=bool(d.get("status_open", True)),
        creator_id=int(d.get("creator_id", 0)),
        creator_tag=str(d.get("creator_tag", ""))[:80],
        closes_at=_dt_parse_opt(d.get("closes_at")),
        channel_id=int(d["channel_id"]),
        scheduled_at=_dt_parse_opt(d.get("scheduled_at")),
    )


def contract_state_to_dict(st: ContractState) -> dict:
    return {
        "channel_id": st.channel_id,
        "creator_id": st.creator_id,
        "creator_tag": st.creator_tag,
        "title": st.title,
        "veksels": st.veksels,
        "time_slot": st.time_slot,
        "razdel_100": st.razdel_100,
        "people_note": st.people_note,
        "max_participants": st.max_participants,
        "participant_ids": list(st.participant_ids),
        "status_open": st.status_open,
    }


def contract_state_from_dict(d: dict) -> ContractState:
    """Восстановление из JSON; слабые/старые поля не должны приводить к молчаливому пропуску контракта."""
    if not isinstance(d, dict):
        raise TypeError("contract state must be a dict")
    raw_pids = d.get("participant_ids", [])
    pids: list[int] = []
    if isinstance(raw_pids, list):
        for x in raw_pids:
            try:
                pids.append(int(x))
            except (TypeError, ValueError):
                pass
    try:
        max_p = int(d.get("max_participants", 40))
    except (TypeError, ValueError):
        max_p = 40
    max_p = max(1, min(400, max_p))
    so = d.get("status_open", True)
    if isinstance(so, str):
        status_open = so.strip().lower() in ("1", "true", "yes", "on")
    else:
        status_open = bool(so)
    try:
        channel_id = int(d.get("channel_id", 0))
        creator_id = int(d.get("creator_id", 0))
    except (TypeError, ValueError):
        channel_id = 0
        creator_id = 0
    return ContractState(
        channel_id=channel_id,
        creator_id=creator_id,
        creator_tag=str(d.get("creator_tag", ""))[:80],
        title=str(d.get("title", ""))[:200],
        veksels=str(d.get("veksels", ""))[:500],
        time_slot=str(d.get("time_slot", ""))[:80],
        razdel_100=str(d.get("razdel_100", ""))[:32],
        people_note=str(d.get("people_note", ""))[:500],
        max_participants=max_p,
        participant_ids=pids,
        status_open=status_open,
    )


def _kontrakt_reload_state_for_message(message_id: int) -> ContractState | None:
    """Если контракт есть в panel_extra_state.json, но не в RAM (рестарт, редкий сбой)."""
    if message_id in CONTRACT_MESSAGES:
        return CONTRACT_MESSAGES[message_id]
    path = PANEL_EXTRA_STATE_FILE
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    ko = data.get("kontrakt")
    if not isinstance(ko, dict):
        return None
    raw = ko.get(str(message_id))
    if not isinstance(raw, dict):
        return None
    try:
        st = contract_state_from_dict(raw)
    except (TypeError, ValueError):
        return None
    CONTRACT_MESSAGES[message_id] = st
    return st


def persist_panel_extra_state() -> None:
    """Сохранить таймеры, журнал ВЗП, сборы и контракты."""
    path = PANEL_EXTRA_STATE_FILE
    try:
        entries = VZP_STATS_ENTRIES[-_VZP_STATS_MAX_ENTRIES:]
        payload = {
            "v": 1,
            "applications": {
                APPLICATION_RP: APPLICATIONS_STATE.get(APPLICATION_RP, True),
                APPLICATION_VZP: APPLICATIONS_STATE.get(APPLICATION_VZP, True),
            },
            "war_timer": {
                str(mid): war_timer_state_to_dict(st)
                for mid, st in WAR_TIMER_MESSAGES.items()
            },
            "vzp_stats": entries,
            "gather": {
                str(mid): gather_state_to_dict(st)
                for mid, st in GATHER_MESSAGES.items()
            },
            "kontrakt": {
                str(mid): contract_state_to_dict(st)
                for mid, st in CONTRACT_MESSAGES.items()
            },
        }
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except OSError:
        pass


def load_panel_extra_state() -> None:
    """Загрузить таймеры, журнал ВЗП, сборы и контракты при старте."""
    path = PANEL_EXTRA_STATE_FILE
    if not os.path.isfile(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(data, dict):
        return
    app = data.get("applications")
    if isinstance(app, dict):
        if APPLICATION_RP in app:
            APPLICATIONS_STATE[APPLICATION_RP] = bool(app[APPLICATION_RP])
        if APPLICATION_VZP in app:
            APPLICATIONS_STATE[APPLICATION_VZP] = bool(app[APPLICATION_VZP])
    wt = data.get("war_timer")
    WAR_TIMER_MESSAGES.clear()
    if isinstance(wt, dict):
        for k, v in wt.items():
            try:
                mid = int(k)
                if isinstance(v, dict):
                    WAR_TIMER_MESSAGES[mid] = war_timer_state_from_dict(v)
            except (KeyError, TypeError, ValueError):
                continue
    vz = data.get("vzp_stats")
    VZP_STATS_ENTRIES.clear()
    if isinstance(vz, list):
        for item in vz[-_VZP_STATS_MAX_ENTRIES:]:
            if isinstance(item, dict):
                VZP_STATS_ENTRIES.append(item)
    ga = data.get("gather")
    GATHER_MESSAGES.clear()
    if isinstance(ga, dict):
        for k, v in ga.items():
            try:
                mid = int(k)
                if isinstance(v, dict):
                    GATHER_MESSAGES[mid] = gather_state_from_dict(v)
            except (KeyError, TypeError, ValueError):
                continue
    ko = data.get("kontrakt")
    CONTRACT_MESSAGES.clear()
    if isinstance(ko, dict):
        for k, v in ko.items():
            try:
                mid = int(k)
                if isinstance(v, dict):
                    CONTRACT_MESSAGES[mid] = contract_state_from_dict(v)
            except (KeyError, TypeError, ValueError):
                continue


def gather_apply_auto_close_if_due(state: GatherState) -> bool:
    if state.closes_at is None or not state.status_open:
        return False
    if state.closes_at <= datetime.now(timezone.utc):
        state.status_open = False
        return True
    return False


async def refresh_gather_message(bot: commands.Bot, message_id: int, state: GatherState) -> None:
    ch = bot.get_channel(state.channel_id)
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return
    guild = ch.guild
    if guild is None:
        return
    try:
        msg = await ch.fetch_message(message_id)
    except discord.HTTPException:
        return
    embed = build_gather_embed(state, guild)
    await msg.edit(embed=embed, view=GatherSignView())


async def gather_try_edit_panel(
    message: discord.Message, *, embed: discord.Embed, view: discord.ui.View
) -> bool:
    """Обновить плашку сбора. False — сообщение удалено (запись из GATHER_MESSAGES снята)."""
    try:
        await message.edit(embed=embed, view=view)
        return True
    except discord.NotFound:
        GATHER_MESSAGES.pop(message.id, None)
        persist_panel_extra_state()
        return False


async def gather_auto_close_loop() -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = datetime.now(timezone.utc)
            for mid, state in list(GATHER_MESSAGES.items()):
                if state.closes_at is None or not state.status_open:
                    continue
                if state.closes_at <= now:
                    state.status_open = False
                    await refresh_gather_message(bot, mid, state)
                    persist_panel_extra_state()
        except Exception:
            pass
        await asyncio.sleep(30)


async def daily_role_ping_loop() -> None:
    await bot.wait_until_ready()
    if not DAILY_ROLE_PING_CHANNEL_ID or not DAILY_ROLE_PING_ROLE_ID:
        return
    ch_id = DAILY_ROLE_PING_CHANNEL_ID
    rid = DAILY_ROLE_PING_ROLE_ID
    interval_sec = DAILY_ROLE_PING_INTERVAL_HOURS * 3600
    tz = DAILY_ROLE_PING_TZ
    schedule = DAILY_ROLE_PING_SCHEDULE

    async def send_role_ping() -> Optional[discord.Message]:
        ch = bot.get_channel(ch_id)
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return None
        body = DAILY_ROLE_PING_MESSAGE or "Напоминание."
        content = f"<@&{rid}>\n{body}"
        return await ch.send(
            content=content,
            allowed_mentions=discord.AllowedMentions(
                roles=[discord.Object(id=rid)]
            ),
        )

    async def delete_bot_message(mid: int) -> None:
        if bot.user is None:
            return
        ch_del = bot.get_channel(ch_id)
        if not isinstance(ch_del, (discord.TextChannel, discord.Thread)):
            return
        try:
            old = await ch_del.fetch_message(mid)
            if old.author.id == bot.user.id:
                await old.delete()
        except discord.HTTPException:
            pass

    if tz is not None and schedule:
        last_mid: Optional[int] = None
        while not bot.is_closed():
            try:
                nxt = next_daily_role_ping_fire(tz, schedule)
                delay = max(1.0, (nxt - datetime.now(tz)).total_seconds())
                await asyncio.sleep(delay)
                if last_mid is not None:
                    await delete_bot_message(last_mid)
                msg = await send_role_ping()
                last_mid = msg.id if msg is not None else None
            except Exception:
                await asyncio.sleep(60)
        return

    while not bot.is_closed():
        msg: Optional[discord.Message] = None
        try:
            msg = await send_role_ping()
            if msg is None:
                await asyncio.sleep(60)
                continue
            await asyncio.sleep(interval_sec)
            if msg is not None:
                await delete_bot_message(msg.id)
        except Exception:
            await asyncio.sleep(60)


async def dm_gather_ping_role_notify(
    guild: discord.Guild,
    gather_message: discord.Message,
    kind_key: str,
) -> None:
    """ЛС всем с ролью пинга при создании сбора /sbor: «Поставь реаку на …» + ссылка на пост."""
    rid = gather_ping_role_id(kind_key)
    if not rid:
        return
    role = guild.get_role(rid)
    if role is None:
        return
    if not guild.chunked:
        try:
            await guild.chunk(cache=True)
        except (discord.HTTPException, asyncio.TimeoutError):
            pass
    tag = GATHER_KIND_DM_REACTION_TAG.get(
        kind_key, GATHER_KIND_LABELS.get(kind_key, kind_key)
    )
    dm_content = f"> ## Поставь реаку на {tag}\n\n{gather_message.jump_url}"
    if len(dm_content) > 2000:
        dm_content = dm_content[:1999] + "…"
    for member in role.members:
        if member.bot:
            continue
        try:
            await member.send(dm_content)
        except discord.HTTPException:
            pass
        await asyncio.sleep(0.35)


async def dm_podarok_ping_role_notify(
    guild: discord.Guild,
    podarok_message: discord.Message,
    prize: str,
) -> None:
    """ЛС о новом розыгрыше: если задан **PODAROK_CHANNEL_ACCESS_ROLE_IDS** — всем с этими ролями;
    иначе всем с **PODAROK_PING_ROLE_ID** (одна роль). Без обоих — не шлём."""
    access_ids = PODAROK_CHANNEL_ACCESS_ROLE_IDS
    ping_rid = PODAROK_PING_ROLE_ID
    if not access_ids and not ping_rid:
        return
    if not guild.chunked:
        try:
            await guild.chunk(cache=True)
        except (discord.HTTPException, asyncio.TimeoutError):
            pass
    prize_line = prize.strip()[:200] or "—"
    dm_content = f"> ## Новый розыгрыш\n> **{prize_line}**\n\n{podarok_message.jump_url}"
    if len(dm_content) > 2000:
        dm_content = dm_content[:1999] + "…"

    if access_ids:
        seen: set[int] = set()
        for rid in access_ids:
            role = guild.get_role(rid)
            if role is None:
                continue
            for member in role.members:
                if member.bot or member.id in seen:
                    continue
                seen.add(member.id)
                try:
                    await member.send(dm_content)
                except discord.HTTPException:
                    pass
                await asyncio.sleep(0.35)
        return

    role = guild.get_role(ping_rid)
    if role is None:
        return
    for member in role.members:
        if member.bot:
            continue
        try:
            await member.send(dm_content)
        except discord.HTTPException:
            pass
        await asyncio.sleep(0.35)


def user_can_trigger_role_mention_dm(member: discord.Member) -> bool:
    if member.guild.owner_id == member.id:
        return True
    if member.guild_permissions.manage_guild:
        return True
    if not ROLE_MENTION_DM_ALLOWED_ROLE_IDS:
        return True
    return any(r.id in ROLE_MENTION_DM_ALLOWED_ROLE_IDS for r in member.roles)


def _role_mention_dm_channel_category_id(
    channel: discord.TextChannel
    | discord.VoiceChannel
    | discord.StageChannel
    | discord.Thread,
) -> Optional[int]:
    if isinstance(channel, discord.Thread):
        parent = channel.parent
        if parent is None:
            return None
        return getattr(parent, "category_id", None)
    return channel.category_id


def role_mention_dm_watchlist_matches_channel(
    channel: discord.TextChannel
    | discord.VoiceChannel
    | discord.StageChannel
    | discord.Thread,
) -> bool:
    """Совпадение по категории и/или по списку каналов (ветка — категория родителя)."""
    by_category = False
    if ROLE_MENTION_DM_CATEGORY_IDS:
        cat_id = _role_mention_dm_channel_category_id(channel)
        if cat_id is not None and cat_id in ROLE_MENTION_DM_CATEGORY_IDS:
            by_category = True
    by_channel = False
    if ROLE_MENTION_DM_CHANNEL_IDS:
        if channel.id in ROLE_MENTION_DM_CHANNEL_IDS:
            by_channel = True
        elif isinstance(channel, discord.Thread):
            pid = channel.parent_id
            if pid is not None and pid in ROLE_MENTION_DM_CHANNEL_IDS:
                by_channel = True
    return by_category or by_channel


async def dm_role_mention_channel_broadcast(
    message: discord.Message,
    target_roles: list[discord.Role],
) -> None:
    """ЛС участникам упомянутых ролей: текст сообщения, сверху ъ автора в канале."""
    guild = message.guild
    if guild is None:
        return
    if not guild.chunked:
        try:
            await guild.chunk(cache=True)
        except (discord.HTTPException, asyncio.TimeoutError):
            pass
    author = message.author
    author_label = (
        author.display_name if isinstance(author, discord.Member) else str(author)
    )
    ch = message.channel
    body = (message.clean_content or "").strip()
    if message.attachments and not body:
        body = "_(в сообщении только вложения — открой по ссылке ниже)_"
    elif message.attachments:
        body += "\n\n*(есть вложения — смотри в канале по ссылке)*"
    if not body:
        body = "_(упоминание роли в канале — открой сообщение по ссылке)_"
    header = f"**{author_label}** · сообщение в <#{ch.id}>:\n\n"
    footer = f"\n\n{message.jump_url}"
    dm_content = header + body + footer
    if len(dm_content) > 2000:
        over = len(dm_content) - 2000
        keep = max(0, len(body) - over - 1)
        body = body[:keep] + "…"
        dm_content = header + body + footer
        if len(dm_content) > 2000:
            dm_content = dm_content[:1999] + "…"

    recipients: dict[int, discord.Member] = {}
    for role in target_roles:
        for m in role.members:
            if m.bot or m.id == message.author.id:
                continue
            recipients[m.id] = m
    for member in recipients.values():
        try:
            await member.send(dm_content)
        except discord.HTTPException:
            pass
        await asyncio.sleep(0.35)


def format_gather_participant_lines(user_ids: list[int], guild: discord.Guild) -> str:
    lines: list[str] = []
    for i, uid in enumerate(user_ids, start=1):
        lines.append(f"{i}. <@{uid}>")
    body = "\n".join(lines) if lines else "—"
    if len(body) > 1000:
        body = body[:997] + "…"
    return body


def gather_moderate_participant_ids_ordered(state: GatherState) -> list[int]:
    """Порядок: основа, затем доп. Без дублей (списки не пересекаются)."""
    return list(state.main_ids) + list(state.extra_ids)


# Лимит Discord: до 25 пунктов в одном Select; до 4 селектов в рядах 0–3 + кнопка «Создать список» в ряду 4.
_GATHER_MODERATE_MAX_CHUNKS = 4
GATHER_MODERATE_MAX_UI_PARTICIPANTS = 25 * _GATHER_MODERATE_MAX_CHUNKS


def gather_moderate_build_option_chunk(
    state: GatherState, uids: list[int], guild: discord.Guild
) -> list[discord.SelectOption]:
    """Галочки (default=True) — кто остаётся в итоговом списке."""
    out: list[discord.SelectOption] = []
    for uid in uids:
        m = guild.get_member(uid)
        label = (m.display_name if m else str(uid))[:72]
        if uid in state.main_ids:
            prefix = "Основа"
            desc = "В основе"
        else:
            prefix = "Доп"
            desc = "В допе"
        out.append(
            discord.SelectOption(
                label=f"{prefix} · {label}"[:100],
                value=str(uid),
                description=desc,
                default=True,
            )
        )
    return out


def gather_moderate_total_participants(state: GatherState) -> int:
    return len(state.main_ids) + len(state.extra_ids)


_GATHER_MINUTES_ONLY = re.compile(r"^[0-9]+$")
# Не больше недели — иначе считаем, что это не «минуты», а просто текст из цифр.
_GATHER_MAX_MINUTES_FROM_NOW = 7 * 24 * 60


def parse_gather_vremya(vremya_raw: str) -> tuple[str, Optional[datetime]]:
    """Текст для поля «Время» и опционально момент сбора (UTC) для Discord <t:…>."""
    s = vremya_raw.strip()
    if not s:
        return "", None
    if not _GATHER_MINUTES_ONLY.fullmatch(s):
        return s[:256], None
    minutes = int(s)
    if minutes > _GATHER_MAX_MINUTES_FROM_NOW:
        return s[:256], None
    at_utc = (datetime.now(timezone.utc) + timedelta(minutes=minutes)).replace(
        microsecond=0
    )
    return "", at_utc


def gather_time_field_value(state: GatherState) -> str:
    if state.scheduled_at is not None:
        ts = int(state.scheduled_at.timestamp())
        return f"<t:{ts}:F>\n<t:{ts}:R>"
    return state.title or "—"


def build_gather_embed(state: GatherState, guild: discord.Guild) -> discord.Embed:
    label = GATHER_KIND_LABELS.get(state.kind_key, state.kind_key)
    status_text = "Открыт" if state.status_open else "Закрыт"
    footer_time = datetime.now().strftime("%d.%m.%Y")
    embed = discord.Embed(
        title=f"📋 Сбор · **{label}**",
        color=discord.Color.dark_theme(),
    )
    embed.add_field(name="**Время:**", value=gather_time_field_value(state), inline=False)
    embed.add_field(
        name=f"**Участники ({len(state.main_ids)}/{state.max_main}):**",
        value=format_gather_participant_lines(state.main_ids, guild),
        inline=False,
    )
    embed.add_field(
        name=f"**Доп. слоты ({len(state.extra_ids)}/{state.max_extra}):**",
        value=format_gather_participant_lines(state.extra_ids, guild),
        inline=False,
    )
    embed.add_field(name="**Статус**", value=status_text, inline=False)
    embed.set_footer(text=f"Создатель: {state.creator_tag} - {footer_time}")
    return embed


def format_war_countdown(target: Optional[datetime]) -> str:
    if target is None:
        return "Нету кд"
    now = datetime.now(timezone.utc)
    if target <= now:
        return "уже прошло"
    total_sec = int((target - now).total_seconds())
    days, rem = divmod(total_sec, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days} д.")
    if hours:
        parts.append(f"{hours} ч.")
    if minutes or not parts:
        parts.append(f"{minutes} мин.")
    return "через " + " ".join(parts)


def parse_war_timer_datetime_msk(raw: str) -> datetime:
    s = raw.strip()
    if not s:
        raise ValueError("empty")
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S"):
        try:
            naive = datetime.strptime(s, fmt)
            break
        except ValueError:
            naive = None
    else:
        raise ValueError("bad format")
    aware = naive.replace(tzinfo=MSK_TZ)
    return aware.astimezone(timezone.utc)


def build_war_timer_embed(state: WarTimerState) -> discord.Embed:
    embed = discord.Embed(
        description=(
            "# КД att/deff\n\n"
            f"## att\n- {format_war_countdown(state.attack_at)}\n\n"
            f"## deff\n- {format_war_countdown(state.defense_at)}"
        ),
        color=discord.Color.dark_theme(),
    )
    return embed


async def refresh_war_timer_message(
    message_id: int, state: WarTimerState, *, persist: bool = True
) -> None:
    ch = bot.get_channel(state.channel_id)
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        WAR_TIMER_MESSAGES.pop(message_id, None)
        persist_panel_extra_state()
        return
    try:
        msg = await ch.fetch_message(message_id)
    except discord.HTTPException:
        WAR_TIMER_MESSAGES.pop(message_id, None)
        persist_panel_extra_state()
        return
    try:
        await msg.edit(embed=build_war_timer_embed(state), view=WarTimerView())
        if persist:
            persist_panel_extra_state()
    except discord.HTTPException:
        pass


class WarTimerEditModal(discord.ui.Modal, title="Время атаки и дефа"):
    attack_input = discord.ui.TextInput(
        label="Атака (МСК)",
        required=False,
        placeholder="05.04.2026 21:30",
        max_length=32,
    )
    defense_input = discord.ui.TextInput(
        label="Деф (МСК)",
        required=False,
        placeholder="06.04.2026 03:00",
        max_length=32,
    )

    def __init__(self, message_id: int) -> None:
        super().__init__()
        self._message_id = message_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not user_can_war_timer(interaction):
            await interaction.response.send_message(
                "Недостаточно прав на панель таймера.", ephemeral=True
            )
            return
        state = WAR_TIMER_MESSAGES.get(self._message_id)
        if state is None:
            await interaction.response.send_message(
                "Панель устарела — создай новую командой **/timer_ata_def**.",
                ephemeral=True,
            )
            return
        a_raw = str(self.attack_input.value).strip()
        d_raw = str(self.defense_input.value).strip()
        if not a_raw and not d_raw:
            await interaction.response.send_message(
                "Заполни хотя бы одно поле или отмени.", ephemeral=True
            )
            return
        errors: list[str] = []
        new_a: Optional[datetime] = None
        new_d: Optional[datetime] = None
        if a_raw:
            try:
                new_a = parse_war_timer_datetime_msk(a_raw)
            except ValueError:
                errors.append("атака")
        if d_raw:
            try:
                new_d = parse_war_timer_datetime_msk(d_raw)
            except ValueError:
                errors.append("деф")
        if errors:
            await interaction.response.send_message(
                f"Неверный формат: {', '.join(errors)}. Пример: `05.04.2026 21:30`",
                ephemeral=True,
            )
            return
        if new_a is not None:
            state.attack_at = new_a
        if new_d is not None:
            state.defense_at = new_d
        await interaction.response.defer(ephemeral=True)
        await refresh_war_timer_message(self._message_id, state, persist=True)
        await interaction.followup.send("Время обновлено.", ephemeral=True)


class WarTimerView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    async def _require_state(
        self, interaction: discord.Interaction
    ) -> tuple[int, WarTimerState] | None:
        if interaction.message is None:
            await interaction.response.send_message("Ошибка.", ephemeral=True)
            return None
        mid = interaction.message.id
        state = WAR_TIMER_MESSAGES.get(mid)
        if state is None:
            await interaction.response.send_message(
                "Панель устарела — создай новую **/timer_ata_def**.", ephemeral=True
            )
            return None
        if not user_can_war_timer(interaction):
            await interaction.response.send_message(
                "Недостаточно прав на панель таймера.", ephemeral=True
            )
            return None
        return mid, state

    @discord.ui.button(
        label="Атака",
        style=discord.ButtonStyle.danger,
        custom_id="war_timer_attack",
        row=0,
    )
    async def btn_attack(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        got = await self._require_state(interaction)
        if got is None:
            return
        mid, state = got
        state.attack_at = datetime.now(timezone.utc) + timedelta(
            hours=WAR_TIMER_ATTACK_BUMP_HOURS
        )
        await interaction.response.defer(ephemeral=True)
        await refresh_war_timer_message(mid, state, persist=True)
        await interaction.followup.send(
            f"Атака: **+{WAR_TIMER_ATTACK_BUMP_HOURS:g} ч.** от сейчас.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="Деф",
        style=discord.ButtonStyle.primary,
        custom_id="war_timer_defense",
        row=0,
    )
    async def btn_defense(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        got = await self._require_state(interaction)
        if got is None:
            return
        mid, state = got
        state.defense_at = datetime.now(timezone.utc) + timedelta(
            hours=WAR_TIMER_DEFENSE_BUMP_HOURS
        )
        await interaction.response.defer(ephemeral=True)
        await refresh_war_timer_message(mid, state, persist=True)
        await interaction.followup.send(
            f"Деф: **+{WAR_TIMER_DEFENSE_BUMP_HOURS:g} ч.** от сейчас.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="Модерация",
        style=discord.ButtonStyle.secondary,
        custom_id="war_timer_moderation",
        row=0,
    )
    async def btn_moderation(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.message is None:
            await interaction.response.send_message("Ошибка.", ephemeral=True)
            return
        if not user_can_war_timer(interaction):
            await interaction.response.send_message(
                "Недостаточно прав на панель таймера.", ephemeral=True
            )
            return
        if WAR_TIMER_MESSAGES.get(interaction.message.id) is None:
            await interaction.response.send_message(
                "Панель устарела — создай новую **/timer_ata_def**.", ephemeral=True
            )
            return
        await interaction.response.send_modal(
            WarTimerEditModal(interaction.message.id)
        )


async def war_timer_refresh_loop() -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            for mid, state in list(WAR_TIMER_MESSAGES.items()):
                await refresh_war_timer_message(mid, state, persist=False)
        except Exception:
            pass
        await asyncio.sleep(45)


def build_kaptik_prompt_embed() -> discord.Embed:
    return discord.Embed(
        title="Добавить статистику",
        color=discord.Color.dark_theme(),
    )


def _parse_att_deff_and_points(raw: str) -> tuple[str, str]:
    """Att//deff и число точек: «att win · 3», «lose | 0» или «att win 3» (последнее — число)."""
    s = " ".join(raw.split())
    if not s:
        return "—", "—"
    for sep in ("·", "|", "—", "–"):
        if sep in s:
            left, right = s.rsplit(sep, 1)
            left, right = left.strip(), right.strip()
            if right.isdigit():
                return (left or "—"), right
    parts = s.split()
    if len(parts) >= 2 and parts[-1].lstrip("-").isdigit():
        return (" ".join(parts[:-1]).strip() or "—"), parts[-1]
    return s, "—"


def _format_kaptik_winrate(raw: str) -> str:
    """Два числа через пробел → «N win M lose», иначе текст как ввели."""
    s = raw.strip()
    if not s:
        return "—"
    parts = s.split()
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return f"{parts[0]} win {parts[1]} lose"
    return s


def _format_kaptik_sailor_vs(raw: str) -> str:
    """Только противник или полная строка — в посте всегда «Sailor vs …»."""
    s = raw.strip()
    if not s:
        return "—"
    m = re.match(r"(?i)sailor\s+vs\s*(.*)", s)
    if m:
        rest = (m.group(1) or "").strip()
        return f"Sailor vs {rest}" if rest else "Sailor vs —"
    return f"Sailor vs {s}"


def _format_kaptik_quantity(raw: str) -> str:
    """Одно целое число → NxN; уже «5x7» и т.п. — как ввели (нижний регистр x)."""
    s = raw.strip()
    if not s:
        return "—"
    if re.fullmatch(r"\d+", s):
        return f"{s}x{s}"
    if re.fullmatch(r"\d+[xX]\d+", s):
        a, _, b = s.lower().partition("x")
        return f"{a}x{b}"
    return s


def _kaptik_blockquote_bold(value: str) -> str:
    """Значение как **текст** без blockquote — меньше горизонтальный отступ в эмбеде."""
    t = value.strip()
    if not t:
        return "**—**"

    def safe(s: str) -> str:
        return s.replace("**", "·")

    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    if not lines:
        return "**—**"
    if len(lines) == 1:
        return f"**{safe(lines[0])}**"
    return "\n".join(f"**{safe(line)}**" for line in lines)


def build_kaptik_result_embed(
    *,
    att_deff: str,
    points: str,
    vs_who: str,
    size_xy: str,
    territory: str,
    winrate: str,
    author: discord.abc.User,
) -> discord.Embed:
    """Заголовки ## и жирные значения; между секциями без пустых строк."""
    body = (
        f"# 📋 Каптик\n"
        f"## att/def\n{_kaptik_blockquote_bold(att_deff)}\n"
        f"## Точки\n{_kaptik_blockquote_bold(points)}\n"
        f"## Sailor vs\n{_kaptik_blockquote_bold(vs_who)}\n"
        f"## Количество\n{_kaptik_blockquote_bold(size_xy)}\n"
        f"## Территория\n{_kaptik_blockquote_bold(territory)}\n"
        f"## winrate\n{_kaptik_blockquote_bold(winrate)}"
    )
    embed = discord.Embed(description=body, color=discord.Color.dark_theme())
    embed.set_footer(text=f"От {author}")
    return embed


class KaptikAddModal(discord.ui.Modal, title="Добавить статистику"):
    att_deff = discord.ui.TextInput(
        label="1. att/def, сколько точек",
        required=True,
        placeholder="att win  3",
        max_length=120,
    )
    vs_who = discord.ui.TextInput(
        label="2. Против кого",
        required=True,
        placeholder="школьники",
        max_length=200,
    )
    size_xy = discord.ui.TextInput(
        label="3. Количество",
        required=True,
        placeholder="7",
        max_length=40,
    )
    territory = discord.ui.TextInput(
        label="4. Какая территория",
        required=True,
        placeholder="Ветряки",
        max_length=120,
    )
    score = discord.ui.TextInput(
        label="5. winrate",
        required=True,
        style=discord.TextStyle.paragraph,
        placeholder="15 17",
        max_length=300,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not stats_allowed_in_channel(interaction):
            await interaction.response.send_message(
                stats_channel_restriction_message(), ephemeral=True
            )
            return
        if not user_can_stats_panel(interaction):
            await interaction.response.send_message(
                "Недостаточно прав на статистику.", ephemeral=True
            )
            return

        att_txt, pts_txt = _parse_att_deff_and_points(str(self.att_deff))
        vs_line = _format_kaptik_sailor_vs(str(self.vs_who))
        qty_line = _format_kaptik_quantity(str(self.size_xy))
        winrate_line = _format_kaptik_winrate(str(self.score))
        terr = str(self.territory).strip() or "—"
        embed = build_kaptik_result_embed(
            att_deff=att_txt,
            points=pts_txt,
            vs_who=vs_line,
            size_xy=qty_line,
            territory=terr,
            winrate=winrate_line,
            author=interaction.user,
        )

        VZP_STATS_ENTRIES.append(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "guild_id": interaction.guild.id,
                "channel_id": interaction.channel.id,
                "author_id": interaction.user.id,
                "att_deff": att_txt,
                "points": pts_txt,
                "vs_who": vs_line,
                "size_xy": qty_line,
                "territory": terr,
                "winrate": winrate_line,
            }
        )
        if len(VZP_STATS_ENTRIES) > _VZP_STATS_MAX_ENTRIES:
            del VZP_STATS_ENTRIES[: -_VZP_STATS_MAX_ENTRIES]
        persist_panel_extra_state()

        await interaction.response.defer(ephemeral=True)
        await interaction.channel.send(embed=embed)
        await interaction.followup.send(
            "Статистика добавлена в канал.", ephemeral=True
        )


class KaptikAddView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="+",
        style=discord.ButtonStyle.success,
        custom_id="stats_kaptik_add",
        row=0,
    )
    async def btn_add_kaptik(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not stats_allowed_in_channel(interaction):
            await interaction.response.send_message(
                stats_channel_restriction_message(), ephemeral=True
            )
            return
        if not user_can_stats_panel(interaction):
            await interaction.response.send_message(
                "Недостаточно прав на статистику.", ephemeral=True
            )
            return
        await interaction.response.send_modal(KaptikAddModal())


def build_material_report_prompt_embed() -> discord.Embed:
    return discord.Embed(
        title="Отчёт: ВЗХ",
        color=discord.Color.dark_theme(),
    )


def build_material_report_result_embed(
    *,
    report_date: str,
    faction: str,
    materials: str,
    author: discord.abc.User,
) -> discord.Embed:
    body = (
        f"# 📦 Отчёт · ВЗХ\n"
        f"## Дата\n{_kaptik_blockquote_bold(report_date)}\n"
        f"## Фракция\n{_kaptik_blockquote_bold(faction)}\n"
        f"## ВЗХ\n{_kaptik_blockquote_bold(materials)}"
    )
    embed = discord.Embed(description=body, color=discord.Color.dark_theme())
    embed.set_footer(text=f"От {author}")
    return embed


def build_activity_report_prompt_embed() -> discord.Embed:
    return discord.Embed(
        title="Отчёт: МП",
        color=discord.Color.dark_theme(),
    )


def build_activity_report_result_embed(
    *,
    report_date: str,
    activity: str,
    faction: str,
    outcome: str,
    author: discord.abc.User,
) -> discord.Embed:
    body = (
        f"# 📋 Отчёт · МП\n"
        f"## Дата\n{_kaptik_blockquote_bold(report_date)}\n"
        f"## МП\n{_kaptik_blockquote_bold(activity)}\n"
        f"## Фракция\n{_kaptik_blockquote_bold(faction)}\n"
        f"## Итог\n{_kaptik_blockquote_bold(outcome)}"
    )
    embed = discord.Embed(description=body, color=discord.Color.dark_theme())
    embed.set_footer(text=f"От {author}")
    return embed


class MaterialReportModal(discord.ui.Modal, title="Отчёт: ВЗХ"):
    report_date = discord.ui.TextInput(
        label="1. За какое число",
        required=True,
        placeholder="05.04.2026",
        max_length=40,
    )
    faction = discord.ui.TextInput(
        label="2. За какую фракцию",
        required=True,
        placeholder="ММ",
        max_length=80,
    )
    materials = discord.ui.TextInput(
        label="3. ВЗХ (количество / детали)",
        required=True,
        placeholder="5к",
        max_length=120,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not material_report_allowed_in_channel(interaction):
            await interaction.response.send_message(
                material_report_channel_restriction_message(), ephemeral=True
            )
            return
        if not user_can_material_report_panel(interaction):
            await interaction.response.send_message(
                "Нет прав на этот отчёт.", ephemeral=True
            )
            return

        d = str(self.report_date).strip() or "—"
        f = str(self.faction).strip() or "—"
        m = str(self.materials).strip() or "—"
        embed = build_material_report_result_embed(
            report_date=d, faction=f, materials=m, author=interaction.user
        )
        await interaction.response.defer(ephemeral=True)
        await interaction.channel.send(embed=embed)
        await interaction.followup.send("Отчёт отправлен в канал.", ephemeral=True)


class MaterialReportAddView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="+",
        style=discord.ButtonStyle.success,
        custom_id="material_report_add",
        row=0,
    )
    async def btn_add(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not material_report_allowed_in_channel(interaction):
            await interaction.response.send_message(
                material_report_channel_restriction_message(), ephemeral=True
            )
            return
        if not user_can_material_report_panel(interaction):
            await interaction.response.send_message(
                "Нет прав на этот отчёт.", ephemeral=True
            )
            return
        await interaction.response.send_modal(MaterialReportModal())


class ActivityReportModal(discord.ui.Modal, title="Отчёт: МП"):
    report_date = discord.ui.TextInput(
        label="1. За какое число",
        required=True,
        placeholder="05.04.2026",
        max_length=40,
    )
    activity = discord.ui.TextInput(
        label="2. МП (тип / событие)",
        required=True,
        placeholder="ГШ / Вагонетка / Флаг",
        max_length=120,
    )
    faction = discord.ui.TextInput(
        label="3. За какую фракцию",
        required=True,
        placeholder="ММ",
        max_length=80,
    )
    outcome = discord.ui.TextInput(
        label="4. Итог",
        required=True,
        placeholder="Win / Lose",
        max_length=40,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not activity_report_allowed_in_channel(interaction):
            await interaction.response.send_message(
                activity_report_channel_restriction_message(), ephemeral=True
            )
            return
        if not user_can_activity_report_panel(interaction):
            await interaction.response.send_message(
                "Нет прав на этот отчёт.", ephemeral=True
            )
            return

        d = str(self.report_date).strip() or "—"
        a = str(self.activity).strip() or "—"
        f = str(self.faction).strip() or "—"
        o = str(self.outcome).strip() or "—"
        embed = build_activity_report_result_embed(
            report_date=d,
            activity=a,
            faction=f,
            outcome=o,
            author=interaction.user,
        )
        await interaction.response.defer(ephemeral=True)
        await interaction.channel.send(embed=embed)
        await interaction.followup.send("Отчёт отправлен в канал.", ephemeral=True)


class ActivityReportAddView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="+",
        style=discord.ButtonStyle.success,
        custom_id="activity_report_add",
        row=0,
    )
    async def btn_add(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not activity_report_allowed_in_channel(interaction):
            await interaction.response.send_message(
                activity_report_channel_restriction_message(), ephemeral=True
            )
            return
        if not user_can_activity_report_panel(interaction):
            await interaction.response.send_message(
                "Нет прав на этот отчёт.", ephemeral=True
            )
            return
        await interaction.response.send_modal(ActivityReportModal())


def build_inactiv_prompt_embed() -> discord.Embed:
    return discord.Embed(
        title="Взять инактив",
        color=discord.Color.dark_theme(),
    )


def build_inactiv_request_embed(
    author: discord.abc.User,
    *,
    period: str,
    reason: str,
) -> discord.Embed:
    """Заявка в канал: как анкета — поля + User ID в футере для модерации."""
    embed = discord.Embed(
        title="Новая заявка: Инактив",
        color=discord.Color.green(),
    )
    embed.add_field(
        name=_application_field_label("Пользователь"),
        value=author.mention,
        inline=False,
    )
    embed.add_field(
        name=_application_field_label("Период"),
        value=_application_value_in_box(period),
        inline=False,
    )
    embed.add_field(
        name=_application_field_label("Причина"),
        value=_application_value_in_box(reason),
        inline=False,
    )
    embed.set_footer(text=f"User ID: {author.id} · {_today_date_str()}")
    return embed


def build_afk_prompt_embed() -> discord.Embed:
    return discord.Embed(
        title="Взять AFK",
        color=discord.Color.dark_theme(),
    )


def build_afk_request_embed(
    author: discord.abc.User,
    *,
    time_range: str,
    reason: str,
) -> discord.Embed:
    embed = discord.Embed(
        title="Новая заявка: AFK",
        color=discord.Color.green(),
    )
    embed.add_field(
        name=_application_field_label("Пользователь"),
        value=author.mention,
        inline=False,
    )
    embed.add_field(
        name=_application_field_label("Время"),
        value=_application_value_in_box(time_range),
        inline=False,
    )
    embed.add_field(
        name=_application_field_label("Причина"),
        value=_application_value_in_box(reason),
        inline=False,
    )
    embed.set_footer(text=f"User ID: {author.id} · {_today_date_str()}")
    return embed


class StatusRequestRejectModal(discord.ui.Modal):
    reason = discord.ui.TextInput(
        label="Укажите причину отказа",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=400,
        placeholder="Например: даты не сходятся с правилами",
    )

    def __init__(self, request_message: discord.Message, *, kind_title: str):
        super().__init__(title=f"Отклонить: {kind_title}")
        self.request_message = request_message
        self._kind_title = kind_title

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return

        if not self.request_message.embeds:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        status_embed = discord.Embed(
            title=f"❌ {self._kind_title}: отклонено",
            color=discord.Color.red(),
        )
        status_embed.add_field(
            name="Модератор",
            value=interaction.user.mention,
            inline=False,
        )
        status_embed.add_field(
            name="Причина",
            value=_rejection_reason_embed_value(self.reason),
            inline=False,
        )
        status_embed.set_footer(text=_today_date_str())

        msg = self.request_message
        try:
            await msg.edit(view=None)
        except discord.HTTPException:
            pass

        posted, _ = await _status_request_post_decision_in_thread(
            msg,
            embed=status_embed,
            thread_title=f"Отказ · {self._kind_title}",
        )

        if posted:
            await interaction.followup.send(
                "Отклонение записано в ветке (или ответом под заявкой, если ветку создать нельзя).",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                "Не удалось отправить отказ (права бота / тип канала). Кнопки сняты.",
                ephemeral=True,
            )


class InactivReviewView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Одобрить",
        style=discord.ButtonStyle.success,
        emoji="✅",
        custom_id="status_inactiv_approve",
        row=0,
    )
    async def btn_approve(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return
        msg = interaction.message
        if msg is None or not msg.embeds:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        msg = await _fetch_message_for_status_review(interaction) or msg
        if not msg.embeds:
            await interaction.followup.send(
                "Не удалось прочитать заявку (нет эмбеда).", ephemeral=True
            )
            return
        embed = msg.embeds[0]
        uid = parse_user_id_from_status_request_embed(embed)
        period_plain = _embed_field_plain_by_name_part(embed, "ПЕРИОД") or ""
        until_disp = _until_part_from_dash_range(period_plain)

        member: discord.Member | None = None
        if uid is not None:
            member = guild.get_member(uid)
            if member is None:
                try:
                    member = await guild.fetch_member(uid)
                except discord.NotFound:
                    member = None

        role_note = ""
        if not INACTIV_APPROVE_ROLE_ID:
            role_note = " В .env не задан **INACTIV_APPROVE_ROLE_ID** — роль не выдана."
        elif uid is None:
            role_note = " В заявке нет User ID в футере."
        elif member is None:
            role_note = " Участник не на сервере — роль не выдана."
        else:
            role = guild.get_role(INACTIV_APPROVE_ROLE_ID)
            if role is None:
                role_note = " Роль одобрения не найдена на сервере."
            else:
                try:
                    await member.add_roles(
                        role, reason="Инактив: заявка одобрена"
                    )
                    role_note = f" Выдана роль {role.mention}."
                except discord.Forbidden:
                    role_note = (
                        " Не удалось выдать роль: права бота и порядок ролей."
                    )
                except discord.HTTPException:
                    role_note = " Ошибка Discord при выдаче роли."

        nick_note = ""
        if member is not None and until_disp:
            nick_note = await _apply_server_nick_until_suffix(
                member,
                until_disp,
                audit_label="Инактив",
            )

        done = discord.Embed(
            title="✅ Инактив одобрен",
            color=discord.Color.green(),
        )
        done.add_field(
            name="Модератор",
            value=interaction.user.mention,
            inline=False,
        )
        done.set_footer(text=_today_date_str())
        try:
            await msg.edit(view=None)
        except discord.HTTPException:
            pass
        posted, _ = await _status_request_post_decision_in_thread(
            msg,
            embed=done,
            thread_title="Одобрено · Инактив",
        )
        note = "" if posted else " Не удалось отправить в ветку — см. канал."
        await interaction.followup.send(
            "Готово." + role_note + nick_note + note, ephemeral=True
        )

    @discord.ui.button(
        label="Отклонить",
        style=discord.ButtonStyle.danger,
        emoji="❌",
        custom_id="status_inactiv_reject",
        row=0,
    )
    async def btn_reject(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return
        if interaction.message is None:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return
        await interaction.response.send_modal(
            StatusRequestRejectModal(interaction.message, kind_title="Инактив")
        )


class AfkReviewView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Одобрить",
        style=discord.ButtonStyle.success,
        emoji="✅",
        custom_id="status_afk_approve",
        row=0,
    )
    async def btn_approve(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return
        msg = interaction.message
        if msg is None or not msg.embeds:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        msg = await _fetch_message_for_status_review(interaction) or msg
        if not msg.embeds:
            await interaction.followup.send(
                "Не удалось прочитать заявку (нет эмбеда).", ephemeral=True
            )
            return

        embed = msg.embeds[0]
        uid = parse_user_id_from_status_request_embed(embed)
        time_plain = _embed_field_plain_by_name_part(embed, "ВРЕМЯ") or ""
        until_disp = _until_part_from_dash_range(time_plain)

        member: discord.Member | None = None
        if uid is not None:
            member = guild.get_member(uid)
            if member is None:
                try:
                    member = await guild.fetch_member(uid)
                except discord.NotFound:
                    member = None

        nick_note = ""
        if member is not None and until_disp:
            nick_note = await _apply_server_nick_until_suffix(
                member,
                until_disp,
                audit_label="AFK",
            )

        done = discord.Embed(
            title="✅ AFK одобрен",
            color=discord.Color.green(),
        )
        done.add_field(
            name="Модератор",
            value=interaction.user.mention,
            inline=False,
        )
        done.set_footer(text=_today_date_str())
        try:
            await msg.edit(view=None)
        except discord.HTTPException:
            pass
        posted, _ = await _status_request_post_decision_in_thread(
            msg,
            embed=done,
            thread_title="Одобрено · AFK",
        )
        extra = "" if posted else " Не удалось отправить в ветку — см. канал."
        await interaction.followup.send(
            "Готово. Заявка AFK закрыта — роль не выдаётся." + nick_note + extra,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Отклонить",
        style=discord.ButtonStyle.danger,
        emoji="❌",
        custom_id="status_afk_reject",
        row=0,
    )
    async def btn_reject(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return
        if interaction.message is None:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return
        await interaction.response.send_modal(
            StatusRequestRejectModal(interaction.message, kind_title="AFK")
        )


class InactivAddModal(discord.ui.Modal, title="Взять инактив"):
    period = discord.ui.TextInput(
        label="1. От какого — до какого",
        required=True,
        placeholder="12.02.26-15.03.26",
        max_length=120,
    )
    reason = discord.ui.TextInput(
        label="2. Причина",
        required=True,
        placeholder="Уехал в рехаб",
        style=discord.TextStyle.paragraph,
        max_length=500,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not inactiv_allowed_in_channel(interaction):
            await interaction.response.send_message(
                inactiv_channel_restriction_message(), ephemeral=True
            )
            return

        period_s = str(self.period).strip() or "—"
        reason_s = str(self.reason).strip() or "—"
        embed = build_inactiv_request_embed(
            interaction.user, period=period_s, reason=reason_s
        )
        mention, allowed = _status_request_ping_mentions(
            INACTIV_NOTIFY_ROLE_IDS, interaction.user
        )
        await interaction.response.defer(ephemeral=True)
        await interaction.channel.send(
            content=mention,
            embed=embed,
            view=InactivReviewView(),
            allowed_mentions=allowed,
        )
        await interaction.followup.send(
            "Заявка отправлена в канал. Ожидай решения модерации (без ЛС).",
            ephemeral=True,
        )


class InactivAddView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="+",
        style=discord.ButtonStyle.success,
        custom_id="status_inactiv_add",
        row=0,
    )
    async def btn_add_inactiv(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not inactiv_allowed_in_channel(interaction):
            await interaction.response.send_message(
                inactiv_channel_restriction_message(), ephemeral=True
            )
            return
        await interaction.response.send_modal(InactivAddModal())


class AfkAddModal(discord.ui.Modal, title="Взять AFK"):
    time_range = discord.ui.TextInput(
        label="1. С какого до какого времени",
        required=True,
        placeholder="21:23-02:23",
        max_length=80,
    )
    reason = discord.ui.TextInput(
        label="2. Причина",
        required=True,
        placeholder="Уехал в больницу",
        style=discord.TextStyle.paragraph,
        max_length=500,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not afk_allowed_in_channel(interaction):
            await interaction.response.send_message(
                afk_channel_restriction_message(), ephemeral=True
            )
            return

        tr = str(self.time_range).strip() or "—"
        reason_s = str(self.reason).strip() or "—"
        embed = build_afk_request_embed(
            interaction.user, time_range=tr, reason=reason_s
        )
        mention, allowed = _status_request_ping_mentions(
            AFK_NOTIFY_ROLE_IDS, interaction.user
        )
        await interaction.response.defer(ephemeral=True)
        await interaction.channel.send(
            content=mention,
            embed=embed,
            view=AfkReviewView(),
            allowed_mentions=allowed,
        )
        await interaction.followup.send(
            "Заявка отправлена в канал. Ожидай решения модерации (без ЛС).",
            ephemeral=True,
        )


class AfkAddView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="+",
        style=discord.ButtonStyle.success,
        custom_id="status_afk_add",
        row=0,
    )
    async def btn_add_afk(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Только в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not afk_allowed_in_channel(interaction):
            await interaction.response.send_message(
                afk_channel_restriction_message(), ephemeral=True
            )
            return
        await interaction.response.send_modal(AfkAddModal())


class GatherModerateMultiSelect(discord.ui.Select):
    """Мультивыбор: снятая галочка — участник не попадёт в итог (в этом блоке меню)."""

    def __init__(
        self,
        message_id: int,
        chunk_index: int,
        options: list[discord.SelectOption],
    ) -> None:
        n = len(options)
        super().__init__(
            placeholder=f"Участники (часть {chunk_index + 1}) — сними лишних…",
            min_values=0,
            max_values=n,
            options=options,
            custom_id=f"gather_mod_ms_{message_id}_{chunk_index}"[:100],
            row=chunk_index,
        )
        self._chunk_index = chunk_index

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, GatherModerateView):
            return
        view._chunk_selections[self._chunk_index] = {int(x) for x in self.values}
        view._recompute_selected()
        await interaction.response.defer()


class GatherModerateView(discord.ui.View):
    def __init__(
        self, message_id: int, guild: discord.Guild, state: GatherState
    ) -> None:
        super().__init__(timeout=300)
        self._message_id = message_id
        self._selected_ids: set[int] = set()
        self._ids_in_ui: set[int] = set()
        self._chunk_selections: dict[int, set[int]] = {}

        ordered = gather_moderate_participant_ids_ordered(state)
        ui_slice = ordered[:GATHER_MODERATE_MAX_UI_PARTICIPANTS]
        self._ids_in_ui = set(ui_slice)
        chunks = [ui_slice[i : i + 25] for i in range(0, len(ui_slice), 25)]
        for i, chunk_uids in enumerate(chunks[:_GATHER_MODERATE_MAX_CHUNKS]):
            opts = gather_moderate_build_option_chunk(state, chunk_uids, guild)
            if not opts:
                continue
            self._chunk_selections[i] = set(chunk_uids)
            self.add_item(GatherModerateMultiSelect(message_id, i, opts))
        self._recompute_selected()

    def _recompute_selected(self) -> None:
        merged: set[int] = set()
        for s in self._chunk_selections.values():
            merged |= s
        self._selected_ids = merged

    @discord.ui.button(
        label="Создать список",
        style=discord.ButtonStyle.success,
        row=4,
    )
    async def create_list(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        state = GATHER_MESSAGES.get(self._message_id)
        if state is None:
            await interaction.response.send_message(
                "Сбор устарел — перезапусти бота или создай новый.", ephemeral=True
            )
            return
        if not user_can_manage_gather_panel(interaction, state):
            await interaction.response.send_message(
                "Нет прав на модерацию этого сбора.", ephemeral=True
            )
            return
        if interaction.guild is None:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return

        self._recompute_selected()
        if not self._selected_ids:
            await interaction.response.send_message(
                "Отметь хотя бы одного участника (или открой модерацию снова).",
                ephemeral=True,
            )
            return

        to_remove = self._ids_in_ui - self._selected_ids
        for uid in to_remove:
            if uid in state.main_ids:
                state.main_ids.remove(uid)
            if uid in state.extra_ids:
                state.extra_ids.remove(uid)

        await interaction.response.defer(ephemeral=True)

        await refresh_gather_message(bot, self._message_id, state)
        persist_panel_extra_state()

        final_ids = list(state.main_ids) + list(state.extra_ids)
        out_embed = discord.Embed(color=discord.Color.dark_theme())
        out_embed.add_field(
            name="Участники",
            value=format_gather_participant_lines(final_ids, interaction.guild),
            inline=False,
        )

        ch_reply = interaction.channel
        if isinstance(ch_reply, (discord.TextChannel, discord.Thread)):
            await ch_reply.send(embed=out_embed)

        await interaction.edit_original_response(
            content="✅ Модерация закрыта. Плашка сбора обновлена, итог отправлен в канал.",
            view=None,
        )


class GatherSignView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="В основу",
        style=discord.ButtonStyle.success,
        custom_id="gather_join_main",
        row=0,
    )
    async def join_main(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._handle_join(interaction, main=True)

    @discord.ui.button(
        label="В доп",
        style=discord.ButtonStyle.primary,
        custom_id="gather_join_extra",
        row=0,
    )
    async def join_extra(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._handle_join(interaction, main=False)

    @discord.ui.button(
        label="Выйти",
        style=discord.ButtonStyle.secondary,
        custom_id="gather_leave",
        row=1,
    )
    async def leave_list(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._handle_leave(interaction)

    @discord.ui.button(
        label="Откр./закрыть",
        style=discord.ButtonStyle.danger,
        custom_id="gather_toggle_status",
        row=1,
    )
    async def toggle_status(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._handle_toggle(interaction)

    @discord.ui.button(
        label="Модерация списка",
        style=discord.ButtonStyle.secondary,
        custom_id="gather_moderate_list",
        row=2,
    )
    async def moderate_list(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.message is None:
            await interaction.response.send_message("Ошибка.", ephemeral=True)
            return
        state = GATHER_MESSAGES.get(interaction.message.id)
        if state is None:
            await interaction.response.send_message(
                "Запись устарела (перезапустите бота).", ephemeral=True
            )
            return
        if not user_can_manage_gather_panel(interaction, state):
            await interaction.response.send_message(
                "Нет прав: создатель сбора, модерация сервера или роли /sbor для этого типа.",
                ephemeral=True,
            )
            return
        if interaction.guild is None:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        ordered = gather_moderate_participant_ids_ordered(state)
        if not ordered:
            await interaction.response.send_message(
                "В списке пока никого нет.", ephemeral=True
            )
            return
        total = gather_moderate_total_participants(state)
        over_note = ""
        if total > GATHER_MODERATE_MAX_UI_PARTICIPANTS:
            over_note = (
                f"\n\n*В меню только первые **{GATHER_MODERATE_MAX_UI_PARTICIPANTS}** "
                f"из **{total}** участников (лимит Discord). Остальные слоты не меняются, "
                "пока не освободишь места в списке.*"
            )
        view = GatherModerateView(interaction.message.id, interaction.guild, state)
        await interaction.response.send_message(
            content=(
                "Отметь галочками, кто **останется** в списке (сними лишних). "
                "Затем **Создать список** — плашка сбора **останется** и обновится, "
                "в канал уйдёт итог."
                f"{over_note}"
            ),
            view=view,
            ephemeral=True,
        )

    async def _handle_join(
        self, interaction: discord.Interaction, main: bool
    ) -> None:
        try:
            await interaction.response.defer(thinking=True, ephemeral=True)
        except discord.NotFound:
            return
        if interaction.guild is None or interaction.message is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        state = GATHER_MESSAGES.get(interaction.message.id)
        if state is None:
            await interaction.followup.send(
                "Запись устарела (перезапустите бота).", ephemeral=True
            )
            return
        changed = gather_apply_auto_close_if_due(state)
        if changed:
            embed = build_gather_embed(state, interaction.guild)
            if not await gather_try_edit_panel(
                interaction.message, embed=embed, view=self
            ):
                await interaction.followup.send(
                    "Плашка сбора удалена — создай новый сбор.", ephemeral=True
                )
                return
            persist_panel_extra_state()
        if not state.status_open:
            await interaction.followup.send("Сбор закрыт.", ephemeral=True)
            return
        uid = interaction.user.id
        if uid in state.main_ids or uid in state.extra_ids:
            await interaction.followup.send("Вы уже в списке.", ephemeral=True)
            return
        if main:
            if len(state.main_ids) >= state.max_main:
                await interaction.followup.send("Основа заполнена.", ephemeral=True)
                return
            if uid in state.extra_ids:
                state.extra_ids.remove(uid)
            state.main_ids.append(uid)
        else:
            if state.max_extra <= 0:
                await interaction.followup.send("Доп. слоты отключены.", ephemeral=True)
                return
            if len(state.extra_ids) >= state.max_extra:
                await interaction.followup.send("Доп. слоты заполнены.", ephemeral=True)
                return
            if uid in state.main_ids:
                state.main_ids.remove(uid)
            state.extra_ids.append(uid)
        embed = build_gather_embed(state, interaction.guild)
        if not await gather_try_edit_panel(
            interaction.message, embed=embed, view=self
        ):
            if main:
                if uid in state.main_ids:
                    state.main_ids.remove(uid)
            else:
                if uid in state.extra_ids:
                    state.extra_ids.remove(uid)
            await interaction.followup.send(
                "Плашка сбора не найдена (удалена?). Запись отменена.", ephemeral=True
            )
            return
        await interaction.followup.send("Записано.", ephemeral=True)
        persist_panel_extra_state()

    async def _handle_leave(self, interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(thinking=True, ephemeral=True)
        except discord.NotFound:
            return
        if interaction.guild is None or interaction.message is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        state = GATHER_MESSAGES.get(interaction.message.id)
        if state is None:
            await interaction.followup.send("Запись устарела.", ephemeral=True)
            return
        uid = interaction.user.id
        if uid not in state.main_ids and uid not in state.extra_ids:
            await interaction.followup.send("Вас нет в списке.", ephemeral=True)
            return
        if uid in state.main_ids:
            state.main_ids.remove(uid)
        if uid in state.extra_ids:
            state.extra_ids.remove(uid)
        embed = build_gather_embed(state, interaction.guild)
        if not await gather_try_edit_panel(
            interaction.message, embed=embed, view=self
        ):
            await interaction.followup.send(
                "Плашка сбора удалена или недоступна — создай новый сбор.",
                ephemeral=True,
            )
            return
        await interaction.followup.send("Вы вышли из списка.", ephemeral=True)
        persist_panel_extra_state()

    async def _handle_toggle(self, interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(thinking=True, ephemeral=True)
        except discord.NotFound:
            return
        if interaction.guild is None or interaction.message is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        state = GATHER_MESSAGES.get(interaction.message.id)
        if state is None:
            await interaction.followup.send("Запись устарела.", ephemeral=True)
            return
        changed = gather_apply_auto_close_if_due(state)
        if changed:
            embed = build_gather_embed(state, interaction.guild)
            if not await gather_try_edit_panel(
                interaction.message, embed=embed, view=self
            ):
                await interaction.followup.send(
                    "Плашка сбора удалена — создай новый сбор.", ephemeral=True
                )
                return
            persist_panel_extra_state()
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.followup.send("Ошибка.", ephemeral=True)
            return
        if not user_can_manage_gather_panel(interaction, state):
            await interaction.followup.send(
                "Только создатель, модерация сервера или роли /sbor для этого типа.",
                ephemeral=True,
            )
            return
        prev_open = state.status_open
        state.status_open = not state.status_open
        if state.status_open and state.closes_at is not None:
            if state.closes_at <= datetime.now(timezone.utc):
                state.closes_at = None
        embed = build_gather_embed(state, interaction.guild)
        if not await gather_try_edit_panel(
            interaction.message, embed=embed, view=self
        ):
            state.status_open = prev_open
            await interaction.followup.send(
                "Плашка сбора удалена или недоступна — создай новый сбор.",
                ephemeral=True,
            )
            return
        st = "открыт" if state.status_open else "закрыт"
        await interaction.followup.send(f"Сбор {st}.", ephemeral=True)
        persist_panel_extra_state()


def podarok_entries_closed(state: PodarokState) -> bool:
    return datetime.now(timezone.utc) >= state.ends_at


def build_podarok_embed(state: PodarokState, guild: discord.Guild) -> discord.Embed:
    ts = int(state.ends_at.timestamp())
    deadline = f"<t:{ts}:F>\n<t:{ts}:R>"
    if state.finished:
        status = "Завершён"
    elif podarok_entries_closed(state):
        status = "Набор закрыт (ожидает розыгрыша)"
    else:
        status = "Открыт — жми **Участвовать**"
    n = len(state.participant_ids)
    embed = discord.Embed(
        title="🎁 Розыгрыш",
        color=discord.Color.gold(),
    )
    embed.add_field(
        name="**На что розыгрыш**",
        value=state.prize[:1024] if state.prize else "—",
        inline=False,
    )
    embed.add_field(
        name="**Участники**",
        value=f"{n} / {state.max_participants}",
        inline=True,
    )
    embed.add_field(
        name="**Победителей (слотов)**",
        value=str(state.winner_count),
        inline=True,
    )
    embed.add_field(
        name="**До какого (МСК в вводе)**",
        value=deadline,
        inline=False,
    )
    embed.add_field(
        name="**Список участников**",
        value=format_gather_participant_lines(state.participant_ids, guild),
        inline=False,
    )
    embed.add_field(name="**Статус**", value=status, inline=False)
    if state.finished and state.winner_ids:
        embed.add_field(
            name="**Победители**",
            value=format_gather_participant_lines(state.winner_ids, guild),
            inline=False,
        )
    ft = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")
    embed.set_footer(text=f"Организатор: {state.creator_tag} · {ft} UTC")
    return embed


def make_podarok_view(state: PodarokState) -> discord.ui.View:
    v = PodarokView()
    for c in v.children:
        if not isinstance(c, discord.ui.Button):
            continue
        if c.custom_id == "podarok_join":
            c.disabled = state.finished or podarok_entries_closed(state)
        elif c.custom_id == "podarok_draw":
            c.disabled = state.finished
    return v


async def podarok_try_edit_panel(
    message: discord.Message, *, embed: discord.Embed, view: discord.ui.View
) -> bool:
    try:
        await message.edit(embed=embed, view=view)
        return True
    except discord.NotFound:
        PODAROK_MESSAGES.pop(message.id, None)
        persist_podarok_sbormoney()
        return False


async def refresh_podarok_message(message_id: int) -> None:
    st = PODAROK_MESSAGES.get(message_id)
    if st is None:
        return
    ch = bot.get_channel(st.channel_id)
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        PODAROK_MESSAGES.pop(message_id, None)
        persist_podarok_sbormoney()
        return
    guild = ch.guild
    if guild is None:
        return
    try:
        msg = await ch.fetch_message(message_id)
    except discord.HTTPException:
        PODAROK_MESSAGES.pop(message_id, None)
        persist_podarok_sbormoney()
        return
    embed = build_podarok_embed(st, guild)
    try:
        await msg.edit(embed=embed, view=make_podarok_view(st))
    except discord.HTTPException:
        pass


async def podarok_deadline_refresh_loop() -> None:
    """Один раз после дедлайна обновляет панель (отключает «Участвовать»)."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = datetime.now(timezone.utc)
            for mid, st in list(PODAROK_MESSAGES.items()):
                if st.finished or st.after_deadline_embed_done:
                    continue
                if st.ends_at <= now:
                    st.after_deadline_embed_done = True
                    await refresh_podarok_message(mid)
                    persist_podarok_sbormoney()
        except Exception:
            pass
        await asyncio.sleep(25)


async def podarok_dm_winners(
    guild: discord.Guild, user_ids: list[int], prize: str, jump_url: str
) -> None:
    text = (
        f"🎉 **Ты выиграл** в розыгрыше!\n\n"
        f"**Приз:** {prize[:500]}\n"
        f"Сообщение с итогом: {jump_url}"
    )
    for uid in user_ids:
        member = guild.get_member(uid)
        if member is not None and not member.bot:
            try:
                await member.send(text[:2000])
            except discord.HTTPException:
                pass
            await asyncio.sleep(0.35)
            continue
        try:
            u = await bot.fetch_user(uid)
            if not u.bot:
                await u.send(text[:2000])
        except discord.HTTPException:
            pass
        await asyncio.sleep(0.35)


class PodarokView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Участвовать",
        style=discord.ButtonStyle.success,
        custom_id="podarok_join",
        row=0,
    )
    async def btn_join(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.guild is None or interaction.message is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        mid = interaction.message.id
        st = PODAROK_MESSAGES.get(mid)
        if st is None:
            await interaction.response.send_message(
                "Панель устарела — создай новую **/podarok**.", ephemeral=True
            )
            return
        if st.finished:
            await interaction.response.send_message(
                "Розыгрыш уже завершён, участие недоступно.", ephemeral=True
            )
            return
        if podarok_entries_closed(st):
            await interaction.response.send_message(
                "Срок записи истёк.", ephemeral=True
            )
            return
        uid = interaction.user.id
        if uid in st.participant_ids:
            await interaction.response.send_message(
                "Ты уже в списке участников.", ephemeral=True
            )
            return
        if len(st.participant_ids) >= st.max_participants:
            await interaction.response.send_message(
                "Набрано максимум участников.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        st.participant_ids.append(uid)
        embed = build_podarok_embed(st, interaction.guild)
        if not await podarok_try_edit_panel(
            interaction.message, embed=embed, view=make_podarok_view(st)
        ):
            st.participant_ids.pop()
            await interaction.followup.send(
                "Сообщение розыгрыша не найдено — запись отменена.", ephemeral=True
            )
            return
        await interaction.followup.send("Ты участвуешь в розыгрыше.", ephemeral=True)
        persist_podarok_sbormoney()

    @discord.ui.button(
        label="Разыграть",
        style=discord.ButtonStyle.danger,
        custom_id="podarok_draw",
        row=0,
    )
    async def btn_draw(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.guild is None or interaction.message is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        mid = interaction.message.id
        st = PODAROK_MESSAGES.get(mid)
        if st is None:
            await interaction.response.send_message(
                "Панель устарела — создай новую **/podarok**.", ephemeral=True
            )
            return
        if not user_can_podarok_draw(interaction, st):
            await interaction.response.send_message(
                "Разыграть может только тот, кто создал этот розыгрыш.", ephemeral=True
            )
            return
        if st.finished:
            await interaction.response.send_message(
                "Розыгрыш уже проведён.", ephemeral=True
            )
            return
        if not st.participant_ids:
            await interaction.response.send_message(
                "В списке никого нет — некого выбирать.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        pool = list(dict.fromkeys(st.participant_ids))
        k = min(st.winner_count, len(pool))
        st.winner_ids = random.sample(pool, k=k)
        st.finished = True
        embed = build_podarok_embed(st, interaction.guild)
        if not await podarok_try_edit_panel(
            interaction.message, embed=embed, view=make_podarok_view(st)
        ):
            st.finished = False
            st.winner_ids = []
            await interaction.followup.send(
                "Не удалось обновить сообщение — попробуй ещё раз.", ephemeral=True
            )
            return
        jump = interaction.message.jump_url
        asyncio.create_task(
            podarok_dm_winners(interaction.guild, st.winner_ids, st.prize, jump)
        )
        winners_mentions = ", ".join(f"<@{w}>" for w in st.winner_ids)
        await interaction.followup.send(
            f"Готово. Победители: {winners_mentions}", ephemeral=True
        )
        persist_podarok_sbormoney()


class PodarokCreateModal(discord.ui.Modal, title="Новый розыгрыш"):
    prize = discord.ui.TextInput(
        label="1. На что розыгрыш",
        style=discord.TextStyle.short,
        required=True,
        max_length=256,
        placeholder="Например: подписка Nitro",
    )
    max_participants = discord.ui.TextInput(
        label="2. Макс. участников (число)",
        style=discord.TextStyle.short,
        required=True,
        max_length=4,
        placeholder="50",
    )
    winner_count = discord.ui.TextInput(
        label="3. Сколько победителей",
        style=discord.TextStyle.short,
        required=True,
        max_length=3,
        placeholder="1",
    )
    ends_at = discord.ui.TextInput(
        label="4. До какого (МСК)",
        style=discord.TextStyle.short,
        required=True,
        max_length=32,
        placeholder="05.04.2026 21:30",
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        if not isinstance(
            interaction.channel, (discord.TextChannel, discord.Thread)
        ):
            await interaction.response.send_message(
                "Используй в текстовом канале или ветке.", ephemeral=True
            )
            return
        if not user_can_post_podarok(interaction):
            await interaction.response.send_message(
                "Нет прав на **/podarok**: **MODERATION_ROLE_IDS** или "
                "**PODAROK_POST_ROLE_IDS** (если задан).",
                ephemeral=True,
            )
            return
        raw_max = str(self.max_participants.value).strip()
        raw_win = str(self.winner_count.value).strip()
        try:
            mx = int(raw_max)
            wn = int(raw_win)
        except ValueError:
            await interaction.response.send_message(
                "В полях **участников** и **победителей** нужны целые числа.", ephemeral=True
            )
            return
        if mx < 1 or mx > 500:
            await interaction.response.send_message(
                "Макс. участников: от 1 до 500.", ephemeral=True
            )
            return
        if wn < 1 or wn > mx:
            await interaction.response.send_message(
                f"Число победителей: от 1 до {mx} (не больше лимита участников).",
                ephemeral=True,
            )
            return
        ends_raw = str(self.ends_at.value).strip()
        try:
            ends_utc = parse_war_timer_datetime_msk(ends_raw)
        except ValueError:
            await interaction.response.send_message(
                "Неверная дата «до какого». Пример: `05.04.2026 21:30` (МСК).",
                ephemeral=True,
            )
            return
        if ends_utc <= datetime.now(timezone.utc):
            await interaction.response.send_message(
                "Момент «до какого» должен быть **в будущем**.", ephemeral=True
            )
            return
        prize_s = str(self.prize.value).strip()
        if not prize_s:
            await interaction.response.send_message(
                "Укажи, на что розыгрыш.", ephemeral=True
            )
            return
        creator = interaction.user
        creator_tag = creator.display_name
        if isinstance(creator, discord.Member) and creator.nick:
            creator_tag = f"{creator.nick}/{creator.display_name}"
        elif isinstance(creator, discord.Member):
            creator_tag = creator.display_name
        st = PodarokState(
            prize=prize_s[:2000],
            max_participants=mx,
            winner_count=wn,
            ends_at=ends_utc,
            creator_id=creator.id,
            creator_tag=creator_tag[:80],
            channel_id=interaction.channel.id,
        )
        embed = build_podarok_embed(st, interaction.guild)
        view = make_podarok_view(st)
        await interaction.response.defer(ephemeral=True)
        ch = interaction.channel
        assert isinstance(ch, (discord.TextChannel, discord.Thread))
        guild = interaction.guild
        assert guild is not None
        ping_content, ping_allowed = podarok_post_content_and_mentions(guild)
        msg = await ch.send(
            content=ping_content,
            embed=embed,
            view=view,
            allowed_mentions=ping_allowed,
        )
        PODAROK_MESSAGES[msg.id] = st
        persist_podarok_sbormoney()
        if PODAROK_CHANNEL_ACCESS_ROLE_IDS or PODAROK_PING_ROLE_ID:
            asyncio.create_task(
                dm_podarok_ping_role_notify(interaction.guild, msg, st.prize)
            )
        await interaction.followup.send("Розыгрыш отправлен в канал.", ephemeral=True)


class KontraktRejectModal(discord.ui.Modal, title="Отказ по контракту"):
    reason = discord.ui.TextInput(
        label="Причина отказа",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=900,
        placeholder="Кратко укажи причину",
    )

    def __init__(self, message_id: int) -> None:
        super().__init__()
        self._message_id = message_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None or interaction.channel_id is None:
            await interaction.response.send_message("Ошибка.", ephemeral=True)
            return
        state = CONTRACT_MESSAGES.get(
            self._message_id
        ) or _kontrakt_reload_state_for_message(self._message_id)
        if state is None or not state.status_open:
            await interaction.response.send_message(
                "Контракт уже закрыт или устарел.", ephemeral=True
            )
            return
        if not user_can_manage_kontrakt_contract(interaction, state):
            await interaction.response.send_message(
                embed=_kontrakt_manage_forbidden_embed(),
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True, ephemeral=True)
        ch = interaction.channel
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            await interaction.followup.send("Канал недоступен.", ephemeral=True)
            return
        try:
            msg = await ch.fetch_message(self._message_id)
        except discord.HTTPException:
            CONTRACT_MESSAGES.pop(self._message_id, None)
            persist_panel_extra_state()
            await interaction.followup.send("Сообщение не найдено.", ephemeral=True)
            return

        reason_text = str(self.reason.value).strip().replace("```", "'''")[:900]
        reason_heading = " ".join(reason_text.split()) or "—"
        actor = interaction.user
        thread_body = (
            f"Отказ от {actor.mention} ({actor.id})\n\n> ## {reason_heading}"
        )

        if msg.thread is not None:
            try:
                await msg.thread.send(thread_body)
            except discord.HTTPException:
                pass
        else:
            try:
                tname = (f"Отказ · {state.title}")[:90] or "Отказ"
                th = await msg.create_thread(
                    name=tname,
                    auto_archive_duration=10080,
                )
                await th.send(thread_body)
            except discord.HTTPException:
                try:
                    await ch.send(f"{thread_body}\n↪ {msg.jump_url}"[:2000])
                except discord.HTTPException:
                    pass

        state.status_open = False
        embed = build_contract_listing_embed(state, guild)
        await kontrakt_try_edit_panel(msg, embed=embed, view=None)
        CONTRACT_MESSAGES.pop(self._message_id, None)
        await interaction.followup.send("Отказ записан, контракт закрыт.", ephemeral=True)
        persist_panel_extra_state()


class KontraktProposeModal(discord.ui.Modal, title="Предложить контракт"):
    name_in = discord.ui.TextInput(
        label="1. Название контракта",
        placeholder="Напр. Лазурный берег",
        required=True,
        max_length=120,
    )
    veksels_in = discord.ui.TextInput(
        label="2. На сколько векселей",
        placeholder="Напр. 376",
        required=True,
        max_length=32,
    )
    time_in = discord.ui.TextInput(
        label="3. На какое время",
        placeholder="Напр. 1:30–2:20",
        required=True,
        max_length=80,
    )
    people_in = discord.ui.TextInput(
        label="4. На сколько человек",
        placeholder="Напр. От 2–6",
        required=True,
        max_length=80,
    )
    razdel_100_in = discord.ui.TextInput(
        label="5. Раздел на 100%",
        placeholder="Да / Нет",
        required=True,
        max_length=32,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        ch = interaction.channel
        if guild is None or not isinstance(
            ch, (discord.TextChannel, discord.Thread)
        ):
            await interaction.followup.send(
                "Только в текстовом канале сервера.", ephemeral=True
            )
            return
        creator = interaction.user
        if not isinstance(creator, discord.Member):
            await interaction.followup.send("Ошибка.", ephemeral=True)
            return

        creator_tag = creator.display_name
        if creator.nick:
            creator_tag = f"{creator.nick}/{creator.display_name}"

        title = str(self.name_in.value).strip()[:200]
        vek = str(self.veksels_in.value).strip()[:80]
        tim = str(self.time_in.value).strip()[:80]
        raw_people = str(self.people_in.value)
        cap, note = parse_kontrakt_people_cap(raw_people)
        razdel = str(self.razdel_100_in.value).strip()[:32]
        if not title:
            await interaction.followup.send("Укажи название контракта.", ephemeral=True)
            return
        if not razdel:
            await interaction.followup.send(
                "Укажи **раздел на 100%** (например Да или Нет).", ephemeral=True
            )
            return

        state = ContractState(
            channel_id=ch.id,
            creator_id=creator.id,
            creator_tag=creator_tag[:80],
            title=title,
            veksels=vek,
            time_slot=tim,
            razdel_100=razdel,
            people_note=note,
            max_participants=cap,
            participant_ids=[],
            status_open=True,
        )
        embed = build_contract_listing_embed(state, guild)
        view = KontraktContractView()
        ping_ids = sorted(KONTRAKT_NEW_CONTRACT_PING_ROLE_IDS)
        try:
            if ping_ids:
                msg = await ch.send(
                    content=" ".join(f"<@&{rid}>" for rid in ping_ids),
                    embed=embed,
                    view=view,
                    allowed_mentions=discord.AllowedMentions(
                        roles=[discord.Object(id=rid) for rid in ping_ids]
                    ),
                )
            else:
                msg = await ch.send(embed=embed, view=view)
        except discord.HTTPException as exc:
            await interaction.followup.send(
                f"Не удалось отправить сообщение ({getattr(exc, 'code', '')}).",
                ephemeral=True,
            )
            return
        CONTRACT_MESSAGES[msg.id] = state
        persist_panel_extra_state()
        await interaction.followup.send("Контракт опубликован в канале.", ephemeral=True)


class KontraktPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Предложить",
        style=discord.ButtonStyle.success,
        custom_id="kontrakt_propose",
        row=0,
    )
    async def btn_propose(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        await interaction.response.send_modal(KontraktProposeModal())


class KontraktContractView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Участвовать",
        style=discord.ButtonStyle.success,
        custom_id="kontrakt_join",
        row=0,
    )
    async def btn_join(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        try:
            await interaction.response.defer(thinking=True, ephemeral=True)
        except discord.NotFound:
            return
        if interaction.guild is None or interaction.message is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        mid = interaction.message.id
        state = CONTRACT_MESSAGES.get(mid) or _kontrakt_reload_state_for_message(mid)
        if state is None or not state.status_open:
            await interaction.followup.send(
                "Контракт закрыт или запись устарела.", ephemeral=True
            )
            return
        uid = interaction.user.id
        if uid in state.participant_ids:
            await interaction.followup.send("Ты уже в списке.", ephemeral=True)
            return
        if len(state.participant_ids) >= state.max_participants:
            await interaction.followup.send("Набор заполнен.", ephemeral=True)
            return
        state.participant_ids.append(uid)
        embed = build_contract_listing_embed(state, interaction.guild)
        if not await kontrakt_try_edit_panel(
            interaction.message, embed=embed, view=self
        ):
            state.participant_ids.pop()
            await interaction.followup.send(
                "Не удалось обновить сообщение.", ephemeral=True
            )
            return
        await interaction.followup.send("Ты записан в участники.", ephemeral=True)
        persist_panel_extra_state()

    @discord.ui.button(
        label="Пикнул",
        style=discord.ButtonStyle.primary,
        custom_id="kontrakt_pick",
        row=0,
    )
    async def btn_pick(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        try:
            await interaction.response.defer(thinking=True, ephemeral=True)
        except discord.NotFound:
            return
        if interaction.guild is None or interaction.message is None:
            await interaction.followup.send("Только на сервере.", ephemeral=True)
            return
        mid = interaction.message.id
        state = CONTRACT_MESSAGES.get(mid) or _kontrakt_reload_state_for_message(mid)
        if state is None or not state.status_open:
            await interaction.followup.send(
                "Контракт уже закрыт или устарел.", ephemeral=True
            )
            return
        if not user_can_manage_kontrakt_contract(interaction, state):
            await interaction.followup.send(
                embed=_kontrakt_manage_forbidden_embed(),
                ephemeral=True,
            )
            return
        state.status_open = False
        embed = build_contract_listing_embed(state, interaction.guild)
        if not await kontrakt_try_edit_panel(
            interaction.message, embed=embed, view=None
        ):
            state.status_open = True
            await interaction.followup.send(
                "Не удалось обновить сообщение.", ephemeral=True
            )
            return
        CONTRACT_MESSAGES.pop(interaction.message.id, None)
        await interaction.followup.send("Контракт закрыт (пикнули).", ephemeral=True)
        persist_panel_extra_state()

    @discord.ui.button(
        label="Отказ",
        style=discord.ButtonStyle.danger,
        custom_id="kontrakt_reject",
        row=0,
    )
    async def btn_reject(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if interaction.guild is None or interaction.message is None:
            await interaction.response.send_message(
                "Только на сервере.", ephemeral=True
            )
            return
        mid = interaction.message.id
        state = CONTRACT_MESSAGES.get(mid) or _kontrakt_reload_state_for_message(mid)
        if state is None or not state.status_open:
            await interaction.response.send_message(
                "Контракт уже закрыт или устарел.", ephemeral=True
            )
            return
        if not user_can_manage_kontrakt_contract(interaction, state):
            await interaction.response.send_message(
                embed=_kontrakt_manage_forbidden_embed(),
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(KontraktRejectModal(mid))


def _temp_vc_display_name(member: discord.Member) -> str:
    base = (member.nick or member.display_name or member.name or "комната").strip()
    base = re.sub(r"[^\w\s\-_.А-Яа-яЁё]", "", base)[:80]
    return base or "комната"


def temp_vc_session_from_channel(
    interaction: discord.Interaction,
) -> Optional[TempVoiceSession]:
    if interaction.channel is None:
        return None
    # Панель в Text-in-Voice: interaction.channel — тот же id, что и голосовой канал.
    return TEMP_VC_BY_VOICE.get(interaction.channel.id)


def temp_vc_can_control(interaction: discord.Interaction, sess: TempVoiceSession) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    u = interaction.user
    if u.id == sess.owner_id:
        return True
    return u.guild_permissions.manage_guild


def build_temp_vc_panel_embed() -> discord.Embed:
    return discord.Embed(
        title="Панель управления",
        description=(
            "Кнопки ниже — настройки **твоей** голосовой комнаты (чат справа у этого войса). "
            "**Название**, **Лимит**, **Регион** — сразу меняют войс; "
            "**Кикнуть** — из списка; **Прихожая** — закрыть вход для всех, кроме тебя; "
            "**Забрать** — передать владельца из списка; **Друзья** / **Баны** — выбор из списка."
        ),
        color=discord.Color.dark_theme(),
    )


async def _temp_vc_delete_session(sess: TempVoiceSession) -> None:
    TEMP_VC_BY_VOICE.pop(sess.voice_channel_id, None)
    vch = bot.get_channel(sess.voice_channel_id)
    if isinstance(vch, discord.VoiceChannel):
        try:
            await vch.delete(reason="Временная комната пуста")
        except discord.HTTPException:
            pass


async def temp_vc_maybe_delete_empty(voice_channel_id: int) -> None:
    await asyncio.sleep(4)
    sess = TEMP_VC_BY_VOICE.get(voice_channel_id)
    if sess is None:
        return
    vc = bot.get_channel(voice_channel_id)
    if not isinstance(vc, discord.VoiceChannel):
        await _temp_vc_delete_session(sess)
        return
    if len(vc.members) == 0:
        await _temp_vc_delete_session(sess)


class TempVcRenameModal(discord.ui.Modal, title="Название канала"):
    name_in = discord.ui.TextInput(
        label="Новое имя (без 🔊)",
        placeholder="Например: Сбор на ВЗП",
        max_length=90,
        required=True,
    )

    def __init__(self, voice_id: int) -> None:
        super().__init__()
        self._voice_id = voice_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        if interaction.guild is None:
            return
        vc = interaction.guild.get_channel(self._voice_id)
        if not isinstance(vc, discord.VoiceChannel):
            await interaction.response.send_message("Войс не найден.", ephemeral=True)
            return
        name = str(self.name_in.value).strip()[:100] or "комната"
        try:
            await vc.edit(name=f"🔊 {name}"[:100])
        except discord.HTTPException as e:
            await interaction.response.send_message(f"Ошибка: {e}", ephemeral=True)
            return
        await interaction.response.send_message("Название обновлено.", ephemeral=True)


class TempVcLimitModal(discord.ui.Modal, title="Лимит участников"):
    limit_in = discord.ui.TextInput(
        label="Сколько человек (0 — без лимита, макс. 99)",
        placeholder="0",
        max_length=2,
        required=True,
    )

    def __init__(self, voice_id: int) -> None:
        super().__init__()
        self._voice_id = voice_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        if interaction.guild is None:
            return
        vc = interaction.guild.get_channel(self._voice_id)
        if not isinstance(vc, discord.VoiceChannel):
            await interaction.response.send_message("Войс не найден.", ephemeral=True)
            return
        try:
            n = int(str(self.limit_in.value).strip())
        except ValueError:
            await interaction.response.send_message("Введи число.", ephemeral=True)
            return
        n = max(0, min(99, n))
        try:
            await vc.edit(user_limit=n)
        except discord.HTTPException as e:
            await interaction.response.send_message(f"Ошибка: {e}", ephemeral=True)
            return
        await interaction.response.send_message(f"Лимит: **{n or 'нет'}**.", ephemeral=True)


class TempVcRegionModal(discord.ui.Modal, title="Регион голоса"):
    region_in = discord.ui.TextInput(
        label="Код региона или пусто = авто",
        placeholder="us-east, rotterdam, russia … пусто",
        max_length=32,
        required=False,
    )

    def __init__(self, voice_id: int) -> None:
        super().__init__()
        self._voice_id = voice_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        if interaction.guild is None:
            return
        vc = interaction.guild.get_channel(self._voice_id)
        if not isinstance(vc, discord.VoiceChannel):
            await interaction.response.send_message("Войс не найден.", ephemeral=True)
            return
        raw = str(self.region_in.value).strip().lower()
        region: Optional[str] = raw if raw else None
        try:
            await vc.edit(rtc_region=region)
        except discord.HTTPException as e:
            await interaction.response.send_message(
                f"Не вышло (проверь код региона): {e}", ephemeral=True
            )
            return
        await interaction.response.send_message(
            f"Регион: **{region or 'авто'}**.", ephemeral=True
        )


class TempVcUserPickView(discord.ui.View):
    """Выбор участника через список Discord (UserSelect), без ввода ID."""

    _PLACEHOLDERS = {
        "kick": "Кого выкинуть из войса?",
        "claim": "Кому передать комнату?",
        "friends": "Кому открыть доступ в войс?",
        "bans": "Кого не пускать в войс?",
    }

    def __init__(self, voice_id: int, mode: str) -> None:
        super().__init__(timeout=180)
        self._voice_id = voice_id
        self._mode = mode
        sel = discord.ui.UserSelect(
            placeholder=self._PLACEHOLDERS.get(mode, "Выбери участника"),
            min_values=1,
            max_values=1,
        )

        async def _cb(interaction: discord.Interaction) -> None:
            await self._on_pick(interaction, sel)

        sel.callback = _cb
        self.add_item(sel)

    async def _on_pick(
        self, interaction: discord.Interaction, select: discord.ui.UserSelect
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        if interaction.guild is None or not select.values:
            await interaction.response.send_message("Некого выбрать.", ephemeral=True)
            return
        vc = interaction.guild.get_channel(self._voice_id)
        if not isinstance(vc, discord.VoiceChannel):
            await interaction.response.send_message("Войс не найден.", ephemeral=True)
            return
        target = select.values[0]
        if isinstance(target, discord.Member):
            member = target
        else:
            member = interaction.guild.get_member(target.id)
        if member is None:
            await interaction.response.send_message(
                "Участник не на сервере.", ephemeral=True
            )
            return

        if self._mode == "kick":
            if member.voice is None or member.voice.channel != vc:
                await interaction.response.send_message(
                    "Этот пользователь не в этом войсе.", ephemeral=True
                )
                return
            try:
                await member.move_to(None, reason="Кик с панели")
            except discord.HTTPException as e:
                await interaction.response.send_message(f"Ошибка: {e}", ephemeral=True)
                return
            await interaction.response.send_message("Готово.", ephemeral=True)
            self.stop()
            return

        if self._mode == "claim":
            if member.bot:
                await interaction.response.send_message("Нельзя передать боту.", ephemeral=True)
                return
            old = interaction.guild.get_member(sess.owner_id)
            normal = discord.PermissionOverwrite(view_channel=True, connect=True)
            n_po = discord.PermissionOverwrite(
                view_channel=True,
                connect=True,
                manage_channels=True,
                move_members=True,
                mute_members=True,
                deafen_members=True,
                send_messages=True,
                read_message_history=True,
            )
            try:
                await vc.set_permissions(member, overwrite=n_po)
                if old and old.id != member.id:
                    await vc.set_permissions(old, overwrite=normal)
            except discord.HTTPException as e:
                await interaction.response.send_message(f"Ошибка: {e}", ephemeral=True)
                return
            sess.owner_id = member.id
            await interaction.response.send_message(
                f"Владелец: {member.mention}.", ephemeral=True
            )
            self.stop()
            return

        if self._mode == "friends":
            try:
                await vc.set_permissions(
                    member,
                    overwrite=discord.PermissionOverwrite(
                        view_channel=True,
                        connect=True,
                        send_messages=True,
                        read_message_history=True,
                    ),
                )
            except discord.HTTPException as e:
                await interaction.response.send_message(f"Ошибка: {e}", ephemeral=True)
                return
            await interaction.response.send_message(
                f"Доступ для {member.mention} добавлен.", ephemeral=True
            )
            self.stop()
            return

        if self._mode == "bans":
            try:
                await vc.set_permissions(
                    member,
                    overwrite=discord.PermissionOverwrite(
                        connect=False, view_channel=True
                    ),
                )
            except discord.HTTPException as e:
                await interaction.response.send_message(f"Ошибка: {e}", ephemeral=True)
                return
            if member.voice and member.voice.channel == vc:
                try:
                    await member.move_to(None)
                except discord.HTTPException:
                    pass
            await interaction.response.send_message(
                f"{member.mention} не сможет зайти (пока не снимешь бан в настройках канала).",
                ephemeral=True,
            )
            self.stop()


class TempVcPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Название",
        style=discord.ButtonStyle.secondary,
        custom_id="tempvc_name",
        row=0,
        emoji="🔤",
    )
    async def btn_name(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        await interaction.response.send_modal(TempVcRenameModal(sess.voice_channel_id))

    @discord.ui.button(
        label="Лимит",
        style=discord.ButtonStyle.secondary,
        custom_id="tempvc_limit",
        row=0,
        emoji="👥",
    )
    async def btn_limit(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        await interaction.response.send_modal(TempVcLimitModal(sess.voice_channel_id))

    @discord.ui.button(
        label="Регион",
        style=discord.ButtonStyle.secondary,
        custom_id="tempvc_region",
        row=0,
        emoji="🌐",
    )
    async def btn_region(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        await interaction.response.send_modal(TempVcRegionModal(sess.voice_channel_id))

    @discord.ui.button(
        label="Кикнуть",
        style=discord.ButtonStyle.secondary,
        custom_id="tempvc_kick",
        row=1,
        emoji="📞",
    )
    async def btn_kick(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Выбери участника в списке ниже.",
            view=TempVcUserPickView(sess.voice_channel_id, "kick"),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Гайд",
        style=discord.ButtonStyle.primary,
        custom_id="tempvc_guide",
        row=1,
        emoji="ℹ️",
    )
    async def btn_guide(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None:
            await interaction.response.send_message("Панель не активна.", ephemeral=True)
            return
        await interaction.response.send_message(
            "**Название / Лимит / Регион** — меняют твой войс.\n"
            "**Кикнуть** — выкинуть из **этого** войса (выбор из списка; человек должен сидеть у тебя).\n"
            "**Прихожая** — закрыть вход: зайти можешь только ты (и модерация сервера).\n"
            "**Забрать** — передать комнату другому (выбор из списка); ты остаёшься без прав владельца.\n"
            "**Друзья** / **Баны** — доступ или запрет по выбору из списка.\n\n"
            "Когда в комнате **никого нет** ~4 сек — **войс удаляется**.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="Прихожая",
        style=discord.ButtonStyle.secondary,
        custom_id="tempvc_wait",
        row=2,
        emoji="🕒",
    )
    async def btn_wait(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        if interaction.guild is None:
            return
        vc = interaction.guild.get_channel(sess.voice_channel_id)
        if not isinstance(vc, discord.VoiceChannel):
            await interaction.response.send_message("Войс не найден.", ephemeral=True)
            return
        me = interaction.guild.me
        owner = interaction.guild.get_member(sess.owner_id)
        sess.wait_room = not sess.wait_room
        try:
            if sess.wait_room:
                await vc.set_permissions(
                    interaction.guild.default_role,
                    overwrite=discord.PermissionOverwrite(connect=False, view_channel=True),
                )
                if owner:
                    await vc.set_permissions(
                        owner,
                        overwrite=discord.PermissionOverwrite(
                            view_channel=True,
                            connect=True,
                            manage_channels=True,
                            move_members=True,
                            mute_members=True,
                            deafen_members=True,
                            send_messages=True,
                            read_message_history=True,
                        ),
                    )
                if me:
                    await vc.set_permissions(
                        me,
                        overwrite=discord.PermissionOverwrite(
                            view_channel=True,
                            connect=True,
                            manage_channels=True,
                            send_messages=True,
                        ),
                    )
            else:
                await vc.set_permissions(
                    interaction.guild.default_role,
                    overwrite=discord.PermissionOverwrite(
                        view_channel=True, connect=True, send_messages=False
                    ),
                )
        except discord.HTTPException as e:
            sess.wait_room = not sess.wait_room
            await interaction.response.send_message(f"Ошибка: {e}", ephemeral=True)
            return
        st = "закрыт (только ты и модерация могут зайти)" if sess.wait_room else "открыт"
        await interaction.response.send_message(f"Вход **{st}**.", ephemeral=True)

    @discord.ui.button(
        label="Забрать",
        style=discord.ButtonStyle.secondary,
        custom_id="tempvc_claim",
        row=2,
        emoji="⭐",
    )
    async def btn_claim(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Выбери нового владельца в списке ниже.",
            view=TempVcUserPickView(sess.voice_channel_id, "claim"),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Друзья",
        style=discord.ButtonStyle.success,
        custom_id="tempvc_friends",
        row=3,
        emoji="👥",
    )
    async def btn_friends(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Выбери участника в списке ниже.",
            view=TempVcUserPickView(sess.voice_channel_id, "friends"),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Баны",
        style=discord.ButtonStyle.danger,
        custom_id="tempvc_bans",
        row=3,
        emoji="⚖️",
    )
    async def btn_bans(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        sess = temp_vc_session_from_channel(interaction)
        if sess is None or not temp_vc_can_control(interaction, sess):
            await interaction.response.send_message("Нет доступа.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Выбери участника в списке ниже.",
            view=TempVcUserPickView(sess.voice_channel_id, "bans"),
            ephemeral=True,
        )


async def create_temp_voice_session(member: discord.Member) -> None:
    if not TEMP_VC_HUB_CHANNEL_ID:
        return
    guild = member.guild
    hub = guild.get_channel(TEMP_VC_HUB_CHANNEL_ID)
    if not isinstance(hub, discord.VoiceChannel):
        return
    cat = guild.get_channel(TEMP_VC_CATEGORY_ID) if TEMP_VC_CATEGORY_ID else hub.category
    if not isinstance(cat, discord.CategoryChannel):
        print("temp_vc: нет категории")
        return
    base = _temp_vc_display_name(member)
    me = guild.me
    if me is None:
        return
    # Text-in-Voice: панель в чате голосового канала; «левым» не даём писать в TiV.
    v_over: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=True, connect=True, send_messages=False
        ),
        member: discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            manage_channels=True,
            move_members=True,
            mute_members=True,
            deafen_members=True,
            send_messages=True,
            read_message_history=True,
        ),
        me: discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            manage_channels=True,
            move_members=True,
            send_messages=True,
            embed_links=True,
        ),
    }
    try:
        vc = await cat.create_voice_channel(
            name=f"🔊 {base}"[:100],
            overwrites=v_over,
            reason=f"Временный войс для {member}",
        )
    except discord.HTTPException as e:
        print(f"temp_vc: create {e}")
        return
    sess = TempVoiceSession(
        guild_id=guild.id,
        voice_channel_id=vc.id,
        owner_id=member.id,
    )
    TEMP_VC_BY_VOICE[vc.id] = sess
    try:
        await member.move_to(vc, reason="Личная комната")
    except discord.HTTPException:
        await _temp_vc_delete_session(sess)
        return
    try:
        await vc.send(embed=build_temp_vc_panel_embed(), view=TempVcPanelView())
    except discord.HTTPException as e:
        print(f"temp_vc: panel {e}")


def user_can_moderate(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False
    if interaction.guild.owner_id == interaction.user.id:
        return True
    user = interaction.user
    if isinstance(user, discord.Member) and user.guild_permissions.manage_guild:
        return True
    if not MODERATION_ROLE_IDS or not isinstance(user, discord.Member):
        return False
    return any(role.id in MODERATION_ROLE_IDS for role in user.roles)


def user_can_post_podarok(interaction: discord.Interaction) -> bool:
    """Кто может вызвать /podarok. Пустой PODAROK_POST_ROLE_IDS → MODERATION_ROLE_IDS."""
    if interaction.guild is None:
        return False
    user = interaction.user
    if interaction.guild.owner_id == user.id:
        return True
    if isinstance(user, discord.Member) and user.guild_permissions.manage_guild:
        return True
    if not isinstance(user, discord.Member):
        return False
    pr = role_ids_or_moderation(PODAROK_POST_ROLE_IDS)
    if not pr:
        return False
    return any(role.id in pr for role in user.roles)


def sbormoney_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not SBORMONEY_CHANNEL_IDS:
        return False
    return interaction.channel_id in SBORMONEY_CHANNEL_IDS


def sbormoney_channel_restriction_message() -> str:
    if not SBORMONEY_CHANNEL_IDS:
        return (
            "**/sbormoney** недоступен: в **.env** задай **SBORMONEY_CHANNEL_IDS** "
            "(ID текстовых каналов или веток через запятую — только туда можно постить)."
        )
    parts = ", ".join(f"<#{cid}>" for cid in sorted(SBORMONEY_CHANNEL_IDS))
    return f"**/sbormoney** можно только здесь: {parts}."


def user_can_post_sbormoney(interaction: discord.Interaction) -> bool:
    """Кто может вызвать /sbormoney. Пустой SBORMONEY_POST_ROLE_IDS → MODERATION_ROLE_IDS."""
    if interaction.guild is None:
        return False
    user = interaction.user
    if interaction.guild.owner_id == user.id:
        return True
    if isinstance(user, discord.Member) and user.guild_permissions.manage_guild:
        return True
    if not isinstance(user, discord.Member):
        return False
    pr = role_ids_or_moderation(SBORMONEY_POST_ROLE_IDS)
    if not pr:
        return False
    return any(role.id in pr for role in user.roles)


def _sbormoney_image_filename(att: discord.Attachment) -> str:
    ct = (att.content_type or "").lower()
    if "png" in ct:
        return "sbormoney.png"
    if "jpeg" in ct or "jpg" in ct:
        return "sbormoney.jpg"
    if "gif" in ct:
        return "sbormoney.gif"
    if "webp" in ct:
        return "sbormoney.webp"
    return "sbormoney.png"


def format_money_dotted(n: int) -> str:
    """Формат 5.000.000 (разделитель тысяч — точка)."""
    if n < 0:
        return "-" + format_money_dotted(-n)
    parts: list[str] = []
    x = n
    while True:
        parts.append(str(x % 1000))
        x //= 1000
        if x == 0:
            break
    for i in range(len(parts) - 1):
        parts[i] = parts[i].zfill(3)
    return ".".join(reversed(parts))


def parse_money_amount(raw: str) -> int | None:
    """
    Разбор суммы: 5000000, 5.000.000, 5 млн, 5кк/kk, 2к/k, 200 тыс.
    кк / kk = миллион; к / k = тысяча.
    """
    if not raw or not str(raw).strip():
        return None
    s0 = str(raw).strip().lower()
    t_compact = s0.replace(",", ".").replace(" ", "")
    em = re.fullmatch(r"(\d{1,3}(?:\.\d{3})+)", t_compact)
    if em:
        return int(em.group(1).replace(".", ""))
    if re.fullmatch(r"[\d\s,]+", s0):
        digits = re.sub(r"\D", "", s0)
        if digits:
            return int(digits)
    s = re.sub(r"\s+", "", s0)
    s = s.replace(",", ".")
    s = re.sub(r"миллионов|миллиона|миллион", "млн", s)
    s = re.sub(r"тысяч|тысячи|тысяча", "тыс", s)
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(млн|кк|kk|тыс|к|k)?", s)
    if m:
        num = float(m.group(1))
        suf = m.group(2) or ""
        if suf in ("млн", "кк", "kk"):
            return int(round(num * 1_000_000))
        if suf in ("тыс", "к", "k"):
            return int(round(num * 1_000))
        return int(round(num))
    try:
        return int(round(float(s)))
    except ValueError:
        return None


def build_sbormoney_embed(state: SbormoneyState) -> discord.Embed:
    embed = discord.Embed(
        title="💰 Денежный сбор",
        color=discord.Color.dark_green(),
    )
    embed.add_field(
        name="На что сбор",
        value=(state.na_chto.strip()[:1024] if state.na_chto.strip() else "—"),
        inline=False,
    )
    summa_show = (state.summa_text.strip()[:1024] if state.summa_text.strip() else "—")
    embed.add_field(
        name="Итоговая сумма",
        value=summa_show,
        inline=False,
    )
    prog = (
        f"{format_money_dotted(state.collected)} / "
        f"{format_money_dotted(state.goal_amount)}"
    )
    embed.add_field(
        name="Собрано",
        value=prog,
        inline=False,
    )
    ts = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")
    embed.set_footer(text=f"{state.author_tag} · {ts}"[:2048])
    return embed


class SbormoneyModerateModal(discord.ui.Modal, title="Собранная сумма"):
    def __init__(self, message_id: int) -> None:
        super().__init__()
        self._message_id = message_id
        st = SBORMONEY_MESSAGES.get(message_id)
        default = format_money_dotted(st.collected) if st is not None else ""
        self.add_item(
            discord.ui.TextInput(
                label="Сколько собрали",
                style=discord.TextStyle.short,
                placeholder="Например: 2000, 2к, 1.5млн, 500к",
                required=True,
                max_length=40,
                default=default,
            )
        )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        st = SBORMONEY_MESSAGES.get(self._message_id)
        if st is None:
            await interaction.response.send_message(
                "Пост устарел — создай новый **/sbormoney**.", ephemeral=True
            )
            return
        if not user_can_post_sbormoney(interaction):
            await interaction.response.send_message(
                "Нет прав на модерацию: **MODERATION_ROLE_IDS** или "
                "**SBORMONEY_POST_ROLE_IDS** (если задан).",
                ephemeral=True,
            )
            return
        raw = str(self.children[0].value).strip()
        parsed = parse_money_amount(raw)
        if parsed is None or parsed < 0:
            await interaction.response.send_message(
                "Не удалось разобрать число. Примеры: `2000`, `2к`, `1.5млн`, `500.000`.",
                ephemeral=True,
            )
            return
        st.collected = parsed
        await interaction.response.defer(ephemeral=True)
        embed = build_sbormoney_embed(st)
        ch = bot.get_channel(st.channel_id)
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            await interaction.followup.send(
                "Канал недоступен — не удалось обновить пост.", ephemeral=True
            )
            return
        try:
            msg = await ch.fetch_message(self._message_id)
        except discord.NotFound:
            SBORMONEY_MESSAGES.pop(self._message_id, None)
            persist_podarok_sbormoney()
            await interaction.followup.send(
                "Сообщение удалено — состояние сбросилось.", ephemeral=True
            )
            return
        if not await sbormoney_try_edit_panel(
            msg, embed=embed, view=make_sbormoney_view()
        ):
            await interaction.followup.send(
                "Не удалось обновить пост (удалён?).", ephemeral=True
            )
            return
        await interaction.followup.send("Сумма «собрано» обновлена.", ephemeral=True)
        persist_podarok_sbormoney()


class SbormoneyView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Модерировать",
        style=discord.ButtonStyle.primary,
        custom_id="sbormoney_moderate",
        row=0,
    )
    async def btn_moderate(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        # Ответ на interaction должен уйти в течение ~3 с — сразу открываем модалку;
        # проверки прав и состояния — в on_submit у модалки.
        if interaction.message is None:
            await interaction.response.send_message(
                "Не удалось открыть форму.", ephemeral=True
            )
            return
        await interaction.response.send_modal(
            SbormoneyModerateModal(interaction.message.id)
        )


def make_sbormoney_view() -> discord.ui.View:
    return SbormoneyView()


async def sbormoney_try_edit_panel(
    message: discord.Message, *, embed: discord.Embed, view: discord.ui.View
) -> bool:
    try:
        await message.edit(embed=embed, view=view)
        return True
    except discord.NotFound:
        SBORMONEY_MESSAGES.pop(message.id, None)
        persist_podarok_sbormoney()
        return False


def user_can_podarok_draw(
    interaction: discord.Interaction, state: PodarokState
) -> bool:
    """Разыграть может только тот, кто создал этот розыгрыш."""
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    return state.creator_id == interaction.user.id


def user_can_war_timer(interaction: discord.Interaction) -> bool:
    """/timer_ata_def и кнопки. Пустой WAR_TIMER_ROLE_IDS → MODERATION_ROLE_IDS."""
    if interaction.guild is None:
        return False
    user = interaction.user
    if interaction.guild.owner_id == user.id:
        return True
    if isinstance(user, discord.Member) and user.guild_permissions.manage_guild:
        return True
    if not isinstance(user, discord.Member):
        return False
    wt = role_ids_or_moderation(WAR_TIMER_ROLE_IDS)
    if not wt:
        return False
    return any(role.id in wt for role in user.roles)


def user_can_stats_panel(interaction: discord.Interaction) -> bool:
    """/stats_panel и «+». Пустой STATS_ROLE_IDS → MODERATION_ROLE_IDS."""
    if interaction.guild is None:
        return False
    user = interaction.user
    if interaction.guild.owner_id == user.id:
        return True
    if isinstance(user, discord.Member) and user.guild_permissions.manage_guild:
        return True
    if not isinstance(user, discord.Member):
        return False
    st = role_ids_or_moderation(STATS_ROLE_IDS)
    if not st:
        return False
    return any(role.id in st for role in user.roles)


async def build_obzvon_channel_overwrites(
    guild: discord.Guild,
    applicant_id: int | None,
    moderator: discord.User,
) -> dict[discord.abc.Snowflake, discord.PermissionOverwrite] | None:
    """@everyone скрыт; видят: роли ADMIN_ROLE_IDS, заявитель, модератор нажавший «Обзвон», бот."""
    if not ADMIN_ROLE_IDS:
        return None

    def _participant_perms() -> discord.PermissionOverwrite:
        return discord.PermissionOverwrite(
            view_channel=True,
            read_messages=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        )

    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            read_messages=True,
            send_messages=True,
            read_message_history=True,
            manage_messages=True,
            manage_channels=True,
            attach_files=True,
            embed_links=True,
        ),
    }
    for role_id in ADMIN_ROLE_IDS:
        role = guild.get_role(role_id)
        if role is not None:
            overwrites[role] = _participant_perms()

    if isinstance(moderator, discord.Member) and moderator.guild.id == guild.id:
        overwrites[moderator] = _participant_perms()

    if applicant_id is not None:
        applicant = guild.get_member(applicant_id)
        if applicant is None:
            try:
                applicant = await guild.fetch_member(applicant_id)
            except discord.NotFound:
                applicant = None
        if applicant is not None:
            overwrites[applicant] = _participant_perms()

    return overwrites


def user_can_open_moderation(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False
    user = interaction.user
    if interaction.guild.owner_id == user.id:
        return True
    if isinstance(user, discord.Member) and user.guild_permissions.manage_guild:
        return True
    if not MODERATION_ROLE_IDS:
        return False
    if not isinstance(user, discord.Member):
        return False
    return any(role.id in MODERATION_ROLE_IDS for role in user.roles)


def user_can_use_panel(interaction: discord.Interaction) -> bool:
    """/panel, /karta_terry. Пустой PANEL_ROLE_IDS → MODERATION_ROLE_IDS."""
    if interaction.guild is None:
        return False
    user = interaction.user
    if interaction.guild.owner_id == user.id:
        return True
    if isinstance(user, discord.Member) and user.guild_permissions.manage_guild:
        return True
    if not isinstance(user, discord.Member):
        return False
    pr = role_ids_or_moderation(PANEL_ROLE_IDS)
    if not pr:
        return False
    return any(role.id in pr for role in user.roles)


def panel_channel_restriction_message() -> str:
    if not PANEL_CHANNEL_IDS:
        return ""
    if len(PANEL_CHANNEL_IDS) == 1:
        cid = next(iter(PANEL_CHANNEL_IDS))
        return f"**Панель** можно отправить только в <#{cid}>."
    parts = ", ".join(f"<#{cid}>" for cid in sorted(PANEL_CHANNEL_IDS))
    return f"**Панель** можно отправить только в каналах: {parts}."


def panel_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not PANEL_CHANNEL_IDS:
        return True
    cid = interaction.channel_id
    return cid is not None and cid in PANEL_CHANNEL_IDS


def terra_map_channel_restriction_message() -> str:
    if not TERRA_MAP_CHANNEL_IDS:
        return ""
    if len(TERRA_MAP_CHANNEL_IDS) == 1:
        cid = next(iter(TERRA_MAP_CHANNEL_IDS))
        return f"**VZP** только в <#{cid}>."
    parts = ", ".join(f"<#{cid}>" for cid in sorted(TERRA_MAP_CHANNEL_IDS))
    return f"**VZP** только в каналах: {parts}."


def terra_map_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not TERRA_MAP_CHANNEL_IDS:
        return True
    cid = interaction.channel_id
    return cid is not None and cid in TERRA_MAP_CHANNEL_IDS


def war_timer_channel_restriction_message() -> str:
    if not WAR_TIMER_CHANNEL_IDS:
        return ""
    if len(WAR_TIMER_CHANNEL_IDS) == 1:
        cid = next(iter(WAR_TIMER_CHANNEL_IDS))
        return f"**Таймер атаки/дефа** можно отправить только в <#{cid}>."
    parts = ", ".join(f"<#{cid}>" for cid in sorted(WAR_TIMER_CHANNEL_IDS))
    return f"**Таймер атаки/дефа** только в каналах: {parts}."


def war_timer_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not WAR_TIMER_CHANNEL_IDS:
        return True
    cid = interaction.channel_id
    return cid is not None and cid in WAR_TIMER_CHANNEL_IDS


def stats_channel_restriction_message() -> str:
    if not STATS_CHANNEL_IDS:
        return ""
    if len(STATS_CHANNEL_IDS) == 1:
        cid = next(iter(STATS_CHANNEL_IDS))
        return f"**Статистика** можно отправить только в <#{cid}>."
    parts = ", ".join(f"<#{cid}>" for cid in sorted(STATS_CHANNEL_IDS))
    return f"**Статистика** только в каналах: {parts}."


def stats_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not STATS_CHANNEL_IDS:
        return True
    cid = interaction.channel_id
    return cid is not None and cid in STATS_CHANNEL_IDS


def material_report_channel_restriction_message() -> str:
    if not MATERIAL_REPORT_CHANNEL_IDS:
        return ""
    if len(MATERIAL_REPORT_CHANNEL_IDS) == 1:
        cid = next(iter(MATERIAL_REPORT_CHANNEL_IDS))
        return f"**Отчёт по ВЗХ** только в <#{cid}>."
    parts = ", ".join(f"<#{cid}>" for cid in sorted(MATERIAL_REPORT_CHANNEL_IDS))
    return f"**Отчёт по ВЗХ** только в каналах: {parts}."


def material_report_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not MATERIAL_REPORT_CHANNEL_IDS:
        return True
    cid = interaction.channel_id
    return cid is not None and cid in MATERIAL_REPORT_CHANNEL_IDS


def activity_report_channel_restriction_message() -> str:
    if not ACTIVITY_REPORT_CHANNEL_IDS:
        return ""
    if len(ACTIVITY_REPORT_CHANNEL_IDS) == 1:
        cid = next(iter(ACTIVITY_REPORT_CHANNEL_IDS))
        return f"**Отчёт по МП** только в <#{cid}>."
    parts = ", ".join(f"<#{cid}>" for cid in sorted(ACTIVITY_REPORT_CHANNEL_IDS))
    return f"**Отчёт по МП** только в каналах: {parts}."


def activity_report_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not ACTIVITY_REPORT_CHANNEL_IDS:
        return True
    cid = interaction.channel_id
    return cid is not None and cid in ACTIVITY_REPORT_CHANNEL_IDS


def user_can_material_report_panel(interaction: discord.Interaction) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    if interaction.guild.owner_id == interaction.user.id:
        return True
    if interaction.user.guild_permissions.manage_guild:
        return True
    pr = role_ids_or_moderation(MATERIAL_REPORT_PANEL_ROLE_IDS)
    if not pr:
        return False
    return any(role.id in pr for role in interaction.user.roles)


def user_can_activity_report_panel(interaction: discord.Interaction) -> bool:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    if interaction.guild.owner_id == interaction.user.id:
        return True
    if interaction.user.guild_permissions.manage_guild:
        return True
    pr = role_ids_or_moderation(ACTIVITY_REPORT_PANEL_ROLE_IDS)
    if not pr:
        return False
    return any(role.id in pr for role in interaction.user.roles)


def _status_request_ping_mentions(
    role_ids: set[int], user: discord.abc.User
) -> tuple[str | None, discord.AllowedMentions]:
    """Строка с <@&…> и AllowedMentions для отправки заявки инактив/AFK."""
    line = (
        " ".join(f"<@&{rid}>" for rid in sorted(role_ids)) + "\n"
        if role_ids
        else None
    )
    am = discord.AllowedMentions(
        roles=[discord.Object(id=rid) for rid in sorted(role_ids)],
        users=[user],
    )
    return line, am


def inactiv_channel_restriction_message() -> str:
    if not INACTIV_CHANNEL_IDS:
        return ""
    if len(INACTIV_CHANNEL_IDS) == 1:
        cid = next(iter(INACTIV_CHANNEL_IDS))
        return f"**Инактив** можно отправить только в <#{cid}>."
    parts = ", ".join(f"<#{cid}>" for cid in sorted(INACTIV_CHANNEL_IDS))
    return f"**Инактив** только в каналах: {parts}."


def inactiv_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not INACTIV_CHANNEL_IDS:
        return True
    cid = interaction.channel_id
    return cid is not None and cid in INACTIV_CHANNEL_IDS


def afk_channel_restriction_message() -> str:
    if not AFK_CHANNEL_IDS:
        return ""
    if len(AFK_CHANNEL_IDS) == 1:
        cid = next(iter(AFK_CHANNEL_IDS))
        return f"**AFK** можно отправить только в <#{cid}>."
    parts = ", ".join(f"<#{cid}>" for cid in sorted(AFK_CHANNEL_IDS))
    return f"**AFK** только в каналах: {parts}."


def afk_allowed_in_channel(interaction: discord.Interaction) -> bool:
    if not AFK_CHANNEL_IDS:
        return True
    cid = interaction.channel_id
    return cid is not None and cid in AFK_CHANNEL_IDS


def user_can_post_inactiv_panel(interaction: discord.Interaction) -> bool:
    """Кто может вызвать /inactiv и опубликовать панель. Пустой INACTIV_PANEL_ROLE_IDS → MODERATION_ROLE_IDS."""
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    if interaction.guild.owner_id == interaction.user.id:
        return True
    if interaction.user.guild_permissions.manage_guild:
        return True
    pr = role_ids_or_moderation(INACTIV_PANEL_ROLE_IDS)
    if not pr:
        return False
    return any(role.id in pr for role in interaction.user.roles)


def user_can_post_afk_panel(interaction: discord.Interaction) -> bool:
    """Кто может вызвать /afk и опубликовать панель. Пустой AFK_PANEL_ROLE_IDS → MODERATION_ROLE_IDS."""
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        return False
    if interaction.guild.owner_id == interaction.user.id:
        return True
    if interaction.user.guild_permissions.manage_guild:
        return True
    pr = role_ids_or_moderation(AFK_PANEL_ROLE_IDS)
    if not pr:
        return False
    return any(role.id in pr for role in interaction.user.roles)


async def mod_action_log_send(
    embed: discord.Embed, *, channel_id: Optional[int] = None
) -> None:
    cid = channel_id if channel_id is not None else MOD_ACTION_LOG_CHANNEL_ID
    if not cid:
        return
    ch = bot.get_channel(cid)
    if not isinstance(ch, discord.TextChannel):
        return
    try:
        await ch.send(embed=embed)
    except discord.HTTPException:
        pass


def _today_date_str() -> str:
    """Сегодня по UTC: дд.мм.гггг."""
    return datetime.now(timezone.utc).strftime("%d.%m.%Y")


def _rejection_reason_embed_value(reason: str, *, max_inner: int = 988) -> str:
    """Текст причины в markdown code-block для эмбеда (заметнее обычного текста)."""
    s = str(reason).replace("```", "'''").strip()
    if len(s) > max_inner:
        s = s[: max_inner - 1] + "…"
    return f"```{s}```"


def _mod_log_footer_text(user_id: int) -> str:
    return f"{user_id} · {_today_date_str()}"


def _mod_log_embed_base(
    *, title_suffix: str, user_id: int, description: Optional[str] = None
) -> discord.Embed:
    """Тёмная тема, короткий заголовок, дата в футере."""
    embed = discord.Embed(
        title=f"📋 {title_suffix}",
        color=discord.Color.dark_theme(),
        description=description,
    )
    embed.set_footer(text=_mod_log_footer_text(user_id))
    return embed


def _mod_log_user_list_value(user: discord.abc.User) -> str:
    """Одна строка: упоминание, отображаемое имя, тег, ID."""
    return _mod_log_user_line(user)


def _mod_log_user_line(user: discord.abc.User) -> str:
    if isinstance(user, discord.Member):
        return f"{user.mention} · `{user.id}`"
    return f"{user.mention} · `{user.id}`"


def _mod_log_datetime_field() -> str:
    """Дата события (без времени)."""
    return _today_date_str()


def _mod_log_numbered_lines(items: list[str]) -> str:
    if not items:
        return "—"
    body = "\n".join(items)
    return body[:1024] if len(body) > 1024 else body


async def _audit_find_recent(
    guild: discord.Guild,
    action: discord.AuditLogAction,
    target_id: int,
    *,
    max_age_sec: float = 18.0,
) -> Optional[discord.AuditLogEntry]:
    """Журнал аудита приходит с задержкой — ждём и ищем запись по цели."""
    await asyncio.sleep(0.55)
    now = datetime.now(timezone.utc)
    async for entry in guild.audit_logs(limit=45, action=action):
        if entry.target is None or entry.target.id != target_id:
            continue
        if (now - entry.created_at).total_seconds() <= max_age_sec:
            return entry
    return None


def _audit_entry_covers_server_voice_mute_deaf(entry: discord.AuditLogEntry) -> bool:
    """member_update с изменением серверного mute/deaf (не ник/роли)."""
    if entry.action is not discord.AuditLogAction.member_update:
        return False
    bd, ad = entry.before.__dict__, entry.after.__dict__
    return "mute" in bd or "mute" in ad or "deaf" in bd or "deaf" in ad


async def _audit_find_recent_member_voice_server_flags(
    guild: discord.Guild,
    target_id: int,
    *,
    max_age_sec: float = 22.0,
) -> Optional[discord.AuditLogEntry]:
    """Кто-то другой сменил серверный мик/наушники у участника; без записи в аудите — не логируем."""
    await asyncio.sleep(0.55)
    now = datetime.now(timezone.utc)
    async for entry in guild.audit_logs(limit=80, action=discord.AuditLogAction.member_update):
        if entry.target is None or entry.target.id != target_id:
            continue
        if (now - entry.created_at).total_seconds() > max_age_sec:
            continue
        if not _audit_entry_covers_server_voice_mute_deaf(entry):
            continue
        if entry.user is None or entry.user.id == target_id:
            continue
        return entry
    return None


def _audit_entry_matches_role_delta(
    entry: discord.AuditLogEntry,
    added_ids: set[int],
    removed_ids: set[int],
) -> bool:
    """Сверка $add/$remove в записи member_role_update с фактической дельтой ролей."""
    if entry.action is not discord.AuditLogAction.member_role_update:
        return False
    if added_ids:
        ar = getattr(entry.after, "roles", None) or []
        audit_added = {getattr(r, "id", 0) for r in ar}
        if not audit_added & added_ids:
            return False
    if removed_ids:
        br = getattr(entry.before, "roles", None) or []
        audit_rem = {getattr(r, "id", 0) for r in br}
        if not audit_rem & removed_ids:
            return False
    return True


async def _audit_find_recent_member_role_update(
    guild: discord.Guild,
    target_id: int,
    added_ids: set[int],
    removed_ids: set[int],
    *,
    max_age_sec: float = 22.0,
) -> Optional[discord.AuditLogEntry]:
    """Кто-то другой выдал/снял роли; запись должна совпадать с дельтой по аудиту."""
    await asyncio.sleep(0.55)
    now = datetime.now(timezone.utc)
    async for entry in guild.audit_logs(
        limit=80, action=discord.AuditLogAction.member_role_update
    ):
        if entry.target is None or entry.target.id != target_id:
            continue
        if (now - entry.created_at).total_seconds() > max_age_sec:
            continue
        if entry.user is None or entry.user.id == target_id:
            continue
        if not _audit_entry_matches_role_delta(entry, added_ids, removed_ids):
            continue
        return entry
    return None


def _executor_field_plain(entry: Optional[discord.AuditLogEntry], target_id: int) -> str:
    if entry is None or entry.user is None:
        return "—"
    if entry.user.id == target_id:
        return "сам"
    return f"{entry.user.mention} · `{entry.user.id}`"


def _voice_channel_label(ch: discord.abc.GuildChannel | None) -> str:
    if ch is None:
        return "—"
    return f"{ch.mention} · `{ch.id}`"


def _mod_voice_toggle_ru(_before: bool, after: bool, *, kind: str) -> str:
    """Серверный мьют/деф; kind: микрофон / наушники."""
    if kind == "микрофон":
        return "мик **выкл**" if after else "мик **вкл**"
    return "науш **глух**" if after else "науш **звук**"


async def _mod_log_voice_move(
    member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
) -> None:
    guild = member.guild
    entry = await _audit_find_recent(
        guild, discord.AuditLogAction.member_move, member.id, max_age_sec=22.0
    )
    if entry is None or entry.user is None or entry.user.id == member.id:
        return

    embed = _mod_log_embed_base(
        title_suffix="Голос · перенос",
        user_id=member.id,
    )
    embed.add_field(name="**Кому:**", value=_mod_log_user_line(member), inline=False)
    embed.add_field(
        name="**Кто:**",
        value=_executor_field_plain(entry, member.id),
        inline=False,
    )
    embed.add_field(name="**Из:**", value=_voice_channel_label(before.channel), inline=False)
    embed.add_field(name="**В:**", value=_voice_channel_label(after.channel), inline=False)
    embed.add_field(name="**Дата:**", value=_mod_log_datetime_field(), inline=False)
    await mod_action_log_send(embed)


async def _mod_log_voice_flags(
    member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
) -> None:
    """Серверный мик/наушники только если в аудите есть смена mute/deaf и исполнитель — не сам участник."""
    server_changed = before.mute != after.mute or before.deaf != after.deaf
    if not server_changed:
        return

    guild = member.guild
    entry = await _audit_find_recent_member_voice_server_flags(
        guild, member.id, max_age_sec=22.0
    )
    if entry is None:
        return

    embed = _mod_log_embed_base(
        title_suffix="Голос · мик и наушники",
        user_id=member.id,
    )
    embed.add_field(name="**Кому:**", value=_mod_log_user_line(member), inline=False)
    embed.add_field(
        name="**Кто:**",
        value=_executor_field_plain(entry, member.id),
        inline=False,
    )
    if before.mute != after.mute:
        embed.add_field(
            name="**Микрофон**",
            value=_mod_voice_toggle_ru(before.mute, after.mute, kind="микрофон"),
            inline=False,
        )
    if before.deaf != after.deaf:
        embed.add_field(
            name="**Наушники**",
            value=_mod_voice_toggle_ru(before.deaf, after.deaf, kind="наушники"),
            inline=False,
        )
    embed.add_field(name="**Дата:**", value=_mod_log_datetime_field(), inline=False)
    await mod_action_log_send(embed)


async def _mod_log_voice_events(
    member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
) -> None:
    try:
        channel_changed = before.channel != after.channel
        server_flags_changed = before.mute != after.mute or before.deaf != after.deaf
        if channel_changed:
            await _mod_log_voice_move(member, before, after)
        if server_flags_changed:
            await _mod_log_voice_flags(member, before, after)
    except discord.HTTPException:
        pass


@bot.event
async def on_member_join(member: discord.Member) -> None:
    join_log_ch = MEMBER_JOIN_LOG_CHANNEL_ID
    if not join_log_ch:
        return
    created = member.created_at
    age = datetime.now(timezone.utc) - created
    embed = _mod_log_embed_base(
        title_suffix="Join",
        user_id=member.id,
    )
    embed.add_field(name="**Кто:**", value=_mod_log_user_line(member), inline=False)
    embed.add_field(name="**Дата:**", value=_mod_log_datetime_field(), inline=False)
    embed.add_field(
        name="**Акк. Discord:**",
        value=f"<t:{int(created.timestamp())}:D> · {age.days} дн.",
        inline=False,
    )
    if age.days < 7:
        embed.add_field(name="**⚠**", value="<7 дн.", inline=False)
    if member.bot:
        embed.add_field(name="**Тип**", value="бот", inline=False)
    await mod_action_log_send(embed, channel_id=join_log_ch)


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member) -> None:
    if not MOD_ACTION_LOG_CHANNEL_ID or before.bot:
        return
    guild = after.guild

    br = {r.id for r in before.roles}
    ar = {r.id for r in after.roles}
    br.discard(guild.id)
    ar.discard(guild.id)
    added_ids = ar - br
    removed_ids = br - ar

    if added_ids or removed_ids:
        entry = await _audit_find_recent_member_role_update(
            guild, after.id, added_ids, removed_ids, max_age_sec=22.0
        )
        if entry is None:
            return

        lines_add: list[str] = []
        lines_rem: list[str] = []
        for rid in sorted(added_ids):
            role = guild.get_role(rid)
            if role:
                lines_add.append(f"{role.mention} `{rid}`")
            else:
                lines_add.append(f"`{rid}`")
        for rid in sorted(removed_ids):
            role = guild.get_role(rid)
            if role:
                lines_rem.append(f"{role.mention} `{rid}`")
            else:
                lines_rem.append(f"`{rid}`")

        embed = _mod_log_embed_base(
            title_suffix="Роли · выдача / снятие",
            user_id=after.id,
        )
        embed.add_field(name="**Кому:**", value=_mod_log_user_line(after), inline=False)
        embed.add_field(
            name="**Кто:**",
            value=_executor_field_plain(entry, after.id),
            inline=False,
        )
        embed.add_field(
            name=f"**Выдано ({len(lines_add)})**",
            value=_mod_log_numbered_lines(lines_add),
            inline=False,
        )
        embed.add_field(
            name=f"**Снято ({len(lines_rem)})**",
            value=_mod_log_numbered_lines(lines_rem),
            inline=False,
        )
        embed.add_field(name="**Дата:**", value=_mod_log_datetime_field(), inline=False)
        await mod_action_log_send(embed)


def resolve_terra_map_banner_path() -> Optional[str]:
    """Файл баннера карты терры: абсолютный путь или имя рядом со скриптом / cwd."""
    name = (TERRA_MAP_BANNER_FILE or "").strip()
    if not name:
        return None
    if os.path.isabs(name):
        return name if os.path.isfile(name) else None
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for base in (script_dir, os.getcwd()):
        candidate = os.path.join(base, name)
        if os.path.isfile(candidate):
            return candidate
    return None


def resolve_karta_terry_banner_path() -> Optional[str]:
    """Сначала TERRA_MAP_BANNER_FILE; если не задан или файла нет — баннер /panel."""
    p = resolve_terra_map_banner_path()
    if p:
        return p
    return resolve_panel_image_path()


def resolve_terra_map_file(filename: str) -> Optional[str]:
    base = os.path.abspath(os.path.join(os.getcwd(), TERRA_MAP_IMAGE_DIR))
    safe = os.path.abspath(os.path.join(base, os.path.basename(filename)))
    try:
        if os.path.commonpath([base, safe]) != base:
            return None
    except ValueError:
        return None
    if not os.path.isfile(safe):
        return None
    return safe


def resolve_terra_map_files(filenames: list[str]) -> Optional[list[str]]:
    paths: list[str] = []
    for fn in filenames:
        p = resolve_terra_map_file(fn)
        if p is None:
            return None
        paths.append(p)
    return paths


def applications_channel_id_for(kind: str) -> int:
    if kind == APPLICATION_RP:
        return APPLICATIONS_RP_CHANNEL_ID
    if kind == APPLICATION_VZP:
        return APPLICATIONS_VZP_CHANNEL_ID
    return 0


def _load_ticket_counters() -> dict[str, int]:
    default = {"rp": -1, "vzp": 749}
    try:
        with open(_TICKET_COUNTERS_PATH, encoding="utf-8") as f:
            raw = json.load(f)
            out = default.copy()
            if isinstance(raw, dict):
                if "rp" in raw:
                    out["rp"] = int(raw["rp"])
                if "vzp" in raw:
                    out["vzp"] = int(raw["vzp"])
            return out
    except (OSError, ValueError, TypeError, KeyError):
        return default


def _save_ticket_counters(data: dict[str, int]) -> None:
    try:
        with open(_TICKET_COUNTERS_PATH, "w", encoding="utf-8") as f:
            json.dump({"rp": data["rp"], "vzp": data["vzp"]}, f, indent=2)
    except OSError:
        pass


def next_application_ticket_number(kind: str) -> int:
    """Первый номер РП — 0, первый VZP — 750."""
    d = _load_ticket_counters()
    if kind == APPLICATION_RP:
        d["rp"] = d.get("rp", -1) + 1
        n = d["rp"]
    else:
        d["vzp"] = d.get("vzp", 749) + 1
        n = d["vzp"]
    _save_ticket_counters(d)
    return n


def _application_field_label(text: str) -> str:
    return f"**{text.strip().upper()}**"


def _application_value_in_box(text: object, max_inner: int = 988) -> str:
    s = str(text).strip() if text is not None else ""
    if not s:
        return "```\n—\n```"
    s = s.replace("```", "'''")
    if len(s) > max_inner:
        s = s[: max_inner - 1] + "…"
    return f"```\n{s}\n```"


def _plain_text_from_embed_field_value(value: str) -> str:
    v = value.strip()
    if v.startswith("```"):
        inner = v[3:]
        if inner.endswith("```"):
            inner = inner[:-3]
        return inner.strip()
    if len(v) >= 2 and v.startswith("`") and v.endswith("`"):
        return v[1:-1].strip()
    return v


def _embed_field_plain_by_name_part(
    embed: discord.Embed, name_part: str
) -> str | None:
    """Значение поля заявки инактив/AFK по фрагменту заголовка (**ПЕРИОД**, **ВРЕМЯ**)."""
    needle = name_part.strip().upper()
    if not needle:
        return None
    for field in embed.fields:
        name_clean = re.sub(r"\*+", "", field.name or "").strip().upper()
        if needle in name_clean:
            return _plain_text_from_embed_field_value(field.value or "")
    return None


def _until_part_from_dash_range(raw: str) -> str | None:
    """'12.02.26-15.03.26' → '15.03.26'; '21:23-02:23' → '02:23'."""
    s = (raw or "").strip()
    if not s or s in ("—", "−", "-"):
        return None
    if "-" in s or "—" in s or "–" in s:
        normalized = s.replace("—", "-").replace("–", "-")
        parts = [p.strip() for p in normalized.split("-")]
        right = parts[-1]
        return right if right else None
    return s


async def _apply_server_nick_until_suffix(
    member: discord.Member,
    until_display: str,
    *,
    audit_label: str,
) -> str:
    """
    Дописывает к серверному нику « до …» (лимит 32 символа Discord).
    Возвращает короткую строку для ephemeral модератору.
    """
    until_display = " ".join(until_display.split())
    if not until_display:
        return ""
    base = member.nick if member.nick else member.name
    base = re.sub(r"\s+до\s+.+$", "", base, flags=re.DOTALL).strip()
    if not base:
        base = member.name
    tag = f" до {until_display}"
    if len(base) + len(tag) <= 32:
        new_nick = (base + tag)[:32]
    else:
        room = max(1, 32 - len(tag))
        new_nick = (base[:room] + tag)[:32]
    try:
        await member.edit(
            nick=new_nick,
            reason=f"{audit_label}: одобрено, {tag.strip()}"[:450],
        )
        return f" Ник: `{new_nick}`."
    except discord.Forbidden:
        return " Ник не изменён (нет прав / роль бота ниже / владелец сервера)."
    except discord.HTTPException:
        return " Ник: ошибка Discord."


def parse_ticket_number_from_embed(embed: discord.Embed) -> int | None:
    if embed.footer and embed.footer.text:
        m = re.search(r"Тикет\s*№\s*(\d+)", embed.footer.text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    title = embed.title or ""
    m = re.search(r"#\s*(\d+)", title)
    if m:
        return int(m.group(1))
    return None


def parse_user_id_from_embed(embed: discord.Embed) -> int | None:
    if not embed.footer or not embed.footer.text:
        return None
    footer_text = embed.footer.text.strip()
    match = re.search(r"User ID:\s*(\d+)", footer_text, re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def parse_user_id_from_status_request_embed(embed: discord.Embed) -> int | None:
    """Инактив/AFK: футер `User ID:` или упоминание в полях (у кнопок Discord иногда шлёт эмбед без футера)."""
    uid = parse_user_id_from_embed(embed)
    if uid is not None:
        return uid
    for field in embed.fields:
        val = field.value or ""
        m = re.search(r"<@!?(\d{17,20})>", val)
        if m:
            return int(m.group(1))
    return None


async def _fetch_message_for_status_review(
    interaction: discord.Interaction,
) -> discord.Message | None:
    """Полное сообщение с API — в interaction.message эмбед может быть урезан."""
    msg = interaction.message
    if msg is None:
        return None
    ch = interaction.channel
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return msg
    try:
        return await ch.fetch_message(msg.id)
    except discord.HTTPException:
        return msg


async def _status_request_post_decision_in_thread(
    msg: discord.Message,
    *,
    embed: discord.Embed,
    thread_title: str,
) -> tuple[bool, discord.Message]:
    """Эмбед решения в ветку к заявке (или новую ветку), иначе reply. Возвращает (успех, актуальное msg)."""
    try:
        msg = await msg.channel.fetch_message(msg.id)
    except discord.HTTPException:
        pass
    posted = False
    try:
        if msg.thread is not None:
            await msg.thread.send(embed=embed)
            posted = True
        else:
            tname = (thread_title)[:90] or "Решение"
            th = await msg.create_thread(
                name=tname,
                auto_archive_duration=10080,
            )
            await th.send(embed=embed)
            posted = True
    except discord.HTTPException:
        pass
    if not posted:
        try:
            await msg.reply(embed=embed)
            posted = True
        except discord.HTTPException:
            pass
    return posted, msg


def parse_type_from_embed(embed: discord.Embed) -> str:
    for field in embed.fields:
        if field.name.strip().lower() == "тип":
            v = field.value.strip().lower()
            if v in ("rp", "рп"):
                return "rp"
            if v in ("vzp", "взп"):
                return "vzp"
    title = (embed.title or "").lower()
    if "рп" in title:
        return "rp"
    if "vzp" in title:
        return "vzp"
    return "application"


def _application_combined_profile_field_label() -> str:
    return "Ваш никнейм, возраст, среднее время в игре"


def _format_profile_line_for_embed(nick: str, age: str, online: str) -> str:
    return f"{nick} · {age} · {online}"


def _parse_application_profile_line(text: str) -> Optional[tuple[str, str, str]]:
    """Ник, возраст, онлайн: через пробелы (первые два слова + остаток) или через |."""
    raw = text.strip()
    if not raw:
        return None
    if "|" in raw:
        parts = [p.strip() for p in raw.split("|")]
        parts = [p for p in parts if p]
        if len(parts) < 3:
            return None
        return parts[0], parts[1], " | ".join(parts[2:])
    segs = raw.split(None, 2)
    if len(segs) < 3:
        return None
    return segs[0], segs[1], segs[2]


def parse_nick_from_embed(embed: discord.Embed) -> str:
    for field in embed.fields:
        name_l = field.name.strip().lower().replace("*", "")
        val = _plain_text_from_embed_field_value(field.value)
        if "никнейм" in name_l:
            if "|" in val:
                raw = val.split("|", 1)[0].strip()
            else:
                parts = val.split(None, 2)
                raw = parts[0] if parts else val
        elif "ник в игре" in name_l:
            raw = val
        else:
            continue
        safe = "".join(ch for ch in raw.lower() if ch.isalnum() or ch in {"-", "_"})
        return safe[:40] or "user"
    return "user"


def copy_application_embed(embed: discord.Embed) -> discord.Embed:
    return discord.Embed.from_dict(embed.to_dict())


def is_application_enabled(application_type: str) -> bool:
    return bool(APPLICATIONS_STATE.get(application_type, True))


def moderation_status_lines() -> str:
    def state_icon(value: bool) -> str:
        return "✅ Включено" if value else "⛔ Выключено"

    return (
        f"**РП:** {state_icon(APPLICATIONS_STATE[APPLICATION_RP])}\n"
        f"**VZP:** {state_icon(APPLICATIONS_STATE[APPLICATION_VZP])}"
    )


def build_moderation_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Модерация заявок",
        description=(
            "Переключите прием заявок по типам.\n\n"
            f"{moderation_status_lines()}"
        ),
        color=discord.Color.orange(),
    )
    embed.set_footer(text=_today_date_str())
    return embed


def build_terra_map_ticket_embed(*, author_icon_url: str | None = None) -> discord.Embed:
    embed = discord.Embed(
        title="Карты VZP",
        description=(
            "> **Байкерка Большой миррор Веспуччи**\n"
            "> **Ветряки Киностудия Лесопилка Маленький миррор**\n"
            "> **Муравейник Мусорка Мясо Нефть Палетка**\n"
            "> **Порт бизвар Сендик Стройка Татушка**\n\n"
            "**Выбери карту:**"
        ),
        color=discord.Color.from_rgb(255, 255, 255),
    )
    embed.set_author(name="Sailor famq", icon_url=author_icon_url)
    return embed


def build_main_embed(*, author_icon_url: str | None = None) -> discord.Embed:
    embed = discord.Embed(
        title="Оформление заявки.",
        description=(
            "**Приглашение на собеседование приходит в личные сообщения**.\n\n"
            "> В среднем заявки обрабатываются в течение **2–3 рабочих дней**.\n\n"
            "**Пожалуйста, убедитесь, что ваш профиль доступен для связи, "
            "и следите за уведомлениями, чтобы не пропустить приглашение.**\n\n"
            "> Мы стараемся обработать все заявки максимально быстро и внимательно.\n\n"
            "**Подать заявку:**"
        ),
        color=discord.Color.from_rgb(255, 255, 255),
    )
    embed.set_author(name="Sailor famq", icon_url=author_icon_url)
    return embed


def _build_rp_application_embed(
    user: discord.abc.User,
    *,
    nick: str,
    age: str,
    families: str,
    sailor_rename: str,
    online_hours: str,
    source: str,
    clip: str,
    ticket_num: int,
) -> discord.Embed:
    embed = discord.Embed(
        title=f"Новая заявка: РП · #{ticket_num}",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name=_application_field_label("Пользователь"),
        value=user.mention,
        inline=False,
    )
    embed.add_field(
        name=_application_field_label(_application_combined_profile_field_label()),
        value=_application_value_in_box(
            _format_profile_line_for_embed(nick, age, online_hours)
        ),
        inline=False,
    )
    embed.add_field(
        name=_application_field_label("Список семей"),
        value=_application_value_in_box(families),
        inline=False,
    )
    embed.add_field(
        name=_application_field_label(
            "Готовы сменить фамилию на Sailor (обязательно)"
        ),
        value=_application_value_in_box(sailor_rename),
        inline=False,
    )
    embed.add_field(
        name=_application_field_label("Откуда узнали"),
        value=_application_value_in_box(source),
        inline=False,
    )
    embed.add_field(
        name=_application_field_label("Откат DM"),
        value=_application_value_in_box(clip),
        inline=False,
    )
    embed.set_footer(
        text=f"User ID: {user.id} · Тикет №{ticket_num} · {_today_date_str()}"
    )
    return embed


class RpApplicationModal(discord.ui.Modal, title="Заявка РП"):
    profile_line = discord.ui.TextInput(
        label="Ник | возраст | онлайн *",
        required=True,
        placeholder="Playername 23 4–6 часов в день",
        max_length=200,
    )
    families = discord.ui.TextInput(
        label="Список семей в которых были",
        required=True,
        placeholder="Пример: Killa / ...",
        max_length=200,
    )
    sailor_rename = discord.ui.TextInput(
        label="Смена фамилии на Sailor? (Обязательно)",
        required=True,
        placeholder="Пример: Да/Нет",
        max_length=80,
    )
    source = discord.ui.TextInput(
        label="Откуда узнали о семье Sailor",
        required=True,
        style=discord.TextStyle.paragraph,
        placeholder="Пример: От друга / Из рекламы",
        max_length=300,
    )
    clip = discord.ui.TextInput(
        label="Откат стрельбы DM 10.500 урона",
        required=True,
        style=discord.TextStyle.paragraph,
        placeholder="Ссылка на YouTube",
        max_length=300,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not is_application_enabled(APPLICATION_RP):
            await interaction.response.send_message(
                "Сейчас заявки **РП** не принимаются.",
                ephemeral=True,
            )
            return

        parsed = _parse_application_profile_line(str(self.profile_line))
        if parsed is None:
            await interaction.response.send_message(
                "В первой строке: **ник**, **возраст**, **среднее время** — "
                "**через пробел** (ник одним словом; дальше всё до конца — онлайн).\n"
                "Пример: `Playername 23 4–6 часов в день`\n"
                "Можно и через `|`, если ник из нескольких слов: `Имя Фам | 20 | 5 ч`",
                ephemeral=True,
            )
            return
        nick, age, online_hours = parsed

        ch_id = applications_channel_id_for(APPLICATION_RP)
        channel = interaction.client.get_channel(ch_id)
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message(
                "Канал заявок **РП** не настроен: укажи **APPLICATIONS_RP_CHANNEL_ID** в .env.",
                ephemeral=True,
            )
            return

        ticket_num = next_application_ticket_number(APPLICATION_RP)
        mention = f"<@&{MOD_ROLE_ID}>\n" if MOD_ROLE_ID else ""
        embed = _build_rp_application_embed(
            interaction.user,
            nick=nick,
            age=age,
            families=str(self.families),
            sailor_rename=str(self.sailor_rename),
            online_hours=online_hours,
            source=str(self.source),
            clip=str(self.clip),
            ticket_num=ticket_num,
        )

        await channel.send(content=mention, embed=embed, view=ApplicationReviewView())
        await interaction.response.send_message(
            "Ваша заявка **РП** отправлена. Ожидайте ответа в ЛС.",
            ephemeral=True,
        )


class VzpModal(discord.ui.Modal, title="Форма заявки VZP"):
    profile_line = discord.ui.TextInput(
        label="Ник | возраст | онлайн",
        required=True,
        placeholder="Playername 23 4–6 часов в день",
        max_length=200,
    )
    families = discord.ui.TextInput(
        label="В каких семьях были",
        required=True,
        placeholder="Пример: Killa, Ballas / никаких",
        max_length=200,
    )
    gta_hours = discord.ui.TextInput(
        label="Сколько часов в GTA",
        required=True,
        placeholder="Пример: 1200 ч / 500+",
        max_length=80,
    )
    experience = discord.ui.TextInput(
        label="Опыт игры",
        required=True,
        placeholder="Пример: 2 года",
        max_length=120,
    )
    about = discord.ui.TextInput(
        label="Откат с ВЗП",
        required=True,
        style=discord.TextStyle.paragraph,
        placeholder="Ссылка на YouTube",
        max_length=400,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not is_application_enabled(APPLICATION_VZP):
            await interaction.response.send_message(
                "Сейчас заявки **VZP** не принимаются.",
                ephemeral=True,
            )
            return

        parsed = _parse_application_profile_line(str(self.profile_line))
        if parsed is None:
            await interaction.response.send_message(
                "В первой строке: **ник**, **возраст**, **онлайн** — **через пробел** "
                "(ник одним словом).\n"
                "Пример: `Playername 23 4–6 часов в день`\n"
                "Или через `|`: `Имя Фам | 20 | 5 ч`",
                ephemeral=True,
            )
            return
        nick, age, online = parsed

        ch_id = applications_channel_id_for(APPLICATION_VZP)
        channel = interaction.client.get_channel(ch_id)
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message(
                "Канал заявок **VZP** не настроен: укажи **APPLICATIONS_VZP_CHANNEL_ID** в .env.",
                ephemeral=True,
            )
            return

        ticket_num = next_application_ticket_number(APPLICATION_VZP)
        mention = f"<@&{MOD_ROLE_ID}>\n" if MOD_ROLE_ID else ""
        embed = discord.Embed(
            title=f"Новая заявка: VZP · #{ticket_num}",
            color=discord.Color.green(),
        )
        embed.add_field(
            name=_application_field_label("Пользователь"),
            value=interaction.user.mention,
            inline=False,
        )
        embed.add_field(
            name=_application_field_label(_application_combined_profile_field_label()),
            value=_application_value_in_box(
                _format_profile_line_for_embed(nick, age, online)
            ),
            inline=False,
        )
        embed.add_field(
            name=_application_field_label("В каких семьях были"),
            value=_application_value_in_box(self.families),
            inline=False,
        )
        embed.add_field(
            name=_application_field_label("Сколько часов в GTA"),
            value=_application_value_in_box(self.gta_hours),
            inline=True,
        )
        embed.add_field(
            name=_application_field_label("Опыт игры"),
            value=_application_value_in_box(self.experience),
            inline=True,
        )
        embed.add_field(
            name=_application_field_label("Откат с ВЗП"),
            value=_application_value_in_box(self.about),
            inline=False,
        )
        embed.set_footer(
            text=f"User ID: {interaction.user.id} · Тикет №{ticket_num} · {_today_date_str()}"
        )

        await channel.send(content=mention, embed=embed, view=ApplicationReviewView())
        await interaction.response.send_message(
            "Ваша заявка **VZP** отправлена. Ожидайте ответа в ЛС.",
            ephemeral=True,
        )


class TicketSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [
            discord.SelectOption(
                label="Подать Заявку РП",
                description="Нажмите, чтобы заполнить анкету RP",
                emoji="📝",
                value="rp",
            ),
            discord.SelectOption(
                label="Подать Заявку VZP",
                description="Нажмите, чтобы заполнить анкету VZP",
                emoji="📋",
                value="vzp",
            ),
            discord.SelectOption(
                label="Модерация",
                description="Управление приемом заявок",
                emoji="⚙️",
                value="manage",
            ),
        ]
        super().__init__(
            placeholder="Подать заявку — выберите тип",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="ticket_select_menu",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        selected = self.values[0]
        if selected == APPLICATION_RP:
            if not is_application_enabled(APPLICATION_RP):
                await interaction.response.send_message(
                    "Сейчас заявки **РП** не принимаются.",
                    ephemeral=True,
                )
                return
            await interaction.response.send_modal(RpApplicationModal())
            return
        if selected == APPLICATION_VZP:
            if not is_application_enabled(APPLICATION_VZP):
                await interaction.response.send_message(
                    "Сейчас заявки **VZP** не принимаются.",
                    ephemeral=True,
                )
                return
            await interaction.response.send_modal(VzpModal())
            return

        if selected == "manage":
            if not user_can_open_moderation(interaction):
                await interaction.response.send_message(
                    "Раздел **Модерация** доступен только выбранным ролям.",
                    ephemeral=True,
                )
                return
            await interaction.response.send_message(
                embed=build_moderation_embed(),
                view=ModerationSettingsView(),
                ephemeral=True,
            )
            return


class OpenMenuView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        self.add_item(TicketSelect())


class TerraMapSelect(discord.ui.Select):
    def __init__(self) -> None:
        opts = [
            discord.SelectOption(
                label=label[:100],
                value=str(i),
                description=None,
            )
            for i, (label, _) in enumerate(TERRA_MAP_CHOICES)
        ]
        super().__init__(
            placeholder="Выбирай",
            min_values=1,
            max_values=1,
            options=opts,
            custom_id="terra_map_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not TERRA_MAP_CHOICES:
            await interaction.response.send_message(
                "Список карт не настроен.", ephemeral=True
            )
            return
        try:
            idx = int(self.values[0])
        except ValueError:
            await interaction.response.send_message("Ошибка выбора.", ephemeral=True)
            return
        if idx < 0 or idx >= len(TERRA_MAP_CHOICES):
            await interaction.response.send_message("Ошибка выбора.", ephemeral=True)
            return
        label, fnames = TERRA_MAP_CHOICES[idx]
        paths = resolve_terra_map_files(fnames)
        if paths is None:
            missing = ", ".join(f"`{f}`" for f in fnames)
            await interaction.response.send_message(
                f"Не найдены файлы в `{TERRA_MAP_IMAGE_DIR}`: {missing}",
                ephemeral=True,
            )
            return
        icon_url = (
            str(interaction.client.user.display_avatar.url)
            if interaction.client.user
            else None
        )
        panel_embed = build_terra_map_ticket_embed(author_icon_url=icon_url)
        await interaction.response.defer(ephemeral=True)
        discord_files = [
            discord.File(p, filename=os.path.basename(p)) for p in paths
        ]
        await interaction.followup.send(files=discord_files, ephemeral=True)
        msg = interaction.message
        if isinstance(msg, discord.Message):
            try:
                await msg.edit(embed=panel_embed, view=TerraMapView())
            except discord.HTTPException:
                pass


class TerraMapView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        self.add_item(TerraMapSelect())


class ModerationSettingsView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=300)

    @discord.ui.button(label="РП", style=discord.ButtonStyle.secondary, emoji="📝")
    async def toggle_rp(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return
        APPLICATIONS_STATE[APPLICATION_RP] = not APPLICATIONS_STATE[APPLICATION_RP]
        persist_panel_extra_state()
        await interaction.response.edit_message(
            embed=build_moderation_embed(), view=ModerationSettingsView()
        )

    @discord.ui.button(label="VZP", style=discord.ButtonStyle.secondary, emoji="📋")
    async def toggle_vzp(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return
        APPLICATIONS_STATE[APPLICATION_VZP] = not APPLICATIONS_STATE[APPLICATION_VZP]
        persist_panel_extra_state()
        await interaction.response.edit_message(
            embed=build_moderation_embed(), view=ModerationSettingsView()
        )


class RejectReasonModal(discord.ui.Modal, title="Причина отказа"):
    reason = discord.ui.TextInput(
        label="Укажите причину отказа",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=400,
        placeholder="Например: недостаточно информации в анкете",
    )

    def __init__(self, application_message: discord.Message):
        super().__init__()
        self.application_message = application_message

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return

        if not self.application_message.embeds:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return

        embed = self.application_message.embeds[0]
        applicant_id = parse_user_id_from_embed(embed)
        if applicant_id is None:
            await interaction.response.send_message(
                "Не удалось определить пользователя заявки.", ephemeral=True
            )
            return

        applicant = interaction.client.get_user(applicant_id)
        if applicant is None:
            try:
                applicant = await interaction.client.fetch_user(applicant_id)
            except discord.NotFound:
                applicant = None

        status_embed = discord.Embed(
            title="❌ Заявка отклонена",
            color=discord.Color.red(),
        )
        status_embed.add_field(
            name="Модератор",
            value=interaction.user.mention,
            inline=False,
        )
        status_embed.add_field(
            name="Причина",
            value=_rejection_reason_embed_value(self.reason),
            inline=False,
        )
        status_embed.set_footer(text=_today_date_str())
        await self.application_message.reply(embed=status_embed)
        await self.application_message.edit(view=None)

        if applicant is not None:
            try:
                dm = discord.Embed(
                    title="❌ Заявка отклонена",
                    description="Модератор рассмотрел анкету. **Главное — блок ниже.**",
                    color=discord.Color.red(),
                )
                dm.add_field(
                    name="Причина отказа",
                    value=_rejection_reason_embed_value(self.reason),
                    inline=False,
                )
                dm.add_field(
                    name="Дальше",
                    value=(
                        "Повторная заявка — **через 1–3 дня**.\n"
                        "Исправь то, что указано в причине."
                    ),
                    inline=False,
                )
                dm.set_footer(text=_today_date_str())
                await applicant.send(embed=dm)
            except discord.Forbidden:
                pass

        await interaction.response.send_message("Отказ отправлен.", ephemeral=True)


class TicketChannelRejectModal(discord.ui.Modal, title="Причина отказа"):
    reason = discord.ui.TextInput(
        label="Укажите причину отказа",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=400,
        placeholder="Например: не подошли по требованиям",
    )

    def __init__(self, ticket_message: discord.Message):
        super().__init__()
        self.ticket_message = ticket_message

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return

        if not self.ticket_message.embeds:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return

        embed = self.ticket_message.embeds[0]
        applicant_id = parse_user_id_from_embed(embed)

        await interaction.response.send_message(
            "Отказ отправлен пользователю в ЛС.", ephemeral=True
        )

        if applicant_id is not None:
            try:
                applicant = interaction.client.get_user(applicant_id)
                if applicant is None:
                    applicant = await interaction.client.fetch_user(applicant_id)
                dm = discord.Embed(
                    title="❌ Отказ после обзвона",
                    description="Решение по тикету. **Причина — в отдельном блоке.**",
                    color=discord.Color.red(),
                )
                dm.add_field(
                    name="Причина отказа",
                    value=_rejection_reason_embed_value(self.reason),
                    inline=False,
                )
                dm.add_field(
                    name="Дальше",
                    value=(
                        "Повторная заявка — **через 1–3 дня**.\n"
                        "Исправь то, что указано в причине."
                    ),
                    inline=False,
                )
                dm.set_footer(text=_today_date_str())
                await applicant.send(embed=dm)
            except (discord.Forbidden, discord.NotFound):
                pass

        ch = interaction.channel
        if isinstance(ch, discord.TextChannel):
            try:
                await ch.delete(reason="Отказ после обзвона")
            except discord.Forbidden:
                await interaction.followup.send(
                    "Не удалось удалить канал (нет прав у бота).", ephemeral=True
                )


class TicketChannelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Принять",
        style=discord.ButtonStyle.success,
        emoji="✅",
        custom_id="ticket_channel_accept",
    )
    async def final_accept(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message(
                "Действие доступно только в канале обзвона.", ephemeral=True
            )
            return

        role_note = ""
        msg = interaction.message
        guild = interaction.guild
        if msg is not None and msg.embeds and guild is not None:
            embed = msg.embeds[0]
            app_type = parse_type_from_embed(embed)
            role_id = 0
            audit_reason = ""
            env_hint = ""
            if app_type == APPLICATION_RP:
                role_id = APPLICATION_RP_ACCEPT_ROLE_ID
                audit_reason = "РП: принято после обзвона"
                env_hint = "APPLICATION_RP_ACCEPT_ROLE_ID"
            elif app_type == APPLICATION_VZP:
                role_id = APPLICATION_VZP_ACCEPT_ROLE_ID
                audit_reason = "VZP: принято после обзвона"
                env_hint = "APPLICATION_VZP_ACCEPT_ROLE_ID"
            if role_id:
                uid = parse_user_id_from_embed(embed)
                if uid is not None:
                    role = guild.get_role(role_id)
                    member = guild.get_member(uid)
                    if member is None:
                        try:
                            member = await guild.fetch_member(uid)
                        except discord.NotFound:
                            member = None
                    if role is None:
                        role_note = f" Роль по {env_hint} не найдена на сервере."
                    elif member is None:
                        role_note = " Заявитель не на сервере — роль не выдана."
                    else:
                        try:
                            await member.add_roles(role, reason=audit_reason)
                            role_note = f" Роль {role.mention} выдана."
                        except discord.Forbidden:
                            role_note = (
                                " Не удалось выдать роль: проверьте права бота "
                                "и порядок ролей (роль бота выше выдаваемой)."
                            )
                        except discord.HTTPException:
                            role_note = " Ошибка Discord при выдаче роли."

        await interaction.response.send_message(
            "Готово." + role_note, ephemeral=True
        )
        try:
            await ch.delete(reason="Принято после обзвона")
        except discord.Forbidden:
            await interaction.followup.send(
                "Не удалось удалить канал (нет прав у бота).", ephemeral=True
            )

    @discord.ui.button(
        label="Отказать",
        style=discord.ButtonStyle.danger,
        emoji="❌",
        custom_id="ticket_channel_reject",
    )
    async def final_reject(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return

        if interaction.message is None:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return

        await interaction.response.send_modal(
            TicketChannelRejectModal(interaction.message)
        )


class ApplicationReviewView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Обзвон",
        style=discord.ButtonStyle.success,
        emoji="📞",
        custom_id="application_obzvon",
    )
    async def obzvon(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return

        message = interaction.message
        if message is None or not message.embeds:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return

        embed = message.embeds[0]
        app_type = parse_type_from_embed(embed)
        nick = parse_nick_from_embed(embed)
        ticket_no = parse_ticket_number_from_embed(embed)
        prefix = f"{ticket_no}-" if ticket_no is not None else ""
        channel_name = f"{prefix}{app_type}-{nick}"
        channel_name = channel_name[:95]

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Действие доступно только на сервере.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        applicant_id = parse_user_id_from_embed(embed)
        overwrites = await build_obzvon_channel_overwrites(
            guild, applicant_id, interaction.user
        )
        create_kw: dict = {"name": channel_name, "reason": "Обзвон"}
        if overwrites is not None:
            create_kw["overwrites"] = overwrites
        try:
            created_channel = await guild.create_text_channel(**create_kw)
        except discord.Forbidden:
            await interaction.followup.send(
                "Не удалось создать канал: у бота нужны «Управление каналами», "
                "и роль бота должна быть **выше** ролей из ADMIN_ROLE_IDS в списке ролей сервера.",
                ephemeral=True,
            )
            return
        applicant_mention = (
            f"<@{applicant_id}>" if applicant_id is not None else "—"
        )
        ticket_intro = f"**Анкета заявителя** {applicant_mention}"
        await created_channel.send(
            content=ticket_intro,
            embed=copy_application_embed(embed),
            view=TicketChannelView(),
        )

        status_embed = discord.Embed(
            title="🕐 На рассмотрении",
            description="Анкету взяли в работу — дальше общение в канале обзвона.",
            color=discord.Color.gold(),
        )
        status_embed.add_field(
            name="Модератор",
            value=interaction.user.mention,
            inline=False,
        )
        status_embed.add_field(
            name="Канал обзвона",
            value=created_channel.mention,
            inline=False,
        )
        status_embed.set_footer(text=_today_date_str())
        await message.reply(embed=status_embed)
        await message.edit(view=None)

        if applicant_id is not None:
            try:
                applicant = interaction.client.get_user(applicant_id)
                if applicant is None:
                    applicant = await interaction.client.fetch_user(applicant_id)
                dm = discord.Embed(
                    title="🕐 Тикет на рассмотрении",
                    description=(
                        "Заявку в **Sailor** приняли на рассмотрение. "
                        "Зайди в канал ниже — там продолжится общение."
                    ),
                    color=discord.Color.gold(),
                )
                dm.add_field(
                    name="Канал",
                    value=created_channel.mention,
                    inline=False,
                )
                dm.set_footer(text=_today_date_str())
                await applicant.send(embed=dm)
            except (discord.Forbidden, discord.NotFound):
                pass

        await interaction.followup.send("Канал для обзвона создан.", ephemeral=True)

    @discord.ui.button(
        label="Отказать",
        style=discord.ButtonStyle.danger,
        emoji="❌",
        custom_id="application_reject",
    )
    async def reject(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not user_can_moderate(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для этого действия.", ephemeral=True
            )
            return

        if interaction.message is None:
            await interaction.response.send_message(
                "Не удалось обработать заявку.", ephemeral=True
            )
            return

        await interaction.response.send_modal(RejectReasonModal(interaction.message))


@bot.event
async def on_ready() -> None:
    print(f"ready {bot.user.id}")
    warn: list[str] = []
    if not ADMIN_ROLE_IDS:
        warn.append("admin_roles")
    if not APPLICATIONS_RP_CHANNEL_ID:
        warn.append("rp_ch")
    if not APPLICATIONS_VZP_CHANNEL_ID:
        warn.append("vzp_ch")
    if warn:
        print("warn " + ",".join(warn))


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot or message.webhook_id or message.guild is None:
        return
    ch = message.channel
    if not isinstance(
        ch,
        (
            discord.TextChannel,
            discord.VoiceChannel,
            discord.StageChannel,
            discord.Thread,
        ),
    ):
        return
    if not ROLE_MENTION_DM_TARGET_ROLE_IDS:
        return
    if (
        not ROLE_MENTION_DM_CATEGORY_IDS
        and not ROLE_MENTION_DM_CHANNEL_IDS
    ):
        return
    if not role_mention_dm_watchlist_matches_channel(ch):
        return
    if not message.role_mentions:
        return
    target_roles = [
        r
        for r in message.role_mentions
        if r.id in ROLE_MENTION_DM_TARGET_ROLE_IDS
    ]
    if not target_roles:
        return
    if not isinstance(message.author, discord.Member):
        return
    if not user_can_trigger_role_mention_dm(message.author):
        return
    asyncio.create_task(dm_role_mention_channel_broadcast(message, target_roles))


@bot.event
async def on_voice_state_update(
    member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
) -> None:
    if not member.bot and TEMP_VC_HUB_CHANNEL_ID:
        try:
            if after.channel and after.channel.id == TEMP_VC_HUB_CHANNEL_ID:
                await create_temp_voice_session(member)
            if before.channel and isinstance(before.channel, discord.VoiceChannel):
                if before.channel.id in TEMP_VC_BY_VOICE:
                    asyncio.create_task(temp_vc_maybe_delete_empty(before.channel.id))
        except Exception:
            pass

    if MOD_ACTION_LOG_CHANNEL_ID and not member.bot:
        asyncio.create_task(_mod_log_voice_events(member, before, after))


def _load_persistent_panel_state_sync() -> None:
    load_podarok_sbormoney_state()
    load_panel_extra_state()
    autopark_load_panels_state()


async def slash_sync_background() -> None:
    """Slash sync после ready — не блокирует connect; кнопки начинают обрабатываться раньше."""
    await bot.wait_until_ready()
    if not SYNC_SLASH_ON_START:
        return
    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild)
            try:
                await bot.tree.sync(guild=guild)
                print(f"sync {GUILD_ID}")
                if bot.application_id:
                    await asyncio.sleep(1.25)
                    await bot.http.bulk_upsert_global_commands(bot.application_id, [])
            except discord.Forbidden:
                print("sync err guild")
                await bot.tree.sync()
        else:
            await bot.tree.sync()
    except Exception as exc:
        print(f"sync err: {exc!r}")


@bot.event
async def setup_hook() -> None:
    bot.add_view(OpenMenuView())
    if TERRA_MAP_CHOICES:
        bot.add_view(TerraMapView())
    bot.add_view(ApplicationReviewView())
    bot.add_view(TicketChannelView())
    bot.add_view(GatherSignView())
    bot.add_view(WarTimerView())
    bot.add_view(KaptikAddView())
    bot.add_view(MaterialReportAddView())
    bot.add_view(ActivityReportAddView())
    bot.add_view(InactivAddView())
    bot.add_view(InactivReviewView())
    bot.add_view(AfkAddView())
    bot.add_view(AfkReviewView())
    bot.add_view(KontraktPanelView())
    bot.add_view(KontraktContractView())
    bot.add_view(AutoparkView())
    bot.add_view(PodarokView())
    bot.add_view(SbormoneyView())
    bot.add_view(TempVcPanelView())
    await asyncio.to_thread(_load_persistent_panel_state_sync)
    bot.loop.create_task(autopark_bootstrap_after_load())
    bot.loop.create_task(gather_auto_close_loop())
    bot.loop.create_task(podarok_deadline_refresh_loop())
    bot.loop.create_task(autopark_expire_sweep_loop())
    bot.loop.create_task(daily_role_ping_loop())
    bot.loop.create_task(war_timer_refresh_loop())

    if not SYNC_SLASH_ON_START:
        print(
            "SYNC_SLASH_ON_START=0: пропуск sync slash-команд "
            "(после изменения /команд запусти с 1 или выполни sync вручную)."
        )
    else:
        bot.loop.create_task(slash_sync_background())


@bot.tree.command(
    name="sbor",
    description="Создать лист записи (сбор): ВЗП, ВЗХ, Поставка, МП",
)
@app_commands.rename(
    vid="тип",
    uchastnikov="участников",
    dop_slotov="доп-слотов",
    vremya="время",
)
@app_commands.describe(
    vid="Вид сбора",
    uchastnikov="Максимум человек в основе (1–40)",
    dop_slotov="Доп. слоты (0 — без допа, до 30)",
    vremya="Только число = минуты до сбора (время в МСК); иначе любой текст",
)
@app_commands.choices(
    vid=[
        app_commands.Choice(name="ВЗП", value="vzp"),
        app_commands.Choice(name="ВЗХ", value="vzh"),
        app_commands.Choice(name="Поставка", value="postavka"),
        app_commands.Choice(name="МП", value="mp"),
    ],
)
async def sbor(
    interaction: discord.Interaction,
    vid: str,
    uchastnikov: app_commands.Range[int, 1, 40],
    dop_slotov: app_commands.Range[int, 0, 30],
    vremya: str,
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Используйте в текстовом канале или ветке.", ephemeral=True
        )
        return

    if not user_can_post_gather(interaction, vid):
        _, need_ch = gather_roles_and_channel(vid)
        if need_ch != 0 and interaction.channel_id != need_ch:
            await interaction.response.send_message(
                f"Этот тип сбора можно создавать только в канале <#{need_ch}>.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            "Нет прав на этот тип сбора: **MODERATION_ROLE_IDS** или роли `*_GATHER_ROLE_IDS` для вида (если заданы).",
            ephemeral=True,
        )
        return

    title, scheduled_at = parse_gather_vremya(vremya)
    if not title and scheduled_at is None:
        await interaction.response.send_message(
            "Заполни поле **время** (хотя бы кратко).",
            ephemeral=True,
        )
        return

    creator = interaction.user
    creator_tag = creator.display_name
    if isinstance(creator, discord.Member) and creator.nick:
        creator_tag = f"{creator.nick}/{creator.display_name}"
    elif isinstance(creator, discord.Member):
        creator_tag = creator.display_name

    state = GatherState(
        kind_key=vid,
        title=title,
        max_main=uchastnikov,
        max_extra=dop_slotov,
        status_open=True,
        creator_id=creator.id,
        creator_tag=creator_tag[:80],
        closes_at=None,
        channel_id=interaction.channel.id,
        scheduled_at=scheduled_at,
    )
    embed = build_gather_embed(state, interaction.guild)
    view = GatherSignView()
    ping_rid = gather_ping_role_id(vid)
    ch = interaction.channel
    assert isinstance(ch, (discord.TextChannel, discord.Thread))
    await interaction.response.defer(ephemeral=True)
    if ping_rid:
        msg = await ch.send(
            content=f"<@&{ping_rid}>",
            embed=embed,
            view=view,
            allowed_mentions=discord.AllowedMentions(
                roles=[discord.Object(id=ping_rid)]
            ),
        )
    else:
        msg = await ch.send(embed=embed, view=view)
    GATHER_MESSAGES[msg.id] = state
    persist_panel_extra_state()
    if gather_ping_role_id(vid):
        asyncio.create_task(dm_gather_ping_role_notify(interaction.guild, msg, vid))
    await interaction.followup.send("Сбор отправлен в канал.", ephemeral=True)


@bot.tree.command(
    name="podarok",
    description="Розыгрыш: приз, лимит участников, число победителей, дедлайн; участвовать и разыграть",
)
async def podarok(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Используй в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not user_can_post_podarok(interaction):
        await interaction.response.send_message(
            "Нет прав на **/podarok**: **MODERATION_ROLE_IDS** или "
            "**PODAROK_POST_ROLE_IDS** (если задан).",
            ephemeral=True,
        )
        return
    await interaction.response.send_modal(PodarokCreateModal())


@bot.tree.command(
    name="sbormoney",
    description="Сбор денег: на что, фото и итоговая сумма — публикуется в канал",
)
@app_commands.rename(
    na_chto="на_что_сбор",
    foto="фото_на_что",
    summa="итоговая_сумма",
)
@app_commands.describe(
    na_chto="Например: Шапка",
    foto="Прикрепи изображение (что собираем)",
    summa="Цель в числах: 5 млн, 5кк, 5.000.000, 200к (кк=млн, к=тыс)",
)
async def sbormoney(
    interaction: discord.Interaction,
    na_chto: str,
    foto: discord.Attachment,
    summa: str,
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Используй в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not sbormoney_allowed_in_channel(interaction):
        await interaction.response.send_message(
            sbormoney_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_post_sbormoney(interaction):
        await interaction.response.send_message(
            "Нет прав на **/sbormoney**: **MODERATION_ROLE_IDS** или "
            "**SBORMONEY_POST_ROLE_IDS** (если задан).",
            ephemeral=True,
        )
        return
    ct = (foto.content_type or "").lower()
    if not ct.startswith("image/"):
        await interaction.response.send_message(
            "В параметр **фото_на_что** нужно прикрепить **изображение** (png, jpg, gif, webp).",
            ephemeral=True,
        )
        return
    if foto.size is not None and foto.size > SBORMONEY_MAX_ATTACHMENT_BYTES:
        await interaction.response.send_message(
            f"Файл слишком большой (лимит **{SBORMONEY_MAX_ATTACHMENT_BYTES // (1024 * 1024)}** МБ).",
            ephemeral=True,
        )
        return
    goal = parse_money_amount(summa)
    if goal is None or goal <= 0:
        await interaction.response.send_message(
            "Не удалось разобрать **итоговую сумму**. Примеры: `5000000`, `5.000.000`, "
            "`5 млн`, `5кк`, `200к` (кк = миллион, к = тысяча).",
            ephemeral=True,
        )
        return
    creator = interaction.user
    author_tag = creator.display_name
    if isinstance(creator, discord.Member) and creator.nick:
        author_tag = f"{creator.nick}/{creator.display_name}"
    elif isinstance(creator, discord.Member):
        author_tag = creator.display_name
    fn = _sbormoney_image_filename(foto)
    await interaction.response.defer(ephemeral=True)
    ch = interaction.channel
    assert isinstance(ch, (discord.TextChannel, discord.Thread))
    try:
        img_file = await foto.to_file(filename=fn, spoiler=False)
    except discord.HTTPException as exc:
        await interaction.followup.send(
            f"Не удалось загрузить файл с Discord: `{getattr(exc, 'code', '')}`.",
            ephemeral=True,
        )
        return
    st = SbormoneyState(
        na_chto=na_chto[:2000],
        summa_text=summa.strip()[:1024],
        goal_amount=goal,
        collected=0,
        author_tag=author_tag[:80],
        channel_id=ch.id,
    )
    embed = build_sbormoney_embed(st)
    embed.set_image(url=f"attachment://{fn}")
    try:
        msg = await ch.send(
            content="@everyone",
            embed=embed,
            file=img_file,
            view=make_sbormoney_view(),
            allowed_mentions=discord.AllowedMentions(everyone=True),
        )
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить сообщение в канал (права бота или размер). "
            f"Код: `{getattr(exc, 'code', '')}`.",
            ephemeral=True,
        )
        return
    SBORMONEY_MESSAGES[msg.id] = st
    persist_podarok_sbormoney()
    await interaction.followup.send("Пост со сбором отправлен в канал.", ephemeral=True)


@bot.tree.command(
    name="karta_terry",
    description="Плашка VZP в канал: текст и список карт (карты по выбору — только вызывающему)",
)
async def karta_terry(interaction: discord.Interaction) -> None:
    if not user_can_use_panel(interaction):
        await interaction.response.send_message(
            "Нет прав: роли из **MODERATION_ROLE_IDS** (или **PANEL_ROLE_IDS**, если задан).",
            ephemeral=True,
        )
        return
    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            "Только в текстовом канале.", ephemeral=True
        )
        return
    if not terra_map_allowed_in_channel(interaction):
        await interaction.response.send_message(
            terra_map_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not TERRA_MAP_CHOICES:
        await interaction.response.send_message(
            "В .env задай **TERRA_MAP_OPTIONS**: `Подпись:файл` или `Подпись:a.png+b.png` через `|`, "
            f"файлы в `{TERRA_MAP_IMAGE_DIR}`.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)

    icon_url = (
        str(interaction.client.user.display_avatar.url)
        if interaction.client.user
        else None
    )
    embed = build_terra_map_ticket_embed(author_icon_url=icon_url)
    banner_path = resolve_karta_terry_banner_path()
    image_file: discord.File | None = None
    size_skip_note: Optional[str] = None
    if banner_path:
        try:
            file_size = os.path.getsize(banner_path)
        except OSError:
            banner_path = None
        else:
            if file_size > PANEL_IMAGE_MAX_UPLOAD_BYTES:
                mb = max(1, file_size // (1024 * 1024))
                size_skip_note = (
                    f"Баннер не прикреплён: **~{mb} МБ** — лимит Discord **25 МБ**."
                )
            else:
                image_file = discord.File(
                    banner_path, filename=os.path.basename(banner_path)
                )

    send_kw: dict = {"embeds": [embed], "view": TerraMapView()}
    if image_file is not None:
        send_kw["file"] = image_file
    try:
        await interaction.channel.send(**send_kw)
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить в канал. "
            f"({getattr(exc, 'status', '')} / {getattr(exc, 'code', '')}). "
            "Проверь вложение и права **Прикреплять файлы**.",
            ephemeral=True,
        )
        return

    followup = "Плашка карт отправлена."
    if size_skip_note:
        followup = f"{followup}\n\n{size_skip_note}"
    elif banner_path is None and (
        (TERRA_MAP_BANNER_FILE or "").strip() or (PANEL_IMAGE_FILE or "").strip()
    ):
        followup = (
            f"{followup}\n\nБаннер не найден: искал "
            f"`{TERRA_MAP_BANNER_FILE or '(не задан)'}` и `{PANEL_IMAGE_FILE}` "
            "рядом с ботом или по полному пути."
        )
    await interaction.followup.send(followup, ephemeral=True)


@bot.tree.command(name="panel", description="Отправить панель заявок")
async def panel(interaction: discord.Interaction) -> None:
    if not user_can_use_panel(interaction):
        await interaction.response.send_message(
            "Нет прав на **/panel**: **MODERATION_ROLE_IDS** (+ владелец / manage_guild); "
            "опционально узкий список **PANEL_ROLE_IDS**.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            "Эту команду нужно запускать в текстовом канале.", ephemeral=True
        )
        return

    if not panel_allowed_in_channel(interaction):
        await interaction.response.send_message(
            panel_channel_restriction_message(),
            ephemeral=True,
        )
        return

    # Ответ на слэш-команду нужен ≤ ~3 с; загрузка большого баннера дольше — сначала defer.
    await interaction.response.defer(ephemeral=True)

    icon_url = str(interaction.client.user.display_avatar.url) if interaction.client.user else None
    panel_embed = build_main_embed(author_icon_url=icon_url)
    image_path = resolve_panel_image_path()
    image_file: discord.File | None = None
    size_skip_note: Optional[str] = None
    if image_path:
        try:
            file_size = os.path.getsize(image_path)
        except OSError:
            image_path = None
        else:
            if file_size > PANEL_IMAGE_MAX_UPLOAD_BYTES:
                mb = max(1, file_size // (1024 * 1024))
                size_skip_note = (
                    f"Баннер не прикреплён: **~{mb} МБ** — лимит Discord **25 МБ**. "
                    "Сожми gif или укажи другой **PANEL_IMAGE_FILE**."
                )
            else:
                image_file = discord.File(
                    image_path, filename=os.path.basename(image_path)
                )

    # Одно сообщение: гиф вложением сверху (set_image нельзя — в API картинка эмбеда всегда снизу под текстом).
    response_kwargs: dict = {"embeds": [panel_embed], "view": OpenMenuView()}
    if image_file is not None:
        response_kwargs["file"] = image_file

    try:
        await interaction.channel.send(**response_kwargs)
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить сообщение в канал. "
            f"({getattr(exc, 'status', '')} / код {getattr(exc, 'code', '')}). "
            "Часто: слишком тяжёлый файл, нет права **Прикреплять файлы** или **Отправлять сообщения**.",
            ephemeral=True,
        )
        return

    followup_text = "Панель отправлена."
    if size_skip_note:
        followup_text = f"{followup_text}\n\n{size_skip_note}"
    elif image_path is None and not size_skip_note and (PANEL_IMAGE_FILE or "").strip():
        followup_text = (
            f"{followup_text}\n\nБаннер не найден: искал `{PANEL_IMAGE_FILE}` "
            f"рядом с ботом (`{os.path.dirname(os.path.abspath(__file__))}`) и в `{os.getcwd()}`."
        )
    await interaction.followup.send(followup_text, ephemeral=True)


@bot.tree.command(name="moderation", description="Управление приемом заявок")
async def moderation(interaction: discord.Interaction) -> None:
    if not user_can_open_moderation(interaction):
        await interaction.response.send_message(
            "Команда доступна только модераторам.", ephemeral=True
        )
        return

    await interaction.response.send_message(
        embed=build_moderation_embed(),
        view=ModerationSettingsView(),
        ephemeral=True,
    )


@bot.tree.command(
    name="autopark",
    description="Панель автопарка: бронь на время из .env, ЛС при взятии и освобождении",
)
async def autopark(interaction: discord.Interaction) -> None:
    try:
        await interaction.response.defer(ephemeral=True)
    except discord.NotFound:
        return
    if interaction.guild is None:
        await interaction.followup.send(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.followup.send(
            "Только в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not autopark_allowed_in_channel(interaction):
        await interaction.followup.send(
            autopark_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_post_autopark(interaction):
        await interaction.followup.send(
            "Нет прав на **/autopark**: **MODERATION_ROLE_IDS** или **AUTOPARK_ROLE_IDS** (если задан).",
            ephemeral=True,
        )
        return
    state = AutoparkState(
        channel_id=interaction.channel.id,
        guild_id=interaction.guild.id,
    )
    embed = build_autopark_embed(state, interaction.guild)
    try:
        msg = await interaction.channel.send(embed=embed, view=AutoparkView())
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить панель. Проверь права бота в канале. "
            f"(код {getattr(exc, 'code', '')}).",
            ephemeral=True,
        )
        return
    AUTOPARK_MESSAGES[msg.id] = state
    autopark_save_panels_state()
    await interaction.followup.send("Панель автопарка отправлена в канал.", ephemeral=True)


@bot.tree.command(
    name="spam",
    description="Рассылка в ЛС: выбери роль и текст — бот напишет каждому участнику с этой ролью",
)
@app_commands.rename(role="роль", text="сообщение")
@app_commands.describe(
    role="Получатели — участники сервера с этой ролью",
    text="Текст личного сообщения от бота",
)
async def spam(
    interaction: discord.Interaction,
    role: discord.Role,
    text: str,
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not user_can_moderate(interaction):
        await interaction.response.send_message(
            "Недостаточно прав (роли `MODERATION_ROLE_IDS` / владелец / manage_guild).",
            ephemeral=True,
        )
        return
    guild = interaction.guild
    if role.guild.id != guild.id:
        await interaction.response.send_message(
            "Выбери роль с этого сервера.", ephemeral=True
        )
        return
    if role.is_default():
        await interaction.response.send_message(
            "Нельзя выбрать @everyone — укажи конкретную роль.",
            ephemeral=True,
        )
        return
    body = text.strip()
    if not body:
        await interaction.response.send_message(
            "Сообщение не может быть пустым.", ephemeral=True
        )
        return
    dm_prefix = "Вам сделали рассылку: "
    if len(dm_prefix) + len(body) > 2000:
        await interaction.response.send_message(
            f"Вместе с текстом «{dm_prefix.strip()}…» максимум 2000 символов в одном ЛС.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)

    if not guild.chunked:
        try:
            await guild.chunk(cache=True)
        except (discord.HTTPException, asyncio.TimeoutError):
            pass

    targets = [m for m in role.members if not m.bot]
    if not targets:
        await interaction.followup.send(
            f"Никого с ролью {role.mention} не найдено в кэше. "
            "Проверь, что в [Developer Portal](https://discord.com/developers/applications) "
            "у бота включён **Server Members Intent**.",
            ephemeral=True,
        )
        return

    ok = 0
    failed: list[str] = []
    dm_content = f"{dm_prefix}{body}"
    for member in targets:
        try:
            await member.send(dm_content)
            ok += 1
        except discord.HTTPException:
            failed.append(member.display_name[:48])
        await asyncio.sleep(0.35)

    lines = [f"Отправлено в ЛС: **{ok}** из {len(targets)}."]
    if failed:
        preview = ", ".join(failed[:20])
        if len(failed) > 20:
            preview += f" … (+{len(failed) - 20})"
        lines.append(f"Не доставлено (закрыты ЛС и т.п.): {preview}")
    await interaction.followup.send("\n".join(lines), ephemeral=True)


@bot.tree.command(
    name="timer_ata_def",
    description="Панель: таймеры атаки и дефа (кнопки +ч из .env, модерация — своя дата)",
)
async def timer_ata_def(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Только в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not war_timer_allowed_in_channel(interaction):
        await interaction.response.send_message(
            war_timer_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_war_timer(interaction):
        await interaction.response.send_message(
            "Нет прав на **/timer_ata_def**: **MODERATION_ROLE_IDS** или **WAR_TIMER_ROLE_IDS** (если задан).",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    state = WarTimerState(channel_id=interaction.channel.id)
    embed = build_war_timer_embed(state)
    ping_rid = WAR_TIMER_PING_ROLE_ID
    if ping_rid:
        msg = await interaction.channel.send(
            content=f"<@&{ping_rid}>",
            embed=embed,
            view=WarTimerView(),
            allowed_mentions=discord.AllowedMentions(
                roles=[discord.Object(id=ping_rid)]
            ),
        )
    else:
        msg = await interaction.channel.send(embed=embed, view=WarTimerView())
    WAR_TIMER_MESSAGES[msg.id] = state
    persist_panel_extra_state()
    await interaction.followup.send("Панель таймеров отправлена.", ephemeral=True)


@bot.tree.command(
    name="kontrakt",
    description="Панель контрактов: правила и кнопка «Предложить»",
)
async def kontrakt(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Только в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not kontrakt_allowed_in_channel(interaction):
        await interaction.response.send_message(
            kontrakt_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_post_kontrakt_panel(interaction):
        await interaction.response.send_message(
            "Нет прав на **/kontrakt**: **MODERATION_ROLE_IDS** или **KONTRAKT_POST_ROLE_IDS** (если задан).",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    embed = build_kontrakt_panel_embed()
    view = KontraktPanelView()
    try:
        await interaction.channel.send(embed=embed, view=view)
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить панель. Проверь права бота в канале. "
            f"(код {getattr(exc, 'code', '')}).",
            ephemeral=True,
        )
        return
    await interaction.followup.send("Панель контрактов отправлена в канал.", ephemeral=True)


@bot.tree.command(
    name="stats_panel",
    description="Панель «Добавить статистику»: кнопка + и форма записи матча",
)
async def stats_panel(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Только в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not stats_allowed_in_channel(interaction):
        await interaction.response.send_message(
            stats_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_stats_panel(interaction):
        await interaction.response.send_message(
            "Нет прав на **/stats_panel**: **MODERATION_ROLE_IDS** или **STATS_ROLE_IDS** (если задан).",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    kaptik_embed = build_kaptik_prompt_embed()
    ping_rid = STATS_PING_ROLE_ID
    if ping_rid:
        await interaction.channel.send(
            content=f"<@&{ping_rid}>",
            embed=kaptik_embed,
            view=KaptikAddView(),
            allowed_mentions=discord.AllowedMentions(
                roles=[discord.Object(id=ping_rid)]
            ),
        )
    else:
        await interaction.channel.send(
            embed=kaptik_embed,
            view=KaptikAddView(),
        )
    await interaction.followup.send("Панель «Добавить статистику» отправлена.", ephemeral=True)


@bot.tree.command(
    name="mpitog",
    description="Панель отчёта ВЗХ (дата, фракция, сводка) — сразу в канал",
)
async def mpitog(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Только в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not material_report_allowed_in_channel(interaction):
        await interaction.response.send_message(
            material_report_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_material_report_panel(interaction):
        await interaction.response.send_message(
            "Нет прав на **/mpitog**: **MODERATION_ROLE_IDS** или **MATERIAL_REPORT_PANEL_ROLE_IDS**.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    embed = build_material_report_prompt_embed()
    try:
        await interaction.channel.send(embed=embed, view=MaterialReportAddView())
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить панель. Проверь права бота в канале. "
            f"(код {getattr(exc, 'code', '')}).",
            ephemeral=True,
        )
        return
    await interaction.followup.send("Панель отчёта ВЗХ отправлена.", ephemeral=True)


@bot.tree.command(
    name="vzhitog",
    description="Панель отчёта МП (дата, тип, фракция, итог) — сразу в канал",
)
async def vzhitog(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Только в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not activity_report_allowed_in_channel(interaction):
        await interaction.response.send_message(
            activity_report_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_activity_report_panel(interaction):
        await interaction.response.send_message(
            "Нет прав на **/vzhitog**: **MODERATION_ROLE_IDS** или **ACTIVITY_REPORT_PANEL_ROLE_IDS**.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    embed = build_activity_report_prompt_embed()
    try:
        await interaction.channel.send(embed=embed, view=ActivityReportAddView())
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить панель. Проверь права бота в канале. "
            f"(код {getattr(exc, 'code', '')}).",
            ephemeral=True,
        )
        return
    await interaction.followup.send("Панель отчёта МП отправлена.", ephemeral=True)


@bot.tree.command(
    name="inactiv",
    description="Панель «Взять инактив»: кнопка + и форма (период дат и причина)",
)
async def inactiv(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Только в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not inactiv_allowed_in_channel(interaction):
        await interaction.response.send_message(
            inactiv_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_post_inactiv_panel(interaction):
        await interaction.response.send_message(
            "Нет прав на **/inactiv**: **MODERATION_ROLE_IDS** или **INACTIV_PANEL_ROLE_IDS** (если задан).",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    embed = build_inactiv_prompt_embed()
    try:
        await interaction.channel.send(embed=embed, view=InactivAddView())
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить панель. Проверь права бота в канале. "
            f"(код {getattr(exc, 'code', '')}).",
            ephemeral=True,
        )
        return
    await interaction.followup.send("Панель инактива отправлена в канал.", ephemeral=True)


@bot.tree.command(
    name="afk",
    description="Панель «Взять AFK»: кнопка + и форма (время и причина)",
)
async def afk(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда только на сервере.", ephemeral=True
        )
        return
    if not isinstance(
        interaction.channel, (discord.TextChannel, discord.Thread)
    ):
        await interaction.response.send_message(
            "Только в текстовом канале или ветке.", ephemeral=True
        )
        return
    if not afk_allowed_in_channel(interaction):
        await interaction.response.send_message(
            afk_channel_restriction_message(),
            ephemeral=True,
        )
        return
    if not user_can_post_afk_panel(interaction):
        await interaction.response.send_message(
            "Нет прав на **/afk**: **MODERATION_ROLE_IDS** или **AFK_PANEL_ROLE_IDS** (если задан).",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    embed = build_afk_prompt_embed()
    try:
        await interaction.channel.send(embed=embed, view=AfkAddView())
    except discord.HTTPException as exc:
        await interaction.followup.send(
            "Не удалось отправить панель. Проверь права бота в канале. "
            f"(код {getattr(exc, 'code', '')}).",
            ephemeral=True,
        )
        return
    await interaction.followup.send("Панель AFK отправлена в канал.", ephemeral=True)


bot.run(TOKEN)
