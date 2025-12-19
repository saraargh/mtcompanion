# =========================
# MapTap Companion Bot
# FULL FILE (PART 1/2)
# =========================

import os
import json
import re
import base64
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from threading import Thread
from typing import Any, Dict, Tuple, Optional, List

import discord
from discord.ext import tasks
from discord import app_commands
from flask import Flask

# =====================================================
# CONFIG
# =====================================================
UK_TZ = ZoneInfo("Europe/London")
UTC = ZoneInfo("UTC")

TOKEN = os.getenv("TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # e.g. "saraargh/the-pilot"

SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")
SETTINGS_PATH = os.getenv("MAPTAP_SETTINGS_PATH", "data/maptap_settings.json")

MAPTAP_URL = os.getenv("MAPTAP_URL", "https://www.maptap.gg")
CLEANUP_DAYS = int(os.getenv("MAPTAP_CLEANUP_DAYS", "69"))
MAX_SCORE = int(os.getenv("MAPTAP_MAX_SCORE", "1000"))

# Parse "Final score: 606"
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)

WEEKDAYS = [("Monday", 0), ("Tuesday", 1), ("Wednesday", 2), ("Thursday", 3),
            ("Friday", 4), ("Saturday", 5), ("Sunday", 6)]

def weekday_name(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        n = 6
    for name, num in WEEKDAYS:
        if num == n:
            return name
    return "Sunday"

def parse_hhmm(hhmm: str) -> Tuple[int, int]:
    dt = datetime.strptime(hhmm, "%H:%M")
    return dt.hour, dt.minute

def today_key(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(UK_TZ)
    return dt.date().isoformat()

def monday_key(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(UK_TZ)
    mon = dt.date() - timedelta(days=dt.weekday())
    return mon.isoformat()

def pretty_day(date_key: str) -> str:
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")

def monday_of_week(d: datetime) -> datetime.date:
    return d.date() - timedelta(days=d.weekday())

# =====================================================
# DEFAULT SETTINGS (GitHub-backed)
# =====================================================
DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,
    "admin_role_ids": [],

    "daily_post_enabled": True,
    "daily_scoreboard_enabled": True,
    "weekly_roundup_enabled": True,
    "rivalry_enabled": True,

    "emojis": {
        "recorded": "🌏",
        "too_high": "❌",
        "rescan_ingested": "🔁",
    },

    # Schedule values are strings/ints saved in GitHub
    "schedule": {
        "daily_post": "00:00",
        "daily_scoreboard": "23:30",
        "weekly_day": 6,
        "weekly_time": "23:45",
        "rivalry_day": 4,
        "rivalry_time": "12:00",
        "rivalry_gap": 25,
    },

    # last_run: daily keys store YYYY-MM-DD, weekly/rivalry store Monday YYYY-MM-DD (week key)
    "last_run": {
        "daily_post": None,
        "daily_scoreboard": None,
        "weekly_roundup": None,
        "rivalry": None,
    }
}

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}" if GITHUB_TOKEN else "",
    "Accept": "application/vnd.github.v3+json",
}

# =====================================================
# KEEP ALIVE (Render)
# =====================================================
app = Flask("maptap")

@app.get("/")
def home():
    return "MapTap bot running"

def run_web():
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

# =====================================================
# GITHUB CONTENTS API JSON HELPERS
# =====================================================
def _gh_url(path: str) -> str:
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"

def gh_load_json(path: str, default: Any) -> Tuple[Any, Optional[str]]:
    r = requests.get(_gh_url(path), headers=HEADERS, timeout=20)
    if r.status_code == 404:
        return default, None
    r.raise_for_status()
    payload = r.json()
    content_b64 = payload.get("content", "")
    content = base64.b64decode(content_b64).decode("utf-8") if content_b64 else ""
    if not content.strip():
        return default, payload.get("sha")
    return json.loads(content), payload.get("sha")

def gh_save_json(path: str, data: Any, sha: Optional[str], message: str) -> str:
    encoded = base64.b64encode(json.dumps(data, indent=2).encode("utf-8")).decode("utf-8")
    body: Dict[str, Any] = {"message": message, "content": encoded}
    if sha:
        body["sha"] = sha
    r = requests.put(_gh_url(path), headers=HEADERS, json=body, timeout=20)
    r.raise_for_status()
    return r.json().get("content", {}).get("sha") or sha or ""

# =====================================================
# SETTINGS HELPERS
# =====================================================
def _merge(default: Dict[str, Any], incoming: Any) -> Dict[str, Any]:
    out = dict(default)
    if isinstance(incoming, dict):
        out.update(incoming)
    return out

def _normalize_hhmm(value: Any, fallback: str) -> str:
    s = str(value).strip()
    try:
        datetime.strptime(s, "%H:%M")
        return s
    except Exception:
        return fallback

def _normalize_int(value: Any, fallback: int, lo: int, hi: int) -> int:
    try:
        i = int(value)
        if lo <= i <= hi:
            return i
    except Exception:
        pass
    return fallback

def load_settings() -> Tuple[Dict[str, Any], Optional[str]]:
    raw, sha = gh_load_json(SETTINGS_PATH, DEFAULT_SETTINGS.copy())
    s = DEFAULT_SETTINGS.copy()
    if isinstance(raw, dict):
        s.update(raw)

    # channel_id
    try:
        s["channel_id"] = int(s["channel_id"]) if s.get("channel_id") is not None else None
    except Exception:
        s["channel_id"] = None

    # admin roles
    s["admin_role_ids"] = [int(x) for x in s.get("admin_role_ids", []) if str(x).isdigit()]

    # toggles
    for k in ["enabled", "daily_post_enabled", "daily_scoreboard_enabled", "weekly_roundup_enabled", "rivalry_enabled"]:
        s[k] = bool(s.get(k, DEFAULT_SETTINGS[k]))

    # emojis + schedule + last_run
    s["emojis"] = _merge(DEFAULT_SETTINGS["emojis"], s.get("emojis"))
    sch_in = _merge(DEFAULT_SETTINGS["schedule"], s.get("schedule"))
    s["schedule"] = {
        "daily_post": _normalize_hhmm(sch_in.get("daily_post"), DEFAULT_SETTINGS["schedule"]["daily_post"]),
        "daily_scoreboard": _normalize_hhmm(sch_in.get("daily_scoreboard"), DEFAULT_SETTINGS["schedule"]["daily_scoreboard"]),
        "weekly_day": _normalize_int(sch_in.get("weekly_day"), DEFAULT_SETTINGS["schedule"]["weekly_day"], 0, 6),
        "weekly_time": _normalize_hhmm(sch_in.get("weekly_time"), DEFAULT_SETTINGS["schedule"]["weekly_time"]),
        "rivalry_day": _normalize_int(sch_in.get("rivalry_day"), DEFAULT_SETTINGS["schedule"]["rivalry_day"], 0, 6),
        "rivalry_time": _normalize_hhmm(sch_in.get("rivalry_time"), DEFAULT_SETTINGS["schedule"]["rivalry_time"]),
        "rivalry_gap": _normalize_int(sch_in.get("rivalry_gap"), DEFAULT_SETTINGS["schedule"]["rivalry_gap"], 1, 100000),
    }
    s["last_run"] = _merge(DEFAULT_SETTINGS["last_run"], s.get("last_run"))
    if not isinstance(s["last_run"], dict):
        s["last_run"] = DEFAULT_SETTINGS["last_run"].copy()

    return s, sha

def save_settings(settings: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    return gh_save_json(SETTINGS_PATH, settings, sha, message)

# =====================================================
# STATS HELPERS
# =====================================================
def cleanup_old_scores(scores: Dict[str, Any], keep_days: int) -> Dict[str, Any]:
    cutoff = datetime.now(UK_TZ).date() - timedelta(days=keep_days)
    out: Dict[str, Any] = {}
    for dk, day in (scores or {}).items():
        try:
            dd = datetime.strptime(dk, "%Y-%m-%d").date()
        except Exception:
            continue
        if dd >= cutoff:
            out[dk] = day
    return out

def calculate_current_streak(scores: Dict[str, Any], uid: str) -> int:
    played = set()
    for dk, bucket in (scores or {}).items():
        if isinstance(bucket, dict) and uid in bucket:
            try:
                played.add(datetime.strptime(dk, "%Y-%m-%d").date())
            except Exception:
                pass
    if not played:
        return 0
    day = datetime.now(UK_TZ).date()
    streak = 0
    while day in played:
        streak += 1
        day -= timedelta(days=1)
    return streak

def calculate_rank(users: Dict[str, Any], uid: str) -> Tuple[int, int]:
    board = []
    for u, data in (users or {}).items():
        try:
            board.append((u, int(data.get("total_points", 0)), int(data.get("days_played", 0))))
        except Exception:
            pass
    board.sort(key=lambda x: (x[1], x[2]), reverse=True)
    total = len(board)
    for i, (u, _, __) in enumerate(board, start=1):
        if u == uid:
            return i, total
    return total, total

async def react_safe(msg: discord.Message, emoji: str, fallback: str):
    try:
        await msg.add_reaction(emoji)
    except Exception:
        try:
            await msg.add_reaction(fallback)
        except Exception:
            pass

# =====================================================
# MESSAGE BUILDERS
# =====================================================
def build_daily_prompt() -> str:
    return (
        "🗺️ **Daily MapTap is live!**\n"
        f"👉 {MAPTAP_URL}\n\n"
        "Post your results **exactly as shared from the app** so I can track scores ✈️\n"
        f"_(Scores over **{MAX_SCORE}** won’t be counted.)_"
    )

def build_daily_scoreboard_text(date_key: str, rows: List[Tuple[str, int]]) -> str:
    header = f"🗺️ **MapTap — Daily Scores**\n*{pretty_day(date_key)}*\n\n"
    if not rows:
        return header + "😶 No scores today."
    lines = [f"{i}. <@{uid}> — **{score}**" for i, (uid, score) in enumerate(rows, start=1)]
    return header + "\n".join(lines) + f"\n\n✈️ Players today: **{len(rows)}**"

def build_weekly_roundup_text(mon: datetime.date, sun: datetime.date, rows: List[Tuple[str, int, int]]) -> str:
    header = (
        "🗺️ **MapTap — Weekly Round-Up**\n"
        f"*Mon {mon.strftime('%d %b')} → Sun {sun.strftime('%d %b')}*\n\n"
    )
    if not rows:
        return header + "😶 No scores this week."
    lines = [f"{i}. <@{uid}> — **{total} pts** ({days}/7 days)" for i, (uid, total, days) in enumerate(rows, start=1)]
    return header + "\n".join(lines) + f"\n\n✈️ Weekly players: **{len(rows)}**"

# =====================================================
# DISCORD CLIENT
# =====================================================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

class MapTapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        try:
            await self.tree.sync()
        except Exception as e:
            print("Command sync failed:", e)

client = MapTapBot()

# =====================================================
# PERMISSIONS / CHANNEL
# =====================================================
def get_channel(settings: Dict[str, Any]) -> Optional[discord.TextChannel]:
    cid = settings.get("channel_id")
    if not cid:
        return None
    return client.get_channel(int(cid))

def has_admin(member: discord.Member, settings: Dict[str, Any]) -> bool:
    if member.guild_permissions.administrator:
        return True
    allowed = settings.get("admin_role_ids", [])
    if not allowed:
        return False
    return any(r.id in allowed for r in member.roles)

# =========================
# FULL FILE (PART 2/2)
# =========================

# =====================================================
# SETTINGS UI
# =====================================================
class EmojiModal(discord.ui.Modal, title="MapTap Reaction Emojis"):
    recorded = discord.ui.TextInput(
        label="Score recorded emoji",
        placeholder="e.g. ✅ or <:maptapp:1451532874590191647>",
        required=True,
        max_length=64
    )
    too_high = discord.ui.TextInput(
        label="Too high score emoji",
        placeholder="e.g. ❌",
        required=True,
        max_length=64
    )
    rescan_ingested = discord.ui.TextInput(
        label="Rescan ingested emoji",
        placeholder="e.g. 🔁",
        required=True,
        max_length=64
    )

    def __init__(self, view_ref: "SettingsView"):
        super().__init__()
        self.view_ref = view_ref
        em = self.view_ref.settings.get("emojis", {})
        self.recorded.default = str(em.get("recorded", "🌏"))
        self.too_high.default = str(em.get("too_high", "❌"))
        self.rescan_ingested.default = str(em.get("rescan_ingested", "🔁"))

    async def on_submit(self, interaction: discord.Interaction):
        self.view_ref.settings.setdefault("emojis", {})
        self.view_ref.settings["emojis"]["recorded"] = str(self.recorded.value).strip()
        self.view_ref.settings["emojis"]["too_high"] = str(self.too_high.value).strip()
        self.view_ref.settings["emojis"]["rescan_ingested"] = str(self.rescan_ingested.value).strip()
        await self.view_ref.save_refresh(interaction, "MapTap: update emojis")

class ScheduleModal(discord.ui.Modal, title="MapTap Schedule (UK)"):
    daily_post = discord.ui.TextInput(label="Daily post time (HH:MM)", placeholder="00:00", required=True, max_length=5)
    daily_scoreboard = discord.ui.TextInput(label="Daily scoreboard time (HH:MM)", placeholder="23:30", required=True, max_length=5)
    weekly_time = discord.ui.TextInput(label="Weekly roundup time (HH:MM)", placeholder="23:45", required=True, max_length=5)
    rivalry_time = discord.ui.TextInput(label="Rivalry time (HH:MM)", placeholder="12:00", required=True, max_length=5)
    rivalry_gap = discord.ui.TextInput(label="Rivalry max gap (points)", placeholder="25", required=True, max_length=6)

    def __init__(self, view_ref: "SettingsView"):
        super().__init__()
        self.view_ref = view_ref
        sch = self.view_ref.settings.get("schedule", {})
        self.daily_post.default = str(sch.get("daily_post", "00:00"))
        self.daily_scoreboard.default = str(sch.get("daily_scoreboard", "23:30"))
        self.weekly_time.default = str(sch.get("weekly_time", "23:45"))
        self.rivalry_time.default = str(sch.get("rivalry_time", "12:00"))
        self.rivalry_gap.default = str(sch.get("rivalry_gap", 25))

    async def on_submit(self, interaction: discord.Interaction):
        dp = str(self.daily_post.value).strip()
        ds = str(self.daily_scoreboard.value).strip()
        wt = str(self.weekly_time.value).strip()
        rt = str(self.rivalry_time.value).strip()

        try:
            datetime.strptime(dp, "%H:%M")
            datetime.strptime(ds, "%H:%M")
            datetime.strptime(wt, "%H:%M")
            datetime.strptime(rt, "%H:%M")
        except Exception:
            await interaction.response.send_message("❌ Invalid time. Use **HH:MM** (24h). Example: **23:30**", ephemeral=True)
            return

        try:
            gap = int(str(self.rivalry_gap.value).strip())
            if gap < 1:
                raise ValueError
        except Exception:
            await interaction.response.send_message("❌ Rivalry gap must be a whole number ≥ 1.", ephemeral=True)
            return

        self.view_ref.settings.setdefault("schedule", {})
        self.view_ref.settings["schedule"]["daily_post"] = dp
        self.view_ref.settings["schedule"]["daily_scoreboard"] = ds
        self.view_ref.settings["schedule"]["weekly_time"] = wt
        self.view_ref.settings["schedule"]["rivalry_time"] = rt
        self.view_ref.settings["schedule"]["rivalry_gap"] = gap

        await self.view_ref.save_refresh(interaction, "MapTap: update schedule")

class WeekdaySelect(discord.ui.Select):
    def __init__(self, label: str, current: int):
        options = []
        for name, num in WEEKDAYS:
            options.append(discord.SelectOption(label=name, value=str(num), default=(int(current) == num)))
        super().__init__(placeholder=label, options=options, min_values=1, max_values=1)

class WeeklyDaySelect(WeekdaySelect):
    def __init__(self, current: int):
        super().__init__("Weekly roundup day", current)

    async def callback(self, interaction: discord.Interaction):
        view: "SettingsView" = self.view  # type: ignore
        view.settings.setdefault("schedule", {})
        view.settings["schedule"]["weekly_day"] = int(self.values[0])
        await view.save_refresh(interaction, "MapTap: set weekly day")

class RivalryDaySelect(WeekdaySelect):
    def __init__(self, current: int):
        super().__init__("Rivalry day", current)

    async def callback(self, interaction: discord.Interaction):
        view: "SettingsView" = self.view  # type: ignore
        view.settings.setdefault("schedule", {})
        view.settings["schedule"]["rivalry_day"] = int(self.values[0])
        await view.save_refresh(interaction, "MapTap: set rivalry day")

class SettingsView(discord.ui.View):
    def __init__(self, settings: Dict[str, Any], sha: Optional[str]):
        super().__init__(timeout=300)
        self.settings = settings
        self.sha = sha

        # Channel select
        self.channel_select = discord.ui.ChannelSelect(
            placeholder="Select the MapTap channel",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1
        )
        self.channel_select.callback = self._on_channel_select  # type: ignore
        self.add_item(self.channel_select)

        # Role select
        self.role_select = discord.ui.RoleSelect(
            placeholder="Select admin roles (optional)",
            min_values=0,
            max_values=10
        )
        self.role_select.callback = self._on_role_select  # type: ignore
        self.add_item(self.role_select)

        # Weekday dropdowns
        sch = self.settings.get("schedule", DEFAULT_SETTINGS["schedule"])
        self.add_item(WeeklyDaySelect(int(sch.get("weekly_day", 6))))
        self.add_item(RivalryDaySelect(int(sch.get("rivalry_day", 4))))

        # Buttons
        self.btn_emojis = discord.ui.Button(label="Edit Reaction Emojis", style=discord.ButtonStyle.primary)
        self.btn_emojis.callback = self._on_emojis  # type: ignore
        self.add_item(self.btn_emojis)

        self.btn_schedule = discord.ui.Button(label="Edit Schedule (UK)", style=discord.ButtonStyle.primary)
        self.btn_schedule.callback = self._on_schedule  # type: ignore
        self.add_item(self.btn_schedule)

        self.btn_bot = discord.ui.Button(label=self._lbl("Bot", bool(self.settings.get("enabled", True))), style=discord.ButtonStyle.secondary)
        self.btn_bot.callback = self._toggle_bot  # type: ignore
        self.add_item(self.btn_bot)

        self.btn_post = discord.ui.Button(label=self._lbl("Daily Post", bool(self.settings.get("daily_post_enabled", True))), style=discord.ButtonStyle.secondary)
        self.btn_post.callback = self._toggle_post  # type: ignore
        self.add_item(self.btn_post)

        self.btn_board = discord.ui.Button(label=self._lbl("Daily Board", bool(self.settings.get("daily_scoreboard_enabled", True))), style=discord.ButtonStyle.secondary)
        self.btn_board.callback = self._toggle_board  # type: ignore
        self.add_item(self.btn_board)

        self.btn_weekly = discord.ui.Button(label=self._lbl("Weekly Roundup", bool(self.settings.get("weekly_roundup_enabled", True))), style=discord.ButtonStyle.secondary)
        self.btn_weekly.callback = self._toggle_weekly  # type: ignore
        self.add_item(self.btn_weekly)

        self.btn_rivalry = discord.ui.Button(label=self._lbl("Rivalry", bool(self.settings.get("rivalry_enabled", True))), style=discord.ButtonStyle.secondary)
        self.btn_rivalry.callback = self._toggle_rivalry  # type: ignore
        self.add_item(self.btn_rivalry)

        self.btn_close = discord.ui.Button(label="Close", style=discord.ButtonStyle.danger)
        self.btn_close.callback = self._close  # type: ignore
        self.add_item(self.btn_close)

    def _lbl(self, name: str, state: bool) -> str:
        return f"{name}: {'ON' if state else 'OFF'}"

    def embed(self) -> discord.Embed:
        ch = f"<#{self.settings['channel_id']}>" if self.settings.get("channel_id") else "Not set"
        roles = self.settings.get("admin_role_ids", [])
        roles_str = ", ".join(f"<@&{rid}>" for rid in roles) if roles else "Admins only"

        em = self.settings.get("emojis", {})
        sch = self.settings.get("schedule", {})

        # Wrap emoji strings in backticks so custom emoji strings always show in the embed
        emoji_block = (
            f"Recorded: `{em.get('recorded', '🌏')}`\n"
            f"Too high: `{em.get('too_high', '❌')}`\n"
            f"Rescan: `{em.get('rescan_ingested', '🔁')}`"
        )

        schedule_block = (
            f"Daily post: **{sch.get('daily_post', '00:00')}**\n"
            f"Daily scoreboard: **{sch.get('daily_scoreboard', '23:30')}**\n"
            f"Weekly roundup: **{weekday_name(sch.get('weekly_day', 6))} {sch.get('weekly_time', '23:45')}**\n"
            f"Rivalry: **{weekday_name(sch.get('rivalry_day', 4))} {sch.get('rivalry_time', '12:00')}** (≤ {sch.get('rivalry_gap', 25)} pts)"
        )

        e = discord.Embed(
            title="🗺️ MapTap Settings",
            description=(
                f"**Channel:** {ch}\n"
                f"**Admin roles:** {roles_str}\n\n"
                f"**{self._lbl('Bot', bool(self.settings.get('enabled', True)))}**\n"
                f"**{self._lbl('Daily Post', bool(self.settings.get('daily_post_enabled', True)))}**\n"
                f"**{self._lbl('Daily Board', bool(self.settings.get('daily_scoreboard_enabled', True)))}**\n"
                f"**{self._lbl('Weekly Roundup', bool(self.settings.get('weekly_roundup_enabled', True)))}**\n"
                f"**{self._lbl('Rivalry', bool(self.settings.get('rivalry_enabled', True)))}**\n\n"
                f"**Schedule (UK):**\n{schedule_block}\n\n"
                f"**Reactions:**\n{emoji_block}"
            ),
            color=0xF1C40F
        )
        e.set_footer(text="Changes save to GitHub immediately.")
        return e

    async def save_refresh(self, interaction: discord.Interaction, message: str):
        current, cur_sha = load_settings()
        current.update(self.settings)
        save_settings(current, cur_sha, message)
        self.settings = current

        # refresh button labels
        self.btn_bot.label = self._lbl("Bot", bool(self.settings.get("enabled", True)))
        self.btn_post.label = self._lbl("Daily Post", bool(self.settings.get("daily_post_enabled", True)))
        self.btn_board.label = self._lbl("Daily Board", bool(self.settings.get("daily_scoreboard_enabled", True)))
        self.btn_weekly.label = self._lbl("Weekly Roundup", bool(self.settings.get("weekly_roundup_enabled", True)))
        self.btn_rivalry.label = self._lbl("Rivalry", bool(self.settings.get("rivalry_enabled", True)))

        await interaction.response.edit_message(embed=self.embed(), view=self)

    async def _on_channel_select(self, interaction: discord.Interaction):
        # channel_select.values is a list of channels
        picked = self.channel_select.values[0]
        self.settings["channel_id"] = picked.id
        await self.save_refresh(interaction, "MapTap: set channel")

    async def _on_role_select(self, interaction: discord.Interaction):
        self.settings["admin_role_ids"] = [r.id for r in self.role_select.values]
        await self.save_refresh(interaction, "MapTap: set admin roles")

    async def _on_emojis(self, interaction: discord.Interaction):
        await interaction.response.send_modal(EmojiModal(self))

    async def _on_schedule(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ScheduleModal(self))

    async def _toggle_bot(self, interaction: discord.Interaction):
        self.settings["enabled"] = not bool(self.settings.get("enabled", True))
        await self.save_refresh(interaction, "MapTap: toggle bot")

    async def _toggle_post(self, interaction: discord.Interaction):
        self.settings["daily_post_enabled"] = not bool(self.settings.get("daily_post_enabled", True))
        await self.save_refresh(interaction, "MapTap: toggle daily post")

    async def _toggle_board(self, interaction: discord.Interaction):
        self.settings["daily_scoreboard_enabled"] = not bool(self.settings.get("daily_scoreboard_enabled", True))
        await self.save_refresh(interaction, "MapTap: toggle daily board")

    async def _toggle_weekly(self, interaction: discord.Interaction):
        self.settings["weekly_roundup_enabled"] = not bool(self.settings.get("weekly_roundup_enabled", True))
        await self.save_refresh(interaction, "MapTap: toggle weekly")

    async def _toggle_rivalry(self, interaction: discord.Interaction):
        self.settings["rivalry_enabled"] = not bool(self.settings.get("rivalry_enabled", True))
        await self.save_refresh(interaction, "MapTap: toggle rivalry")

    async def _close(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="✅ Closed.", embed=None, view=None)

# =====================================================
# SLASH COMMANDS
# =====================================================
@client.tree.command(name="maptapsettings", description="Configure MapTap bot settings")
async def maptapsettings(interaction: discord.Interaction):
    settings, sha = load_settings()
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Use this in the server.", ephemeral=True)
        return
    if not has_admin(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission.", ephemeral=True)
        return

    view = SettingsView(settings, sha)
    await interaction.response.send_message(embed=view.embed(), view=view, ephemeral=False)

@client.tree.command(name="mymaptap", description="View MapTap stats (yours or someone else)")
@app_commands.describe(user="Optional: view another user")
async def mymaptap(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    users, _ = gh_load_json(USERS_PATH, {})
    scores, _ = gh_load_json(SCORES_PATH, {})

    target = user or interaction.user
    uid = str(target.id)

    stats = (users or {}).get(uid)
    if not stats or int(stats.get("days_played", 0)) <= 0:
        await interaction.response.send_message(f"{target.display_name} hasn’t recorded any MapTap scores yet 🗺️", ephemeral=False)
        return

    # Best day
    best_day = None
    best_score = -1
    for dk, bucket in (scores or {}).items():
        if isinstance(bucket, dict) and uid in bucket:
            try:
                sc = int(bucket[uid].get("score", 0))
                if sc > best_score:
                    best_score = sc
                    best_day = dk
            except Exception:
                pass

    rank, total = calculate_rank(users, uid)
    streak = calculate_current_streak(scores, uid)
    avg = round(int(stats.get("total_points", 0)) / max(1, int(stats.get("days_played", 1))))

    await interaction.response.send_message(
        f"🗺️ **MapTap Stats — {target.display_name}**\n\n"
        f"• Server Rank: 🏅 **#{rank} of {total}**\n"
        f"• Total points: **{stats.get('total_points', 0)}**\n"
        f"• Days played: **{stats.get('days_played', 0)}**\n"
        f"• Average score: **{avg}**\n"
        f"• Current streak: 🔥 **{streak} days**\n"
        f"• Best streak: 🏆 **{stats.get('best_streak', 0)} days**\n"
        f"• Best day: 🌟 **{pretty_day(best_day) if best_day else 'N/A'} — {best_score if best_day else 'N/A'}**",
        ephemeral=False
    )

@client.tree.command(name="rescan", description="Re-scan recent MapTap messages for missed scores (admin only)")
@app_commands.describe(messages="How many recent messages to scan (max 50)")
async def rescan(interaction: discord.Interaction, messages: int = 10):
    settings, _ = load_settings()
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Server only.", ephemeral=True)
        return
    if not has_admin(interaction.user, settings):
        await interaction.response.send_message("❌ No permission.", ephemeral=True)
        return

    ch = get_channel(settings)
    if not ch:
        await interaction.response.send_message("❌ MapTap channel not configured.", ephemeral=True)
        return

    messages = max(1, min(messages, 50))
    await interaction.response.send_message(f"🔍 Scanning last **{messages}** messages…", ephemeral=True)

    em = settings.get("emojis", DEFAULT_SETTINGS["emojis"])

    scores, ssha = gh_load_json(SCORES_PATH, {})
    users, usha = gh_load_json(USERS_PATH, {})
    if not isinstance(scores, dict): scores = {}
    if not isinstance(users, dict): users = {}

    matched = 0
    ingested = 0
    skipped = 0

    async for msg in ch.history(limit=messages):
        if msg.author.bot or not msg.content:
            continue

        m = SCORE_REGEX.search(msg.content)
        if not m:
            continue

        matched += 1
        score = int(m.group(1))

        if score > MAX_SCORE:
            skipped += 1
            # (optional) react too high, but not required on rescan
            continue

        msg_time = msg.created_at.replace(tzinfo=UTC).astimezone(UK_TZ)
        dk = today_key(msg_time)
        uid = str(msg.author.id)

        scores.setdefault(dk, {})
        bucket = scores[dk]

        # NO DUPLICATE REACTION — silent skip
        if uid in bucket:
            skipped += 1
            continue

        bucket[uid] = {"score": score, "updated_at": msg_time.isoformat()}

        user_stats = users.setdefault(uid, {"total_points": 0, "days_played": 0, "best_streak": 0})
        user_stats["days_played"] += 1
        user_stats["total_points"] += score

        ingested += 1
        await react_safe(msg, em.get("rescan_ingested", "🔁"), "🔁")

    gh_save_json(SCORES_PATH, scores, ssha, f"MapTap: rescan last {messages}")
    gh_save_json(USERS_PATH, users, usha, "MapTap: rescan user stats")

    await interaction.followup.send(
        f"✅ **Rescan complete**\n"
        f"• Matches found: **{matched}**\n"
        f"• Newly ingested: **{ingested}**\n"
        f"• Skipped: **{skipped}**",
        ephemeral=True
    )

# =====================================================
# SCHEDULED ACTIONS
# =====================================================
async def do_daily_scoreboard(settings: Dict[str, Any]):
    ch = get_channel(settings)
    if not ch:
        return

    scores, ssha = gh_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        scores = {}

    dk = today_key()
    bucket = scores.get(dk, {})
    rows: List[Tuple[str, int]] = []
    if isinstance(bucket, dict):
        for uid, entry in bucket.items():
            if isinstance(entry, dict) and "score" in entry:
                try:
                    rows.append((uid, int(entry["score"])))
                except Exception:
                    pass

    rows.sort(key=lambda x: x[1], reverse=True)
    await ch.send(build_daily_scoreboard_text(dk, rows))

    cleaned = cleanup_old_scores(scores, CLEANUP_DAYS)
    if cleaned != scores:
        gh_save_json(SCORES_PATH, cleaned, ssha, f"MapTap: cleanup keep {CLEANUP_DAYS} days")

async def do_weekly_roundup(settings: Dict[str, Any]):
    ch = get_channel(settings)
    if not ch:
        return

    scores, _ = gh_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        scores = {}

    now = datetime.now(UK_TZ)
    mon = monday_of_week(now)
    sun = mon + timedelta(days=6)
    days = [(mon + timedelta(days=i)).isoformat() for i in range(7)]

    weekly: Dict[str, Dict[str, int]] = {}
    for dk in days:
        bucket = scores.get(dk, {})
        if not isinstance(bucket, dict):
            continue
        for uid, entry in bucket.items():
            if not isinstance(entry, dict) or "score" not in entry:
                continue
            try:
                sc = int(entry["score"])
            except Exception:
                continue
            weekly.setdefault(uid, {"total": 0, "days": 0})
            weekly[uid]["total"] += sc
            weekly[uid]["days"] += 1

    rows: List[Tuple[str, int, int]] = [(uid, v["total"], v["days"]) for uid, v in weekly.items()]
    rows.sort(key=lambda x: x[1], reverse=True)
    await ch.send(build_weekly_roundup_text(mon, sun, rows))

async def do_rivalry(settings: Dict[str, Any]):
    # Pick top 2 totals THIS WEEK; only post if within rivalry_gap
    ch = get_channel(settings)
    if not ch:
        return

    scores, _ = gh_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        return

    now = datetime.now(UK_TZ)
    mon = monday_of_week(now)
    days = [(mon + timedelta(days=i)).isoformat() for i in range(7)]

    totals: Dict[str, int] = {}
    for dk in days:
        bucket = scores.get(dk, {})
        if not isinstance(bucket, dict):
            continue
        for uid, entry in bucket.items():
            if not isinstance(entry, dict) or "score" not in entry:
                continue
            try:
                sc = int(entry["score"])
            except Exception:
                continue
            totals[uid] = totals.get(uid, 0) + sc

    if len(totals) < 2:
        return

    board = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    (a_uid, a_total), (b_uid, b_total) = board[0], board[1]

    gap = abs(a_total - b_total)
    max_gap = int(settings.get("schedule", {}).get("rivalry_gap", 25))
    if gap > max_gap:
        return

    await ch.send(
        "⚔️ **Rivalry Watch (this week)**\n\n"
        f"1) <@{a_uid}> — **{a_total}**\n"
        f"2) <@{b_uid}> — **{b_total}**\n\n"
        f"Only **{gap}** points between them 👀"
    )

# =====================================================
# SCHEDULER (minute tick)
# =====================================================
@tasks.loop(minutes=1)
async def scheduler_tick():
    settings, sha = load_settings()
    if not settings.get("enabled", True):
        return

    ch = get_channel(settings)
    if not ch:
        return

    now = datetime.now(UK_TZ)
    hhmm = now.strftime("%H:%M")
    today = today_key(now)
    week = monday_key(now)

    sch = settings.get("schedule", DEFAULT_SETTINGS["schedule"])
    last = settings.get("last_run", DEFAULT_SETTINGS["last_run"])

    fired = False

    # Daily post
    if settings.get("daily_post_enabled", True) and hhmm == sch.get("daily_post", "00:00"):
        if last.get("daily_post") != today:
            await ch.send(build_daily_prompt())
            settings["last_run"]["daily_post"] = today
            fired = True

    # Daily scoreboard
    if settings.get("daily_scoreboard_enabled", True) and hhmm == sch.get("daily_scoreboard", "23:30"):
        if last.get("daily_scoreboard") != today:
            await do_daily_scoreboard(settings)
            settings["last_run"]["daily_scoreboard"] = today
            fired = True

    # Weekly roundup (day selectable)
    if settings.get("weekly_roundup_enabled", True) and hhmm == sch.get("weekly_time", "23:45"):
        if now.weekday() == int(sch.get("weekly_day", 6)) and last.get("weekly_roundup") != week:
            await do_weekly_roundup(settings)
            settings["last_run"]["weekly_roundup"] = week
            fired = True

    # Rivalry (once per week, day selectable)
    if settings.get("rivalry_enabled", True) and hhmm == sch.get("rivalry_time", "12:00"):
        if now.weekday() == int(sch.get("rivalry_day", 4)) and last.get("rivalry") != week:
            await do_rivalry(settings)
            settings["last_run"]["rivalry"] = week
            fired = True

    # Save only if something fired (avoids constant GitHub writes)
    if fired:
        try:
            save_settings(settings, sha, f"MapTap: update last_run {today} {hhmm}")
        except Exception as e:
            print("Failed to save last_run:", e)

# =====================================================
# SCORE INGESTION
# =====================================================
@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    settings, _ = load_settings()
    if not settings.get("enabled", True):
        return

    cid = settings.get("channel_id")
    if not cid or message.channel.id != int(cid):
        return

    m = SCORE_REGEX.search(message.content or "")
    if not m:
        return

    score = int(m.group(1))
    em = settings.get("emojis", DEFAULT_SETTINGS["emojis"])

    if score > MAX_SCORE:
        await react_safe(message, em.get("too_high", "❌"), "❌")
        return

    msg_time = message.created_at.replace(tzinfo=UTC).astimezone(UK_TZ)
    dk = today_key(msg_time)
    uid = str(message.author.id)

    scores, ssha = gh_load_json(SCORES_PATH, {})
    users, usha = gh_load_json(USERS_PATH, {})
    if not isinstance(scores, dict): scores = {}
    if not isinstance(users, dict): users = {}

    scores.setdefault(dk, {})
    bucket = scores[dk]

    user_stats = users.setdefault(uid, {"total_points": 0, "days_played": 0, "best_streak": 0})

    prev = bucket.get(uid)
    if prev and isinstance(prev, dict) and "score" in prev:
        # overwrite: subtract old score from total first
        try:
            user_stats["total_points"] -= int(prev["score"])
        except Exception:
            pass
    else:
        user_stats["days_played"] += 1

    user_stats["total_points"] += score
    bucket[uid] = {"score": score, "updated_at": msg_time.isoformat()}

    cur_streak = calculate_current_streak(scores, uid)
    if cur_streak > int(user_stats.get("best_streak", 0)):
        user_stats["best_streak"] = cur_streak

    gh_save_json(SCORES_PATH, scores, ssha, f"MapTap: score {dk}")
    gh_save_json(USERS_PATH, users, usha, f"MapTap: user {uid}")

    await react_safe(message, em.get("recorded", "🌏"), "✅")

# =====================================================
# STARTUP
# =====================================================
@client.event
async def on_ready():
    print(f"✅ Logged in as {client.user} (MapTap)")
    if not scheduler_tick.is_running():
        scheduler_tick.start()

# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("Missing TOKEN env var")
    if not GITHUB_TOKEN or not GITHUB_REPO:
        raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPO env vars")

    Thread(target=run_web, daemon=True).start()
    client.run(TOKEN)