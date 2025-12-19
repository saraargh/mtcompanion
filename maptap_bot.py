# =========================
# MapTap Companion Bot (FULL FILE)
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

TOKEN = os.getenv("TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # e.g. "saraargh/the-pilot"

SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")
SETTINGS_PATH = os.getenv("MAPTAP_SETTINGS_PATH", "data/maptap_settings.json")

MAPTAP_URL = os.getenv("MAPTAP_URL", "https://www.maptap.gg")
CLEANUP_DAYS = int(os.getenv("MAPTAP_CLEANUP_DAYS", "69"))
MAX_SCORE = int(os.getenv("MAPTAP_MAX_SCORE", "1000"))

SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)
WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# =====================================================
# DEFAULT SETTINGS (GitHub-backed)
# =====================================================
DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,

    "daily_post_enabled": True,
    "daily_scoreboard_enabled": True,
    "weekly_roundup_enabled": True,
    "rivalry_enabled": True,

    "admin_role_ids": [],

    "emojis": {
        "recorded": "🌏",
        "too_high": "❌",
        "rescan_ingested": "🔁",
    },

    # All schedule in one place (UK time, HH:MM)
    "schedule": {
        "daily_post": "00:00",
        "daily_scoreboard": "23:30",
        "weekly_day": 6,        # Sunday (0=Mon..6=Sun)
        "weekly_time": "23:45",
        "rivalry_day": 4,       # Friday
        "rivalry_time": "19:30",
        "rivalry_gap": 25,      # points
    },

    # Prevent double posting
    "last_run": {
        "daily_post": None,
        "daily_scoreboard": None,
        "weekly_roundup": None,
        "rivalry": None,
    },
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

def github_load_json(path: str, default: Any) -> Tuple[Any, Optional[str]]:
    url = _gh_url(path)
    r = requests.get(url, headers=HEADERS, timeout=20)

    if r.status_code == 404:
        return default, None

    r.raise_for_status()
    payload = r.json()
    content_b64 = payload.get("content", "")
    content = base64.b64decode(content_b64).decode("utf-8") if content_b64 else ""
    if not content.strip():
        return default, payload.get("sha")

    return json.loads(content), payload.get("sha")

def github_save_json(path: str, data: Any, sha: Optional[str], message: str) -> str:
    url = _gh_url(path)
    encoded = base64.b64encode(json.dumps(data, indent=2).encode("utf-8")).decode("utf-8")

    body: Dict[str, Any] = {
        "message": message,
        "content": encoded,
    }
    if sha:
        body["sha"] = sha

    r = requests.put(url, headers=HEADERS, json=body, timeout=20)
    r.raise_for_status()
    new_sha = r.json().get("content", {}).get("sha")
    return new_sha or sha or ""

# =====================================================
# SETTINGS HELPERS
# =====================================================
def _merge_dict(default: Dict[str, Any], incoming: Any) -> Dict[str, Any]:
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

def _normalize_int(value: Any, fallback: int, min_v: int, max_v: int) -> int:
    try:
        v = int(value)
        if min_v <= v <= max_v:
            return v
    except Exception:
        pass
    return fallback

def load_settings() -> Tuple[Dict[str, Any], Optional[str]]:
    settings, sha = github_load_json(SETTINGS_PATH, DEFAULT_SETTINGS.copy())

    merged = DEFAULT_SETTINGS.copy()
    if isinstance(settings, dict):
        merged.update(settings)

    # channel_id
    if merged.get("channel_id") is not None:
        try:
            merged["channel_id"] = int(merged["channel_id"])
        except Exception:
            merged["channel_id"] = None

    # roles
    merged["admin_role_ids"] = [
        int(x) for x in merged.get("admin_role_ids", [])
        if str(x).isdigit()
    ]

    # emojis
    merged["emojis"] = _merge_dict(DEFAULT_SETTINGS["emojis"], merged.get("emojis"))

    # schedule
    sch_in = _merge_dict(DEFAULT_SETTINGS["schedule"], merged.get("schedule"))
    merged["schedule"] = {
        "daily_post": _normalize_hhmm(sch_in.get("daily_post"), DEFAULT_SETTINGS["schedule"]["daily_post"]),
        "daily_scoreboard": _normalize_hhmm(sch_in.get("daily_scoreboard"), DEFAULT_SETTINGS["schedule"]["daily_scoreboard"]),
        "weekly_day": _normalize_int(sch_in.get("weekly_day"), DEFAULT_SETTINGS["schedule"]["weekly_day"], 0, 6),
        "weekly_time": _normalize_hhmm(sch_in.get("weekly_time"), DEFAULT_SETTINGS["schedule"]["weekly_time"]),
        "rivalry_day": _normalize_int(sch_in.get("rivalry_day"), DEFAULT_SETTINGS["schedule"]["rivalry_day"], 0, 6),
        "rivalry_time": _normalize_hhmm(sch_in.get("rivalry_time"), DEFAULT_SETTINGS["schedule"]["rivalry_time"]),
        "rivalry_gap": _normalize_int(sch_in.get("rivalry_gap"), DEFAULT_SETTINGS["schedule"]["rivalry_gap"], 1, 100000),
    }

    # last_run
    merged["last_run"] = _merge_dict(DEFAULT_SETTINGS["last_run"], merged.get("last_run"))
    if not isinstance(merged["last_run"], dict):
        merged["last_run"] = DEFAULT_SETTINGS["last_run"].copy()

    # booleans
    for k in ["enabled", "daily_post_enabled", "daily_scoreboard_enabled", "weekly_roundup_enabled", "rivalry_enabled"]:
        merged[k] = bool(merged.get(k, DEFAULT_SETTINGS[k]))

    return merged, sha

def save_settings(settings: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    return github_save_json(SETTINGS_PATH, settings, sha, message)

# =====================================================
# HELPERS
# =====================================================
def today_key(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(UK_TZ)
    return dt.date().isoformat()

def pretty_day(date_key: str) -> str:
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")

def monday_of_week(d: datetime) -> datetime.date:
    return d.date() - timedelta(days=d.weekday())

def cleanup_old_scores(scores: Dict[str, Any], keep_days: int) -> Dict[str, Any]:
    cutoff = datetime.now(UK_TZ).date() - timedelta(days=keep_days)
    cleaned: Dict[str, Any] = {}
    for date_key, day in scores.items():
        try:
            day_date = datetime.strptime(date_key, "%Y-%m-%d").date()
        except Exception:
            continue
        if day_date >= cutoff:
            cleaned[date_key] = day
    return cleaned

def calculate_current_streak(scores: Dict[str, Any], user_id: str) -> int:
    played_dates: List[datetime.date] = []
    for date_key, day in scores.items():
        if isinstance(day, dict) and user_id in day:
            try:
                played_dates.append(datetime.strptime(date_key, "%Y-%m-%d").date())
            except Exception:
                pass

    if not played_dates:
        return 0

    played_set = set(played_dates)
    day = datetime.now(UK_TZ).date()
    streak = 0
    while day in played_set:
        streak += 1
        day -= timedelta(days=1)
    return streak

def calculate_all_time_rank(users: Dict[str, Any], user_id: str) -> Tuple[int, int]:
    leaderboard: List[Tuple[str, int, int]] = []
    for uid, data in users.items():
        try:
            leaderboard.append((
                uid,
                int(data.get("total_points", 0)),
                int(data.get("days_played", 0)),
            ))
        except Exception:
            continue

    leaderboard.sort(key=lambda x: (x[1], x[2]), reverse=True)
    total_players = len(leaderboard)

    for idx, (uid, _, _) in enumerate(leaderboard, start=1):
        if uid == user_id:
            return idx, total_players

    return total_players, total_players

async def react_safe(msg: discord.Message, emoji: str, fallback: str):
    # Works with unicode (✅) and custom emoji strings (<:name:id>)
    try:
        await msg.add_reaction(emoji)
    except Exception:
        try:
            await msg.add_reaction(fallback)
        except Exception:
            pass

def build_daily_prompt() -> str:
    return (
        "🗺️ **Daily MapTap is live!**\n"
        f"👉 {MAPTAP_URL}\n\n"
        "Post your results **exactly as shared from the app** so I can track scores ✈️\n"
        f"_(Scores over **{MAX_SCORE}** won’t be counted.)_"
    )

def build_daily_scoreboard_text(date_key: str, sorted_rows: List[Tuple[str, int]]) -> str:
    header = f"🗺️ **MapTap — Daily Scores**\n*{pretty_day(date_key)}*\n\n"
    if not sorted_rows:
        return header + "😶 No scores today."

    lines = [f"{i}. <@{uid}> — **{score}**" for i, (uid, score) in enumerate(sorted_rows, start=1)]
    footer = f"\n\n✈️ Players today: **{len(sorted_rows)}**"
    return header + "\n".join(lines) + footer

def build_weekly_roundup_text(mon: datetime.date, sun: datetime.date, sorted_rows: List[Tuple[str, int, int]]) -> str:
    header = (
        "🗺️ **MapTap — Weekly Round-Up**\n"
        f"*Mon {mon.strftime('%d %b')} → Sun {sun.strftime('%d %b')}*\n\n"
    )
    if not sorted_rows:
        return header + "😶 No scores this week."

    lines = [
        f"{i}. <@{uid}> — **{total} pts** ({days}/7 days)"
        for i, (uid, total, days) in enumerate(sorted_rows, start=1)
    ]
    footer = f"\n\n✈️ Weekly players: **{len(sorted_rows)}**"
    return header + "\n".join(lines) + footer

def build_rivalry_message(leader_uid: str, chaser_uid: str, gap: int) -> str:
    return (
        "⚔️ **Rivalry Watch**\n\n"
        f"<@{chaser_uid}> is just **{gap} points** behind <@{leader_uid}> this week 👀"
    )

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
def has_admin_access(member: discord.Member, settings: Dict[str, Any]) -> bool:
    if member.guild_permissions.administrator:
        return True
    allowed = settings.get("admin_role_ids", [])
    if not allowed:
        return False
    return any(r.id in allowed for r in member.roles)

def get_configured_channel(settings: Dict[str, Any]) -> Optional[discord.TextChannel]:
    cid = settings.get("channel_id")
    if not cid:
        return None
    return client.get_channel(int(cid))

# =====================================================
# SETTINGS UI — MODALS + VIEW (ON/OFF buttons + schedule modal)
# =====================================================
class EmojiSettingsModal(discord.ui.Modal, title="MapTap Reaction Emojis"):
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

    def __init__(self, view_ref: "MapTapSettingsView"):
        super().__init__()
        self.view_ref = view_ref
        em = self.view_ref.settings.get("emojis", {})
        self.recorded.default = str(em.get("recorded", DEFAULT_SETTINGS["emojis"]["recorded"]))
        self.too_high.default = str(em.get("too_high", DEFAULT_SETTINGS["emojis"]["too_high"]))
        self.rescan_ingested.default = str(em.get("rescan_ingested", DEFAULT_SETTINGS["emojis"]["rescan_ingested"]))

    async def on_submit(self, interaction: discord.Interaction):
        self.view_ref.settings.setdefault("emojis", {})
        self.view_ref.settings["emojis"]["recorded"] = str(self.recorded.value).strip()
        self.view_ref.settings["emojis"]["too_high"] = str(self.too_high.value).strip()
        self.view_ref.settings["emojis"]["rescan_ingested"] = str(self.rescan_ingested.value).strip()
        await self.view_ref.save_and_refresh(interaction, "MapTap: update reaction emojis")

class ScheduleSettingsModal(discord.ui.Modal, title="MapTap Schedule (UK)"):
    daily_post = discord.ui.TextInput(label="Daily post time (HH:MM)", placeholder="00:00", required=True, max_length=5)
    daily_scoreboard = discord.ui.TextInput(label="Daily scoreboard time (HH:MM)", placeholder="23:30", required=True, max_length=5)

    weekly_day = discord.ui.TextInput(label="Weekly roundup day (0=Mon … 6=Sun)", placeholder="6", required=True, max_length=1)
    weekly_time = discord.ui.TextInput(label="Weekly roundup time (HH:MM)", placeholder="23:45", required=True, max_length=5)

    rivalry_day = discord.ui.TextInput(label="Rivalry day (0=Mon … 6=Sun)", placeholder="4", required=True, max_length=1)
    rivalry_time = discord.ui.TextInput(label="Rivalry time (HH:MM)", placeholder="19:30", required=True, max_length=5)
    rivalry_gap = discord.ui.TextInput(label="Rivalry max gap (points)", placeholder="25", required=True, max_length=5)

    def __init__(self, view_ref: "MapTapSettingsView"):
        super().__init__()
        self.view_ref = view_ref
        sch = self.view_ref.settings.get("schedule", {})
        self.daily_post.default = str(sch.get("daily_post", DEFAULT_SETTINGS["schedule"]["daily_post"]))
        self.daily_scoreboard.default = str(sch.get("daily_scoreboard", DEFAULT_SETTINGS["schedule"]["daily_scoreboard"]))
        self.weekly_day.default = str(sch.get("weekly_day", DEFAULT_SETTINGS["schedule"]["weekly_day"]))
        self.weekly_time.default = str(sch.get("weekly_time", DEFAULT_SETTINGS["schedule"]["weekly_time"]))
        self.rivalry_day.default = str(sch.get("rivalry_day", DEFAULT_SETTINGS["schedule"]["rivalry_day"]))
        self.rivalry_time.default = str(sch.get("rivalry_time", DEFAULT_SETTINGS["schedule"]["rivalry_time"]))
        self.rivalry_gap.default = str(sch.get("rivalry_gap", DEFAULT_SETTINGS["schedule"]["rivalry_gap"]))

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
            await interaction.response.send_message("❌ Invalid time. Use **HH:MM** (24h), e.g. **23:30**.", ephemeral=True)
            return

        try:
            wd = int(str(self.weekly_day.value).strip())
            rd = int(str(self.rivalry_day.value).strip())
            gap = int(str(self.rivalry_gap.value).strip())
            if not (0 <= wd <= 6 and 0 <= rd <= 6 and gap >= 1):
                raise ValueError
        except Exception:
            await interaction.response.send_message("❌ Invalid day/gap. Days must be **0–6** and gap must be **>= 1**.", ephemeral=True)
            return

        self.view_ref.settings.setdefault("schedule", {})
        self.view_ref.settings["schedule"].update({
            "daily_post": dp,
            "daily_scoreboard": ds,
            "weekly_day": wd,
            "weekly_time": wt,
            "rivalry_day": rd,
            "rivalry_time": rt,
            "rivalry_gap": gap,
        })
        await self.view_ref.save_and_refresh(interaction, "MapTap: update schedule")

class ChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(
            placeholder="Select the MapTap channel",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        view: "MapTapSettingsView" = self.view  # type: ignore
        view.settings["channel_id"] = self.values[0].id
        await view.save_and_refresh(interaction, "MapTap: set channel")

class RoleSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(
            placeholder="Select admin roles (optional)",
            min_values=0,
            max_values=10
        )

    async def callback(self, interaction: discord.Interaction):
        view: "MapTapSettingsView" = self.view  # type: ignore
        view.settings["admin_role_ids"] = [r.id for r in self.values]
        await view.save_and_refresh(interaction, "MapTap: set admin roles")

class MapTapSettingsView(discord.ui.View):
    def __init__(self, settings: Dict[str, Any], sha: Optional[str]):
        super().__init__(timeout=300)
        self.settings = settings
        self.sha = sha

        # Selects
        self.add_item(ChannelSelect())
        self.add_item(RoleSelect())

        # Buttons
        self.btn_emojis = discord.ui.Button(label="Edit Reaction Emojis", style=discord.ButtonStyle.primary)
        self.btn_schedule = discord.ui.Button(label="Edit Schedule (UK)", style=discord.ButtonStyle.primary)

        self.btn_bot = discord.ui.Button()
        self.btn_daily_post = discord.ui.Button()
        self.btn_daily_board = discord.ui.Button()
        self.btn_weekly = discord.ui.Button()
        self.btn_rivalry = discord.ui.Button()

        self.btn_close = discord.ui.Button(label="Close", style=discord.ButtonStyle.danger)

        self.btn_emojis.callback = self._on_edit_emojis
        self.btn_schedule.callback = self._on_edit_schedule
        self.btn_bot.callback = self._on_toggle_bot
        self.btn_daily_post.callback = self._on_toggle_daily_post
        self.btn_daily_board.callback = self._on_toggle_daily_board
        self.btn_weekly.callback = self._on_toggle_weekly
        self.btn_rivalry.callback = self._on_toggle_rivalry
        self.btn_close.callback = self._on_close

        self.add_item(self.btn_emojis)
        self.add_item(self.btn_schedule)
        self.add_item(self.btn_bot)
        self.add_item(self.btn_daily_post)
        self.add_item(self.btn_daily_board)
        self.add_item(self.btn_weekly)
        self.add_item(self.btn_rivalry)
        self.add_item(self.btn_close)

        self._apply_toggle_labels()

    def _toggle_label_style(self, name: str, enabled: bool) -> Tuple[str, discord.ButtonStyle]:
        label = f"{name}: ON" if enabled else f"{name}: OFF"
        style = discord.ButtonStyle.success if enabled else discord.ButtonStyle.secondary
        return label, style

    def _apply_toggle_labels(self):
        label, style = self._toggle_label_style("Bot", bool(self.settings.get("enabled", True)))
        self.btn_bot.label, self.btn_bot.style = label, style

        label, style = self._toggle_label_style("Daily Post", bool(self.settings.get("daily_post_enabled", True)))
        self.btn_daily_post.label, self.btn_daily_post.style = label, style

        label, style = self._toggle_label_style("Daily Scoreboard", bool(self.settings.get("daily_scoreboard_enabled", True)))
        self.btn_daily_board.label, self.btn_daily_board.style = label, style

        label, style = self._toggle_label_style("Weekly Roundup", bool(self.settings.get("weekly_roundup_enabled", True)))
        self.btn_weekly.label, self.btn_weekly.style = label, style

        label, style = self._toggle_label_style("Rivalry", bool(self.settings.get("rivalry_enabled", True)))
        self.btn_rivalry.label, self.btn_rivalry.style = label, style

    def _embed(self) -> discord.Embed:
        channel_str = f"<#{self.settings['channel_id']}>" if self.settings.get("channel_id") else "Not set"
        roles = self.settings.get("admin_role_ids", [])
        roles_str = ", ".join(f"<@&{rid}>" for rid in roles) if roles else "Admins only"

        em = self.settings.get("emojis", {})
        emoji_block = (
            f"Recorded: {em.get('recorded', DEFAULT_SETTINGS['emojis']['recorded'])}\n"
            f"Too high: {em.get('too_high', DEFAULT_SETTINGS['emojis']['too_high'])}\n"
            f"Rescan ingested: {em.get('rescan_ingested', DEFAULT_SETTINGS['emojis']['rescan_ingested'])}"
        )

        sch = self.settings.get("schedule", DEFAULT_SETTINGS["schedule"])
        weekly_day_name = WEEKDAY_NAMES[int(sch.get("weekly_day", 6))]
        rivalry_day_name = WEEKDAY_NAMES[int(sch.get("rivalry_day", 4))]

        schedule_block = (
            f"Daily post: **{sch.get('daily_post','00:00')}**\n"
            f"Daily scoreboard: **{sch.get('daily_scoreboard','23:30')}**\n"
            f"Weekly roundup: **{weekly_day_name} {sch.get('weekly_time','23:45')}**\n"
            f"Rivalry: **{rivalry_day_name} {sch.get('rivalry_time','19:30')}** (gap ≤ **{sch.get('rivalry_gap',25)}**)"
        )

        e = discord.Embed(
            title="🗺️ MapTap Settings",
            description=(
                f"**Channel:** {channel_str}\n"
                f"**Admin roles:** {roles_str}\n\n"
                f"**Schedule (UK):**\n{schedule_block}\n\n"
                f"**Reactions:**\n{emoji_block}"
            ),
            color=0xF1C40F
        )
        e.set_footer(text="Changes save to GitHub immediately.")
        return e

    async def save_and_refresh(self, interaction: discord.Interaction, message: str):
        current, current_sha = load_settings()
        current.update(self.settings)

        # Ensure nested dicts exist and are merged
        current["emojis"] = _merge_dict(DEFAULT_SETTINGS["emojis"], current.get("emojis"))
        current["schedule"] = _merge_dict(DEFAULT_SETTINGS["schedule"], current.get("schedule"))
        current["last_run"] = _merge_dict(DEFAULT_SETTINGS["last_run"], current.get("last_run"))

        new_sha = save_settings(current, current_sha, message)
        self.settings = current
        self.sha = new_sha or current_sha

        self._apply_toggle_labels()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def _on_edit_emojis(self, interaction: discord.Interaction):
        await interaction.response.send_modal(EmojiSettingsModal(self))

    async def _on_edit_schedule(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ScheduleSettingsModal(self))

    async def _on_toggle_bot(self, interaction: discord.Interaction):
        self.settings["enabled"] = not bool(self.settings.get("enabled", True))
        await self.save_and_refresh(interaction, "MapTap: toggle bot")

    async def _on_toggle_daily_post(self, interaction: discord.Interaction):
        self.settings["daily_post_enabled"] = not bool(self.settings.get("daily_post_enabled", True))
        await self.save_and_refresh(interaction, "MapTap: toggle daily post")

    async def _on_toggle_daily_board(self, interaction: discord.Interaction):
        self.settings["daily_scoreboard_enabled"] = not bool(self.settings.get("daily_scoreboard_enabled", True))
        await self.save_and_refresh(interaction, "MapTap: toggle daily scoreboard")

    async def _on_toggle_weekly(self, interaction: discord.Interaction):
        self.settings["weekly_roundup_enabled"] = not bool(self.settings.get("weekly_roundup_enabled", True))
        await self.save_and_refresh(interaction, "MapTap: toggle weekly roundup")

    async def _on_toggle_rivalry(self, interaction: discord.Interaction):
        self.settings["rivalry_enabled"] = not bool(self.settings.get("rivalry_enabled", True))
        await self.save_and_refresh(interaction, "MapTap: toggle rivalry")

    async def _on_close(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="✅ Closed.", embed=None, view=None)

# =====================================================
# SLASH COMMAND: /maptapsettings
# =====================================================
@client.tree.command(name="maptapsettings", description="Configure MapTap settings")
async def maptapsettings(interaction: discord.Interaction):
    settings, sha = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Use this in the server, not DMs.", ephemeral=True)
        return

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission to manage MapTap settings.", ephemeral=True)
        return

    view = MapTapSettingsView(settings, sha)
    await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=False)

# =====================================================
# SCORE INGESTION (message listener)
# =====================================================
@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    settings, _ = load_settings()
    if not settings.get("enabled", True):
        return

    channel_id = settings.get("channel_id")
    if not channel_id or message.channel.id != int(channel_id):
        return

    m = SCORE_REGEX.search(message.content or "")
    if not m:
        return

    em = settings.get("emojis", DEFAULT_SETTINGS["emojis"])

    score = int(m.group(1))
    if score > MAX_SCORE:
        await react_safe(message, em.get("too_high", "❌"), "❌")
        return

    # Use message timestamp in UK time so midnight counts for the correct day
    msg_time_uk = message.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
    date_key = today_key(msg_time_uk)
    user_id = str(message.author.id)

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    if not isinstance(scores, dict):
        scores = {}
    if not isinstance(users, dict):
        users = {}

    scores.setdefault(date_key, {})
    day_bucket = scores[date_key]

    user_stats = users.setdefault(user_id, {
        "total_points": 0,
        "days_played": 0,
        "best_streak": 0,
        "best_day": None,
        "best_day_score": 0,
    })

    prev_entry = day_bucket.get(user_id)
    if prev_entry and isinstance(prev_entry, dict) and "score" in prev_entry:
        try:
            user_stats["total_points"] -= int(prev_entry["score"])
        except Exception:
            pass
    else:
        user_stats["days_played"] += 1

    user_stats["total_points"] += score

    # Best day ever
    if score > int(user_stats.get("best_day_score", 0)):
        user_stats["best_day_score"] = score
        user_stats["best_day"] = date_key

    day_bucket[user_id] = {"score": score, "updated_at": msg_time_uk.isoformat()}

    # Best streak (based on score history kept)
    cur_streak = calculate_current_streak(scores, user_id)
    if cur_streak > int(user_stats.get("best_streak", 0)):
        user_stats["best_streak"] = cur_streak

    github_save_json(SCORES_PATH, scores, scores_sha, f"MapTap: score update {date_key}")
    github_save_json(USERS_PATH, users, users_sha, f"MapTap: user stats update {user_id}")

    await react_safe(message, em.get("recorded", "🌏"), "✅")

# =====================================================
# SLASH COMMAND: /mymaptap (public) + optional user
# =====================================================
@client.tree.command(name="mymaptap", description="View MapTap stats")
@app_commands.describe(user="View stats for another user")
async def mymaptap(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})

    if not isinstance(users, dict):
        users = {}
    if not isinstance(scores, dict):
        scores = {}

    target = user or interaction.user
    uid = str(target.id)
    stats = users.get(uid)

    if not stats or int(stats.get("days_played", 0)) <= 0:
        await interaction.response.send_message(f"{target.display_name} hasn’t recorded any MapTap scores yet 🗺️", ephemeral=False)
        return

    cur = calculate_current_streak(scores, uid)
    avg = round(int(stats.get("total_points", 0)) / max(1, int(stats.get("days_played", 0))))
    rank, total_players = calculate_all_time_rank(users, uid)

    best_day = stats.get("best_day")
    best_day_score = int(stats.get("best_day_score", 0))
    best_day_txt = f"{pretty_day(best_day)} (**{best_day_score}**)" if isinstance(best_day, str) else "—"

    await interaction.response.send_message(
        f"🗺️ **MapTap Stats — {target.display_name}**\n\n"
        f"• Server Rank: 🏅 **#{rank} of {total_players}**\n"
        f"• Total points (all-time): **{stats.get('total_points', 0)}**\n"
        f"• Days played: **{stats.get('days_played', 0)}**\n"
        f"• Average score: **{avg}**\n"
        f"• Best day ever: 🌟 {best_day_txt}\n"
        f"• Current streak: 🔥 **{cur} days**\n"
        f"• Best streak: 🏆 **{stats.get('best_streak', 0)} days**",
        ephemeral=False
    )

# =====================================================
# SLASH COMMAND: /rescan (admin-only) — no duplicate reaction
# =====================================================
@client.tree.command(name="rescan", description="Re-scan recent MapTap messages for missed scores (admin only)")
@app_commands.describe(messages="How many recent messages to scan (max 100)")
async def rescan(interaction: discord.Interaction, messages: int = 10):
    settings, _ = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission to run this.", ephemeral=True)
        return

    ch = get_configured_channel(settings)
    if not ch:
        await interaction.response.send_message("❌ MapTap channel is not configured.", ephemeral=True)
        return

    messages = max(1, min(messages, 100))
    await interaction.response.send_message(f"🔍 Scanning the last **{messages}** messages…", ephemeral=True)

    em = settings.get("emojis", DEFAULT_SETTINGS["emojis"])

    scanned = 0
    ingested = 0
    skipped = 0

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    if not isinstance(scores, dict):
        scores = {}
    if not isinstance(users, dict):
        users = {}

    async for msg in ch.history(limit=messages):
        if msg.author.bot or not msg.content:
            continue

        match = SCORE_REGEX.search(msg.content)
        if not match:
            continue

        scanned += 1
        score = int(match.group(1))

        if score > MAX_SCORE:
            skipped += 1
            # optional: react too_high on rescan for visibility
            await react_safe(msg, em.get("too_high", "❌"), "❌")
            continue

        msg_time_uk = msg.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
        date_key = today_key(msg_time_uk)
        uid = str(msg.author.id)

        scores.setdefault(date_key, {})
        day_bucket = scores[date_key]

        # Silent duplicate skip
        if uid in day_bucket:
            skipped += 1
            continue

        user_stats = users.setdefault(uid, {
            "total_points": 0,
            "days_played": 0,
            "best_streak": 0,
            "best_day": None,
            "best_day_score": 0,
        })

        user_stats["days_played"] += 1
        user_stats["total_points"] += score

        # Best day ever
        if score > int(user_stats.get("best_day_score", 0)):
            user_stats["best_day_score"] = score
            user_stats["best_day"] = date_key

        day_bucket[uid] = {"score": score, "updated_at": msg_time_uk.isoformat()}
        ingested += 1
        await react_safe(msg, em.get("rescan_ingested", "🔁"), "🔁")

    github_save_json(SCORES_PATH, scores, scores_sha, f"MapTap: rescan last {messages} messages")
    github_save_json(USERS_PATH, users, users_sha, "MapTap: rescan user stats")

    await interaction.followup.send(
        "✅ **Rescan complete**\n"
        f"• Matches found: **{scanned}**\n"
        f"• Newly ingested: **{ingested}**\n"
        f"• Skipped: **{skipped}**",
        ephemeral=True
    )

# =====================================================
# SCHEDULED ACTIONS (called by scheduler_tick)
# =====================================================
async def do_daily_post(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if ch:
        await ch.send(build_daily_prompt())

async def do_daily_scoreboard(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        scores = {}

    date_key = today_key()
    today_scores = scores.get(date_key, {})

    rows: List[Tuple[str, int]] = []
    if isinstance(today_scores, dict):
        for uid, entry in today_scores.items():
            if isinstance(entry, dict) and "score" in entry:
                try:
                    rows.append((uid, int(entry["score"])))
                except Exception:
                    pass
    rows.sort(key=lambda x: x[1], reverse=True)

    await ch.send(build_daily_scoreboard_text(date_key, rows))

    # cleanup rolling history
    cleaned = cleanup_old_scores(scores, CLEANUP_DAYS)
    if cleaned != scores:
        github_save_json(SCORES_PATH, cleaned, scores_sha, f"MapTap: cleanup keep {CLEANUP_DAYS} days")

async def do_weekly_roundup(settings: Dict[str, Any], now: datetime):
    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, _ = github_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        scores = {}

    mon = monday_of_week(now)
    sun = mon + timedelta(days=6)
    week_dates = [(mon + timedelta(days=i)).isoformat() for i in range(7)]

    weekly: Dict[str, Dict[str, int]] = {}
    for dkey in week_dates:
        day_bucket = scores.get(dkey, {})
        if not isinstance(day_bucket, dict):
            continue
        for uid, entry in day_bucket.items():
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

async def do_rivalry(settings: Dict[str, Any], now: datetime):
    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, _ = github_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        scores = {}

    sch = settings.get("schedule", DEFAULT_SETTINGS["schedule"])
    gap_limit = int(sch.get("rivalry_gap", DEFAULT_SETTINGS["schedule"]["rivalry_gap"]))

    # This week = Mon..Sun
    mon = monday_of_week(now)
    week_dates = [(mon + timedelta(days=i)).isoformat() for i in range(7)]

    weekly_totals: Dict[str, int] = {}
    for dkey in week_dates:
        day_bucket = scores.get(dkey, {})
        if not isinstance(day_bucket, dict):
            continue
        for uid, entry in day_bucket.items():
            if not isinstance(entry, dict) or "score" not in entry:
                continue
            try:
                weekly_totals[uid] = weekly_totals.get(uid, 0) + int(entry["score"])
            except Exception:
                pass

    # avoid spam if only a couple players
    if len(weekly_totals) < 5:
        return

    ranked = sorted(weekly_totals.items(), key=lambda x: x[1], reverse=True)
    top = ranked[:5]

    # choose closest adjacent pair within top5
    best_pair: Optional[Tuple[str, int, str, int, int]] = None  # (leader_uid, leader_score, chaser_uid, chaser_score, gap)

    for i in range(len(top) - 1):
        (u1, s1) = top[i]
        (u2, s2) = top[i + 1]
        gap = abs(s1 - s2)
        if best_pair is None or gap < best_pair[4]:
            # leader should be higher score
            if s1 >= s2:
                best_pair = (u1, s1, u2, s2, gap)
            else:
                best_pair = (u2, s2, u1, s1, gap)

    if not best_pair:
        return

    leader_uid, _, chaser_uid, _, gap = best_pair
    if gap <= gap_limit:
        await ch.send(build_rivalry_message(leader_uid, chaser_uid, gap))

# =====================================================
# SCHEDULER (runs every minute, uses settings schedule)
# =====================================================
@tasks.loop(minutes=1)
async def scheduler_tick():
    settings, sha = load_settings()
    if not settings.get("enabled", True):
        return

    now = datetime.now(UK_TZ)
    hhmm = now.strftime("%H:%M")
    today = today_key(now)

    sch = settings.get("schedule", DEFAULT_SETTINGS["schedule"])
    last_run = settings.get("last_run", DEFAULT_SETTINGS["last_run"])

    fired = False

    # Daily post
    if settings.get("daily_post_enabled", True) and hhmm == sch.get("daily_post", "00:00"):
        if last_run.get("daily_post") != today:
            await do_daily_post(settings)
            settings["last_run"]["daily_post"] = today
            fired = True

    # Daily scoreboard
    if settings.get("daily_scoreboard_enabled", True) and hhmm == sch.get("daily_scoreboard", "23:30"):
        if last_run.get("daily_scoreboard") != today:
            await do_daily_scoreboard(settings)
            settings["last_run"]["daily_scoreboard"] = today
            fired = True

    # Weekly roundup (configured day)
    if settings.get("weekly_roundup_enabled", True):
        if now.weekday() == int(sch.get("weekly_day", 6)) and hhmm == sch.get("weekly_time", "23:45"):
            if last_run.get("weekly_roundup") != today:
                await do_weekly_roundup(settings, now)
                settings["last_run"]["weekly_roundup"] = today
                fired = True

    # Rivalry (configured day)
    if settings.get("rivalry_enabled", True):
        if now.weekday() == int(sch.get("rivalry_day", 4)) and hhmm == sch.get("rivalry_time", "19:30"):
            if last_run.get("rivalry") != today:
                await do_rivalry(settings, now)
                settings["last_run"]["rivalry"] = today
                fired = True

    # Save settings only if we fired something (avoid constant GitHub writes)
    if fired:
        try:
            save_settings(settings, sha, f"MapTap: update last_run {today} {hhmm}")
        except Exception as e:
            print("Failed to save last_run:", e)

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