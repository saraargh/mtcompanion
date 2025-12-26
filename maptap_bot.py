# =========================
# MapTap Companion Bot (FULL FILE)
# Chunk 1/5
# =========================

from __future__ import annotations

import os
import json
import re
import base64
import requests
import random
from datetime import datetime, timedelta, date
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

# Admin-only destructive action password (used in Settings UI, NOT a slash command)
RESET_PASSWORD = os.getenv("RESET_PASSWORD", "")

# ---------------------------------------------
# Parsing
# ---------------------------------------------
# Final score line
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)

# We ONLY want zero detection from the round-scores line, NOT the final score.
# Example post line:
# "98🎯 97🔥 97🔥 95🏅 90👑"
#
# We detect literal "0" that is followed by a NON-DIGIT (emoji / space / end),
# so we don't accidentally match 90, 100, etc.
ROUND_ZERO_REGEX = re.compile(r"(^|\s)0(?!\d)")

# Extract all round scores (0-1000) from the "rounds" line(s).
# This grabs integers that are directly followed by a non-digit (emoji etc).
ROUND_SCORE_TOKEN_REGEX = re.compile(r"(^|\s)(\d{1,4})(?!\d)")

# Basic "is this a MapTap share?" check (helps avoid false positives)
MAPTAP_HINT_REGEX = re.compile(r"\bmaptap\.gg\b", re.IGNORECASE)


# =====================================================
# DEFAULT SETTINGS (GitHub-backed)
# =====================================================
DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,
    "admin_role_ids": [],

    # Alerts / toggles (all in Configure Alerts modal)
    "alerts": {
        "daily_post_enabled": True,
        "daily_scoreboard_enabled": True,
        "weekly_roundup_enabled": True,
        "rivalry_enabled": True,
        "monthly_leaderboard_enabled": True,   # moved into alerts modal
        "zero_score_roasts_enabled": True,     # moved into alerts modal
        "pb_messages_enabled": True,
        "perfect_score_enabled": True,
    },

    # Emoji defaults (configurable)
    "emojis": {
        "recorded": "🌏",
        "too_high": "❌",
        "rescan_ingested": "🔁",
        "config_issue": "⚠️",
    },

    # Times (UK time, 24h HH:MM)
    "times": {
        "daily_post": "00:00",
        "daily_scoreboard": "23:30",
        "weekly_roundup": "23:45",
        "rivalry": "14:00",
        "monthly_leaderboard": "00:10",  # 1st of month
    },

    # Leaderboard minimum-day rules (exactly what you wanted)
    "minimum_days": {
        "this_week": 3,
        "this_month": 10,
        "all_time": 10,
        "date_range": 0,  # flexible/no minimum
    },

    # Prevent double-posting
    "last_run": {
        "daily_post": None,         # YYYY-MM-DD
        "daily_scoreboard": None,   # YYYY-MM-DD
        "weekly_roundup": None,     # YYYY-MM-DD (Sunday date)
        "rivalry": None,            # YYYY-MM-DD
        "monthly_leaderboard": None # YYYY-MM-DD (the day it ran)
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
    """
    Returns (data, sha). If file not found, returns (default, None).
    """
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
    """
    Writes JSON to GitHub via Contents API. Returns new sha from response.
    """
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
    
# =========================
# MapTap Companion Bot (FULL FILE)
# Chunk 2/5
# =========================

import random  # used for roasts / fun replies


# =====================================================
# SETTINGS HELPERS
# =====================================================
def _merge_nested(default: Dict[str, Any], incoming: Any) -> Dict[str, Any]:
    merged = dict(default)
    if isinstance(incoming, dict):
        merged.update(incoming)
    return merged

def _normalize_hhmm(value: Any, fallback: str) -> str:
    s = str(value).strip()
    try:
        datetime.strptime(s, "%H:%M")
        return s
    except Exception:
        return fallback

def load_settings() -> Tuple[Dict[str, Any], Optional[str]]:
    settings, sha = github_load_json(SETTINGS_PATH, DEFAULT_SETTINGS.copy())

    merged = DEFAULT_SETTINGS.copy()
    if isinstance(settings, dict):
        merged.update(settings)

    # Normalize ids
    if merged.get("channel_id") is not None:
        try:
            merged["channel_id"] = int(merged["channel_id"])
        except Exception:
            merged["channel_id"] = None

    merged["admin_role_ids"] = [int(x) for x in merged.get("admin_role_ids", []) if str(x).isdigit()]

    # Merge nested blocks safely
    merged["alerts"] = _merge_nested(DEFAULT_SETTINGS["alerts"], merged.get("alerts"))
    merged["emojis"] = _merge_nested(DEFAULT_SETTINGS["emojis"], merged.get("emojis"))
    merged["minimum_days"] = _merge_nested(DEFAULT_SETTINGS["minimum_days"], merged.get("minimum_days"))

    times_in = _merge_nested(DEFAULT_SETTINGS["times"], merged.get("times"))
    merged["times"] = {
        "daily_post": _normalize_hhmm(times_in.get("daily_post"), DEFAULT_SETTINGS["times"]["daily_post"]),
        "daily_scoreboard": _normalize_hhmm(times_in.get("daily_scoreboard"), DEFAULT_SETTINGS["times"]["daily_scoreboard"]),
        "weekly_roundup": _normalize_hhmm(times_in.get("weekly_roundup"), DEFAULT_SETTINGS["times"]["weekly_roundup"]),
        "rivalry": _normalize_hhmm(times_in.get("rivalry"), DEFAULT_SETTINGS["times"]["rivalry"]),
        "monthly_leaderboard": _normalize_hhmm(times_in.get("monthly_leaderboard"), DEFAULT_SETTINGS["times"]["monthly_leaderboard"]),
    }

    merged["last_run"] = _merge_nested(DEFAULT_SETTINGS["last_run"], merged.get("last_run"))
    if not isinstance(merged["last_run"], dict):
        merged["last_run"] = DEFAULT_SETTINGS["last_run"].copy()

    return merged, sha

def save_settings(settings: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    return github_save_json(SETTINGS_PATH, settings, sha, message)

def today_key(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(UK_TZ)
    return dt.date().isoformat()

def pretty_day(date_key: str) -> str:
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")

def monday_of_week(d: datetime) -> date:
    return d.date() - timedelta(days=d.weekday())


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
            self.scheduler_tick.start()
        except Exception as e:
            print("Command sync failed:", e)

    @tasks.loop(minutes=1)
    async def scheduler_tick(self):
        settings, sha = load_settings()
        if not settings.get("enabled", True):
            return

        now = datetime.now(UK_TZ)
        now_hm = now.strftime("%H:%M")
        today = today_key(now)

        times = settings.get("times", {})
        last_run = settings.get("last_run", {})
        alerts = settings.get("alerts", {})

        # Daily Post
        if alerts.get("daily_post_enabled", True) and now_hm == times.get("daily_post") and last_run.get("daily_post") != today:
            await do_daily_post(settings)
            settings["last_run"]["daily_post"] = today
            save_settings(settings, sha, "MapTap: auto daily post")

        # Daily Scoreboard
        if alerts.get("daily_scoreboard_enabled", True) and now_hm == times.get("daily_scoreboard") and last_run.get("daily_scoreboard") != today:
            await do_daily_scoreboard(settings)
            settings["last_run"]["daily_scoreboard"] = today
            save_settings(settings, sha, "MapTap: auto daily scoreboard")

        # Weekly Roundup (Sundays)
        if alerts.get("weekly_roundup_enabled", True) and now.weekday() == 6 and now_hm == times.get("weekly_roundup") and last_run.get("weekly_roundup") != today:
            await do_weekly_roundup(settings)
            settings["last_run"]["weekly_roundup"] = today
            save_settings(settings, sha, "MapTap: auto weekly roundup")

        # Rivalry Alerts (Saturdays)
        if alerts.get("rivalry_enabled", True) and now.weekday() == 5 and now_hm == times.get("rivalry") and last_run.get("rivalry") != today:
            await do_rivalry_alert(settings)
            settings["last_run"]["rivalry"] = today
            save_settings(settings, sha, "MapTap: auto rivalry alert")

        # Monthly Leaderboard (1st day of month)
        if alerts.get("monthly_leaderboard_enabled", True) and now.day == 1 and now_hm == times.get("monthly_leaderboard") and last_run.get("monthly_leaderboard") != today:
            await do_monthly_leaderboard(settings)
            settings["last_run"]["monthly_leaderboard"] = today
            save_settings(settings, sha, "MapTap: auto monthly leaderboard")

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
# SETTINGS UI
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
        self.recorded.default = str(em.get("recorded", "🌏"))
        self.too_high.default = str(em.get("too_high", "❌"))
        self.rescan_ingested.default = str(em.get("rescan_ingested", "🔁"))

    async def on_submit(self, interaction: discord.Interaction):
        self.view_ref.settings.setdefault("emojis", {})
        self.view_ref.settings["emojis"]["recorded"] = str(self.recorded.value).strip()
        self.view_ref.settings["emojis"]["too_high"] = str(self.too_high.value).strip()
        self.view_ref.settings["emojis"]["rescan_ingested"] = str(self.rescan_ingested.value).strip()
        await self.view_ref._save_refresh(interaction, "MapTap: update reaction emojis")


class TimeSettingsModal(discord.ui.Modal, title="MapTap Scheduled Times (UK)"):
    daily_post = discord.ui.TextInput(label="Daily post (HH:MM)", placeholder="00:00", required=True, max_length=5)
    daily_scoreboard = discord.ui.TextInput(label="Daily scoreboard (HH:MM)", placeholder="23:30", required=True, max_length=5)
    weekly_roundup = discord.ui.TextInput(label="Weekly roundup (Sun) (HH:MM)", placeholder="23:45", required=True, max_length=5)
    rivalry = discord.ui.TextInput(label="Rivalry alert (Sat) (HH:MM)", placeholder="14:00", required=True, max_length=5)
    monthly_leaderboard = discord.ui.TextInput(label="Monthly leaderboard (1st) (HH:MM)", placeholder="00:10", required=True, max_length=5)

    def __init__(self, view_ref: "MapTapSettingsView"):
        super().__init__()
        self.view_ref = view_ref
        t = self.view_ref.settings.get("times", {})
        self.daily_post.default = str(t.get("daily_post", "00:00"))
        self.daily_scoreboard.default = str(t.get("daily_scoreboard", "23:30"))
        self.weekly_roundup.default = str(t.get("weekly_roundup", "23:45"))
        self.rivalry.default = str(t.get("rivalry", "14:00"))
        self.monthly_leaderboard.default = str(t.get("monthly_leaderboard", "00:10"))

    async def on_submit(self, interaction: discord.Interaction):
        vals = {
            "daily_post": str(self.daily_post.value).strip(),
            "daily_scoreboard": str(self.daily_scoreboard.value).strip(),
            "weekly_roundup": str(self.weekly_roundup.value).strip(),
            "rivalry": str(self.rivalry.value).strip(),
            "monthly_leaderboard": str(self.monthly_leaderboard.value).strip(),
        }
        try:
            for v in vals.values():
                datetime.strptime(v, "%H:%M")
        except Exception:
            await interaction.response.send_message("❌ Invalid time. Use **HH:MM** (24h).", ephemeral=True)
            return

        self.view_ref.settings.setdefault("times", {})
        self.view_ref.settings["times"].update(vals)
        await self.view_ref._save_refresh(interaction, "MapTap: update scheduled times")


def _yn_options(current: bool) -> List[discord.SelectOption]:
    return [
        discord.SelectOption(label="On", value="on", default=bool(current)),
        discord.SelectOption(label="Off", value="off", default=not bool(current)),
    ]

class ConfigureAlertsView(discord.ui.View):
    """
    This is the ONLY place alert toggles live.
    Monthly leaderboard toggle is here (not its own button).
    """
    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__(timeout=240)
        self.settings_view = settings_view

        self._pending = dict(self.settings_view.settings.get("alerts", {}))

        self.daily_post.options = _yn_options(self._pending.get("daily_post_enabled", True))
        self.daily_scoreboard.options = _yn_options(self._pending.get("daily_scoreboard_enabled", True))
        self.weekly_roundup.options = _yn_options(self._pending.get("weekly_roundup_enabled", True))
        self.rivalry.options = _yn_options(self._pending.get("rivalry_enabled", True))
        self.monthly_lb.options = _yn_options(self._pending.get("monthly_leaderboard_enabled", True))
        self.zero_roasts.options = _yn_options(self._pending.get("zero_score_roasts_enabled", True))
        self.pb_msgs.options = _yn_options(self._pending.get("pb_messages_enabled", True))
        self.perfect_score.options = _yn_options(self._pending.get("perfect_score_enabled", True))

    def _set(self, key: str, val: str):
        self._pending[key] = (val == "on")

    @discord.ui.select(placeholder="Daily post", min_values=1, max_values=1, options=[])
    async def daily_post(self, interaction: discord.Interaction, select: discord.ui.Select):
        self._set("daily_post_enabled", select.values[0])
        await interaction.response.defer()

    @discord.ui.select(placeholder="Daily scoreboard", min_values=1, max_values=1, options=[])
    async def daily_scoreboard(self, interaction: discord.Interaction, select: discord.ui.Select):
        self._set("daily_scoreboard_enabled", select.values[0])
        await interaction.response.defer()

    @discord.ui.select(placeholder="Weekly roundup", min_values=1, max_values=1, options=[])
    async def weekly_roundup(self, interaction: discord.Interaction, select: discord.ui.Select):
        self._set("weekly_roundup_enabled", select.values[0])
        await interaction.response.defer()

    @discord.ui.select(placeholder="Rivalry alerts", min_values=1, max_values=1, options=[])
    async def rivalry(self, interaction: discord.Interaction, select: discord.ui.Select):
        self._set("rivalry_enabled", select.values[0])
        await interaction.response.defer()

    @discord.ui.select(placeholder="Monthly leaderboard", min_values=1, max_values=1, options=[])
    async def monthly_lb(self, interaction: discord.Interaction, select: discord.ui.Select):
        self._set("monthly_leaderboard_enabled", select.values[0])
        await interaction.response.defer()

    @discord.ui.select(placeholder="Zero-score roasts", min_values=1, max_values=1, options=[])
    async def zero_roasts(self, interaction: discord.Interaction, select: discord.ui.Select):
        self._set("zero_score_roasts_enabled", select.values[0])
        await interaction.response.defer()

    @discord.ui.select(placeholder="Personal best messages", min_values=1, max_values=1, options=[])
    async def pb_msgs(self, interaction: discord.Interaction, select: discord.ui.Select):
        self._set("pb_messages_enabled", select.values[0])
        await interaction.response.defer()

    @discord.ui.select(placeholder="Perfect score messages", min_values=1, max_values=1, options=[])
    async def perfect_score(self, interaction: discord.Interaction, select: discord.ui.Select):
        self._set("perfect_score_enabled", select.values[0])
        await interaction.response.defer()

    @discord.ui.button(label="Save alerts", style=discord.ButtonStyle.primary)
    async def save_alerts(self, interaction: discord.Interaction, _):
        self.settings_view.settings.setdefault("alerts", {})
        self.settings_view.settings["alerts"].update(self._pending)
        await self.settings_view._save_refresh(interaction, "MapTap: update alert toggles")


class ResetPasswordModal(discord.ui.Modal, title="Reset MapTap Data"):
    password = discord.ui.TextInput(
        label="Admin password",
        placeholder="Enter reset password",
        required=True,
        max_length=128
    )

    def __init__(self, view_ref: "MapTapSettingsView"):
        super().__init__()
        self.view_ref = view_ref

    async def on_submit(self, interaction: discord.Interaction):
        expected = (RESET_PASSWORD or "").strip()
        if not expected:
            await interaction.response.send_message("❌ RESET_PASSWORD is not set.", ephemeral=True)
            return

        if str(self.password.value).strip() != expected:
            await interaction.response.send_message("❌ Wrong password.", ephemeral=True)
            return

        await interaction.response.send_modal(ResetConfirmModal(self.view_ref))


class ResetConfirmModal(discord.ui.Modal, title="Confirm Reset"):
    confirm = discord.ui.TextInput(
        label="Type DELETE to confirm",
        placeholder="DELETE",
        required=True,
        max_length=16
    )

    def __init__(self, view_ref: "MapTapSettingsView"):
        super().__init__()
        self.view_ref = view_ref

    async def on_submit(self, interaction: discord.Interaction):
        if str(self.confirm.value).strip().upper() != "DELETE":
            await interaction.response.send_message("❌ Cancelled.", ephemeral=True)
            return

        scores, scores_sha = github_load_json(SCORES_PATH, {})
        users, users_sha = github_load_json(USERS_PATH, {})

        github_save_json(SCORES_PATH, {}, scores_sha, "MapTap: RESET scores")
        github_save_json(USERS_PATH, {}, users_sha, "MapTap: RESET users")

        ch = get_configured_channel(self.view_ref.settings)
        if ch:
            await ch.send(f"⚠️ **{interaction.user.display_name}** reset all MapTap scores.")

        await interaction.response.send_message("✅ Reset complete.", ephemeral=True)


class MapTapSettingsView(discord.ui.View):
    def __init__(self, settings: Dict[str, Any], sha: Optional[str]):
        super().__init__(timeout=300)
        self.settings = settings
        self.sha = sha

    def _embed(self) -> discord.Embed:
        channel_str = f"<#{self.settings['channel_id']}>" if self.settings.get("channel_id") else "Not set"
        roles = self.settings.get("admin_role_ids", [])
        roles_str = ", ".join(f"<@&{rid}>" for rid in roles) if roles else "Admins only"

        em = self.settings.get("emojis", {})
        emoji_block = (
            f"Recorded: {em.get('recorded','🌏')}\n"
            f"Too high: {em.get('too_high','❌')}\n"
            f"Rescan ingested: {em.get('rescan_ingested','🔁')}"
        )

        t = self.settings.get("times", {})
        times_block = (
            f"Daily post: {t.get('daily_post','00:00')}\n"
            f"Daily scoreboard: {t.get('daily_scoreboard','23:30')}\n"
            f"Weekly roundup (Sun): {t.get('weekly_roundup','23:45')}\n"
            f"Rivalry (Sat): {t.get('rivalry','14:00')}\n"
            f"Monthly leaderboard (1st): {t.get('monthly_leaderboard','00:10')}"
        )

        a = self.settings.get("alerts", {})
        status_block = (
            f"Bot: {'✅' if self.settings.get('enabled') else '❌'}\n"
            f"Daily post: {'✅' if a.get('daily_post_enabled') else '❌'}\n"
            f"Daily scoreboard: {'✅' if a.get('daily_scoreboard_enabled') else '❌'}\n"
            f"Weekly roundup: {'✅' if a.get('weekly_roundup_enabled') else '❌'}\n"
            f"Rivalry: {'✅' if a.get('rivalry_enabled') else '❌'}\n"
            f"Monthly leaderboard: {'✅' if a.get('monthly_leaderboard_enabled') else '❌'}\n"
            f"Zero roasts: {'✅' if a.get('zero_score_roasts_enabled') else '❌'}\n"
            f"PB messages: {'✅' if a.get('pb_messages_enabled') else '❌'}\n"
            f"Perfect score: {'✅' if a.get('perfect_score_enabled') else '❌'}"
        )

        e = discord.Embed(title="🗺️ MapTap Settings", color=0xF1C40F)
        e.description = f"Channel: {channel_str}\nAdmin roles: {roles_str}"
        e.add_field(name="Status", value=status_block, inline=False)
        e.add_field(name="Times (UK)", value=times_block, inline=False)
        e.add_field(name="Reactions", value=emoji_block, inline=False)
        e.set_footer(text="Changes save to GitHub immediately.")
        return e

    async def _save_refresh(self, interaction: discord.Interaction, message: str):
        current, current_sha = load_settings()
        current.update(self.settings)
        new_sha = save_settings(current, current_sha, message)
        self.settings = current
        self.sha = new_sha or current_sha
        await interaction.response.edit_message(embed=self._embed(), view=self)

    # ---- Channel selector
    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        placeholder="Select the MapTap channel",
        channel_types=[discord.ChannelType.text],
        min_values=1,
        max_values=1
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        self.settings["channel_id"] = select.values[0].id
        await self._save_refresh(interaction, "MapTap: set channel")

    # ---- Role selector
    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="Select admin roles (optional)",
        min_values=0,
        max_values=10
    )
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        self.settings["admin_role_ids"] = [r.id for r in select.values]
        await self._save_refresh(interaction, "MapTap: set admin roles")

    @discord.ui.button(label="Edit times (UK)", style=discord.ButtonStyle.primary)
    async def edit_times(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(TimeSettingsModal(self))

    @discord.ui.button(label="Configure alerts", style=discord.ButtonStyle.primary)
    async def configure_alerts(self, interaction: discord.Interaction, _):
        view = ConfigureAlertsView(self)
        embed = discord.Embed(
            title="🔔 Configure Alerts",
            description="Use the dropdowns, then press **Save alerts**.",
            color=0x5865F2
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Edit reaction emojis", style=discord.ButtonStyle.secondary)
    async def edit_emojis(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(EmojiSettingsModal(self))

    @discord.ui.button(label="Toggle bot enabled", style=discord.ButtonStyle.secondary)
    async def toggle_bot(self, interaction: discord.Interaction, _):
        self.settings["enabled"] = not bool(self.settings.get("enabled", True))
        await self._save_refresh(interaction, "MapTap: toggle enabled")

    @discord.ui.button(label="Reset MapTap data", style=discord.ButtonStyle.danger)
    async def reset_data(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(ResetPasswordModal(self))

# =========================
# MapTap Companion Bot (FULL FILE)
# Chunk 3/5
# =========================

# =====================================================
# SAFE REACTION HELPER
# =====================================================
async def react_safe(msg: discord.Message, emoji: str, fallback: str = "✅"):
    try:
        await msg.add_reaction(emoji)
    except Exception:
        try:
            await msg.add_reaction(fallback)
        except Exception:
            pass


# =====================================================
# STATS / RANK HELPERS
# =====================================================
def calculate_current_streak(scores: Dict[str, Any], user_id: str) -> int:
    played_dates: List[date] = []
    for date_key, day in scores.items():
        if isinstance(day, dict) and user_id in day:
            try:
                played_dates.append(datetime.strptime(date_key, "%Y-%m-%d").date())
            except Exception:
                pass

    if not played_dates:
        return 0

    played = set(played_dates)
    d = datetime.now(UK_TZ).date()
    streak = 0
    while d in played:
        streak += 1
        d -= timedelta(days=1)
    return streak


def eligible_users(users: Dict[str, Any]) -> Dict[str, Any]:
    """
    Only users who have actually played at least once.
    Fixes '#1 of #1' nonsense.
    """
    return {uid: u for uid, u in users.items() if int(u.get("days_played", 0)) > 0}


def calculate_all_time_rank(users: Dict[str, Any], user_id: str) -> Tuple[int, int]:
    """
    Rank by AVERAGE score (desc), not total.
    Returns (rank, total_players).
    """
    elig = eligible_users(users)
    rows: List[Tuple[str, float]] = []
    for uid, u in elig.items():
        try:
            avg = float(u["total_points"]) / float(u["days_played"])
            rows.append((uid, avg))
        except Exception:
            continue

    rows.sort(key=lambda x: x[1], reverse=True)
    total_players = len(rows)

    for idx, (uid, _) in enumerate(rows, start=1):
        if uid == user_id:
            return idx, total_players

    return total_players, total_players


# =====================================================
# ROUND PARSING (ZERO DETECTION – NOT FINAL SCORE)
# =====================================================
def extract_round_scores(text: str) -> List[int]:
    """
    Extract round scores from the rounds line(s).
    Ignores the 'Final score:' line completely.
    """
    scores: List[int] = []
    for line in text.splitlines():
        if SCORE_REGEX.search(line):
            continue
        # grab integer tokens followed by non-digits (emoji/space)
        for m in ROUND_SCORE_TOKEN_REGEX.finditer(line):
            try:
                scores.append(int(m.group(2)))
            except Exception:
                pass
    return scores


def has_zero_round(text: str) -> bool:
    """
    True if ANY round score is literally 0.
    Does NOT match 90, 100, etc.
    """
    for line in text.splitlines():
        if SCORE_REGEX.search(line):
            continue
        if ROUND_ZERO_REGEX.search(line):
            return True
    return False


# =====================================================
# SCORE INGESTION (MESSAGE LISTENER)
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

    content = message.content or ""

    # Optional hint check to reduce false positives
    if not MAPTAP_HINT_REGEX.search(content):
        return

    final_match = SCORE_REGEX.search(content)
    if not final_match:
        return

    em = settings.get("emojis", DEFAULT_SETTINGS["emojis"])
    alerts = settings.get("alerts", {})

    score = int(final_match.group(1))
    if score > MAX_SCORE:
        await react_safe(message, em.get("too_high", "❌"), "❌")
        return

    msg_time_uk = message.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
    date_key = today_key(msg_time_uk)
    user_id = str(message.author.id)

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    if not isinstance(scores, dict):
        scores = {}
    scores.setdefault(date_key, {})
    day_bucket = scores[date_key]

    user_stats = users.setdefault(user_id, {
        "total_points": 0,
        "days_played": 0,
        "best_streak": 0,
        "personal_best": {"score": 0, "date": "N/A"},
    })

    had_played_before = int(user_stats.get("days_played", 0)) > 0
    old_pb = int(user_stats.get("personal_best", {}).get("score", 0))

    # Replace score if re-posted same day
    if user_id in day_bucket:
        try:
            user_stats["total_points"] -= int(day_bucket[user_id]["score"])
        except Exception:
            pass
    else:
        user_stats["days_played"] += 1

    user_stats["total_points"] += score
    day_bucket[user_id] = {
        "score": score,
        "updated_at": msg_time_uk.isoformat(),
    }

    # ---- ZERO ROUND ROAST (round-based only)
    if alerts.get("zero_score_roasts_enabled", True) and has_zero_round(content):
        roast = random.choice([
            f"💀 {message.author.mention} found exactly **zero** places correctly",
            f"🗺️ {message.author.mention} explored the map and learned nothing",
            f"😭 {message.author.mention} posted a **0** round",
            f"🥶 {message.author.mention} went full chaos and hit a zero",
        ])
        await message.channel.send(roast)

    # ---- PERFECT SCORE
    if alerts.get("perfect_score_enabled", True) and score >= MAX_SCORE:
        await message.reply(
            f"🎯 **PERFECT SCORE!** {message.author.mention} hit **{MAX_SCORE}**!"
        )

    # ---- PERSONAL BEST
    if alerts.get("pb_messages_enabled", True):
        if had_played_before and score > old_pb:
            await message.reply(
                f"🚀 **New Personal Best!** {message.author.mention} beat {old_pb} with **{score}**"
            )
            user_stats["personal_best"] = {"score": score, "date": date_key}
        elif not had_played_before:
            user_stats["personal_best"] = {"score": score, "date": date_key}

    # ---- STREAKS
    cur_streak = calculate_current_streak(scores, user_id)
    if cur_streak > int(user_stats.get("best_streak", 0)):
        user_stats["best_streak"] = cur_streak

    github_save_json(SCORES_PATH, scores, scores_sha, f"MapTap: score update {date_key}")
    github_save_json(USERS_PATH, users, users_sha, f"MapTap: user stats update {user_id}")

    await react_safe(message, em.get("recorded", "🌏"), "✅")

# =========================
# MapTap Companion Bot (FULL FILE)
# Chunk 4/5
# =========================

# =====================================================
# SLASH: /mymaptap
# =====================================================
@client.tree.command(name="mymaptap", description="View your MapTap stats")
@app_commands.describe(user="View stats for another user")
async def mymaptap(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})

    target = user or interaction.user
    uid = str(target.id)
    stats = users.get(uid)

    if not stats or int(stats.get("days_played", 0)) <= 0:
        await interaction.response.send_message(
            f"{target.display_name} hasn’t recorded any MapTap scores yet 🗺️",
            ephemeral=False,
        )
        return

    # Rank (average-based, eligible users only)
    rank, total_players = calculate_all_time_rank(users, uid)

    cur_streak = calculate_current_streak(scores, uid)
    avg = round(int(stats["total_points"]) / int(stats["days_played"]))

    pb = stats.get("personal_best", {"score": 0, "date": "N/A"})
    pb_date = pb.get("date", "N/A")
    if pb_date != "N/A":
        try:
            pb_date = datetime.strptime(pb_date, "%Y-%m-%d").strftime("%d %b %Y")
        except Exception:
            pass

    msg = (
        f"🗺️ **MapTap Stats — {target.display_name}**\n\n"
        f"🏅 **Server Rank**\n"
        f"All-time: #{rank} of {total_players}\n\n"
        f"📊 **Overall**\n"
        f"Total points: {stats['total_points']}\n"
        f"Days played: {stats['days_played']}\n"
        f"Average score: **{avg}**\n\n"
        f"⭐ **Personal Best**\n"
        f"{pb.get('score', 0)} ({pb_date})\n\n"
        f"🔥 **Streaks**\n"
        f"Current: {cur_streak} days\n"
        f"Best: {stats.get('best_streak', 0)} days"
    )

    await interaction.response.send_message(msg, ephemeral=False)


# =====================================================
# SLASH: /leaderboard
# =====================================================
@client.tree.command(name="leaderboard", description="View MapTap leaderboards")
@app_commands.describe(
    scope="Leaderboard scope",
    limit="How many players to show (default 10)",
)
@app_commands.choices(
    scope=[
        app_commands.Choice(name="All-time", value="all_time"),
        app_commands.Choice(name="This week", value="this_week"),
        app_commands.Choice(name="This month", value="this_month"),
    ]
)
async def leaderboard(
    interaction: discord.Interaction,
    scope: app_commands.Choice[str],
    limit: int = 10,
):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})

    limit = max(3, min(limit, 25))
    elig = eligible_users(users)

    rows: List[Tuple[str, float, int]] = []  # (uid, avg, days)

    today = datetime.now(UK_TZ).date()

    def date_in_scope(d: date) -> bool:
        if scope.value == "this_week":
            return d >= today - timedelta(days=today.weekday())
        if scope.value == "this_month":
            return d.year == today.year and d.month == today.month
        return True  # all_time

    # Build per-user scoped totals
    scoped: Dict[str, Dict[str, int]] = {}
    for dkey, bucket in scores.items():
        try:
            d = datetime.strptime(dkey, "%Y-%m-%d").date()
        except Exception:
            continue
        if not date_in_scope(d):
            continue
        if not isinstance(bucket, dict):
            continue
        for uid, entry in bucket.items():
            try:
                sc = int(entry["score"])
            except Exception:
                continue
            scoped.setdefault(uid, {"total": 0, "days": 0})
            scoped[uid]["total"] += sc
            scoped[uid]["days"] += 1

    min_days = DEFAULT_SETTINGS["minimum_days"].get(scope.value, 0)

    for uid, data in scoped.items():
        if uid not in elig:
            continue
        if data["days"] < min_days:
            continue
        avg = data["total"] / data["days"]
        rows.append((uid, avg, data["days"]))

    rows.sort(key=lambda x: x[1], reverse=True)
    rows = rows[:limit]

    if not rows:
        await interaction.response.send_message("😶 No data yet for this leaderboard.", ephemeral=False)
        return

    lines = []
    for i, (uid, avg, days) in enumerate(rows, start=1):
        member = interaction.guild.get_member(int(uid)) if interaction.guild else None
        name = member.display_name if member else f"<@{uid}>"
        lines.append(f"{i}. **{name}** — {round(avg)}")

    footer = "Scores are ranked by average score over the selected period."

    text = (
        f"🏆 **MapTap Leaderboard — {scope.name}**\n\n"
        + "\n".join(lines)
        + f"\n\n*{footer}*"
    )

    await interaction.response.send_message(text, ephemeral=False)


# =====================================================
# SLASH: /rescan (DATE RANGE)
# =====================================================
@client.tree.command(
    name="rescan",
    description="Re-scan MapTap posts for a date range (admin only)",
)
@app_commands.describe(
    start_date="Start date (YYYY-MM-DD)",
    end_date="End date (YYYY-MM-DD)",
)
async def rescan(
    interaction: discord.Interaction,
    start_date: str,
    end_date: str,
):
    settings, _ = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Use this in a server.", ephemeral=True)
        return

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission.", ephemeral=True)
        return

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except Exception:
        await interaction.response.send_message("❌ Dates must be YYYY-MM-DD.", ephemeral=True)
        return

    if start > end:
        await interaction.response.send_message("❌ Start date must be before end date.", ephemeral=True)
        return

    ch = get_configured_channel(settings)
    if not ch:
        await interaction.response.send_message("❌ MapTap channel not set.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"🔍 Rescanning MapTap posts from **{start} → {end}**…",
        ephemeral=True,
    )

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    ingested = 0
    skipped = 0

    async for msg in ch.history(limit=None, oldest_first=True):
        if msg.author.bot or not msg.content:
            continue

        msg_date = msg.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ).date()
        if not (start <= msg_date <= end):
            continue

        m = SCORE_REGEX.search(msg.content)
        if not m:
            continue

        score = int(m.group(1))
        if score > MAX_SCORE:
            skipped += 1
            continue

        dkey = msg_date.isoformat()
        uid = str(msg.author.id)

        scores.setdefault(dkey, {})
        if uid in scores[dkey]:
            skipped += 1
            continue

        users.setdefault(uid, {
            "total_points": 0,
            "days_played": 0,
            "best_streak": 0,
            "personal_best": {"score": 0, "date": "N/A"},
        })

        users[uid]["days_played"] += 1
        users[uid]["total_points"] += score
        scores[dkey][uid] = {
            "score": score,
            "updated_at": msg.created_at.isoformat(),
        }

        ingested += 1

    github_save_json(SCORES_PATH, scores, scores_sha, "MapTap: rescan date range")
    github_save_json(USERS_PATH, users, users_sha, "MapTap: rescan users")

    await interaction.followup.send(
        f"✅ Rescan complete\n"
        f"Ingested: **{ingested}**\n"
        f"Skipped: {skipped}",
        ephemeral=True,
    )
# =========================
# MapTap Companion Bot (FULL FILE)
# Chunk 5/5
# =========================

# =====================================================
# MESSAGE BUILDERS
# =====================================================
def build_daily_prompt() -> str:
    return (
        "🗺️ **Daily MapTap is live!**\n"
        f"👉 {MAPTAP_URL}\n\n"
        "Post your results exactly as shared from the app ✈️"
    )


def build_daily_scoreboard_text(date_key: str, rows: List[Tuple[str, int]]) -> str:
    try:
        pretty = datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")
    except Exception:
        pretty = date_key

    if not rows:
        return f"🗺️ **MapTap — Daily Scores**\n*{pretty}*\n\n😶 No scores today."

    lines = [
        f"{i}. <@{uid}> — **{score}**"
        for i, (uid, score) in enumerate(rows, start=1)
    ]

    return (
        f"🗺️ **MapTap — Daily Scores**\n*{pretty}*\n\n"
        + "\n".join(lines)
        + f"\n\n✈️ Players today: {len(rows)}"
    )


def build_weekly_roundup_text(rows: List[Tuple[str, float]]) -> str:
    if not rows:
        return "🗺️ **Weekly MapTap Round-Up**\n\n😶 No scores this week."

    lines = [
        f"{i}. <@{uid}> — **{round(avg)}**"
        for i, (uid, avg) in enumerate(rows, start=1)
    ]

    return (
        "🗺️ **Weekly MapTap Round-Up**\n\n"
        + "\n".join(lines)
        + "\n\n*Ranked by average score this week*"
    )


# =====================================================
# SCHEDULED ACTIONS
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
        return

    today = datetime.now(UK_TZ).date().isoformat()
    bucket = scores.get(today, {})

    rows: List[Tuple[str, int]] = []
    for uid, entry in bucket.items():
        try:
            rows.append((uid, int(entry["score"])))
        except Exception:
            pass

    rows.sort(key=lambda x: x[1], reverse=True)
    await ch.send(build_daily_scoreboard_text(today, rows))

    # cleanup
    cutoff = datetime.now(UK_TZ).date() - timedelta(days=CLEANUP_DAYS)
    cleaned = {
        d: v
        for d, v in scores.items()
        if datetime.strptime(d, "%Y-%m-%d").date() >= cutoff
    }
    if cleaned != scores:
        github_save_json(
            SCORES_PATH,
            cleaned,
            scores_sha,
            f"MapTap: cleanup keep {CLEANUP_DAYS} days",
        )


async def do_weekly_roundup(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, _ = github_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        return

    today = datetime.now(UK_TZ).date()
    monday = today - timedelta(days=today.weekday())

    totals: Dict[str, Dict[str, int]] = {}
    for dkey, bucket in scores.items():
        try:
            d = datetime.strptime(dkey, "%Y-%m-%d").date()
        except Exception:
            continue
        if d < monday:
            continue
        for uid, entry in bucket.items():
            try:
                sc = int(entry["score"])
            except Exception:
                continue
            totals.setdefault(uid, {"total": 0, "days": 0})
            totals[uid]["total"] += sc
            totals[uid]["days"] += 1

    rows = [
        (uid, data["total"] / data["days"])
        for uid, data in totals.items()
        if data["days"] >= DEFAULT_SETTINGS["minimum_days"]["this_week"]
    ]
    rows.sort(key=lambda x: x[1], reverse=True)

    await ch.send(build_weekly_roundup_text(rows))


async def do_monthly_leaderboard(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    users, _ = github_load_json(USERS_PATH, {})
    elig = eligible_users(users)

    rows = []
    for uid, u in elig.items():
        try:
            if u["days_played"] >= DEFAULT_SETTINGS["minimum_days"]["this_month"]:
                avg = u["total_points"] / u["days_played"]
                rows.append((uid, avg))
        except Exception:
            pass

    rows.sort(key=lambda x: x[1], reverse=True)
    rows = rows[:10]

    if not rows:
        return

    lines = [
        f"{i}. <@{uid}> — **{round(avg)}**"
        for i, (uid, avg) in enumerate(rows, start=1)
    ]

    await ch.send(
        "🏆 **Monthly MapTap Leaderboard**\n\n"
        + "\n".join(lines)
        + "\n\n*Ranked by average score this month*"
    )


async def do_rivalry_alert(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if ch:
        await ch.send(
            "🔥 **MapTap Rivalry Alert!**\nOnly 24 hours left this week — secure your rank!"
        )


# =====================================================
# SCHEDULER LOOP
# =====================================================
@tasks.loop(minutes=1)
async def scheduler_tick():
    settings, sha = load_settings()
    if not settings.get("enabled", True):
        return

    alerts = settings.get("alerts", {})
    times = settings.get("times", {})
    last = settings.get("last_run", {})

    now = datetime.now(UK_TZ)
    hm = now.strftime("%H:%M")
    today = now.date().isoformat()

    if alerts.get("daily_post_enabled") and hm == times["daily_post"] and last.get("daily_post") != today:
        await do_daily_post(settings)
        last["daily_post"] = today

    if alerts.get("daily_scoreboard_enabled") and hm == times["daily_scoreboard"] and last.get("daily_scoreboard") != today:
        await do_daily_scoreboard(settings)
        last["daily_scoreboard"] = today

    if alerts.get("weekly_roundup_enabled") and now.weekday() == 6 and hm == times["weekly_roundup"] and last.get("weekly_roundup") != today:
        await do_weekly_roundup(settings)
        last["weekly_roundup"] = today

    if alerts.get("rivalry_enabled") and now.weekday() == 5 and hm == times["rivalry"] and last.get("rivalry") != today:
        await do_rivalry_alert(settings)
        last["rivalry"] = today

    if alerts.get("monthly_leaderboard_enabled") and now.day == 1 and hm == times["monthly_leaderboard"] and last.get("monthly_leaderboard") != today:
        await do_monthly_leaderboard(settings)
        last["monthly_leaderboard"] = today

    save_settings(settings, sha, "MapTap: scheduler tick")


# =====================================================
# STARTUP
# =====================================================
class MapTapBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        await self.tree.sync()
        scheduler_tick.start()


client = MapTapBot()

if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    client.run(TOKEN)