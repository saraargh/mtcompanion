# =========================
# MapTap Companion Bot (FULL FILE)
# Chunk 1/5
# =========================

from __future__ import annotations

import os
import json
import re
import base64
import random
import requests
import io
import calendar
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

# Optional: to make slash commands appear instantly (no global propagation delay)
# If set, sync will happen to that guild on startup.
GUILD_ID = os.getenv("MAPTAP_GUILD_ID", "").strip()

# Admin-only destructive action password (used in Settings UI only)
RESET_PASSWORD = os.getenv("RESET_PASSWORD", "")

# ---------------------------------------------
# Parsing (IMPORTANT: zero detection is NOT final score)
# ---------------------------------------------
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)

# Detect literal round "0" not followed by a digit (so it won't match 90/100/etc)
ROUND_ZERO_REGEX = re.compile(r"(^|\s)0(?!\d)")

# Extract score tokens from non-final lines; numbers followed by non-digit
ROUND_SCORE_TOKEN_REGEX = re.compile(r"(^|\s)(\d{1,4})(?!\d)")

# Basic "is this a MapTap share?" hint (reduces false positives)
MAPTAP_HINT_REGEX = re.compile(r"\bmaptap\.gg\b", re.IGNORECASE)


# =====================================================
# DEFAULT SETTINGS (GitHub-backed)
# =====================================================
DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,
    "admin_role_ids": [],

    # Alerts / toggles (ALL in Configure Alerts)
    "alerts": {
        "daily_post_enabled": True,
        "daily_scoreboard_enabled": True,
        "weekly_roundup_enabled": True,
        "rivalry_enabled": True,
        "monthly_leaderboard_enabled": True,
        "zero_score_roasts_enabled": True,
        "pb_messages_enabled": True,
        "perfect_score_enabled": True,
    },

    "emojis": {
        "recorded": "🌏",
        "too_high": "❌",
        "rescan_ingested": "🔁",
        "config_issue": "⚠️",
    },

    # Times (UK, 24h)
    "times": {
        "daily_post": "00:00",
        "daily_scoreboard": "23:30",
        "weekly_roundup": "23:45",     # Sundays
        "rivalry": "14:00",            # Saturdays
        "monthly_leaderboard": "00:10" # 1st of month
    },

    # Leaderboard minimum-day rules
    "minimum_days": {
        "this_week": 3,
        "this_month": 10,
        "all_time": 10,
        "date_range": 0
    },

    # Prevent double posting
    "last_run": {
        "daily_post": None,
        "daily_scoreboard": None,
        "weekly_roundup": None,
        "rivalry": None,
        "monthly_leaderboard": None
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

    body: Dict[str, Any] = {"message": message, "content": encoded}
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

    merged["admin_role_ids"] = [
        int(x) for x in merged.get("admin_role_ids", [])
        if str(x).isdigit()
    ]

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

###leaderboard helper##

def display_user(guild: Optional[discord.Guild], uid: str) -> str:
    """Prefer @DisplayName, fallback to <@id>."""
    try:
        if guild:
            m = guild.get_member(int(uid))
            if m:
                return f"@{m.display_name}"
    except Exception:
        pass
    return f"<@{uid}>"

def week_range_uk(today: date) -> tuple[date, date]:
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday

def month_range_uk(today: date) -> tuple[date, date]:
    first = today.replace(day=1)
    last_day = calendar.monthrange(today.year, today.month)[1]
    last = today.replace(day=last_day)
    return first, last

def compute_period_rows(scores: Dict[str, Any], start_d: Optional[date], end_d: Optional[date]) -> Dict[str, Dict[str, int]]:
    """
    Returns {uid: {'total': int, 'days': int}} for dates within [start_d, end_d].
    If start_d/end_d are None => all-time.
    """
    totals: Dict[str, Dict[str, int]] = {}
    for dkey, bucket in scores.items():
        try:
            d = datetime.strptime(dkey, "%Y-%m-%d").date()
        except Exception:
            continue

        if start_d and d < start_d:
            continue
        if end_d and d > end_d:
            continue

        if not isinstance(bucket, dict):
            continue

        for uid, entry in bucket.items():
            try:
                sc = int(entry["score"])
            except Exception:
                continue
            totals.setdefault(uid, {"total": 0, "days": 0})
            totals[uid]["total"] += sc
            totals[uid]["days"] += 1
    return totals

    
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
        # Sync commands (guild sync if MAPTAP_GUILD_ID is set, otherwise global)
        try:
            if GUILD_ID.isdigit():
                guild_obj = discord.Object(id=int(GUILD_ID))
                self.tree.copy_global_to(guild=guild_obj)
                await self.tree.sync(guild=guild_obj)
                print(f"Synced commands to guild {GUILD_ID}")
            else:
                await self.tree.sync()
                print("Synced commands globally")
        except Exception as e:
            print("Command sync failed:", e)

        self.scheduler_tick.start()

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
    ch = client.get_channel(int(cid))
    return ch if isinstance(ch, discord.TextChannel) else None

# =========================
# MapTap Companion Bot (FULL FILE)
# Chunk 3/5
# =========================

# =====================================================
# SETTINGS UI
# =====================================================

def yn(v: bool) -> str:
    return "✅" if v else "❌"


# ---------- CHANNEL SELECT ----------

class ChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, parent):
        self.parent_view = parent
        super().__init__(
            placeholder="Select MapTap channel…",
            channel_types=[discord.ChannelType.text],
        )

    async def callback(self, interaction: discord.Interaction):
        ch = self.values[0]
        self.parent_view.settings["channel_id"] = ch.id
        await self.parent_view.save_and_refresh(
            interaction,
            "MapTap: update channel",
        )


class AdminRoleSelect(discord.ui.RoleSelect):
    def __init__(self, parent):
        self.parent_view = parent
        super().__init__(
            placeholder="Select admin roles…",
            min_values=0,
            max_values=10,
        )

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.settings["admin_role_ids"] = [r.id for r in self.values]
        await self.parent_view.save_and_refresh(
            interaction,
            "MapTap: update admin roles",
        )

# ---------- TIME MODAL ----------
class TimeSettingsModal(discord.ui.Modal, title="MapTap Times (UK)"):
    daily_post = discord.ui.TextInput(label="Daily post (HH:MM)")
    daily_scoreboard = discord.ui.TextInput(label="Daily scoreboard (HH:MM)")
    weekly_roundup = discord.ui.TextInput(label="Weekly roundup (Sun)")
    rivalry = discord.ui.TextInput(label="Rivalry alert (Sat)")
    monthly_leaderboard = discord.ui.TextInput(label="Monthly leaderboard (1st)")

    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__()
        self.settings_view = settings_view
        t = settings_view.settings["times"]

        self.daily_post.default = t["daily_post"]
        self.daily_scoreboard.default = t["daily_scoreboard"]
        self.weekly_roundup.default = t["weekly_roundup"]
        self.rivalry.default = t["rivalry"]
        self.monthly_leaderboard.default = t["monthly_leaderboard"]

    async def on_submit(self, interaction: discord.Interaction):
        # validate times
        for v in (
            self.daily_post.value,
            self.daily_scoreboard.value,
            self.weekly_roundup.value,
            self.rivalry.value,
            self.monthly_leaderboard.value,
        ):
            datetime.strptime(v, "%H:%M")

        # save
        self.settings_view.settings["times"] = {
            "daily_post": self.daily_post.value,
            "daily_scoreboard": self.daily_scoreboard.value,
            "weekly_roundup": self.weekly_roundup.value,
            "rivalry": self.rivalry.value,
            "monthly_leaderboard": self.monthly_leaderboard.value,
        }

        await self.settings_view.save_and_refresh(
            interaction,
            "MapTap: update times",
        )

# ---------- ALERTS VIEW ----------
class ConfigureAlertsView(discord.ui.View):
    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__(timeout=240)
        self.settings_view = settings_view
        self.alerts = dict(settings_view.settings["alerts"])

    def toggle(self, key: str):
        self.alerts[key] = not self.alerts[key]

    @discord.ui.button(label="Daily post")
    async def daily_post(self, interaction, _):
        self.toggle("daily_post_enabled")
        await interaction.response.defer()

    @discord.ui.button(label="Daily scoreboard")
    async def daily_scoreboard(self, interaction, _):
        self.toggle("daily_scoreboard_enabled")
        await interaction.response.defer()

    @discord.ui.button(label="Weekly roundup")
    async def weekly_roundup(self, interaction, _):
        self.toggle("weekly_roundup_enabled")
        await interaction.response.defer()

    @discord.ui.button(label="Rivalry alerts")
    async def rivalry(self, interaction, _):
        self.toggle("rivalry_enabled")
        await interaction.response.defer()

    @discord.ui.button(label="Monthly leaderboard")
    async def monthly_lb(self, interaction, _):
        self.toggle("monthly_leaderboard_enabled")
        await interaction.response.defer()

    @discord.ui.button(label="Zero-score roasts")
    async def zero(self, interaction, _):
        self.toggle("zero_score_roasts_enabled")
        await interaction.response.defer()

    @discord.ui.button(label="Personal best messages")
    async def pb(self, interaction, _):
        self.toggle("pb_messages_enabled")
        await interaction.response.defer()

    @discord.ui.button(label="Perfect score messages")
    async def perfect(self, interaction, _):
        self.toggle("perfect_score_enabled")
        await interaction.response.defer()

    @discord.ui.button(label="Save alerts", style=discord.ButtonStyle.primary)
    async def save(self, interaction, _):
        self.settings_view.settings["alerts"] = self.alerts
        await self.settings_view.save_and_refresh(interaction, "MapTap: update alerts")


# ---------- RESET MODALS ----------
class ResetPasswordModal(discord.ui.Modal, title="Reset MapTap Data"):
    password = discord.ui.TextInput(label="Admin password")

    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__()
        self.settings_view = settings_view

    async def on_submit(self, interaction: discord.Interaction):
        if self.password.value != RESET_PASSWORD:
            await interaction.response.send_message("❌ Wrong password", ephemeral=True)
            return

        await interaction.response.send_modal(ResetConfirmModal(self.settings_view))


class ResetConfirmModal(discord.ui.Modal, title="Confirm Reset"):
    confirm = discord.ui.TextInput(label="Type DELETE to confirm")

    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__()
        self.settings_view = settings_view

    async def on_submit(self, interaction: discord.Interaction):
        if self.confirm.value != "DELETE":
            await interaction.response.send_message("❌ Cancelled", ephemeral=True)
            return

        github_save_json(SCORES_PATH, {}, None, "MapTap reset scores")
        github_save_json(USERS_PATH, {}, None, "MapTap reset users")

        await interaction.response.send_message("✅ MapTap data reset")


# ---------- MAIN SETTINGS VIEW ----------
class MapTapSettingsView(discord.ui.View):
    def __init__(self, settings: Dict[str, Any], sha: Optional[str]):
        super().__init__(timeout=300)
        self.settings = settings
        self.sha = sha

        # dropdowns
        self.add_item(ChannelSelect(self))
        self.add_item(AdminRoleSelect(self))

    # =====================
    # EMBED
    # =====================
    def embed(self) -> discord.Embed:
        a = self.settings["alerts"]
        t = self.settings["times"]

        def yn(v: bool) -> str:
            return "✅" if v else "❌"

        e = discord.Embed(title="🗺️ MapTap Settings", color=0xF1C40F)

        e.add_field(
            name="Status",
            value=(
                f"Bot enabled: {yn(self.settings['enabled'])}\n"
                f"Daily post: {yn(a['daily_post_enabled'])}\n"
                f"Daily scoreboard: {yn(a['daily_scoreboard_enabled'])}\n"
                f"Weekly roundup: {yn(a['weekly_roundup_enabled'])}\n"
                f"Rivalry alerts: {yn(a['rivalry_enabled'])}\n"
                f"Monthly leaderboard: {yn(a['monthly_leaderboard_enabled'])}\n"
                f"Zero-score roasts: {yn(a['zero_score_roasts_enabled'])}\n"
                f"Personal best messages: {yn(a['pb_messages_enabled'])}\n"
                f"Perfect score messages: {yn(a['perfect_score_enabled'])}"
            ),
            inline=False,
        )

        e.add_field(
            name="Times (UK)",
            value=(
                f"Daily post: {t['daily_post']}\n"
                f"Daily scoreboard: {t['daily_scoreboard']}\n"
                f"Weekly roundup: {t['weekly_roundup']}\n"
                f"Rivalry: {t['rivalry']}\n"
                f"Monthly leaderboard: {t['monthly_leaderboard']}"
            ),
            inline=False,
        )

        channel = self.settings.get("channel_id")
        roles = self.settings.get("admin_role_ids", [])

        e.add_field(
            name="Access",
            value=(
                f"Channel: {f'<#{channel}>' if channel else 'Not set'}\n"
                f"Admin roles: {', '.join(f'<@&{r}>' for r in roles) if roles else 'Admins only'}"
            ),
            inline=False,
        )

        return e

    # =====================
    # SAVE + REFRESH
    # =====================
    async def save_and_refresh(self, interaction: discord.Interaction, msg: str):
        self.sha = save_settings(self.settings, self.sha, msg) or self.sha
        await interaction.response.edit_message(
            embed=self.embed(),
            view=self,
        )

    # =====================
    # BUTTONS
    # =====================
    @discord.ui.button(label="Toggle bot", style=discord.ButtonStyle.secondary)
    async def toggle(self, interaction, _):
        self.settings["enabled"] = not self.settings["enabled"]
        await self.save_and_refresh(interaction, "MapTap toggle bot")

    @discord.ui.button(label="Edit times", style=discord.ButtonStyle.primary)
    async def edit_times(self, interaction, _):
        await interaction.response.send_modal(TimeSettingsModal(self))

    @discord.ui.button(label="Configure alerts", style=discord.ButtonStyle.primary)
    async def configure_alerts(self, interaction, _):
        await interaction.response.edit_message(
            embed=discord.Embed(title="Configure alerts"),
            view=ConfigureAlertsView(self),
        )

    @discord.ui.button(label="Reset data", style=discord.ButtonStyle.danger)
    async def reset(self, interaction, _):
        await interaction.response.send_modal(ResetPasswordModal(self))
    # ---------- BUTTONS ----------
    @discord.ui.button(label="Toggle bot", style=discord.ButtonStyle.secondary)
    async def toggle(self, interaction, _):
        self.settings["enabled"] = not self.settings["enabled"]
        await self.save_and_refresh(interaction, "MapTap toggle bot")

    @discord.ui.button(label="Edit times", style=discord.ButtonStyle.primary)
    async def edit_times(self, interaction, _):
        await interaction.response.send_modal(TimeSettingsModal(self))

    @discord.ui.button(label="Configure alerts", style=discord.ButtonStyle.primary)
    async def configure_alerts(self, interaction, _):
        await interaction.response.edit_message(
            embed=discord.Embed(title="Configure alerts"),
            view=ConfigureAlertsView(self),
        )

    @discord.ui.button(label="Reset data", style=discord.ButtonStyle.danger)
    async def reset(self, interaction, _):
        await interaction.response.send_modal(ResetPasswordModal(self))

# =====================================================
# SAFE REACTION
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
# STREAK / RANK HELPERS
# =====================================================
def calculate_current_streak(scores: Dict[str, Any], user_id: str) -> int:
    played = []
    for dkey, bucket in scores.items():
        if isinstance(bucket, dict) and user_id in bucket:
            try:
                played.append(datetime.strptime(dkey, "%Y-%m-%d").date())
            except Exception:
                pass

    if not played:
        return 0

    played = set(played)
    d = datetime.now(UK_TZ).date()
    streak = 0
    while d in played:
        streak += 1
        d -= timedelta(days=1)
    return streak


def eligible_users(users: Dict[str, Any]) -> Dict[str, Any]:
    return {uid: u for uid, u in users.items() if int(u.get("days_played", 0)) > 0}


def calculate_all_time_rank(users: Dict[str, Any], user_id: str) -> Tuple[int, int]:
    elig = eligible_users(users)
    rows = []
    for uid, u in elig.items():
        try:
            avg = u["total_points"] / u["days_played"]
            rows.append((uid, avg))
        except Exception:
            pass

    rows.sort(key=lambda x: x[1], reverse=True)
    for i, (uid, _) in enumerate(rows, start=1):
        if uid == user_id:
            return i, len(rows)

    return len(rows), len(rows)


# =====================================================
# ROUND PARSING (ZERO ONLY FROM ROUNDS)
# =====================================================
def has_zero_round(text: str) -> bool:
    for line in text.splitlines():
        if SCORE_REGEX.search(line):
            continue
        if ROUND_ZERO_REGEX.search(line):
            return True
    return False


# =====================================================
# MESSAGE LISTENER (SCORE INGEST)
# =====================================================
@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    settings, _ = load_settings()
    if not settings.get("enabled", True):
        return

    if message.channel.id != settings.get("channel_id"):
        return

    if not MAPTAP_HINT_REGEX.search(message.content or ""):
        return

    m = SCORE_REGEX.search(message.content)
    if not m:
        return

    score = int(m.group(1))
    if score > MAX_SCORE:
        await react_safe(message, settings["emojis"]["too_high"], "❌")
        return

    msg_time = message.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
    dkey = today_key(msg_time)
    uid = str(message.author.id)

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    scores.setdefault(dkey, {})
    users.setdefault(uid, {
        "total_points": 0,
        "days_played": 0,
        "best_streak": 0,
        "personal_best": {"score": 0, "date": "N/A"},
    })

    # replace same-day
    if uid in scores[dkey]:
        users[uid]["total_points"] -= scores[dkey][uid]["score"]
    else:
        users[uid]["days_played"] += 1

    users[uid]["total_points"] += score
    scores[dkey][uid] = {"score": score}

    # zero roast (round-based)
    if settings["alerts"]["zero_score_roasts_enabled"] and has_zero_round(message.content):
        await message.channel.send(
            random.choice([
                f"💀 {message.author.mention} dropped a **0** round",
                f"🗺️ {message.author.mention} learned nothing today",
            ])
        )

    # perfect score
    if settings["alerts"]["perfect_score_enabled"] and score >= MAX_SCORE:
        await message.channel.send(
            f"🎯 **Perfect Score!** {message.author.mention} just hit **{score}**!"
        )
    
    # personal best
    old_pb = users[uid]["personal_best"]["score"]

    if score > old_pb:
        users[uid]["personal_best"] = {"score": score, "date": dkey}

        if settings["alerts"]["pb_messages_enabled"] and old_pb > 0:
            await message.channel.send(
                f"🚀 **New Personal Best!**\n"
                f"{message.author.mention} just beat their previous record of "
                f"**{old_pb}** with a **{score}**!"
            )

    # streaks
    cur = calculate_current_streak(scores, uid)
    users[uid]["best_streak"] = max(users[uid].get("best_streak", 0), cur)

    github_save_json(SCORES_PATH, scores, scores_sha, "MapTap score update")
    github_save_json(USERS_PATH, users, users_sha, "MapTap user update")

    await react_safe(message, settings["emojis"]["recorded"], "✅")


# =====================================================
# /mymaptap — EMBED (RESTORED)
##

@client.tree.command(name="mymaptap", description="View your MapTap stats")
async def mymaptap(interaction: discord.Interaction):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})

    uid = str(interaction.user.id)
    stats = users.get(uid)

    if not stats:
        await interaction.response.send_message(
            "🗺️ You don’t have any MapTap scores yet.",
            ephemeral=True,
        )
        return

    # Rankings / stats
    rank, total_players = calculate_all_time_rank(users, uid)
    current_streak = calculate_current_streak(scores, uid)
    average_score = round(stats["total_points"] / stats["days_played"])

    pb = stats["personal_best"]
    pb_date = pb["date"]
    if pb_date != "N/A":
        pb_date = datetime.strptime(pb_date, "%Y-%m-%d").strftime("%d %b %Y")

    embed = discord.Embed(
        title=f"🗺️ MapTap Stats — {interaction.user.display_name}",
        color=0x2ECC71,
    )

    embed.add_field(
        name="📊 Server Rankings",
        value=(
            f"🥇 All-Time: **#{rank} of {total_players}**\n"
            f"🏁 This Week: see leaderboard"
        ),
        inline=False,
    )

    embed.add_field(
        name="⭐ Personal Records",
        value=(
            f"Personal Best: **{pb['score']}** ({pb_date})\n"
            f"Best Streak: 🏆 **{stats['best_streak']} days**\n"
            f"Current Streak: 🔥 **{current_streak} days**"
        ),
        inline=False,
    )

    embed.add_field(
        name="📈 Overall Stats",
        value=(
            f"Total Points: **{stats['total_points']}**\n"
            f"Days Played: **{stats['days_played']}**\n"
            f"Average Score: **{average_score}**"
        ),
        inline=False,
    )

    await interaction.response.send_message(embed=embed)

##settings##
@client.tree.command(name="maptapsettings", description="Configure MapTap settings")
async def maptapsettings(interaction: discord.Interaction):
    settings, sha = load_settings()

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message(
            "You don’t have permission to configure MapTap.",
            ephemeral=True,
        )
        return

    view = MapTapSettingsView(settings, sha)

    await interaction.response.send_message(
        embed=view.embed(),
        view=view,
    )


# # =====================================================
# /leaderboard — DROPDOWN VERSION (FINAL)
# =====================================================

class LeaderboardSelect(discord.ui.Select):
    def __init__(self, settings: Dict[str, Any]):
        self.settings = settings

        options = [
            discord.SelectOption(label="This week", value="this_week"),
            discord.SelectOption(label="This month", value="this_month"),
            discord.SelectOption(label="All-time", value="all_time"),
        ]

        super().__init__(
            placeholder="Choose a leaderboard…",
            options=options,
            min_values=1,
            max_values=1,
        )

    async def callback(self, interaction: discord.Interaction):
        scope = self.values[0]

        users, _ = github_load_json(USERS_PATH, {})
        scores, _ = github_load_json(SCORES_PATH, {})

        today = datetime.now(UK_TZ).date()

        # -------------------------
        # Date range selection
        # -------------------------
        start_d = end_d = None

        if scope == "this_week":
            start_d = today - timedelta(days=today.weekday())
            end_d = today
        elif scope == "this_month":
            start_d = today.replace(day=1)
            end_d = today

        # -------------------------
        # Aggregate scores
        # -------------------------
        totals: Dict[str, Dict[str, int]] = {}

        for dkey, bucket in scores.items():
            try:
                d = datetime.strptime(dkey, "%Y-%m-%d").date()
            except Exception:
                continue

            if start_d and d < start_d:
                continue
            if end_d and d > end_d:
                continue

            if not isinstance(bucket, dict):
                continue

            for uid, entry in bucket.items():
                try:
                    sc = int(entry["score"])
                except Exception:
                    continue

                totals.setdefault(uid, {"total": 0, "days": 0})
                totals[uid]["total"] += sc
                totals[uid]["days"] += 1

        # -------------------------
        # Build rows (avg-based)
        # -------------------------
        min_days = self.settings["minimum_days"].get(scope, 0)

        rows: List[Tuple[str, int]] = []
        for uid, data in totals.items():
            if data["days"] < min_days:
                continue
            avg = round(data["total"] / data["days"])
            rows.append((uid, avg))

        rows.sort(key=lambda x: x[1], reverse=True)
        rows = rows[:10]

        # -------------------------
        # Build embed
        # -------------------------
        embed = discord.Embed(
            title="🗺️ MapTap Leaderboard",
            description=f"*{scope.replace('_', ' ').title()}*",
            color=0x3498DB,
        )

        if not rows:
            embed.add_field(
                name="No data",
                value="No eligible scores for this period.",
                inline=False,
            )
        else:
            lines = []
            for i, (uid, avg) in enumerate(rows, start=1):
                name = display_user(interaction.guild, uid)
                lines.append(f"{i}. {name} — {avg}")

            embed.add_field(
                name="Top Players",
                value="\n".join(lines),
                inline=False,
            )

        embed.set_footer(text="All rankings are based on average score")

        await interaction.response.edit_message(embed=embed, view=self.view)


class LeaderboardView(discord.ui.View):
    def __init__(self, settings: Dict[str, Any]):
        super().__init__(timeout=180)
        self.add_item(LeaderboardSelect(settings))


@client.tree.command(name="leaderboard", description="View MapTap leaderboards")
async def leaderboard(interaction: discord.Interaction):
    settings, _ = load_settings()

    await interaction.response.send_message(
        embed=discord.Embed(
            title="🗺️ MapTap Leaderboard",
            description="Select a leaderboard to view",
            color=0x3498DB,
        ),
        view=LeaderboardView(settings),
    )

# =====================================================
# /rescan — DATE RANGE
# =====================================================
@client.tree.command(name="rescan", description="Re-scan MapTap posts (admin)")
@app_commands.describe(
    days="How many days back to rescan",
    limit="How many messages to scan (max 100)"
)
async def rescan(
    interaction: discord.Interaction,
    days: int,
    limit: int = 50,
):
    settings, _ = load_settings()

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ No permission", ephemeral=True)
        return

    limit = min(max(limit, 1), 100)
    since_date = datetime.now(UK_TZ).date() - timedelta(days=days)

    ch = get_configured_channel(settings)
    if not ch:
        await interaction.response.send_message("❌ Channel not set", ephemeral=True)
        return

    await interaction.response.send_message(
        f"🔍 Rescanning last **{limit}** messages (since {since_date})…",
        ephemeral=True,
    )

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    # wipe affected days
    for d in list(scores.keys()):
        if datetime.strptime(d, "%Y-%m-%d").date() >= since_date:
            scores.pop(d, None)

    async for msg in ch.history(limit=limit, oldest_first=True):
        if msg.author.bot:
            continue
        if not MAPTAP_HINT_REGEX.search(msg.content or ""):
            continue

        m = SCORE_REGEX.search(msg.content)
        if not m:
            continue

        score = int(m.group(1))
        if score > MAX_SCORE:
            continue

        msg_time = msg.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
        dkey = today_key(msg_time)
        uid = str(msg.author.id)

        scores.setdefault(dkey, {})
        users.setdefault(uid, {
            "total_points": 0,
            "days_played": 0,
            "best_streak": 0,
            "personal_best": {"score": 0, "date": "N/A"},
        })

        if uid not in scores[dkey]:
            users[uid]["days_played"] += 1
        else:
            users[uid]["total_points"] -= scores[dkey][uid]["score"]

        users[uid]["total_points"] += score
        scores[dkey][uid] = {"score": score}

        # PB rebuild
        if score > users[uid]["personal_best"]["score"]:
            users[uid]["personal_best"] = {"score": score, "date": dkey}

    github_save_json(SCORES_PATH, scores, scores_sha, "MapTap rescan scores")
    github_save_json(USERS_PATH, users, users_sha, "MapTap rescan users")

    await ch.send(f"🔁 Rescan complete ({days} days, {limit} messages)")
# =========================
# MapTap Companion Bot (FULL FILE)
# Chunk 5/5
# =========================

# =====================================================
# MESSAGE BUILDERS
# =====================================================
def build_daily_prompt() -> str:
    return (
        "🗺️ Daily MapTap is live!\n"
        f"👉 {MAPTAP_URL}\n\n"
        "Post your results exactly as shared from the app ✈️"
    )


def build_daily_scoreboard_text(date_key: str, rows: List[Tuple[str, int]]) -> str:
    try:
        pretty = datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")
    except Exception:
        pretty = date_key

    if not rows:
        return f"🗺️ MapTap — Daily Scores\n{pretty}\n\nNo scores today."

    lines = [
        f"{i}. <@{uid}> — {score}"
        for i, (uid, score) in enumerate(rows, start=1)
    ]

    return (
        f"🗺️ MapTap — Daily Scores\n{pretty}\n\n"
        + "\n".join(lines)
        + f"\n\nPlayers today: {len(rows)}"
    )


def build_weekly_roundup_text(rows: List[Tuple[str, int]]) -> str:
    if not rows:
        return "🗺️ Weekly MapTap Round-Up\n\nNo scores this week."

    lines = [
        f"{i}. <@{uid}> — {avg}"
        for i, (uid, avg) in enumerate(rows, start=1)
    ]

    return (
        "🗺️ Weekly MapTap Round-Up\n\n"
        + "\n".join(lines)
        + "\n\nRanked by average score this week"
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
    today = datetime.now(UK_TZ).date().isoformat()
    bucket = scores.get(today, {})

    rows = []
    for uid, entry in bucket.items():
        try:
            rows.append((uid, int(entry["score"])))
        except Exception:
            pass

    rows.sort(key=lambda x: x[1], reverse=True)
    await ch.send(build_daily_scoreboard_text(today, rows))

    cutoff = datetime.now(UK_TZ).date() - timedelta(days=CLEANUP_DAYS)
    cleaned = {
        d: v for d, v in scores.items()
        if datetime.strptime(d, "%Y-%m-%d").date() >= cutoff
    }
    if cleaned != scores:
        github_save_json(
            SCORES_PATH,
            cleaned,
            scores_sha,
            f"MapTap cleanup ({CLEANUP_DAYS} days)",
        )


async def do_weekly_roundup(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, _ = github_load_json(SCORES_PATH, {})
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
            totals.setdefault(uid, {"total": 0, "days": 0})
            totals[uid]["total"] += entry["score"]
            totals[uid]["days"] += 1

    rows = [
        (uid, round(v["total"] / v["days"]))
        for uid, v in totals.items()
        if v["days"] >= DEFAULT_SETTINGS["minimum_days"]["this_week"]
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
        if u["days_played"] >= DEFAULT_SETTINGS["minimum_days"]["this_month"]:
            avg = round(u["total_points"] / u["days_played"])
            rows.append((uid, avg))

    rows.sort(key=lambda x: x[1], reverse=True)
    rows = rows[:10]

    if not rows:
        return

    lines = [f"{i}. <@{uid}> — {avg}" for i, (uid, avg) in enumerate(rows, 1)]
    await ch.send(
        "🏆 Monthly MapTap Leaderboard\n\n"
        + "\n".join(lines)
        + "\n\nRanked by average score this month"
    )


async def do_rivalry_alert(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if ch:
        await ch.send(
            "🔥 MapTap Rivalry Alert!\n24 hours left this week — secure your rank."
        )



# =====================================================
# STARTUP (ONE CLIENT, ONE TREE)
# =====================================================


if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    client.run(TOKEN)