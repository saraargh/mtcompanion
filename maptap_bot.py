# =========================
# MapTap Companion Bot
# Multi-guild rewrite
# =========================

from __future__ import annotations

import os
import json
import re
import base64
import random
import requests
import calendar
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo, available_timezones, ZoneInfoNotFoundError
from threading import Thread
from typing import Any, Dict, Tuple, Optional, List

import discord
from discord.ext import tasks
from discord import app_commands
from flask import Flask

# =====================================================
# CONFIG
# =====================================================
TOKEN = os.getenv("TOKEN")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # e.g. "saraargh/the-pilot"

SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")
SETTINGS_PATH = os.getenv("MAPTAP_SETTINGS_PATH", "data/maptap_settings.json")

MAPTAP_URL = os.getenv("MAPTAP_URL", "https://www.maptap.gg")
CLEANUP_DAYS = int(os.getenv("MAPTAP_CLEANUP_DAYS", "69"))
MAX_SCORE = int(os.getenv("MAPTAP_MAX_SCORE", "1000"))

# Optional: to make slash commands appear instantly in a specific guild during dev
GUILD_ID = os.getenv("MAPTAP_GUILD_ID", "").strip()

RESET_PASSWORD = os.getenv("RESET_PASSWORD", "")

RIVALRY_THRESHOLD = int(os.getenv("MAPTAP_RIVALRY_THRESHOLD", "15"))
RIVALRY_MIN_PLAYERS = int(os.getenv("MAPTAP_RIVALRY_MIN_PLAYERS", "5"))

# ---------------------------------------------
# Parsing
# ---------------------------------------------
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)
ROUND_ZERO_REGEX = re.compile(r"(^|\s)0(?!\d)")
MAPTAP_HINT_REGEX = re.compile(r"\bmaptap\.gg\b", re.IGNORECASE)

# =====================================================
# DEFAULT SETTINGS (per guild)
# =====================================================
DEFAULT_GUILD_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,
    "admin_role_ids": [],
    "timezone": "Europe/London",

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
        "recorded": "<:maptap:1476982463430660136>",
        "too_high": "❌",
        "rescan_ingested": "🔁",
        "config_issue": "⚠️",
    },

    "times": {
        "daily_post": "00:00",
        "daily_scoreboard": "23:30",
        "weekly_roundup": "23:45",
        "rivalry": "14:00",
        "monthly_leaderboard": "00:10",
    },

    "minimum_days": {
        "this_week": 3,
        "this_month": 7,
        "all_time": 0,
        "date_range": 0,
    },

    "last_run": {
        "daily_post": None,
        "daily_scoreboard": None,
        "weekly_roundup": None,
        "rivalry": None,
        "monthly_leaderboard": None,
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
# GITHUB HELPERS
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

    body: Dict[str, Any] = {"message": message, "content": encoded}
    if sha:
        body["sha"] = sha

    r = requests.put(url, headers=HEADERS, json=body, timeout=20)
    r.raise_for_status()
    new_sha = r.json().get("content", {}).get("sha")
    return new_sha or sha or ""

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

def _normalize_guild_settings(raw: Any) -> Dict[str, Any]:
    """
    Takes raw settings for one guild and merges/normalises against DEFAULT_GUILD_SETTINGS.
    Returns a clean, fully-populated settings dict.
    """
    merged = DEFAULT_GUILD_SETTINGS.copy()
    if isinstance(raw, dict):
        merged.update(raw)

    if merged.get("channel_id") is not None:
        try:
            merged["channel_id"] = int(merged["channel_id"])
        except Exception:
            merged["channel_id"] = None

    merged["admin_role_ids"] = [
        int(x) for x in merged.get("admin_role_ids", [])
        if str(x).isdigit()
    ]

    # Validate timezone — fall back to London if invalid
    tz_str = merged.get("timezone", "Europe/London")
    try:
        ZoneInfo(tz_str)
    except Exception:
        merged["timezone"] = "Europe/London"

    merged["alerts"] = _merge_nested(DEFAULT_GUILD_SETTINGS["alerts"], merged.get("alerts"))
    merged["emojis"] = _merge_nested(DEFAULT_GUILD_SETTINGS["emojis"], merged.get("emojis"))
    merged["minimum_days"] = _merge_nested(DEFAULT_GUILD_SETTINGS["minimum_days"], merged.get("minimum_days"))

    times_in = _merge_nested(DEFAULT_GUILD_SETTINGS["times"], merged.get("times"))
    merged["times"] = {
        k: _normalize_hhmm(times_in.get(k), DEFAULT_GUILD_SETTINGS["times"][k])
        for k in DEFAULT_GUILD_SETTINGS["times"]
    }

    merged["last_run"] = _merge_nested(DEFAULT_GUILD_SETTINGS["last_run"], merged.get("last_run"))
    if not isinstance(merged["last_run"], dict):
        merged["last_run"] = DEFAULT_GUILD_SETTINGS["last_run"].copy()

    return merged

def load_all_settings() -> Tuple[Dict[str, Any], Optional[str]]:
    """Load the entire settings file. Returns {guild_id: settings_dict}, sha."""
    raw, sha = github_load_json(SETTINGS_PATH, {})
    if not isinstance(raw, dict):
        raw = {}

    normalised = {}
    for guild_id, guild_raw in raw.items():
        normalised[str(guild_id)] = _normalize_guild_settings(guild_raw)

    return normalised, sha

def load_guild_settings(guild_id: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """Load settings for a single guild. Also returns the full-file sha for saving."""
    all_settings, sha = load_all_settings()
    return all_settings.get(str(guild_id), _normalize_guild_settings({})), sha

def save_all_settings(all_settings: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    return github_save_json(SETTINGS_PATH, all_settings, sha, message)

def save_guild_settings(guild_id: str, guild_settings: Dict[str, Any], message: str) -> None:
    """Load full file, update one guild's block, save back."""
    all_settings, sha = load_all_settings()
    all_settings[str(guild_id)] = guild_settings
    save_all_settings(all_settings, sha, message)

# =====================================================
# TIMEZONE HELPER
# =====================================================
def get_guild_tz(settings: Dict[str, Any]) -> ZoneInfo:
    try:
        return ZoneInfo(settings.get("timezone", "Europe/London"))
    except Exception:
        return ZoneInfo("Europe/London")

# =====================================================
# DATE / DISPLAY HELPERS
# =====================================================
def today_key(dt: Optional[datetime] = None, tz: Optional[ZoneInfo] = None) -> str:
    if dt is None:
        dt = datetime.now(tz or ZoneInfo("Europe/London"))
    return dt.date().isoformat()

def pretty_day(date_key: str) -> str:
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")

def week_range(today: date) -> Tuple[date, date]:
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday

def month_range(today: date) -> Tuple[date, date]:
    first = today.replace(day=1)
    last_day = calendar.monthrange(today.year, today.month)[1]
    last = today.replace(day=last_day)
    return first, last

def display_user(guild: Optional[discord.Guild], uid: str) -> str:
    try:
        if guild:
            m = guild.get_member(int(uid))
            if m:
                return f"@{m.display_name}"
    except Exception:
        pass
    return f"<@{uid}>"

def yn(v: bool) -> str:
    return "✅" if v else "❌"

def _safe_date(dkey: str) -> Optional[date]:
    try:
        return datetime.strptime(dkey, "%Y-%m-%d").date()
    except Exception:
        return None

def compute_period_rows(
    guild_scores: Dict[str, Any],
    start_d: Optional[date],
    end_d: Optional[date],
) -> Dict[str, Dict[str, int]]:
    """
    guild_scores is the scores dict for ONE guild: {date_key: {uid: {score: int}}}
    Returns {uid: {'total': int, 'days': int}}
    """
    totals: Dict[str, Dict[str, int]] = {}

    if not isinstance(guild_scores, dict):
        return totals

    for dkey, bucket in guild_scores.items():
        d = _safe_date(dkey)
        if not d:
            continue
        if start_d and d < start_d:
            continue
        if end_d and d > end_d:
            continue
        if not isinstance(bucket, dict):
            continue

        for uid, entry in bucket.items():
            try:
                sc = int(entry.get("score", 0))
            except Exception:
                continue

            totals.setdefault(uid, {"total": 0, "days": 0})
            totals[uid]["total"] += sc
            totals[uid]["days"] += 1

    return totals

# =====================================================
# GUILD-SCOPED DATA HELPERS
# =====================================================
def load_guild_scores(guild_id: str) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[str]]:
    """
    Returns (all_scores, guild_scores, sha).
    guild_scores is the slice for this guild only.
    You need all_scores + sha to write back.
    """
    all_scores, sha = github_load_json(SCORES_PATH, {})
    if not isinstance(all_scores, dict):
        all_scores = {}
    guild_scores = all_scores.get(str(guild_id), {})
    if not isinstance(guild_scores, dict):
        guild_scores = {}
    return all_scores, guild_scores, sha

def save_guild_scores(guild_id: str, all_scores: Dict[str, Any], guild_scores: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    all_scores[str(guild_id)] = guild_scores
    return github_save_json(SCORES_PATH, all_scores, sha, message)

def load_guild_users(guild_id: str) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[str]]:
    """
    Returns (all_users, guild_users, sha).
    """
    all_users, sha = github_load_json(USERS_PATH, {})
    if not isinstance(all_users, dict):
        all_users = {}
    guild_users = all_users.get(str(guild_id), {})
    if not isinstance(guild_users, dict):
        guild_users = {}
    return all_users, guild_users, sha

def save_guild_users(guild_id: str, all_users: Dict[str, Any], guild_users: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    all_users[str(guild_id)] = guild_users
    return github_save_json(USERS_PATH, all_users, sha, message)

# =====================================================
# USER STATS DEFAULT
# =====================================================
def default_user_stats() -> Dict[str, Any]:
    return {
        "total_points": 0,
        "days_played": 0,
        "best_streak": 0,
        "personal_best": {"score": 0, "date": "N/A"},
        "personal_low": {"score": 100000, "date": "N/A"},
    }

# =====================================================
# STREAK / RANK HELPERS
# =====================================================
def calculate_current_streak(guild_scores: Dict[str, Any], user_id: str, tz: ZoneInfo) -> int:
    played: List[date] = []
    for dkey, bucket in guild_scores.items():
        if isinstance(bucket, dict) and user_id in bucket:
            d = _safe_date(dkey)
            if d:
                played.append(d)

    if not played:
        return 0

    played_set = set(played)
    d = datetime.now(tz).date()
    streak = 0
    while d in played_set:
        streak += 1
        d -= timedelta(days=1)
    return streak

def eligible_users(guild_users: Dict[str, Any]) -> Dict[str, Any]:
    return {uid: u for uid, u in guild_users.items() if int(u.get("days_played", 0)) > 0}

def calculate_all_time_rank(guild_users: Dict[str, Any], user_id: str) -> Tuple[int, int]:
    elig = eligible_users(guild_users)
    rows: List[Tuple[str, float]] = []
    for uid, u in elig.items():
        try:
            avg = float(u["total_points"]) / float(u["days_played"])
            rows.append((uid, avg))
        except Exception:
            pass

    rows.sort(key=lambda x: x[1], reverse=True)
    for i, (uid, _) in enumerate(rows, start=1):
        if uid == user_id:
            return i, len(rows)

    return len(rows), len(rows)

def calculate_period_rank(
    guild_scores: Dict[str, Any],
    user_id: str,
    start_d: date,
    end_d: date,
) -> Tuple[Optional[int], int]:
    totals = compute_period_rows(guild_scores, start_d, end_d)

    rows = [
        (uid, round(v["total"] / v["days"]))
        for uid, v in totals.items()
        if v["days"] > 0
    ]
    rows.sort(key=lambda x: x[1], reverse=True)

    for i, (uid, _) in enumerate(rows, start=1):
        if uid == user_id:
            return i, len(rows)

    return None, len(rows)

def calculate_global_rank(user_id: str) -> Tuple[Optional[int], int]:
    """
    Flattens all guilds in users.json, deduplicates by user ID
    (a user in multiple servers gets their scores averaged across guilds),
    and returns (rank, total_players) for the given user_id.
    Ranked by average score across all appearances.
    """
    all_users, _ = github_load_json(USERS_PATH, {})
    if not isinstance(all_users, dict):
        return None, 0

    # {uid: [avg_score, avg_score, ...]} — one entry per guild they appear in
    global_avgs: Dict[str, List[float]] = {}

    for guild_id, guild_users in all_users.items():
        if not isinstance(guild_users, dict):
            continue
        for uid, stats in guild_users.items():
            try:
                days = int(stats.get("days_played", 0))
                if days <= 0:
                    continue
                avg = float(stats["total_points"]) / float(days)
                global_avgs.setdefault(uid, []).append(avg)
            except Exception:
                continue

    if not global_avgs:
        return None, 0

    # Final score = mean of their per-guild averages
    rows: List[Tuple[str, float]] = [
        (uid, sum(avgs) / len(avgs))
        for uid, avgs in global_avgs.items()
    ]
    rows.sort(key=lambda x: x[1], reverse=True)

    for i, (uid, _) in enumerate(rows, start=1):
        if uid == user_id:
            return i, len(rows)

    return None, len(rows)

# =====================================================
# ROUND PARSING
# =====================================================
def has_zero_round(text: str) -> bool:
    for line in text.splitlines():
        if SCORE_REGEX.search(line):
            continue
        if ROUND_ZERO_REGEX.search(line):
            return True
    return False

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
            if GUILD_ID.isdigit():
                guild_obj = discord.Object(id=int(GUILD_ID))
                self.tree.copy_global_to(guild=guild_obj)
                await self.tree.sync(guild=guild_obj)
                print(f"✅ Synced commands to guild {GUILD_ID}")
            else:
                await self.tree.sync()
                print("✅ Synced commands globally")
        except Exception as e:
            print("⚠️ Command sync failed:", e)

        if not self.scheduler_tick.is_running():
            self.scheduler_tick.start()
            print("✅ scheduler_tick started in setup_hook()")

    @tasks.loop(minutes=1)
    async def scheduler_tick(self):
        """
        Runs every minute. Loads all guild settings once, iterates over each guild,
        checks whether any scheduled action is due for that guild's local time,
        fires if so, then saves the updated last_run block back in one write.
        """
        all_settings, sha = load_all_settings()
        fired_any = False

        for guild_id, settings in all_settings.items():
            if not settings.get("enabled", True):
                continue

            tz = get_guild_tz(settings)
            now = datetime.now(tz)
            now_hm = now.strftime("%H:%M")
            today = today_key(now, tz)

            times = settings.get("times", {})
            last_run = settings.get("last_run", {})
            alerts = settings.get("alerts", {})

            guild_fired = False

            # Daily Post
            if (
                alerts.get("daily_post_enabled", True)
                and now_hm == times.get("daily_post")
                and last_run.get("daily_post") != today
            ):
                await do_daily_post(guild_id, settings)
                all_settings[guild_id]["last_run"]["daily_post"] = today
                guild_fired = True

            # Daily Scoreboard
            if (
                alerts.get("daily_scoreboard_enabled", True)
                and now_hm == times.get("daily_scoreboard")
                and last_run.get("daily_scoreboard") != today
            ):
                await do_daily_scoreboard(guild_id, settings)
                all_settings[guild_id]["last_run"]["daily_scoreboard"] = today
                guild_fired = True

            # Weekly Roundup (Sundays in the guild's local time)
            if (
                alerts.get("weekly_roundup_enabled", True)
                and now.weekday() == 6
                and now_hm == times.get("weekly_roundup")
                and last_run.get("weekly_roundup") != today
            ):
                await do_weekly_roundup(guild_id, settings)
                all_settings[guild_id]["last_run"]["weekly_roundup"] = today
                guild_fired = True

            # Rivalry Alert
            if (
                alerts.get("rivalry_enabled", True)
                and now_hm == times.get("rivalry")
                and last_run.get("rivalry") != today
            ):
                try:
                    await do_rivalry_alert(guild_id, settings)
                finally:
                    all_settings[guild_id]["last_run"]["rivalry"] = today
                    guild_fired = True

            # Monthly Leaderboard (1st of month in guild's local time)
            if (
                alerts.get("monthly_leaderboard_enabled", True)
                and now.day == 1
                and now_hm == times.get("monthly_leaderboard")
                and last_run.get("monthly_leaderboard") != today
            ):
                await do_monthly_leaderboard(guild_id, settings)
                all_settings[guild_id]["last_run"]["monthly_leaderboard"] = today
                guild_fired = True

            if guild_fired:
                fired_any = True

        if fired_any:
            try:
                save_all_settings(all_settings, sha, f"MapTap: last_run update")
            except Exception as e:
                print("⚠️ Failed to save last_run:", e)

client = MapTapBot()

@client.event
async def on_ready():
    print(f"✅ Logged in as {client.user} (MapTap)")
    try:
        if not client.scheduler_tick.is_running():
            client.scheduler_tick.start()
            print("✅ scheduler_tick started from on_ready() fallback")
    except Exception as e:
        print("❌ Failed to start scheduler_tick in on_ready:", e)

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
# SETTINGS UI
# =====================================================

class ChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, parent: "MapTapSettingsView"):
        self.parent_view = parent
        super().__init__(
            placeholder="Select MapTap channel…",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
        )

    async def callback(self, interaction: discord.Interaction):
        ch = self.values[0]
        self.parent_view.settings["channel_id"] = ch.id
        await self.parent_view.save_and_refresh(interaction, "MapTap: update channel")

class AdminRoleSelect(discord.ui.RoleSelect):
    def __init__(self, parent: "MapTapSettingsView"):
        self.parent_view = parent
        super().__init__(
            placeholder="Select admin roles…",
            min_values=0,
            max_values=10,
        )

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.settings["admin_role_ids"] = [r.id for r in self.values]
        await self.parent_view.save_and_refresh(interaction, "MapTap: update admin roles")

class TimeSettingsModal(discord.ui.Modal, title="MapTap Times"):
    daily_post = discord.ui.TextInput(label="Daily post (HH:MM)")
    daily_scoreboard = discord.ui.TextInput(label="Daily scoreboard (HH:MM)")
    weekly_roundup = discord.ui.TextInput(label="Weekly roundup (Sun) (HH:MM)")
    rivalry = discord.ui.TextInput(label="Rivalry alert (HH:MM)")
    monthly_leaderboard = discord.ui.TextInput(label="Monthly leaderboard (1st day) (HH:MM)")

    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__()
        self.settings_view = settings_view
        t = settings_view.settings.get("times", DEFAULT_GUILD_SETTINGS["times"])

        self.daily_post.default = str(t.get("daily_post", "00:00"))
        self.daily_scoreboard.default = str(t.get("daily_scoreboard", "23:30"))
        self.weekly_roundup.default = str(t.get("weekly_roundup", "23:45"))
        self.rivalry.default = str(t.get("rivalry", "14:00"))
        self.monthly_leaderboard.default = str(t.get("monthly_leaderboard", "00:10"))

    async def on_submit(self, interaction: discord.Interaction):
        values = {
            "daily_post": self.daily_post.value.strip(),
            "daily_scoreboard": self.daily_scoreboard.value.strip(),
            "weekly_roundup": self.weekly_roundup.value.strip(),
            "rivalry": self.rivalry.value.strip(),
            "monthly_leaderboard": self.monthly_leaderboard.value.strip(),
        }

        for k, v in values.items():
            try:
                datetime.strptime(v, "%H:%M")
            except Exception:
                await interaction.response.send_message(
                    f"❌ Invalid time for **{k}**. Use HH:MM (24h), e.g. 23:30",
                    ephemeral=True,
                )
                return

        self.settings_view.settings["times"] = values
        await self.settings_view.save_and_refresh(interaction, "MapTap: update times")

class ConfigureAlertsView(discord.ui.View):
    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__(timeout=240)
        self.settings_view = settings_view
        self.alerts = dict(settings_view.settings.get("alerts", DEFAULT_GUILD_SETTINGS["alerts"]))

    def toggle(self, key: str):
        self.alerts[key] = not bool(self.alerts.get(key, False))

    async def _ack(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True, thinking=False)
        except Exception:
            pass

    @discord.ui.button(label="Daily post", style=discord.ButtonStyle.secondary)
    async def daily_post(self, interaction: discord.Interaction, _):
        self.toggle("daily_post_enabled")
        await self._ack(interaction)

    @discord.ui.button(label="Daily scoreboard", style=discord.ButtonStyle.secondary)
    async def daily_scoreboard(self, interaction: discord.Interaction, _):
        self.toggle("daily_scoreboard_enabled")
        await self._ack(interaction)

    @discord.ui.button(label="Weekly roundup", style=discord.ButtonStyle.secondary)
    async def weekly_roundup(self, interaction: discord.Interaction, _):
        self.toggle("weekly_roundup_enabled")
        await self._ack(interaction)

    @discord.ui.button(label="Rivalry alerts", style=discord.ButtonStyle.secondary)
    async def rivalry(self, interaction: discord.Interaction, _):
        self.toggle("rivalry_enabled")
        await self._ack(interaction)

    @discord.ui.button(label="Monthly leaderboard", style=discord.ButtonStyle.secondary)
    async def monthly_lb(self, interaction: discord.Interaction, _):
        self.toggle("monthly_leaderboard_enabled")
        await self._ack(interaction)

    @discord.ui.button(label="Zero-score roasts", style=discord.ButtonStyle.secondary)
    async def zero(self, interaction: discord.Interaction, _):
        self.toggle("zero_score_roasts_enabled")
        await self._ack(interaction)

    @discord.ui.button(label="Personal best messages", style=discord.ButtonStyle.secondary)
    async def pb(self, interaction: discord.Interaction, _):
        self.toggle("pb_messages_enabled")
        await self._ack(interaction)

    @discord.ui.button(label="Perfect score messages", style=discord.ButtonStyle.secondary)
    async def perfect(self, interaction: discord.Interaction, _):
        self.toggle("perfect_score_enabled")
        await self._ack(interaction)

    @discord.ui.button(label="Save alerts", style=discord.ButtonStyle.primary)
    async def save(self, interaction: discord.Interaction, _):
        self.settings_view.settings["alerts"] = self.alerts
        await self.settings_view.save_and_refresh(interaction, "MapTap: update alerts")

class ResetPasswordModal(discord.ui.Modal, title="Reset MapTap Data"):
    password = discord.ui.TextInput(label="Admin password", required=True)

    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__()
        self.settings_view = settings_view

    async def on_submit(self, interaction: discord.Interaction):
        if not RESET_PASSWORD:
            await interaction.response.send_message("❌ RESET_PASSWORD env var not set.", ephemeral=True)
            return

        if self.password.value != RESET_PASSWORD:
            await interaction.response.send_message("❌ Wrong password.", ephemeral=True)
            return

        await interaction.response.send_modal(ResetConfirmModal(self.settings_view))

class ResetConfirmModal(discord.ui.Modal, title="Confirm Reset"):
    confirm = discord.ui.TextInput(label="Type DELETE to confirm", required=True)

    def __init__(self, settings_view: "MapTapSettingsView"):
        super().__init__()
        self.settings_view = settings_view

    async def on_submit(self, interaction: discord.Interaction):
        if self.confirm.value.strip().upper() != "DELETE":
            await interaction.response.send_message("❌ Cancelled.", ephemeral=True)
            return

        guild_id = str(interaction.guild_id)

        # Reset only this guild's data
        all_scores, scores_sha = github_load_json(SCORES_PATH, {})
        if isinstance(all_scores, dict):
            all_scores.pop(guild_id, None)
            github_save_json(SCORES_PATH, all_scores, scores_sha, f"MapTap reset scores guild {guild_id}")

        all_users, users_sha = github_load_json(USERS_PATH, {})
        if isinstance(all_users, dict):
            all_users.pop(guild_id, None)
            github_save_json(USERS_PATH, all_users, users_sha, f"MapTap reset users guild {guild_id}")

        await interaction.response.send_message("✅ MapTap data reset for this server.", ephemeral=True)

class MapTapSettingsView(discord.ui.View):
    def __init__(self, settings: Dict[str, Any], guild_id: str):
        super().__init__(timeout=300)
        self.settings = settings
        self.guild_id = guild_id

        self.add_item(ChannelSelect(self))
        self.add_item(AdminRoleSelect(self))

    def embed(self) -> discord.Embed:
        a = self.settings.get("alerts", {})
        t = self.settings.get("times", {})
        tz_str = self.settings.get("timezone", "Europe/London")

        e = discord.Embed(title="🗺️ MapTap Settings", color=0xF1C40F)

        e.add_field(
            name="🧭 Status",
            value=(
                f"Bot enabled: {yn(bool(self.settings.get('enabled', True)))}\n"
                f"Daily post: {yn(bool(a.get('daily_post_enabled', True)))}\n"
                f"Daily scoreboard: {yn(bool(a.get('daily_scoreboard_enabled', True)))}\n"
                f"Weekly roundup: {yn(bool(a.get('weekly_roundup_enabled', True)))}\n"
                f"Rivalry alerts: {yn(bool(a.get('rivalry_enabled', True)))}\n"
                f"Monthly leaderboard: {yn(bool(a.get('monthly_leaderboard_enabled', True)))}\n"
                f"Zero-score roasts: {yn(bool(a.get('zero_score_roasts_enabled', True)))}\n"
                f"Personal best messages: {yn(bool(a.get('pb_messages_enabled', True)))}\n"
                f"Perfect score messages: {yn(bool(a.get('perfect_score_enabled', True)))}"
            ),
            inline=False,
        )

        e.add_field(
            name=f"🕒 Times ({tz_str})",
            value=(
                f"Daily post: {t.get('daily_post','00:00')}\n"
                f"Daily scoreboard: {t.get('daily_scoreboard','23:30')}\n"
                f"Weekly roundup: {t.get('weekly_roundup','23:45')}\n"
                f"Rivalry: {t.get('rivalry','14:00')}\n"
                f"Monthly leaderboard: {t.get('monthly_leaderboard','00:10')}"
            ),
            inline=False,
        )

        channel = self.settings.get("channel_id")
        roles = self.settings.get("admin_role_ids", [])

        e.add_field(
            name="🔒 Access",
            value=(
                f"Channel: {f'<#{channel}>' if channel else 'Not set'}\n"
                f"Admin roles: {', '.join(f'<@&{r}>' for r in roles) if roles else 'Admins only'}"
            ),
            inline=False,
        )

        e.set_footer(text="Changes save immediately. Use /settimezone to change timezone.")
        return e

    async def save_and_refresh(self, interaction: discord.Interaction, msg: str):
        save_guild_settings(self.guild_id, self.settings, msg)
        await interaction.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="Toggle bot", style=discord.ButtonStyle.secondary)
    async def toggle(self, interaction: discord.Interaction, _):
        self.settings["enabled"] = not bool(self.settings.get("enabled", True))
        await self.save_and_refresh(interaction, "MapTap: toggle bot")

    @discord.ui.button(label="Edit times", style=discord.ButtonStyle.primary)
    async def edit_times(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(TimeSettingsModal(self))

    @discord.ui.button(label="Configure alerts", style=discord.ButtonStyle.primary)
    async def configure_alerts(self, interaction: discord.Interaction, _):
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="⚙️ Configure alerts",
                description="Toggle what the bot posts, then hit **Save alerts**.",
            ),
            view=ConfigureAlertsView(self),
        )

    @discord.ui.button(label="Reset data", style=discord.ButtonStyle.danger)
    async def reset(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(ResetPasswordModal(self))

# =====================================================
# MESSAGE LISTENER (SCORE INGEST)
# =====================================================
@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if not message.guild:
        return

    guild_id = str(message.guild.id)
    settings, _ = load_guild_settings(guild_id)

    if not settings.get("enabled", True):
        return

    if message.channel.id != settings.get("channel_id"):
        return

    if not MAPTAP_HINT_REGEX.search(message.content or ""):
        return

    m = SCORE_REGEX.search(message.content or "")
    if not m:
        return

    score = int(m.group(1))
    if score > MAX_SCORE:
        await react_safe(message, settings["emojis"]["too_high"], "❌")
        return

    tz = get_guild_tz(settings)
    msg_time = message.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
    dkey = today_key(msg_time, tz)
    uid = str(message.author.id)

    all_scores, guild_scores, scores_sha = load_guild_scores(guild_id)
    all_users, guild_users, users_sha = load_guild_users(guild_id)

    guild_scores.setdefault(dkey, {})
    guild_users.setdefault(uid, default_user_stats())

    # Ensure keys exist for older entries
    guild_users[uid].setdefault("personal_best", {"score": 0, "date": "N/A"})
    guild_users[uid].setdefault("personal_low", {"score": 100000, "date": "N/A"})
    guild_users[uid].setdefault("best_streak", 0)
    guild_users[uid].setdefault("total_points", 0)
    guild_users[uid].setdefault("days_played", 0)

    # Replace same-day entry without double-counting days
    if uid in guild_scores[dkey]:
        try:
            guild_users[uid]["total_points"] -= int(guild_scores[dkey][uid].get("score", 0))
        except Exception:
            pass
    else:
        guild_users[uid]["days_played"] += 1

    guild_users[uid]["total_points"] += score
    guild_scores[dkey][uid] = {"score": score, "updated_at": msg_time.isoformat()}

    alerts = settings.get("alerts", DEFAULT_GUILD_SETTINGS["alerts"])

    # Zero roast
    if alerts.get("zero_score_roasts_enabled", True) and has_zero_round(message.content or ""):
        await message.channel.send(
            random.choice([
                f"💀 {message.author.mention} dropped a **0** round",
                f"🗺️ {message.author.mention} learned nothing today",
            ])
        )

    # Perfect score
    if alerts.get("perfect_score_enabled", True) and score >= MAX_SCORE:
        await message.channel.send(
            f"🎯 **Perfect Score!** {message.author.mention} just hit **{score}**!"
        )

    # Personal Best (highest)
    old_pb = int(guild_users[uid]["personal_best"].get("score", 0))
    if score > old_pb:
        guild_users[uid]["personal_best"] = {"score": score, "date": dkey}
        if alerts.get("pb_messages_enabled", True) and old_pb > 0:
            await message.channel.send(
                f"🚀 **New Personal Best!**\n"
                f"{message.author.mention} just beat their previous record of **{old_pb}** with **{score}**!"
            )

    # Personal Low (lowest)
    old_low = int(guild_users[uid]["personal_low"].get("score", 100000))
    if score < old_low:
        guild_users[uid]["personal_low"] = {"score": score, "date": dkey}
        if alerts.get("pb_messages_enabled", True) and old_low != 100000:
            await message.channel.send(
                f"🧯 **New Personal Low!**\n"
                f"{message.author.mention} just went lower than their previous worst (**{old_low}**) with **{score}** 😭"
            )

    # Streaks
    cur = calculate_current_streak(guild_scores, uid, tz)
    try:
        guild_users[uid]["best_streak"] = max(int(guild_users[uid].get("best_streak", 0)), int(cur))
    except Exception:
        guild_users[uid]["best_streak"] = cur

    save_guild_scores(guild_id, all_scores, guild_scores, scores_sha, "MapTap score update")
    save_guild_users(guild_id, all_users, guild_users, users_sha, "MapTap user update")

    await react_safe(message, settings["emojis"]["recorded"], "✅")

# =====================================================
# SCHEDULED ACTIONS
# =====================================================
def build_daily_prompt() -> str:
    return (
        "🗺️ **Daily MapTap is live!**\n"
        f"👉 {MAPTAP_URL}\n\n"
        "Post your results **exactly as shared from the app** so I can track scores ✈️"
    )

def build_daily_scoreboard_text(date_key: str, rows: List[Tuple[str, int]]) -> str:
    try:
        pretty = datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")
    except Exception:
        pretty = date_key

    if not rows:
        return f"🗺️ **MapTap — Daily Scores**\n*{pretty}*\n\n😶 No scores today."

    lines = [f"{i}. <@{uid}> — **{score}**" for i, (uid, score) in enumerate(rows, start=1)]
    return (
        f"🗺️ **MapTap — Daily Scores**\n*{pretty}*\n\n"
        + "\n".join(lines)
        + f"\n\n✈️ Players today: **{len(rows)}**"
    )

def build_weekly_roundup_text(mon: date, sun: date, rows: List[Tuple[str, int, int]]) -> str:
    header = (
        "🗺️ **MapTap — Weekly Round-Up**\n"
        f"*Mon {mon.strftime('%d %b')} → Sun {sun.strftime('%d %b')}*\n\n"
    )
    if not rows:
        return header + "😶 No scores this week."

    lines = [
        f"{i}. <@{uid}> — **{total} pts** ({days}/7 days)"
        for i, (uid, total, days) in enumerate(rows, start=1)
    ]
    return header + "\n".join(lines) + f"\n\n✈️ Weekly players: **{len(rows)}**"

async def do_daily_post(guild_id: str, settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if ch:
        await ch.send(build_daily_prompt())

async def do_daily_scoreboard(guild_id: str, settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    tz = get_guild_tz(settings)
    _, guild_scores, scores_sha = load_guild_scores(guild_id)

    today = datetime.now(tz).date().isoformat()
    bucket = guild_scores.get(today, {})

    rows: List[Tuple[str, int]] = []
    if isinstance(bucket, dict):
        for uid, entry in bucket.items():
            try:
                rows.append((uid, int(entry["score"])))
            except Exception:
                pass

    rows.sort(key=lambda x: x[1], reverse=True)
    await ch.send(build_daily_scoreboard_text(today, rows))

    # Cleanup old scores for this guild only
    all_scores, _, sha = load_guild_scores(guild_id)
    cutoff = datetime.now(tz).date() - timedelta(days=CLEANUP_DAYS)
    cleaned = {d: v for d, v in guild_scores.items() if (dd := _safe_date(d)) and dd >= cutoff}

    if cleaned != guild_scores:
        save_guild_scores(guild_id, all_scores, cleaned, sha, f"MapTap cleanup guild {guild_id}")

async def do_weekly_roundup(guild_id: str, settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    tz = get_guild_tz(settings)
    _, guild_scores, _ = load_guild_scores(guild_id)

    today = datetime.now(tz).date()
    mon, sun = week_range(today)

    weekly = compute_period_rows(guild_scores, mon, sun)

    rows: List[Tuple[str, int, int]] = [
        (uid, int(v["total"]), int(v["days"]))
        for uid, v in weekly.items()
        if v["days"] > 0
    ]
    rows.sort(key=lambda x: x[1], reverse=True)
    await ch.send(build_weekly_roundup_text(mon, sun, rows))

async def do_monthly_leaderboard(guild_id: str, settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    tz = get_guild_tz(settings)
    _, guild_scores, _ = load_guild_scores(guild_id)

    today = datetime.now(tz).date()
    start_d, end_d = month_range(today)

    totals = compute_period_rows(guild_scores, start_d, end_d)
    min_days = int(settings.get("minimum_days", {}).get("this_month", 0))

    rows: List[Tuple[str, int]] = []
    for uid, v in totals.items():
        if v["days"] < min_days:
            continue
        avg = round(v["total"] / v["days"])
        rows.append((uid, avg))

    rows.sort(key=lambda x: x[1], reverse=True)
    rows = rows[:10]
    if not rows:
        return

    lines = [f"{i}. <@{uid}> — **{avg}**" for i, (uid, avg) in enumerate(rows, 1)]
    await ch.send(
        "🏆 **Monthly MapTap Leaderboard**\n\n"
        + "\n".join(lines)
        + "\n\n*Ranked by average score this month*"
    )

async def do_rivalry_alert(guild_id: str, settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    tz = get_guild_tz(settings)
    _, guild_scores, _ = load_guild_scores(guild_id)

    today = datetime.now(tz).date()
    mon, _ = week_range(today)

    totals = compute_period_rows(guild_scores, mon, today)
    if len(totals) < RIVALRY_MIN_PLAYERS:
        return

    leaderboard: List[Tuple[str, int]] = [
        (uid, int(v["total"]))
        for uid, v in totals.items()
        if v["days"] > 0
    ]
    leaderboard.sort(key=lambda x: x[1], reverse=True)

    best_pair = None
    best_diff = None

    for i in range(len(leaderboard) - 1):
        uid_a, total_a = leaderboard[i]
        uid_b, total_b = leaderboard[i + 1]
        diff = total_a - total_b
        if diff <= 0:
            continue
        if diff <= RIVALRY_THRESHOLD and (best_diff is None or diff < best_diff):
            best_diff = diff
            best_pair = (uid_a, total_a, uid_b, total_b)

    if not best_pair:
        return

    uid_a, total_a, uid_b, total_b = best_pair
    diff = int(total_a) - int(total_b)

    await ch.send(
        "⚔️ **Rivalry Alert!**\n"
        f"<@{uid_b}> is only **{diff} points** behind <@{uid_a}> this week…\n"
        "One day can change everything 👀"
    )

# =====================================================
# SLASH COMMANDS
# =====================================================

# /settimezone — with autocomplete
@client.tree.command(name="settimezone", description="Set your server's timezone")
@app_commands.describe(timezone="Start typing to search — e.g. 'London', 'New York', 'Tokyo'")
async def settimezone(interaction: discord.Interaction, timezone: str):
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Server only.", ephemeral=True)
        return

    guild_id = str(interaction.guild_id)
    settings, _ = load_guild_settings(guild_id)

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don't have permission to do that.", ephemeral=True)
        return

    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, Exception):
        await interaction.response.send_message(
            f"❌ **{timezone}** isn't a valid timezone. Try searching again — e.g. `Europe/London`, `America/New_York`.",
            ephemeral=True,
        )
        return

    settings["timezone"] = timezone
    save_guild_settings(guild_id, settings, f"MapTap: set timezone {timezone}")

    await interaction.response.send_message(
        f"✅ Timezone set to **{timezone}**. All scheduled times will now use this timezone.",
        ephemeral=True,
    )

@settimezone.autocomplete("timezone")
async def timezone_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    all_tz = sorted(available_timezones())
    if current:
        matches = [tz for tz in all_tz if current.lower() in tz.lower()]
    else:
        # Show some sensible defaults before the user has typed anything
        matches = [
            "Europe/London", "Europe/Paris", "Europe/Berlin", "Europe/Amsterdam",
            "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
            "America/Toronto", "America/Vancouver", "Australia/Sydney", "Australia/Melbourne",
            "Asia/Tokyo", "Asia/Singapore", "Asia/Dubai", "Pacific/Auckland",
        ]
    return [app_commands.Choice(name=tz, value=tz) for tz in matches[:25]]


# /mymaptap
@client.tree.command(name="mymaptap", description="View your MapTap stats")
async def mymaptap(interaction: discord.Interaction):
    if not interaction.guild_id:
        await interaction.response.send_message("❌ Server only.", ephemeral=True)
        return

    guild_id = str(interaction.guild_id)
    settings, _ = load_guild_settings(guild_id)
    tz = get_guild_tz(settings)

    _, guild_users, _ = load_guild_users(guild_id)
    _, guild_scores, _ = load_guild_scores(guild_id)

    uid = str(interaction.user.id)
    stats = guild_users.get(uid)

    if not stats:
        await interaction.response.send_message("🗺️ You don't have any MapTap scores yet.", ephemeral=True)
        return

    stats.setdefault("personal_best", {"score": 0, "date": "N/A"})
    stats.setdefault("personal_low", {"score": 100000, "date": "N/A"})
    stats.setdefault("best_streak", 0)
    stats.setdefault("total_points", 0)
    stats.setdefault("days_played", 0)

    rank, total_players = calculate_all_time_rank(guild_users, uid)
    current_streak = calculate_current_streak(guild_scores, uid, tz)
    average_score = round(int(stats["total_points"]) / max(1, int(stats["days_played"])))

    today = datetime.now(tz).date()
    week_start = today - timedelta(days=today.weekday())
    week_rank, week_total = calculate_period_rank(guild_scores, uid, week_start, today)
    global_rank, global_total = calculate_global_rank(uid)

    pb = stats["personal_best"]
    pb_date = pb.get("date", "N/A")
    if pb_date != "N/A":
        try:
            pb_date = datetime.strptime(pb_date, "%Y-%m-%d").strftime("%d %b %Y")
        except Exception:
            pass

    pl = stats["personal_low"]
    low_score = int(pl.get("score", 100000))
    low_date = pl.get("date", "N/A")
    if low_date != "N/A":
        try:
            low_date = datetime.strptime(low_date, "%Y-%m-%d").strftime("%d %b %Y")
        except Exception:
            pass

    low_line = "Personal Low: **—**"
    if low_score != 100000:
        low_line = f"Personal Low: **{low_score}** ({low_date})"

    embed = discord.Embed(
        title=f"🗺️ MapTap Stats — {interaction.user.display_name}",
        color=0x2ECC71,
    )

    embed.add_field(
        name="📊 Server Rankings",
        value=(
            f"🥇 All-Time: **#{rank} of {total_players}**\n"
            f"🏁 This Week: {f'**#{week_rank} of {week_total}**' if week_rank else 'No rank yet'}"
        ),
        inline=False,
    )

    global_rank_line = (
        f"🌍 Global Rank: **#{global_rank} of {global_total}**"
        if global_rank else "🌍 Global Rank: **—**"
    )

    embed.add_field(
        name="🌐 Global",
        value=global_rank_line,
        inline=False,
    )

    embed.add_field(
        name="⭐ Personal Records",
        value=(
            f"Personal Best: **{int(pb.get('score', 0))}** ({pb_date})\n"
            f"{low_line}\n"
            f"Best Streak: 🏆 **{stats.get('best_streak', 0)} days**\n"
            f"Current Streak: 🔥 **{current_streak} days**"
        ),
        inline=False,
    )

    embed.add_field(
        name="📈 Overall Stats",
        value=(
            f"Total Points: **{stats.get('total_points', 0)}**\n"
            f"Days Played: **{stats.get('days_played', 0)}**\n"
            f"Average Score: **{average_score}**"
        ),
        inline=False,
    )

    await interaction.response.send_message(embed=embed)


# /maptapsettings
@client.tree.command(name="maptapsettings", description="Configure MapTap settings")
async def maptapsettings(interaction: discord.Interaction):
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    guild_id = str(interaction.guild_id)
    settings, _ = load_guild_settings(guild_id)

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don't have permission to configure MapTap.", ephemeral=True)
        return

    view = MapTapSettingsView(settings, guild_id)
    await interaction.response.send_message(embed=view.embed(), view=view, ephemeral=True)


# /leaderboard
class LeaderboardSelect(discord.ui.Select):
    def __init__(self, guild_id: str, settings: Dict[str, Any]):
        self.guild_id = guild_id
        self.settings = settings
        options = [
            discord.SelectOption(label="This week", value="this_week"),
            discord.SelectOption(label="This month", value="this_month"),
            discord.SelectOption(label="All-time", value="all_time"),
        ]
        super().__init__(placeholder="Choose a leaderboard…", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        scope = self.values[0]
        tz = get_guild_tz(self.settings)
        _, guild_scores, _ = load_guild_scores(self.guild_id)

        today = datetime.now(tz).date()
        start_d = end_d = None

        if scope == "this_week":
            start_d = today - timedelta(days=today.weekday())
            end_d = today
        elif scope == "this_month":
            start_d = today.replace(day=1)
            end_d = today

        totals = compute_period_rows(guild_scores, start_d, end_d)
        min_days = int(self.settings.get("minimum_days", {}).get(scope, 0))

        rows: List[Tuple[str, int]] = []
        for uid, v in totals.items():
            if v["days"] < min_days:
                continue
            avg = round(v["total"] / v["days"])
            rows.append((uid, avg))

        rows.sort(key=lambda x: x[1], reverse=True)
        rows = rows[:20]

        embed = discord.Embed(
            title="🗺️ MapTap Leaderboard",
            description=f"*{scope.replace('_', ' ').title()}*",
            color=0x3498DB,
        )

        if not rows:
            embed.add_field(name="No data", value="No eligible scores for this period.", inline=False)
        else:
            lines = [
                f"{i}. {display_user(interaction.guild, uid)} — **{avg}**"
                for i, (uid, avg) in enumerate(rows, start=1)
            ]
            embed.add_field(name="Top Players (avg score)", value="\n".join(lines), inline=False)

        await interaction.response.edit_message(embed=embed, view=self.view)

class LeaderboardView(discord.ui.View):
    def __init__(self, guild_id: str, settings: Dict[str, Any]):
        super().__init__(timeout=180)
        self.add_item(LeaderboardSelect(guild_id, settings))

@client.tree.command(name="leaderboard", description="View MapTap leaderboards")
async def leaderboard(interaction: discord.Interaction):
    if not interaction.guild_id:
        await interaction.response.send_message("❌ Server only.", ephemeral=True)
        return

    guild_id = str(interaction.guild_id)
    settings, _ = load_guild_settings(guild_id)

    await interaction.response.send_message(
        embed=discord.Embed(
            title="🗺️ MapTap Leaderboard",
            description="Select a leaderboard to view",
            color=0x3498DB,
        ),
        view=LeaderboardView(guild_id, settings),
    )


# /rescan
@client.tree.command(name="rescan", description="Re-scan ALL MapTap posts and rebuild stats (admin)")
async def rescan(interaction: discord.Interaction):
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Server only.", ephemeral=True)
        return

    guild_id = str(interaction.guild_id)
    settings, _ = load_guild_settings(guild_id)

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ No permission", ephemeral=True)
        return

    channel = get_configured_channel(settings)
    if not channel:
        await interaction.response.send_message("❌ MapTap channel not set", ephemeral=True)
        return

    tz = get_guild_tz(settings)
    await interaction.response.send_message("🔁 Full rescan started… this may take a moment.", ephemeral=True)

    guild_scores: Dict[str, Dict[str, Dict[str, Any]]] = {}
    guild_users: Dict[str, Dict[str, Any]] = {}
    ingested = 0

    async for msg in channel.history(limit=None, oldest_first=True):
        if msg.author.bot:
            continue
        if not MAPTAP_HINT_REGEX.search(msg.content or ""):
            continue

        m = SCORE_REGEX.search(msg.content or "")
        if not m:
            continue

        score = int(m.group(1))
        if score > MAX_SCORE:
            continue

        msg_time = msg.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
        dkey = today_key(msg_time, tz)
        uid = str(msg.author.id)

        guild_scores.setdefault(dkey, {})
        guild_scores[dkey][uid] = {"score": score, "updated_at": msg_time.isoformat()}

        guild_users.setdefault(uid, default_user_stats())
        guild_users[uid]["total_points"] += score
        ingested += 1

        if score > int(guild_users[uid]["personal_best"]["score"]):
            guild_users[uid]["personal_best"] = {"score": score, "date": dkey}

        if score < int(guild_users[uid]["personal_low"]["score"]):
            guild_users[uid]["personal_low"] = {"score": score, "date": dkey}

        await react_safe(msg, settings["emojis"]["rescan_ingested"], "🔁")

    for uid in guild_users:
        played_days = {dkey for dkey, bucket in guild_scores.items() if uid in bucket}
        guild_users[uid]["days_played"] = len(played_days)
        guild_users[uid]["best_streak"] = calculate_current_streak(guild_scores, uid, tz)

    # Merge rebuilt data back into the full files
    all_scores, _, scores_sha = load_guild_scores(guild_id)
    all_users, _, users_sha = load_guild_users(guild_id)

    save_guild_scores(guild_id, all_scores, guild_scores, scores_sha, f"MapTap rescan guild {guild_id}")
    save_guild_users(guild_id, all_users, guild_users, users_sha, f"MapTap rescan guild {guild_id}")

    await channel.send(
        f"✅ **Rescan complete**\n"
        f"• Scores ingested: **{ingested}**\n"
        f"• Players rebuilt: **{len(guild_users)}**\n\n"
        f"_All stats rebuilt from history_"
    )


# /repair_stats
@client.tree.command(name="repair_stats", description="Repair MapTap user stats from existing score data (admin)")
async def repair_stats(interaction: discord.Interaction):
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Server only.", ephemeral=True)
        return

    guild_id = str(interaction.guild_id)
    settings, _ = load_guild_settings(guild_id)

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ No permission", ephemeral=True)
        return

    tz = get_guild_tz(settings)
    await interaction.response.send_message("🛠️ Repairing MapTap stats…", ephemeral=True)

    all_scores, guild_scores, scores_sha = load_guild_scores(guild_id)
    all_users, _, users_sha = load_guild_users(guild_id)

    rebuilt: Dict[str, Dict[str, Any]] = {}
    played_days: Dict[str, set] = {}

    for dkey, bucket in guild_scores.items():
        if not isinstance(bucket, dict):
            continue
        for uid, entry in bucket.items():
            try:
                sc = int(entry["score"])
            except Exception:
                continue

            rebuilt.setdefault(uid, default_user_stats())
            played_days.setdefault(uid, set()).add(dkey)
            rebuilt[uid]["total_points"] += sc

            if sc > int(rebuilt[uid]["personal_best"]["score"]):
                rebuilt[uid]["personal_best"] = {"score": sc, "date": dkey}

            if sc < int(rebuilt[uid]["personal_low"]["score"]):
                rebuilt[uid]["personal_low"] = {"score": sc, "date": dkey}

    for uid, days in played_days.items():
        rebuilt[uid]["days_played"] = len(days)
        rebuilt[uid]["best_streak"] = calculate_current_streak(guild_scores, uid, tz)

    save_guild_users(guild_id, all_users, rebuilt, users_sha, f"MapTap repair stats guild {guild_id}")
    await interaction.followup.send(f"✅ Repair complete — users repaired: **{len(rebuilt)}**", ephemeral=False)


# /help
@client.tree.command(name="help", description="How to use the MapTap bot")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🗺️ MapTap Bot — Help",
        description=(
            f"Track your daily [MapTap]({MAPTAP_URL}) scores, compete on leaderboards, "
            "and get automated posts — all inside Discord."
        ),
        color=0xF1C40F,
    )

    embed.add_field(
        name="🚀 Getting started (admins)",
        value=(
            "1. Run `/maptapsettings` and select your score-posting channel\n"
            "2. Optionally set admin roles who can manage the bot\n"
            "3. Set your server timezone with `/settimezone`\n"
            "4. Adjust posting times and toggle alerts to your liking\n"
            "5. Members post their MapTap results in the configured channel — the bot handles the rest"
        ),
        inline=False,
    )

    embed.add_field(
        name="📋 Commands",
        value=(
            "`/mymaptap` — Your personal stats, streaks, PBs and rankings\n"
            "`/leaderboard` — Server leaderboards (this week / month / all-time)\n"
            "`/settimezone` — Set your server's timezone (admin)\n"
            "`/maptapsettings` — Configure the bot (admin)\n"
            "`/rescan` — Rebuild all stats from channel history (admin)\n"
            "`/repair_stats` — Rebuild user stats from saved scores (admin)\n"
            "`/help` — This message"
        ),
        inline=False,
    )

    embed.add_field(
        name="📬 Automatic posts",
        value=(
            "**Daily post** — Reminds everyone to play\n"
            "**Daily scoreboard** — End-of-day score summary\n"
            "**Weekly roundup** — Sunday summary of the week\n"
            "**Rivalry alert** — Fires when two players are neck and neck\n"
            "**Monthly leaderboard** — Top 10 by average on the 1st of each month"
        ),
        inline=False,
    )

    embed.add_field(
        name="🎯 Score tracking",
        value=(
            "Just post your MapTap results in the configured channel exactly as shared from the app. "
            "The bot will react to confirm it's been recorded. "
            "Personal bests, streaks, and roasts are all automatic."
        ),
        inline=False,
    )

    embed.set_footer(text=f"MapTap → {MAPTAP_URL}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# =====================================================
# STARTUP
# =====================================================
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("Missing TOKEN env var")
    if not GITHUB_TOKEN or not GITHUB_REPO:
        raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPO env vars")

    Thread(target=run_web, daemon=True).start()
    client.run(TOKEN)
