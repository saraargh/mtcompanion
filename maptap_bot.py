# =========================
# MapTap Companion Bot (FULL FILE)
# =========================

import os
import json
import re
import base64
import io
import random
import requests
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
RESET_PASSWORD = os.getenv("RESET_PASSWORD")  # Render env var for /resetmaptap

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # e.g. "saraargh/the-pilot"

SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")
SETTINGS_PATH = os.getenv("MAPTAP_SETTINGS_PATH", "data/maptap_settings.json")

MAPTAP_URL = os.getenv("MAPTAP_URL", "https://www.maptap.gg")
CLEANUP_DAYS = int(os.getenv("MAPTAP_CLEANUP_DAYS", "69"))
MAX_SCORE = int(os.getenv("MAPTAP_MAX_SCORE", "1000"))

# Parse "Final score: 606" (case-insensitive)
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)

# Detect a zero ROUND (NOT final score): matches " 0🎯" / "0🔥" etc, but not "90" or "100"
ZERO_ROUND_REGEX = re.compile(r"(?:^|\s)0(?!\d)")

# =====================================================
# DEFAULT SETTINGS (GitHub-backed)
# =====================================================
DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,  # set via settings panel

    "daily_post_enabled": True,
    "daily_scoreboard_enabled": True,
    "weekly_roundup_enabled": True,
    "rivalry_enabled": True,
    "monthly_leaderboard_enabled": True,      # NEW
    "zero_score_roast_enabled": True,         # NEW

    "admin_role_ids": [],  # set via settings panel

    # Emoji defaults (NOT editable via UI anymore)
    "emojis": {
        "recorded": "🌏",
        "too_high": "❌",
        "rescan_ingested": "🔁",
        "config_issue": "⚠️"
    },

    # Times are configurable (UK time, 24h HH:MM)
    "times": {
        "daily_post": "00:00",
        "daily_scoreboard": "23:30",
        "weekly_roundup": "23:45",
        "rivalry": "14:00",
        "monthly_leaderboard": "00:10"  # NEW: runs on the 1st
    },

    # Prevent double-posting (updated automatically)
    "last_run": {
        "daily_post": None,            # YYYY-MM-DD
        "daily_scoreboard": None,      # YYYY-MM-DD
        "weekly_roundup": None,        # YYYY-MM-DD (Sunday date)
        "rivalry": None,               # YYYY-MM-DD
        "monthly_leaderboard": None    # YYYY-MM (e.g. 2025-12)
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

    # Normalize channel_id
    if merged.get("channel_id") is not None:
        try:
            merged["channel_id"] = int(merged["channel_id"])
        except Exception:
            merged["channel_id"] = None

    merged["admin_role_ids"] = [int(x) for x in merged.get("admin_role_ids", []) if str(x).isdigit()]

    # Merge emojis safely
    merged["emojis"] = _merge_nested(DEFAULT_SETTINGS["emojis"], merged.get("emojis"))

    # Merge times safely + validate HH:MM
    times_in = _merge_nested(DEFAULT_SETTINGS["times"], merged.get("times"))
    merged["times"] = {
        "daily_post": _normalize_hhmm(times_in.get("daily_post"), DEFAULT_SETTINGS["times"]["daily_post"]),
        "daily_scoreboard": _normalize_hhmm(times_in.get("daily_scoreboard"), DEFAULT_SETTINGS["times"]["daily_scoreboard"]),
        "weekly_roundup": _normalize_hhmm(times_in.get("weekly_roundup"), DEFAULT_SETTINGS["times"]["weekly_roundup"]),
        "rivalry": _normalize_hhmm(times_in.get("rivalry"), DEFAULT_SETTINGS["times"]["rivalry"]),
        "monthly_leaderboard": _normalize_hhmm(times_in.get("monthly_leaderboard"), DEFAULT_SETTINGS["times"]["monthly_leaderboard"]),
    }

    # Merge last_run safely
    merged["last_run"] = _merge_nested(DEFAULT_SETTINGS["last_run"], merged.get("last_run"))
    if not isinstance(merged["last_run"], dict):
        merged["last_run"] = DEFAULT_SETTINGS["last_run"].copy()

    # Ensure new toggles exist even for old settings files
    merged["monthly_leaderboard_enabled"] = bool(merged.get("monthly_leaderboard_enabled", True))
    merged["zero_score_roast_enabled"] = bool(merged.get("zero_score_roast_enabled", True))

    return merged, sha

def save_settings(settings: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    return github_save_json(SETTINGS_PATH, settings, sha, message)

def today_key(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(UK_TZ)
    return dt.date().isoformat()

def month_key(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(UK_TZ)
    return f"{dt.year:04d}-{dt.month:02d}"

# =====================================================
# RANKING / LEADERBOARD HELPERS (AVERAGE-BASED)
# =====================================================
def _avg(total: int, days: int) -> float:
    if days <= 0:
        return 0.0
    return total / days

def calculate_all_time_rank(users: Dict[str, Any], user_id: str, min_days: int = 10) -> Tuple[int, int]:
    """
    Average-based rank.
    Sort by (avg desc, days_played desc)
    Only include players with days_played >= min_days.
    Returns (rank, total_players) rank is 1-based.
    """
    leaderboard: List[Tuple[str, float, int]] = []
    for uid, data in users.items():
        try:
            total = int(data.get("total_points", 0))
            days = int(data.get("days_played", 0))
            if days < min_days:
                continue
            leaderboard.append((uid, _avg(total, days), days))
        except Exception:
            continue

    leaderboard.sort(key=lambda x: (x[1], x[2]), reverse=True)
    total_players = len(leaderboard)

    for idx, (uid, _, _) in enumerate(leaderboard, start=1):
        if uid == user_id:
            return idx, total_players

    return total_players, total_players

def calculate_timeframe_stats(scores: Dict[str, Any], start: date, end: date) -> Dict[str, Dict[str, int]]:
    """
    Returns dict: uid -> {"total": int, "days": int}
    Days = number of distinct dates played in range (inclusive).
    """
    out: Dict[str, Dict[str, int]] = {}
    d = start
    while d <= end:
        dkey = d.isoformat()
        day_bucket = scores.get(dkey, {})
        if isinstance(day_bucket, dict):
            for uid, entry in day_bucket.items():
                if not isinstance(entry, dict) or "score" not in entry:
                    continue
                try:
                    sc = int(entry["score"])
                except Exception:
                    continue
                out.setdefault(uid, {"total": 0, "days": 0})
                out[uid]["total"] += sc
                out[uid]["days"] += 1
        d += timedelta(days=1)
    return out

def build_avg_leaderboard_rows(stats: Dict[str, Dict[str, int]], min_days: int) -> List[Tuple[str, float, int, int]]:
    """
    Returns rows: (uid, avg, total, days) sorted by (avg, days) desc.
    """
    rows: List[Tuple[str, float, int, int]] = []
    for uid, v in stats.items():
        days = int(v.get("days", 0))
        total = int(v.get("total", 0))
        if days < min_days:
            continue
        rows.append((uid, _avg(total, days), total, days))
    rows.sort(key=lambda x: (x[1], x[3]), reverse=True)
    return rows

def monday_of_week(d: datetime) -> date:
    return d.date() - timedelta(days=d.weekday())

def month_range(dt: datetime) -> Tuple[date, date]:
    start = date(dt.year, dt.month, 1)
    # find end: first day next month minus 1
    if dt.month == 12:
        next_month = date(dt.year + 1, 1, 1)
    else:
        next_month = date(dt.year, dt.month + 1, 1)
    end = next_month - timedelta(days=1)
    return start, end

# =====================================================
# SAFE REACTION HELPER (supports custom server emoji strings)
# =====================================================
async def react_safe(msg: discord.Message, emoji: str, fallback: str):
    try:
        await msg.add_reaction(emoji)
    except discord.HTTPException:
        try:
            await msg.add_reaction(fallback)
        except Exception:
            pass

# =====================================================
# DATE / STATS HELPERS
# =====================================================
def pretty_day(date_key: str) -> str:
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")

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
    played_dates = []
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

# =====================================================
# EMBED / MESSAGE BUILDERS
# =====================================================
def build_daily_prompt() -> str:
    return (
        "🗺️ **Daily MapTap is live!**\n"
        f"👉 {MAPTAP_URL}\n\n"
        "Post your results **exactly as shared from the app** so I can track scores ✈️"
    )

def build_daily_scoreboard_text(date_key: str, sorted_rows: List[Tuple[str, int]]) -> str:
    header = f"🗺️ **MapTap — Daily Scores**\n*{pretty_day(date_key)}*\n\n"
    if not sorted_rows:
        return header + "😶 No scores today."

    lines = [f"{i}. <@{uid}> — **{score}**" for i, (uid, score) in enumerate(sorted_rows, start=1)]
    footer = f"\n\n✈️ Players today: **{len(sorted_rows)}**"
    return header + "\n".join(lines) + footer

def build_weekly_roundup_text(mon: date, sun: date, sorted_rows: List[Tuple[str, int, int]]) -> str:
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

def leaderboard_to_text(title: str, rows: List[Tuple[str, float, int, int]], subtitle: str = "") -> str:
    out = []
    out.append(title)
    if subtitle:
        out.append(subtitle)
    out.append("")
    if not rows:
        out.append("😶 No qualifying players.")
        return "\n".join(out)

    for i, (uid, avg, total, days) in enumerate(rows, start=1):
        out.append(f"{i}. <@{uid}> — avg **{avg:.1f}** ({total} pts / {days} days)")
    return "\n".join(out)

def make_leaderboard_file(filename: str, text: str) -> discord.File:
    data = text.encode("utf-8")
    return discord.File(fp=io.BytesIO(data), filename=filename)

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
        this_month_key = month_key(now)

        times = settings.get("times", {})
        last_run = settings.get("last_run", {})

        # Daily Post
        if settings.get("daily_post_enabled") and now_hm == times.get("daily_post") and last_run.get("daily_post") != today:
            await do_daily_post(settings)
            settings["last_run"]["daily_post"] = today
            save_settings(settings, sha, "MapTap: auto daily post")

        # Daily Scoreboard
        if settings.get("daily_scoreboard_enabled") and now_hm == times.get("daily_scoreboard") and last_run.get("daily_scoreboard") != today:
            await do_daily_scoreboard(settings)
            settings["last_run"]["daily_scoreboard"] = today
            save_settings(settings, sha, "MapTap: auto daily scoreboard")

        # Weekly Roundup (Sundays)
        if settings.get("weekly_roundup_enabled") and now.weekday() == 6 and now_hm == times.get("weekly_roundup") and last_run.get("weekly_roundup") != today:
            await do_weekly_roundup(settings)
            settings["last_run"]["weekly_roundup"] = today
            save_settings(settings, sha, "MapTap: auto weekly roundup")

        # Rivalry Alerts (Saturdays)
        if settings.get("rivalry_enabled") and now.weekday() == 5 and now_hm == times.get("rivalry") and last_run.get("rivalry") != today:
            await do_rivalry_alert(settings)
            settings["last_run"]["rivalry"] = today
            save_settings(settings, sha, "MapTap: auto rivalry alert")

        # Monthly Leaderboard Auto Post (1st of month)
        if settings.get("monthly_leaderboard_enabled") and now.day == 1 and now_hm == times.get("monthly_leaderboard") and last_run.get("monthly_leaderboard") != this_month_key:
            await do_monthly_leaderboard(settings)
            settings["last_run"]["monthly_leaderboard"] = this_month_key
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
# SETTINGS PANEL VIEW + MODALS
# =====================================================
class TimeSettingsModal(discord.ui.Modal, title="MapTap Scheduled Times (UK)"):
    daily_post = discord.ui.TextInput(label="Daily post time (HH:MM)", placeholder="00:00", required=True, max_length=5)
    daily_scoreboard = discord.ui.TextInput(label="Daily scoreboard time (HH:MM)", placeholder="23:30", required=True, max_length=5)
    weekly_roundup = discord.ui.TextInput(label="Weekly roundup time (HH:MM) (Sunday)", placeholder="23:45", required=True, max_length=5)
    rivalry = discord.ui.TextInput(label="Rivalry alert time (HH:MM) (Saturday)", placeholder="14:00", required=True, max_length=5)
    monthly_leaderboard = discord.ui.TextInput(label="Monthly leaderboard time (HH:MM) (1st)", placeholder="00:10", required=True, max_length=5)

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
            await interaction.response.send_message("❌ Invalid time. Use **HH:MM** (24h), e.g. **23:30**.", ephemeral=True)
            return

        self.view_ref.settings.setdefault("times", {})
        self.view_ref.settings["times"].update(vals)
        await self.view_ref._save_refresh(interaction, "MapTap: update scheduled times")

class AlertsSettingsModal(discord.ui.Modal, title="MapTap Alerts"):
    daily_scoreboard_enabled = discord.ui.TextInput(label="Daily Scoreboard (yes/no)", placeholder="yes", required=True, max_length=3)
    weekly_roundup_enabled = discord.ui.TextInput(label="Weekly Roundup (yes/no)", placeholder="yes", required=True, max_length=3)
    rivalry_enabled = discord.ui.TextInput(label="Rivalry Alerts (yes/no)", placeholder="yes", required=True, max_length=3)
    zero_score_roast_enabled = discord.ui.TextInput(label="Zero Score Roasts (yes/no)", placeholder="yes", required=True, max_length=3)

    def __init__(self, view_ref: "MapTapSettingsView"):
        super().__init__()
        self.view_ref = view_ref
        s = self.view_ref.settings
        self.daily_scoreboard_enabled.default = "yes" if s.get("daily_scoreboard_enabled", True) else "no"
        self.weekly_roundup_enabled.default = "yes" if s.get("weekly_roundup_enabled", True) else "no"
        self.rivalry_enabled.default = "yes" if s.get("rivalry_enabled", True) else "no"
        self.zero_score_roast_enabled.default = "yes" if s.get("zero_score_roast_enabled", True) else "no"

    @staticmethod
    def _parse_bool(val: str, default: bool) -> bool:
        v = (val or "").strip().lower()
        if v in ("y", "yes", "true", "1", "on"):
            return True
        if v in ("n", "no", "false", "0", "off"):
            return False
        return default

    async def on_submit(self, interaction: discord.Interaction):
        s = self.view_ref.settings
        s["daily_scoreboard_enabled"] = self._parse_bool(self.daily_scoreboard_enabled.value, bool(s.get("daily_scoreboard_enabled", True)))
        s["weekly_roundup_enabled"] = self._parse_bool(self.weekly_roundup_enabled.value, bool(s.get("weekly_roundup_enabled", True)))
        s["rivalry_enabled"] = self._parse_bool(self.rivalry_enabled.value, bool(s.get("rivalry_enabled", True)))
        s["zero_score_roast_enabled"] = self._parse_bool(self.zero_score_roast_enabled.value, bool(s.get("zero_score_roast_enabled", True)))
        await self.view_ref._save_refresh(interaction, "MapTap: update alerts")

class MapTapSettingsView(discord.ui.View):
    def __init__(self, settings: Dict[str, Any], sha: Optional[str]):
        super().__init__(timeout=300)
        self.settings = settings
        self.sha = sha

    def _embed(self) -> discord.Embed:
        channel_str = f"<#{self.settings['channel_id']}>" if self.settings.get("channel_id") else "Not set"
        roles = self.settings.get("admin_role_ids", [])
        roles_str = ", ".join(f"<@&{rid}>" for rid in roles) if roles else "Admins only"

        t = self.settings.get("times", {})
        times_block = (
            f"Daily post: **{t.get('daily_post','00:00')}**\n"
            f"Daily scoreboard: **{t.get('daily_scoreboard','23:30')}**\n"
            f"Weekly roundup (Sundays): **{t.get('weekly_roundup','23:45')}**\n"
            f"Rivalry alert (Saturdays): **{t.get('rivalry','14:00')}**\n"
            f"Monthly leaderboard (1st): **{t.get('monthly_leaderboard','00:10')}**"
        )

        e = discord.Embed(title="🗺️ MapTap Settings", color=0xF1C40F)
        e.description = f"**Channel:** {channel_str}\n**Admin roles:** {roles_str}"

        status_block = (
            f"**Bot enabled:** {'✅' if self.settings.get('enabled') else '❌'}\n"
            f"**Daily post:** {'✅' if self.settings.get('daily_post_enabled') else '❌'}\n"
            f"**Daily scoreboard:** {'✅' if self.settings.get('daily_scoreboard_enabled') else '❌'}\n"
            f"**Weekly roundup:** {'✅' if self.settings.get('weekly_roundup_enabled') else '❌'}\n"
            f"**Rivalry alerts:** {'✅' if self.settings.get('rivalry_enabled') else '❌'}\n"
            f"**Monthly leaderboard:** {'✅' if self.settings.get('monthly_leaderboard_enabled') else '❌'}\n"
            f"**Zero score roasts:** {'✅' if self.settings.get('zero_score_roast_enabled') else '❌'}"
        )
        e.add_field(name="🧭 Status", value=status_block, inline=False)
        e.add_field(name="🕒 Times (UK)", value=str(times_block), inline=False)

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

    @discord.ui.button(label="Edit Times (UK)", style=discord.ButtonStyle.primary)
    async def edit_times(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(TimeSettingsModal(self))

    @discord.ui.button(label="Configure Alerts", style=discord.ButtonStyle.primary)
    async def edit_alerts(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(AlertsSettingsModal(self))

    @discord.ui.button(label="Toggle Bot Enabled", style=discord.ButtonStyle.secondary)
    async def toggle_bot_enabled(self, interaction: discord.Interaction, _):
        self.settings["enabled"] = not bool(self.settings.get("enabled", True))
        await self._save_refresh(interaction, "MapTap: toggle enabled")

    @discord.ui.button(label="Toggle Daily Post", style=discord.ButtonStyle.secondary)
    async def toggle_daily_post(self, interaction: discord.Interaction, _):
        self.settings["daily_post_enabled"] = not bool(self.settings.get("daily_post_enabled", True))
        await self._save_refresh(interaction, "MapTap: toggle daily post")

    @discord.ui.button(label="Toggle Monthly Leaderboard", style=discord.ButtonStyle.secondary)
    async def toggle_monthly(self, interaction: discord.Interaction, _):
        self.settings["monthly_leaderboard_enabled"] = not bool(self.settings.get("monthly_leaderboard_enabled", True))
        await self._save_refresh(interaction, "MapTap: toggle monthly leaderboard")

# =====================================================
# SLASH COMMAND: /maptapsettings
# =====================================================
@client.tree.command(name="maptapsettings", description="Configure MapTap bot settings")
async def maptapsettings(interaction: discord.Interaction):
    settings, sha = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Use this in a server, not DMs.", ephemeral=True)
        return

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission to manage MapTap settings.", ephemeral=True)
        return

    view = MapTapSettingsView(settings, sha)
    await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=False)

# =====================================================
# /mymaptap (EMBED + AVG RANK + WEEKLY POSITION)
# =====================================================
@client.tree.command(name="mymaptap", description="View MapTap stats")
@app_commands.describe(user="View stats for another user")
async def mymaptap(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})

    target = user or interaction.user
    user_id = str(target.id)
    stats = users.get(user_id)

    if not stats or int(stats.get("days_played", 0)) <= 0:
        await interaction.response.send_message(f"{target.display_name} hasn’t recorded any MapTap scores yet 🗺️", ephemeral=False)
        return

    total_points = int(stats.get("total_points", 0))
    days_played = int(stats.get("days_played", 0))
    avg_score = round(total_points / max(1, days_played))

    # All-time rank (avg based, min 10 days)
    rank_all, total_players_all = calculate_all_time_rank(users, user_id, min_days=10)

    # Weekly rank (avg based, min 3 days)
    now = datetime.now(UK_TZ)
    mon = monday_of_week(now)
    sun = mon + timedelta(days=6)

    weekly_stats = calculate_timeframe_stats(scores if isinstance(scores, dict) else {}, mon, sun)
    weekly_rows = build_avg_leaderboard_rows(weekly_stats, min_days=3)
    weekly_rank = None
    for i, (uid, _, _, _) in enumerate(weekly_rows, start=1):
        if uid == user_id:
            weekly_rank = i
            break

    # PB display
    pb_data = stats.get("personal_best", {"score": 0, "date": "N/A"})
    pb_score = pb_data.get("score", 0)
    pb_raw_date = pb_data.get("date", "N/A")
    pb_date_display = "N/A"
    if pb_raw_date != "N/A":
        try:
            pb_date_display = datetime.strptime(pb_raw_date, "%Y-%m-%d").strftime("%d/%m/%y")
        except Exception:
            pb_date_display = pb_raw_date

    cur_streak = calculate_current_streak(scores if isinstance(scores, dict) else {}, user_id)
    best_streak = int(stats.get("best_streak", 0))

    emb = discord.Embed(title=f"🗺️ MapTap Stats — {target.display_name}", color=0x2ECC71)
    emb.add_field(
        name="📊 Server Rankings",
        value=(
            f"**All-Time:** 🏅 **#{rank_all} of {total_players_all}** (avg **{avg_score}**)\n"
            f"**This Week:** 🏁 **#{weekly_rank}**" if weekly_rank else
            f"**All-Time:** 🏅 **#{rank_all} of {total_players_all}** (avg **{avg_score}**)\n"
            f"**This Week:** 🏁 Not qualified (min 3 days)"
        ),
        inline=False
    )
    emb.add_field(
        name="⭐ Personal Records",
        value=(
            f"**Personal Best:** **{pb_score}** ({pb_date_display})\n"
            f"**Best Streak:** 🏆 **{best_streak} days**\n"
            f"**Current Streak:** 🔥 **{cur_streak} days**"
        ),
        inline=False
    )
    emb.add_field(
        name="📈 Overall Stats",
        value=(
            f"**Total Points:** **{total_points}**\n"
            f"**Days Played:** **{days_played}**\n"
            f"**Average Score:** **{avg_score}**"
        ),
        inline=False
    )

    await interaction.response.send_message(embed=emb, ephemeral=False)

# =====================================================
# LEADERBOARD UI (DROPDOWN + DATE RANGE MODAL)
# =====================================================
class DateRangeModal(discord.ui.Modal, title="MapTap Date Range"):
    start_date = discord.ui.TextInput(label="Start date (YYYY-MM-DD)", placeholder="2025-12-01", required=True, max_length=10)
    end_date = discord.ui.TextInput(label="End date (YYYY-MM-DD)", placeholder="2025-12-31", required=True, max_length=10)

    def __init__(self, view_ref: "LeaderboardView"):
        super().__init__()
        self.view_ref = view_ref

    async def on_submit(self, interaction: discord.Interaction):
        try:
            s = datetime.strptime(str(self.start_date.value).strip(), "%Y-%m-%d").date()
            e = datetime.strptime(str(self.end_date.value).strip(), "%Y-%m-%d").date()
            if e < s:
                raise ValueError("end before start")
        except Exception:
            await interaction.response.send_message("❌ Invalid dates. Use **YYYY-MM-DD** and ensure end ≥ start.", ephemeral=True)
            return

        await self.view_ref.render(interaction, mode="range", start=s, end=e)

class LeaderboardSelect(discord.ui.Select):
    def __init__(self, view_ref: "LeaderboardView"):
        self.view_ref = view_ref
        options = [
            discord.SelectOption(label="This Week", value="week", description="Average-based (min 3 days)"),
            discord.SelectOption(label="This Month", value="month", description="Average-based (min 10 days)"),
            discord.SelectOption(label="All Time", value="all", description="Average-based (min 10 days)"),
            discord.SelectOption(label="Date Range", value="range", description="Custom dates (no minimum)"),
        ]
        super().__init__(placeholder="Choose a leaderboard…", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        value = self.values[0]
        if value == "range":
            await interaction.response.send_modal(DateRangeModal(self.view_ref))
            return
        await self.view_ref.render(interaction, mode=value)

class LeaderboardView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(LeaderboardSelect(self))

    async def render(self, interaction: discord.Interaction, mode: str, start: Optional[date] = None, end: Optional[date] = None):
        scores, _ = github_load_json(SCORES_PATH, {})
        users, _ = github_load_json(USERS_PATH, {})

        now = datetime.now(UK_TZ)

        if not isinstance(scores, dict):
            scores = {}
        if not isinstance(users, dict):
            users = {}

        title = "🗺️ MapTap Leaderboard"
        subtitle = ""
        rows: List[Tuple[str, float, int, int]] = []
        filename = "maptap_leaderboard.txt"

        if mode == "week":
            mon = monday_of_week(now)
            sun = mon + timedelta(days=6)
            stats = calculate_timeframe_stats(scores, mon, sun)
            rows = build_avg_leaderboard_rows(stats, min_days=3)
            subtitle = f"*This Week — {mon.strftime('%d %b')} to {sun.strftime('%d %b')} (min 3 days)*"
            filename = f"maptap_week_{mon.isoformat()}_{sun.isoformat()}.txt"

        elif mode == "month":
            mstart, mend = month_range(now)
            stats = calculate_timeframe_stats(scores, mstart, mend)
            rows = build_avg_leaderboard_rows(stats, min_days=10)
            subtitle = f"*This Month — {mstart.strftime('%d %b')} to {mend.strftime('%d %b')} (min 10 days)*"
            filename = f"maptap_month_{mstart.year:04d}-{mstart.month:02d}.txt"

        elif mode == "all":
            # Use users.json totals/days for all-time
            temp: Dict[str, Dict[str, int]] = {}
            for uid, d in users.items():
                try:
                    total = int(d.get("total_points", 0))
                    days = int(d.get("days_played", 0))
                except Exception:
                    continue
                temp[uid] = {"total": total, "days": days}
            rows = []
            for uid, v in temp.items():
                if int(v["days"]) < 10:
                    continue
                rows.append((uid, _avg(int(v["total"]), int(v["days"])), int(v["total"]), int(v["days"])))
            rows.sort(key=lambda x: (x[1], x[3]), reverse=True)
            subtitle = "*All Time — average-based (min 10 days)*"
            filename = "maptap_all_time.txt"

        elif mode == "range":
            if start is None or end is None:
                await interaction.response.send_message("❌ Missing date range.", ephemeral=True)
                return
            stats = calculate_timeframe_stats(scores, start, end)
            rows = build_avg_leaderboard_rows(stats, min_days=0)
            subtitle = f"*Date Range — {start.isoformat()} to {end.isoformat()} (no minimum)*"
            filename = f"maptap_range_{start.isoformat()}_{end.isoformat()}.txt"

        text = leaderboard_to_text(title, rows, subtitle)
        emb = discord.Embed(title=title, description=subtitle, color=0x3498DB)

        # show a small preview in embed, but attach FULL list as a .txt so you get EVERYONE
        preview = rows[:25]
        if preview:
            preview_lines = []
            for i, (uid, avg, total, days) in enumerate(preview, start=1):
                preview_lines.append(f"{i}. <@{uid}> — avg **{avg:.1f}** ({total}/{days})")
            emb.add_field(name="Top Preview (full list attached)", value="\n".join(preview_lines), inline=False)
        else:
            emb.add_field(name="Top Preview", value="😶 No qualifying players.", inline=False)

        file = make_leaderboard_file(filename, text)

        # If this is the first render, we edit the original message; otherwise also edit.
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=emb, view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=emb, view=self, attachments=[file])

@client.tree.command(name="leaderboard", description="View MapTap leaderboards (week/month/all-time/date range)")
async def leaderboard(interaction: discord.Interaction):
    settings, _ = load_settings()
    if not settings.get("enabled", True):
        await interaction.response.send_message("❌ MapTap is disabled.", ephemeral=True)
        return
    view = LeaderboardView()
    emb = discord.Embed(title="🗺️ MapTap Leaderboard", description="Choose a leaderboard from the dropdown.", color=0x3498DB)
    await interaction.response.send_message(embed=emb, view=view, ephemeral=False)

# =====================================================
# /resetmaptap (ADMIN + PASSWORD MODAL)
# =====================================================
class ResetModal(discord.ui.Modal, title="Reset MapTap"):
    password = discord.ui.TextInput(label="Reset password", placeholder="Enter password…", required=True, max_length=128)

    def __init__(self, settings: Dict[str, Any]):
        super().__init__()
        self.settings = settings

    async def on_submit(self, interaction: discord.Interaction):
        if not RESET_PASSWORD:
            await interaction.response.send_message("❌ RESET_PASSWORD is not set on Render.", ephemeral=True)
            return

        if str(self.password.value) != str(RESET_PASSWORD):
            await interaction.response.send_message("❌ Wrong password.", ephemeral=True)
            return

        scores, scores_sha = github_load_json(SCORES_PATH, {})
        users, users_sha = github_load_json(USERS_PATH, {})

        # wipe
        new_scores_sha = github_save_json(SCORES_PATH, {}, scores_sha, "MapTap: reset scores")
        new_users_sha = github_save_json(USERS_PATH, {}, users_sha, "MapTap: reset users")

        await interaction.response.send_message("✅ MapTap has been reset.", ephemeral=True)
        await interaction.channel.send(f"⚠️ {interaction.user.mention} has reset all MapTap server scores.")

@client.tree.command(name="resetmaptap", description="Reset all MapTap scores (admin only)")
async def resetmaptap(interaction: discord.Interaction):
    settings, _ = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Use this in a server, not DMs.", ephemeral=True)
        return
    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission to run this.", ephemeral=True)
        return

    await interaction.response.send_modal(ResetModal(settings))

# =====================================================
# SCORE INGESTION (MESSAGE LISTENER) + ZERO ROUND ROAST + PB MESSAGES
# =====================================================
PB_MESSAGES = [
    "🚀 **New Personal Best!** <@{uid}> just smashed their record: **{old} → {new}**!",
    "🔥 <@{uid}> just levelled up — PB destroyed: **{old} → {new}**!",
    "✨ PB ALERT ✨ <@{uid}> beat **{old}** with a spicy **{new}**!",
    "📈 <@{uid}> said ‘watch this’… PB is now **{new}** (was {old})!",
    "🏁 <@{uid}> just set a new PB: **{new}** (old: {old})!",
    "💥 <@{uid}> obliterated their PB — **{new}** (prev {old})!",
    "🧠 BIG BRAIN: <@{uid}> PB upgraded to **{new}** (from {old})!",
    "🥇 <@{uid}> new PB unlocked: **{new}** (was {old})!",
]

ZERO_ROASTS = [
    "💀 <@{uid}> posted a **0** round… geography just laughed.",
    "😭 <@{uid}> hit a **0** round — map said ‘absolutely not’.",
    "🧠 <@{uid}> went full vibes and pulled a **0** round.",
    "🥶 <@{uid}> a **0** round… that’s talent in its own way.",
    "🚨 <@{uid}> found every wrong place possible (0️⃣ round).",
]

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
    m = SCORE_REGEX.search(content)
    if not m:
        return

    em = settings.get("emojis", DEFAULT_SETTINGS["emojis"])

    score = int(m.group(1))
    if score > MAX_SCORE:
        await react_safe(message, em.get("too_high", "❌"), "❌")
        return

    # ✅ Zero-score roast is based on ROUND LINE, not final score
    # Only runs if enabled, and never during rescan (rescan does not call on_message)
    if settings.get("zero_score_roast_enabled", True):
        # Ignore lines that contain "Final score"
        for line in content.splitlines():
            if "final score" in line.lower():
                continue
            # if the line has a "0" token not followed by digit (0🎯 etc)
            if ZERO_ROUND_REGEX.search(line):
                await message.channel.send(random.choice(ZERO_ROASTS).format(uid=message.author.id))
                break

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
        "personal_best": {"score": 0, "date": "N/A"}
    })

    had_played_before = int(user_stats.get("days_played", 0)) > 0
    old_pb = int(user_stats.get("personal_best", {}).get("score", 0) or 0)

    # overwrite same-day: remove old score from totals
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
        "updated_at": msg_time_uk.isoformat()
    }

    # Perfect score alert
    if score >= MAX_SCORE:
        await message.reply(f"🎯 **PERFECT SCORE!** <@{user_id}> hit the maximum of **{MAX_SCORE}** points! Map Master! 🏆")

    # PB alert (randomised)
    if had_played_before and score > old_pb:
        await message.reply(random.choice(PB_MESSAGES).format(uid=user_id, old=old_pb, new=score))
        user_stats["personal_best"] = {"score": score, "date": date_key}
    elif not had_played_before:
        user_stats["personal_best"] = {"score": score, "date": date_key}

    cur_streak = calculate_current_streak(scores, user_id)
    if cur_streak > int(user_stats.get("best_streak", 0)):
        user_stats["best_streak"] = cur_streak

    github_save_json(SCORES_PATH, scores, scores_sha, f"MapTap: score update {date_key}")
    github_save_json(USERS_PATH, users, users_sha, f"MapTap: user stats update {user_id}")

    await react_safe(message, em.get("recorded", "🌏"), "✅")

# =====================================================
# /rescan (ADMIN ONLY) — unchanged behaviour + NO ROASTS
# =====================================================
@client.tree.command(
    name="rescan",
    description="Re-scan recent MapTap messages for missed scores (admin only)"
)
@app_commands.describe(messages="How many recent messages to scan (max 50)")
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

    messages = max(1, min(messages, 50))
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
            await react_safe(msg, em.get("too_high", "❌"), "❌")
            continue

        msg_time_uk = msg.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
        date_key = today_key(msg_time_uk)
        uid = str(msg.author.id)

        scores.setdefault(date_key, {})
        day_bucket = scores[date_key]

        if uid in day_bucket:
            skipped += 1
            continue

        user = users.setdefault(uid, {
            "total_points": 0,
            "days_played": 0,
            "best_streak": 0,
            "personal_best": {"score": 0, "date": "N/A"}
        })

        user["days_played"] += 1
        user["total_points"] += score

        day_bucket[uid] = {"score": score, "updated_at": msg_time_uk.isoformat()}

        # Silent PB update during rescan
        if score > int(user.get("personal_best", {}).get("score", 0) or 0):
            user["personal_best"] = {"score": score, "date": date_key}

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
# SCHEDULED ACTIONS (existing + monthly leaderboard)
# =====================================================
async def do_daily_post(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return
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

    cleaned = cleanup_old_scores(scores, CLEANUP_DAYS)
    if cleaned != scores:
        github_save_json(SCORES_PATH, cleaned, scores_sha, f"MapTap: cleanup keep {CLEANUP_DAYS} days")

async def do_weekly_roundup(settings: Dict[str, Any]):
    now = datetime.now(UK_TZ)
    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, _ = github_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        scores = {}

    mon = monday_of_week(now)
    sun = mon + timedelta(days=6)

    weekly: Dict[str, Dict[str, int]] = {}
    d = mon
    while d <= sun:
        dkey = d.isoformat()
        day_bucket = scores.get(dkey, {})
        if isinstance(day_bucket, dict):
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
        d += timedelta(days=1)

    rows: List[Tuple[str, int, int]] = [(uid, v["total"], v["days"]) for uid, v in weekly.items()]
    rows.sort(key=lambda x: x[1], reverse=True)

    await ch.send(build_weekly_roundup_text(mon, sun, rows))

async def do_rivalry_alert(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return
    await ch.send("🔥 **MapTap Rivalry Alert!** Only 24 hours left in the week! Check the ranks and secure your spot!")

async def do_monthly_leaderboard(settings: Dict[str, Any]):
    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, _ = github_load_json(SCORES_PATH, {})
    if not isinstance(scores, dict):
        scores = {}

    now = datetime.now(UK_TZ)
    mstart, mend = month_range(now)

    stats = calculate_timeframe_stats(scores, mstart, mend)
    rows = build_avg_leaderboard_rows(stats, min_days=10)

    title = "🗺️ MapTap — Monthly Leaderboard"
    subtitle = f"*{mstart.strftime('%d %b')} → {mend.strftime('%d %b')} (min 10 days, avg-based)*"
    text = leaderboard_to_text(title, rows, subtitle)
    file = make_leaderboard_file(f"maptap_month_{mstart.year:04d}-{mstart.month:02d}.txt", text)

    emb = discord.Embed(title=title, description=subtitle, color=0x9B59B6)
    preview = rows[:25]
    if preview:
        emb.add_field(
            name="Top Preview (full list attached)",
            value="\n".join([f"{i}. <@{uid}> — avg **{avg:.1f}** ({total}/{days})"
                             for i, (uid, avg, total, days) in enumerate(preview, start=1)]),
            inline=False
        )
    else:
        emb.add_field(name="Top Preview", value="😶 No qualifying players.", inline=False)

    await ch.send(embed=emb, file=file)

# =====================================================
# STARTUP
# =====================================================
if __name__ == "__main__":
    Thread(target=run_web).start()
    client.run(TOKEN)