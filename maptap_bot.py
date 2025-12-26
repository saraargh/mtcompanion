# =========================
# MapTap Companion Bot
# Chunk 1 / 5
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
GITHUB_REPO = os.getenv("GITHUB_REPO")

SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")
SETTINGS_PATH = os.getenv("MAPTAP_SETTINGS_PATH", "data/maptap_settings.json")

MAPTAP_URL = "https://www.maptap.gg"
MAX_SCORE = 1000
CLEANUP_DAYS = 69

RESET_PASSWORD = os.getenv("RESET_PASSWORD", "")

# =====================================================
# REGEX
# =====================================================
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)
ROUND_ZERO_REGEX = re.compile(r"(^|\s)0(?!\d)")
ROUND_SCORE_TOKEN_REGEX = re.compile(r"(^|\s)(\d{1,4})(?!\d)")
MAPTAP_HINT_REGEX = re.compile(r"\bmaptap\.gg\b", re.IGNORECASE)

# =====================================================
# DEFAULT SETTINGS
# =====================================================
DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,
    "admin_role_ids": [],

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
        "this_month": 10,
        "all_time": 10,
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
# =========================
# MapTap Companion Bot
# Chunk 2 / 5
# =========================

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
# GITHUB CONTENTS API HELPERS
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
    Writes JSON to GitHub via Contents API.
    Returns new sha.
    """
    url = _gh_url(path)
    encoded = base64.b64encode(
        json.dumps(data, indent=2).encode("utf-8")
    ).decode("utf-8")

    body: Dict[str, Any] = {
        "message": message,
        "content": encoded,
    }
    if sha:
        body["sha"] = sha

    r = requests.put(url, headers=HEADERS, json=body, timeout=20)
    r.raise_for_status()
    return r.json().get("content", {}).get("sha", sha or "")


# =====================================================
# SETTINGS HELPERS
# =====================================================
def _merge_nested(default: Dict[str, Any], incoming: Any) -> Dict[str, Any]:
    merged = dict(default)
    if isinstance(incoming, dict):
        merged.update(incoming)
    return merged

def _normalize_hhmm(value: Any, fallback: str) -> str:
    try:
        datetime.strptime(str(value), "%H:%M")
        return str(value)
    except Exception:
        return fallback

def load_settings() -> Tuple[Dict[str, Any], Optional[str]]:
    settings, sha = github_load_json(SETTINGS_PATH, DEFAULT_SETTINGS.copy())

    merged = DEFAULT_SETTINGS.copy()
    if isinstance(settings, dict):
        merged.update(settings)

    # IDs
    try:
        merged["channel_id"] = int(merged["channel_id"]) if merged.get("channel_id") else None
    except Exception:
        merged["channel_id"] = None

    merged["admin_role_ids"] = [
        int(x) for x in merged.get("admin_role_ids", []) if str(x).isdigit()
    ]

    # Nested blocks
    merged["alerts"] = _merge_nested(DEFAULT_SETTINGS["alerts"], merged.get("alerts"))
    merged["emojis"] = _merge_nested(DEFAULT_SETTINGS["emojis"], merged.get("emojis"))
    merged["minimum_days"] = _merge_nested(DEFAULT_SETTINGS["minimum_days"], merged.get("minimum_days"))

    times_in = _merge_nested(DEFAULT_SETTINGS["times"], merged.get("times"))
    merged["times"] = {
        k: _normalize_hhmm(times_in.get(k), DEFAULT_SETTINGS["times"][k])
        for k in DEFAULT_SETTINGS["times"]
    }

    merged["last_run"] = _merge_nested(DEFAULT_SETTINGS["last_run"], merged.get("last_run"))

    return merged, sha

def save_settings(settings: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    return github_save_json(SETTINGS_PATH, settings, sha, message)


# =====================================================
# DATE HELPERS
# =====================================================
def today_key(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(UK_TZ)
    return dt.date().isoformat()

def pretty_day(date_key: str) -> str:
    try:
        return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")
    except Exception:
        return date_key

def monday_of_week(d: datetime) -> date:
    return d.date() - timedelta(days=d.weekday())

# =========================
# MapTap Companion Bot
# Chunk 3 / 5
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
# PERMISSIONS / CHANNEL
# =====================================================
def has_admin_access(member: discord.Member, settings: Dict[str, Any]) -> bool:
    if member.guild_permissions.administrator:
        return True
    allowed = settings.get("admin_role_ids", [])
    if not allowed:
        return False
    return any(r.id in allowed for r in getattr(member, "roles", []))

def get_configured_channel(client: discord.Client, settings: Dict[str, Any]) -> Optional[discord.TextChannel]:
    cid = settings.get("channel_id")
    if not cid:
        return None
    ch = client.get_channel(int(cid))
    return ch if isinstance(ch, discord.TextChannel) else None


# =====================================================
# STATS HELPERS
# =====================================================
def calculate_current_streak(scores: Dict[str, Any], user_id: str) -> int:
    played_dates: List[date] = []
    for date_key, day in (scores or {}).items():
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
    # Only users who actually have days_played > 0
    return {uid: u for uid, u in (users or {}).items() if int(u.get("days_played", 0)) > 0}

def calculate_all_time_rank(users: Dict[str, Any], user_id: str) -> Tuple[int, int]:
    """
    Rank by average score (desc). Returns (rank, total_players).
    Fixes '#1 of #1' by excluding users who never played.
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
    for line in (text or "").splitlines():
        if SCORE_REGEX.search(line):
            continue
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
    for line in (text or "").splitlines():
        if SCORE_REGEX.search(line):
            continue
        if ROUND_ZERO_REGEX.search(line):
            return True
    return False


# =====================================================
# DISCORD CLIENT (ONE SINGLE INSTANCE)
# =====================================================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

class MapTapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        # Sync commands once on startup
        try:
            synced = await self.tree.sync()
            print(f"✅ Synced {len(synced)} commands: {[c.name for c in synced]}")
        except Exception as e:
            print("❌ Command sync failed:", e)

        # Start scheduler loop
        self.scheduler_tick.start()

    @tasks.loop(minutes=1)
    async def scheduler_tick(self):
        settings, sha = load_settings()
        if not settings.get("enabled", True):
            return

        alerts = settings.get("alerts", {})
        times = settings.get("times", {})
        last = settings.get("last_run", {})

        now = datetime.now(UK_TZ)
        hm = now.strftime("%H:%M")
        today = now.date().isoformat()

        # Daily post
        if alerts.get("daily_post_enabled", True) and hm == times.get("daily_post") and last.get("daily_post") != today:
            await do_daily_post(self, settings)
            settings["last_run"]["daily_post"] = today
            save_settings(settings, sha, "MapTap: auto daily post")

        # Daily scoreboard
        if alerts.get("daily_scoreboard_enabled", True) and hm == times.get("daily_scoreboard") and last.get("daily_scoreboard") != today:
            await do_daily_scoreboard(self, settings)
            settings["last_run"]["daily_scoreboard"] = today
            save_settings(settings, sha, "MapTap: auto daily scoreboard")

        # Weekly roundup (Sundays)
        if alerts.get("weekly_roundup_enabled", True) and now.weekday() == 6 and hm == times.get("weekly_roundup") and last.get("weekly_roundup") != today:
            await do_weekly_roundup(self, settings)
            settings["last_run"]["weekly_roundup"] = today
            save_settings(settings, sha, "MapTap: auto weekly roundup")

        # Rivalry alerts (Saturdays)
        if alerts.get("rivalry_enabled", True) and now.weekday() == 5 and hm == times.get("rivalry") and last.get("rivalry") != today:
            await do_rivalry_alert(self, settings)
            settings["last_run"]["rivalry"] = today
            save_settings(settings, sha, "MapTap: auto rivalry alert")

        # Monthly leaderboard (1st of month)
        if alerts.get("monthly_leaderboard_enabled", True) and now.day == 1 and hm == times.get("monthly_leaderboard") and last.get("monthly_leaderboard") != today:
            await do_monthly_leaderboard(self, settings)
            settings["last_run"]["monthly_leaderboard"] = today
            save_settings(settings, sha, "MapTap: auto monthly leaderboard")


# Instantiate ONE client (and never overwrite it)
client = MapTapBot()

# =========================
# MapTap Companion Bot
# Chunk 4 / 5
# =========================

# =====================================================
# SLASH: /maptapsettings
# =====================================================
@client.tree.command(name="maptapsettings", description="Configure MapTap bot settings")
async def maptapsettings(interaction: discord.Interaction):
    settings, sha = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Use this in a server.", ephemeral=True)
        return

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission.", ephemeral=True)
        return

    view = MapTapSettingsView(settings, sha)
    await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=False)


# =====================================================
# SLASH: /mymaptap
# =====================================================
@client.tree.command(name="mymaptap", description="View MapTap stats")
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
@app_commands.describe(scope="Leaderboard scope", limit="How many players to show (default 10)")
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
    today = datetime.now(UK_TZ).date()

    def in_scope(d: date) -> bool:
        if scope.value == "this_week":
            return d >= today - timedelta(days=today.weekday())
        if scope.value == "this_month":
            return d.year == today.year and d.month == today.month
        return True

    scoped: Dict[str, Dict[str, int]] = {}
    for dkey, bucket in (scores or {}).items():
        try:
            d = datetime.strptime(dkey, "%Y-%m-%d").date()
        except Exception:
            continue
        if not in_scope(d):
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

    rows: List[Tuple[str, float]] = []
    for uid, data in scoped.items():
        if uid not in elig:
            continue
        if data["days"] < min_days:
            continue
        rows.append((uid, data["total"] / data["days"]))

    rows.sort(key=lambda x: x[1], reverse=True)
    rows = rows[:limit]

    if not rows:
        await interaction.response.send_message("😶 No leaderboard data yet.", ephemeral=False)
        return

    lines = []
    for i, (uid, avg) in enumerate(rows, start=1):
        member = interaction.guild.get_member(int(uid)) if interaction.guild else None
        name = member.display_name if member else f"<@{uid}>"
        lines.append(f"{i}. **{name}** — {round(avg)}")

    await interaction.response.send_message(
        f"🏆 **MapTap Leaderboard — {scope.name}**\n\n"
        + "\n".join(lines)
        + "\n\n*Ranked by average score for this period*",
        ephemeral=False,
    )


# =====================================================
# SLASH: /rescan (DATE RANGE)
# =====================================================
@client.tree.command(name="rescan", description="Re-scan MapTap posts for a date range (admin only)")
@app_commands.describe(start_date="YYYY-MM-DD", end_date="YYYY-MM-DD")
async def rescan(interaction: discord.Interaction, start_date: str, end_date: str):
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

    ch = get_configured_channel(client, settings)
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
        f"✅ Rescan complete\nIngested: **{ingested}**\nSkipped: {skipped}",
        ephemeral=True,
    )
# =========================
# MapTap Companion Bot
# Chunk 5 / 5
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

    # Must look like a MapTap share
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
    date_key = msg_time_uk.date().isoformat()
    user_id = str(message.author.id)

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    scores.setdefault(date_key, {})
    day_bucket = scores[date_key]

    user_stats = users.setdefault(user_id, {
        "total_points": 0,
        "days_played": 0,
        "best_streak": 0,
        "personal_best": {"score": 0, "date": "N/A"},
    })

    had_played_before = int(user_stats["days_played"]) > 0
    old_pb = int(user_stats["personal_best"].get("score", 0))

    # Replace score if reposted same day
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

    # ---- ZERO ROUND ROAST (round-only)
    if alerts.get("zero_score_roasts_enabled", True) and has_zero_round(content):
        roast = random.choice([
            f"💀 {message.author.mention} found exactly **zero** places correctly",
            f"🗺️ {message.author.mention} explored the map and learned nothing",
            f"🥶 {message.author.mention} went full chaos and hit a zero",
            f"😭 {message.author.mention} posted a **0** round",
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

    # ---- STREAK UPDATE
    cur_streak = calculate_current_streak(scores, user_id)
    if cur_streak > int(user_stats.get("best_streak", 0)):
        user_stats["best_streak"] = cur_streak

    github_save_json(SCORES_PATH, scores, scores_sha, f"MapTap: score update {date_key}")
    github_save_json(USERS_PATH, users, users_sha, f"MapTap: user stats update {user_id}")

    await react_safe(message, em.get("recorded", "🌏"), "✅")


# =====================================================
# SCHEDULED ACTIONS
# =====================================================
async def do_daily_post(client: discord.Client, settings: Dict[str, Any]):
    ch = get_configured_channel(client, settings)
    if ch:
        await ch.send(build_daily_prompt())

async def do_daily_scoreboard(client: discord.Client, settings: Dict[str, Any]):
    ch = get_configured_channel(client, settings)
    if not ch:
        return

    scores, scores_sha = github_load_json(SCORES_PATH, {})
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

    # Cleanup old days
    cutoff = datetime.now(UK_TZ).date() - timedelta(days=CLEANUP_DAYS)
    cleaned = {
        d: v for d, v in scores.items()
        if datetime.strptime(d, "%Y-%m-%d").date() >= cutoff
    }
    if cleaned != scores:
        github_save_json(SCORES_PATH, cleaned, scores_sha, f"MapTap: cleanup keep {CLEANUP_DAYS} days")

async def do_weekly_roundup(client: discord.Client, settings: Dict[str, Any]):
    ch = get_configured_channel(client, settings)
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

async def do_monthly_leaderboard(client: discord.Client, settings: Dict[str, Any]):
    ch = get_configured_channel(client, settings)
    if not ch:
        return

    users, _ = github_load_json(USERS_PATH, {})
    elig = eligible_users(users)

    rows = []
    for uid, u in elig.items():
        try:
            if u["days_played"] >= DEFAULT_SETTINGS["minimum_days"]["this_month"]:
                rows.append((uid, u["total_points"] / u["days_played"]))
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

async def do_rivalry_alert(client: discord.Client, settings: Dict[str, Any]):
    ch = get_configured_channel(client, settings)
    if ch:
        await ch.send(
            "🔥 **MapTap Rivalry Alert!**\nOnly 24 hours left this week — secure your rank!"
        )


# =====================================================
# STARTUP
# =====================================================
if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    client.run(TOKEN)