# =========================
# MapTap Companion Bot (FULL FILE)
# =========================

import os
import json
import re
import base64
import requests
from datetime import datetime, timedelta, time
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

# Parse "Final score: 606" (case-insensitive)
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)

# Emoji defaults (configurable via /maptapsettings modal)
DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,  # set via settings panel
    "daily_post_enabled": True,
    "daily_scoreboard_enabled": True,
    "weekly_roundup_enabled": True,
    "admin_role_ids": [],  # set via settings panel
    "emojis": {
        "recorded": "✅",        # score registered
        "too_high": "❌",        # score > MAX_SCORE
        "rescan_ingested": "🔁", # rescan fixed / ingested
        "config_issue": "⚠️"     # optional future use
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
def load_settings() -> Tuple[Dict[str, Any], Optional[str]]:
    settings, sha = github_load_json(SETTINGS_PATH, DEFAULT_SETTINGS.copy())

    # Merge defaults safely (so missing keys won't crash)
    merged = DEFAULT_SETTINGS.copy()
    if isinstance(settings, dict):
        merged.update(settings)

    # Normalize types
    if merged.get("channel_id") is not None:
        try:
            merged["channel_id"] = int(merged["channel_id"])
        except Exception:
            merged["channel_id"] = None

    merged["admin_role_ids"] = [int(x) for x in merged.get("admin_role_ids", []) if str(x).isdigit()]

    # Ensure emojis dict exists with defaults merged
    emojis = DEFAULT_SETTINGS["emojis"].copy()
    if isinstance(merged.get("emojis"), dict):
        emojis.update(merged["emojis"])
    merged["emojis"] = emojis

    return merged, sha

def save_settings(settings: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    return github_save_json(SETTINGS_PATH, settings, sha, message)

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
    ch = client.get_channel(int(cid))
    return ch

# =====================================================
# SETTINGS PANEL VIEW + EMOJI MODAL
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
        self.recorded.default = str(em.get("recorded", "✅"))
        self.too_high.default = str(em.get("too_high", "❌"))
        self.rescan_ingested.default = str(em.get("rescan_ingested", "🔁"))

    async def on_submit(self, interaction: discord.Interaction):
        self.view_ref.settings.setdefault("emojis", {})
        self.view_ref.settings["emojis"]["recorded"] = str(self.recorded.value).strip()
        self.view_ref.settings["emojis"]["too_high"] = str(self.too_high.value).strip()
        self.view_ref.settings["emojis"]["rescan_ingested"] = str(self.rescan_ingested.value).strip()

        await self.view_ref._save_refresh(interaction, "MapTap: update reaction emojis")

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
            f"Recorded: {em.get('recorded','✅')}\n"
            f"Too high: {em.get('too_high','❌')}\n"
            f"Rescan ingested: {em.get('rescan_ingested','🔁')}"
        )

        e = discord.Embed(
            title="🗺️ MapTap Settings",
            description=(
                f"**Channel:** {channel_str}\n"
                f"**Admin roles:** {roles_str}\n\n"
                f"**Daily post (00:00):** {'✅' if self.settings.get('daily_post_enabled') else '❌'}\n"
                f"**Daily scoreboard (23:30):** {'✅' if self.settings.get('daily_scoreboard_enabled') else '❌'}\n"
                f"**Weekly roundup (Sun 23:45):** {'✅' if self.settings.get('weekly_roundup_enabled') else '❌'}\n"
                f"**Master enabled:** {'✅' if self.settings.get('enabled') else '❌'}\n\n"
                f"**Reactions:**\n{emoji_block}"
            ),
            color=0xF1C40F
        )
        e.set_footer(text="Changes save to GitHub immediately.")
        return e

    async def _save_refresh(self, interaction: discord.Interaction, message: str):
        current, current_sha = load_settings()
        current.update(self.settings)
        new_sha = save_settings(current, current_sha, message)
        self.settings = current
        self.sha = new_sha or current_sha
        await interaction.response.edit_message(embed=self._embed(), view=self)

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

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="Select admin roles (optional)",
        min_values=0,
        max_values=10
    )
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        self.settings["admin_role_ids"] = [r.id for r in select.values]
        await self._save_refresh(interaction, "MapTap: set admin roles")

    @discord.ui.button(label="Edit Reaction Emojis", style=discord.ButtonStyle.primary)
    async def edit_emojis(self, interaction: discord.Interaction, _):
        modal = EmojiSettingsModal(self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Toggle Master Enabled", style=discord.ButtonStyle.secondary)
    async def toggle_master(self, interaction: discord.Interaction, _):
        self.settings["enabled"] = not bool(self.settings.get("enabled", True))
        await self._save_refresh(interaction, "MapTap: toggle enabled")

    @discord.ui.button(label="Toggle Daily Post (00:00)", style=discord.ButtonStyle.secondary)
    async def toggle_daily_post(self, interaction: discord.Interaction, _):
        self.settings["daily_post_enabled"] = not bool(self.settings.get("daily_post_enabled", True))
        await self._save_refresh(interaction, "MapTap: toggle daily post")

    @discord.ui.button(label="Toggle Daily Scoreboard (23:30)", style=discord.ButtonStyle.secondary)
    async def toggle_daily_board(self, interaction: discord.Interaction, _):
        self.settings["daily_scoreboard_enabled"] = not bool(self.settings.get("daily_scoreboard_enabled", True))
        await self._save_refresh(interaction, "MapTap: toggle daily scoreboard")

    @discord.ui.button(label="Toggle Weekly Roundup", style=discord.ButtonStyle.secondary)
    async def toggle_weekly(self, interaction: discord.Interaction, _):
        self.settings["weekly_roundup_enabled"] = not bool(self.settings.get("weekly_roundup_enabled", True))
        await self._save_refresh(interaction, "MapTap: toggle weekly roundup")

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger)
    async def close(self, interaction: discord.Interaction, _):
        await interaction.response.edit_message(content="✅ Closed.", embed=None, view=None)

# =====================================================
# SLASH COMMAND: /maptapsettings
# =====================================================
@client.tree.command(name="maptapsettings", description="Configure MapTap bot settings")
async def maptapsettings(interaction: discord.Interaction):
    settings, sha = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ Use this in the server, not DMs.", ephemeral=True)
        return

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission to manage MapTap settings.", ephemeral=True)
        return

    view = MapTapSettingsView(settings, sha)
    await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)

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

    msg_time_uk = message.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
    date_key = today_key(msg_time_uk)
    user_id = str(message.author.id)

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    if not isinstance(scores, dict):
        scores = {}
    scores.setdefault(date_key, {})
    day_bucket = scores[date_key]

    prev_entry = day_bucket.get(user_id)
    user_stats = users.setdefault(user_id, {"total_points": 0, "days_played": 0, "best_streak": 0})

    if prev_entry and isinstance(prev_entry, dict) and "score" in prev_entry:
        try:
            user_stats["total_points"] -= int(prev_entry["score"])
        except Exception:
            pass
    else:
        user_stats["days_played"] += 1

    user_stats["total_points"] += score

    day_bucket[user_id] = {
        "score": score,
        "updated_at": msg_time_uk.isoformat()
    }

    cur_streak = calculate_current_streak(scores, user_id)
    if cur_streak > int(user_stats.get("best_streak", 0)):
        user_stats["best_streak"] = cur_streak

    github_save_json(SCORES_PATH, scores, scores_sha, f"MapTap: score update {date_key}")
    github_save_json(USERS_PATH, users, users_sha, f"MapTap: user stats update {user_id}")

    # ✅ Score registered emoji (custom supported)
    await react_safe(message, em.get("recorded", "✅"), "✅")

# =====================================================
# SLASH COMMAND: /mymaptap (streak included here)
# =====================================================
@client.tree.command(name="mymaptap", description="View your MapTap stats")
async def mymaptap(interaction: discord.Interaction):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})

    user_id = str(interaction.user.id)
    stats = users.get(user_id)

    if not stats or int(stats.get("days_played", 0)) <= 0:
        await interaction.response.send_message("You haven’t recorded any MapTap scores yet 🗺️", ephemeral=True)
        return

    cur = calculate_current_streak(scores, user_id)
    avg = round(int(stats["total_points"]) / int(stats["days_played"]))

    await interaction.response.send_message(
        "🗺️ **Your MapTap Stats**\n\n"
        f"• Total points (all-time): **{stats['total_points']}**\n"
        f"• Days played (all-time): **{stats['days_played']}**\n"
        f"• Average score: **{avg}**\n"
        f"• Current streak: 🔥 **{cur} days**\n"
        f"• Best streak (all-time): 🏆 **{stats.get('best_streak', 0)} days**",
        ephemeral=True
    )

# =====================================================
# SLASH COMMAND: /rescan (admin-only) — NO DUPLICATE REACTION
# =====================================================
@client.tree.command(
    name="rescan",
    description="Re-scan recent MapTap messages for missed scores (admin only)"
)
@app_commands.describe(
    messages="How many recent messages to scan (max 50)"
)
async def rescan(
    interaction: discord.Interaction,
    messages: int = 10
):
    settings, _ = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            "❌ This command can only be used in a server.",
            ephemeral=True
        )
        return

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message(
            "❌ You don’t have permission to run this.",
            ephemeral=True
        )
        return

    ch = get_configured_channel(settings)
    if not ch:
        await interaction.response.send_message(
            "❌ MapTap channel is not configured.",
            ephemeral=True
        )
        return

    messages = max(1, min(messages, 50))

    await interaction.response.send_message(
        f"🔍 Scanning the last **{messages}** messages…",
        ephemeral=True
    )

    em = settings.get("emojis", DEFAULT_SETTINGS["emojis"])

    scanned = 0
    ingested = 0
    skipped = 0

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

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

        # ✅ Silent duplicate skip (no reaction)
        if uid in day_bucket:
            skipped += 1
            continue

        user = users.setdefault(uid, {
            "total_points": 0,
            "days_played": 0,
            "best_streak": 0
        })

        user["days_played"] += 1
        user["total_points"] += score

        day_bucket[uid] = {
            "score": score,
            "updated_at": msg_time_uk.isoformat()
        }

        ingested += 1
        await react_safe(msg, em.get("rescan_ingested", "🔁"), "🔁")

    github_save_json(
        SCORES_PATH,
        scores,
        scores_sha,
        f"MapTap: rescan last {messages} messages"
    )
    github_save_json(
        USERS_PATH,
        users,
        users_sha,
        "MapTap: rescan user stats"
    )

    await interaction.followup.send(
        "✅ **Rescan complete**\n"
        f"• Matches found: **{scanned}**\n"
        f"• Newly ingested: **{ingested}**\n"
        f"• Skipped: **{skipped}**",
        ephemeral=True
    )

# =====================================================
# TASKS
# =====================================================
@tasks.loop(time=time(hour=0, minute=0, tzinfo=UK_TZ))
async def daily_post_task():
    settings, _ = load_settings()
    if not settings.get("enabled", True):
        return
    if not settings.get("daily_post_enabled", True):
        return

    ch = get_configured_channel(settings)
    if not ch:
        return

    await ch.send(build_daily_prompt())

@tasks.loop(time=time(hour=23, minute=30, tzinfo=UK_TZ))
async def daily_scoreboard_task():
    settings, _ = load_settings()
    if not settings.get("enabled", True):
        return
    if not settings.get("daily_scoreboard_enabled", True):
        return

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
        github_save_json(
            SCORES_PATH,
            cleaned,
            scores_sha,
            f"MapTap: cleanup keep {CLEANUP_DAYS} days"
        )

@tasks.loop(time=time(hour=23, minute=45, tzinfo=UK_TZ))
async def weekly_roundup_task():
    settings, _ = load_settings()
    if not settings.get("enabled", True):
        return
    if not settings.get("weekly_roundup_enabled", True):
        return

    now = datetime.now(UK_TZ)
    if now.weekday() != 6:  # Sunday
        return

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

# =====================================================
# STARTUP
# =====================================================
@client.event
async def on_ready():
    print(f"✅ Logged in as {client.user} (MapTap)")
    if not daily_post_task.is_running():
        daily_post_task.start()
    if not daily_scoreboard_task.is_running():
        daily_scoreboard_task.start()
    if not weekly_roundup_task.is_running():
        weekly_roundup_task.start()

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