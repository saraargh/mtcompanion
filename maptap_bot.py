import os
import json
import re
import base64
import requests
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from threading import Thread

import discord
from discord.ext import tasks
from discord import app_commands
from flask import Flask

# ===================== BASIC CONFIG =====================
TOKEN = os.getenv("TOKEN")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # e.g. "saraargh/the-pilot"

SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")
SETTINGS_PATH = os.getenv("MAPTAP_SETTINGS_PATH", "data/maptap_settings.json")

UK_TZ = ZoneInfo("Europe/London")
MAPTAP_URL = os.getenv("MAPTAP_URL", "https://www.maptap.gg")
CLEANUP_DAYS = int(os.getenv("MAPTAP_CLEANUP_DAYS", "69"))

SCORE_REGEX = re.compile(r"Final score:\s*(\d+)", re.IGNORECASE)

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

DEFAULT_SETTINGS = {
    "enabled": True,
    "channel_id": None,
    "daily_post_enabled": True,
    "daily_scoreboard_enabled": True,
    "weekly_roundup_enabled": True,
    "admin_role_ids": []
}

# ===================== KEEP ALIVE (Render) =====================
app = Flask("maptap")

@app.route("/")
def home():
    return "MapTap bot running"

def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))

# ===================== GITHUB JSON HELPERS =====================
def github_load_json(path: str, default):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers=HEADERS)

    if r.status_code == 404:
        return default, None

    r.raise_for_status()
    payload = r.json()
    content = base64.b64decode(payload["content"]).decode("utf-8")
    return json.loads(content), payload["sha"]

def github_save_json(path: str, data, sha: str | None, message: str):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    encoded = base64.b64encode(json.dumps(data, indent=2).encode("utf-8")).decode("utf-8")

    body = {"message": message, "content": encoded}
    if sha:
        body["sha"] = sha

    r = requests.put(url, headers=HEADERS, json=body)
    r.raise_for_status()

def load_settings():
    return github_load_json(SETTINGS_PATH, DEFAULT_SETTINGS.copy())

def save_settings(settings, sha, message):
    github_save_json(SETTINGS_PATH, settings, sha, message)

# ===================== DATE / STATS HELPERS =====================
def today_key(dt=None) -> str:
    if dt is None:
        dt = datetime.now(UK_TZ)
    return dt.date().isoformat()

def pretty_date(date_key: str) -> str:
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")

def monday_of_week(d: datetime) -> datetime.date:
    # Week = Monday..Sunday
    return (d.date() - timedelta(days=d.weekday()))

def cleanup_old_scores(scores: dict) -> dict:
    cutoff = datetime.now(UK_TZ).date() - timedelta(days=CLEANUP_DAYS)
    cleaned = {}
    for date_key, day in scores.items():
        try:
            day_date = datetime.strptime(date_key, "%Y-%m-%d").date()
        except ValueError:
            continue
        if day_date >= cutoff:
            cleaned[date_key] = day
    return cleaned

def calculate_current_streak(scores: dict, user_id: str) -> int:
    played_dates = sorted(
        datetime.strptime(date_key, "%Y-%m-%d").date()
        for date_key, day in scores.items()
        if user_id in day
    )
    if not played_dates:
        return 0

    today = datetime.now(UK_TZ).date()
    streak = 0
    day = today
    played_set = set(played_dates)
    while day in played_set:
        streak += 1
        day -= timedelta(days=1)
    return streak

# ===================== DISCORD CLIENT =====================
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

class MapTapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        daily_post_task.start()
        daily_scoreboard_task.start()
        weekly_roundup_task.start()
        await self.tree.sync()

client = MapTapBot()

def has_admin_access(member: discord.Member, settings: dict) -> bool:
    if getattr(member, "guild_permissions", None) and member.guild_permissions.administrator:
        return True
    role_ids = settings.get("admin_role_ids", [])
    if not role_ids:
        return False
    return any(r.id in role_ids for r in member.roles)

def get_configured_channel(settings: dict):
    cid = settings.get("channel_id")
    if not cid:
        return None
    return client.get_channel(int(cid))

# ===================== SETTINGS PANEL VIEW =====================
class MapTapSettingsView(discord.ui.View):
    def __init__(self, settings: dict, sha: str | None):
        super().__init__(timeout=300)
        self.settings = settings
        self.sha = sha

    def _panel_embed(self) -> discord.Embed:
        channel_str = f"<#{self.settings['channel_id']}>" if self.settings.get("channel_id") else "Not set"
        roles = self.settings.get("admin_role_ids", [])
        roles_str = ", ".join([f"<@&{rid}>" for rid in roles]) if roles else "Admins only"

        e = discord.Embed(
            title="🗺️ MapTap Settings",
            description=(
                f"**Channel:** {channel_str}\n"
                f"**Admin roles:** {roles_str}\n\n"
                f"**Daily post (11am):** {'✅' if self.settings.get('daily_post_enabled') else '❌'}\n"
                f"**Daily scoreboard (11pm):** {'✅' if self.settings.get('daily_scoreboard_enabled') else '❌'}\n"
                f"**Weekly roundup (Sun 23:05):** {'✅' if self.settings.get('weekly_roundup_enabled') else '❌'}\n"
                f"**Master enabled:** {'✅' if self.settings.get('enabled') else '❌'}"
            ),
            color=0xF1C40F
        )
        return e

    async def _save_and_refresh(self, interaction: discord.Interaction, message: str):
        # re-load sha to reduce conflicts
        current, current_sha = load_settings()
        # merge latest -> ours (ours wins)
        current.update(self.settings)
        save_settings(current, current_sha, message)
        self.settings = current
        self.sha = current_sha
        await interaction.response.edit_message(embed=self._panel_embed(), view=self)

    @discord.ui.channel_select(
        placeholder="Select the MapTap channel",
        channel_types=[discord.ChannelType.text]
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        self.settings["channel_id"] = select.values[0].id
        await self._save_and_refresh(interaction, "MapTap: set channel")

    @discord.ui.role_select(
        placeholder="Select admin roles (optional)",
        min_values=0,
        max_values=10
    )
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        self.settings["admin_role_ids"] = [r.id for r in select.values]
        await self._save_and_refresh(interaction, "MapTap: set admin roles")

    @discord.ui.button(label="Toggle Master Enabled", style=discord.ButtonStyle.secondary)
    async def toggle_master(self, interaction: discord.Interaction, _):
        self.settings["enabled"] = not self.settings.get("enabled", True)
        await self._save_and_refresh(interaction, "MapTap: toggle enabled")

    @discord.ui.button(label="Toggle Daily Post (11am)", style=discord.ButtonStyle.secondary)
    async def toggle_daily_post(self, interaction: discord.Interaction, _):
        self.settings["daily_post_enabled"] = not self.settings.get("daily_post_enabled", True)
        await self._save_and_refresh(interaction, "MapTap: toggle daily post")

    @discord.ui.button(label="Toggle Daily Scoreboard (11pm)", style=discord.ButtonStyle.secondary)
    async def toggle_daily_board(self, interaction: discord.Interaction, _):
        self.settings["daily_scoreboard_enabled"] = not self.settings.get("daily_scoreboard_enabled", True)
        await self._save_and_refresh(interaction, "MapTap: toggle daily scoreboard")

    @discord.ui.button(label="Toggle Weekly Roundup", style=discord.ButtonStyle.secondary)
    async def toggle_weekly(self, interaction: discord.Interaction, _):
        self.settings["weekly_roundup_enabled"] = not self.settings.get("weekly_roundup_enabled", True)
        await self._save_and_refresh(interaction, "MapTap: toggle weekly roundup")

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger)
    async def close(self, interaction: discord.Interaction, _):
        await interaction.response.edit_message(content="✅ Closed.", embed=None, view=None)

# ===================== SLASH: SETTINGS =====================
@client.tree.command(name="maptapsettings", description="Open MapTap admin settings panel")
async def maptapsettings(interaction: discord.Interaction):
    settings, sha = load_settings()

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ This command must be used in a server.", ephemeral=True)
        return

    if not has_admin_access(interaction.user, settings):
        await interaction.response.send_message("❌ You don’t have permission to manage MapTap settings.", ephemeral=True)
        return

    view = MapTapSettingsView(settings, sha)
    await interaction.response.send_message(embed=view._panel_embed(), view=view, ephemeral=True)

# ===================== SLASH: /mymaptap =====================
@client.tree.command(name="mymaptap", description="View your MapTap stats")
async def mymaptap(interaction: discord.Interaction):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})

    user_id = str(interaction.user.id)
    stats = users.get(user_id)

    if not stats or stats.get("days_played", 0) <= 0:
        await interaction.response.send_message("You haven’t recorded any MapTap scores yet 🗺️", ephemeral=True)
        return

    current_streak = calculate_current_streak(scores, user_id)
    avg = round(stats["total_points"] / stats["days_played"])

    await interaction.response.send_message(
        "🗺️ **Your MapTap Stats**\n\n"
        f"• Total points (all-time): **{stats['total_points']}**\n"
        f"• Days played (all-time): **{stats['days_played']}**\n"
        f"• Average score: **{avg}**\n"
        f"• Current streak: 🔥 **{current_streak} days**\n"
        f"• Best streak (all-time): 🏆 **{stats.get('best_streak', 0)} days**",
        ephemeral=True
    )

# ===================== SCORE PICKUP =====================
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

    match = SCORE_REGEX.search(message.content)
    if not match:
        return

    score = int(match.group(1))
    if score > 1000:
        try:
            await message.add_reaction("❌")
        except Exception:
            pass
        return

    # Load GitHub JSON
    scores, scores_sha = github_load_json(SCORES_PATH, {})
    users, users_sha = github_load_json(USERS_PATH, {})

    msg_time = message.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
    date_key = today_key(msg_time)
    user_id = str(message.author.id)

    scores.setdefault(date_key, {})
    existing = scores[date_key].get(user_id)

    user_stats = users.setdefault(user_id, {"total_points": 0, "days_played": 0, "best_streak": 0})

    # Overwrite-safe totals
    if existing:
        user_stats["total_points"] -= int(existing.get("score", 0))
    else:
        user_stats["days_played"] += 1

    user_stats["total_points"] += score

    scores[date_key][user_id] = {"score": score, "updated_at": msg_time.isoformat()}

    # Update best streak from rolling scores
    current_streak = calculate_current_streak(scores, user_id)
    if current_streak > user_stats.get("best_streak", 0):
        user_stats["best_streak"] = current_streak

    # Save back to GitHub
    github_save_json(SCORES_PATH, scores, scores_sha, f"MapTap: score update {date_key}")
    github_save_json(USERS_PATH, users, users_sha, f"MapTap: user stats update {user_id}")

    try:
        await message.add_reaction("🌏")
    except Exception:
        pass

# ===================== DAILY TASK: 11AM POST =====================
@tasks.loop(time=time(hour=11, minute=0, tzinfo=UK_TZ))
async def daily_post_task():
    settings, _ = load_settings()
    if not settings.get("enabled", True) or not settings.get("daily_post_enabled", True):
        return

    ch = get_configured_channel(settings)
    if not ch:
        return

    await ch.send(
        "🗺️ **Daily MapTap is live!**\n"
        f"👉 {MAPTAP_URL}\n\n"
        "Post your results exactly as shared from the app ✈️\n"
        "_Scores over **1000** won’t be counted._"
    )

# ===================== DAILY TASK: 11PM SCOREBOARD =====================
@tasks.loop(time=time(hour=23, minute=0, tzinfo=UK_TZ))
async def daily_scoreboard_task():
    settings, _ = load_settings()
    if not settings.get("enabled", True) or not settings.get("daily_scoreboard_enabled", True):
        return

    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, scores_sha = github_load_json(SCORES_PATH, {})
    date_key = today_key()
    today_scores = scores.get(date_key, {})

    if not today_scores:
        await ch.send(
            f"🗺️ **MapTap — Daily Scores**\n*{pretty_date(date_key)}*\n\n😶 No scores today."
        )
    else:
        sorted_scores = sorted(today_scores.items(), key=lambda x: x[1]["score"], reverse=True)
        lines = [f"{i}. <@{uid}> — **{entry['score']}**" for i, (uid, entry) in enumerate(sorted_scores, start=1)]

        await ch.send(
            f"🗺️ **MapTap — Daily Scores**\n*{pretty_date(date_key)}*\n\n"
            + "\n".join(lines) +
            f"\n\n✈️ Players today: **{len(sorted_scores)}**"
        )

    # Cleanup rolling history
    cleaned = cleanup_old_scores(scores)
    if cleaned != scores:
        github_save_json(SCORES_PATH, cleaned, scores_sha, f"MapTap: cleanup keep {CLEANUP_DAYS} days")

# ===================== WEEKLY TASK: SUNDAY 23:05 ROUNDUP =====================
@tasks.loop(time=time(hour=23, minute=5, tzinfo=UK_TZ))
async def weekly_roundup_task():
    settings, _ = load_settings()
    if not settings.get("enabled", True) or not settings.get("weekly_roundup_enabled", True):
        return

    now = datetime.now(UK_TZ)
    if now.weekday() != 6:  # Sunday
        return

    ch = get_configured_channel(settings)
    if not ch:
        return

    scores, _ = github_load_json(SCORES_PATH, {})

    mon = monday_of_week(now)
    week_dates = [(mon + timedelta(days=i)).isoformat() for i in range(7)]

    weekly = {}
    for d in week_dates:
        for uid, entry in scores.get(d, {}).items():
            weekly.setdefault(uid, {"total": 0, "days": 0})
            weekly[uid]["total"] += int(entry["score"])
            weekly[uid]["days"] += 1

    if not weekly:
        await ch.send("🗺️ **MapTap — Weekly Round-Up**\n\n😶 No scores this week.")
        return

    sorted_week = sorted(weekly.items(), key=lambda x: x[1]["total"], reverse=True)
    lines = [f"{i}. <@{uid}> — **{stats['total']} pts** ({stats['days']}/7 days)" for i, (uid, stats) in enumerate(sorted_week, start=1)]

    await ch.send(
        "🗺️ **MapTap — Weekly Round-Up**\n"
        f"*Mon {mon.strftime('%d %b')} → Sun {now.strftime('%d %b')}*\n\n"
        + "\n".join(lines) +
        f"\n\n✈️ Weekly players: **{len(sorted_week)}**"
    )

# ===================== RUN =====================
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("Missing TOKEN env var")
    if not GITHUB_TOKEN or not GITHUB_REPO:
        raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPO env vars")

    # Start keep-alive web server
    Thread(target=run_web, daemon=True).start()

    client.run(TOKEN)