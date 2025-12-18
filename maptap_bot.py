import discord
from discord.ext import tasks
from discord import app_commands

import os
import json
import re
import base64
import requests
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from flask import Flask
from threading import Thread

# ===================== CONFIG =====================
TOKEN = os.getenv("TOKEN")
MAPTAP_CHANNEL_ID = int(os.getenv("MAPTAP_CHANNEL_ID", "0"))

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # e.g. "saraargh/the-pilot"
SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")

UK_TZ = ZoneInfo("Europe/London")
MAPTAP_URL = "https://www.maptap.gg"
CLEANUP_DAYS = 69

SCORE_REGEX = re.compile(r"Final score:\s*(\d+)", re.IGNORECASE)

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# ===================== KEEP ALIVE =====================
app = Flask("maptap")

@app.route("/")
def home():
    return "MapTap bot running"

def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))

# ===================== GITHUB JSON HELPERS =====================
def github_load_json(path):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers=HEADERS)

    if r.status_code == 404:
        return {}, None

    r.raise_for_status()
    data = r.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    return json.loads(content), data["sha"]

def github_save_json(path, data, sha, message):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    encoded = base64.b64encode(
        json.dumps(data, indent=2).encode("utf-8")
    ).decode("utf-8")

    payload = {
        "message": message,
        "content": encoded
    }

    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=HEADERS, json=payload)
    r.raise_for_status()

# ===================== DATE HELPERS =====================
def today_key(dt=None):
    if not dt:
        dt = datetime.now(UK_TZ)
    return dt.date().isoformat()

def pretty_date(date_key):
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")

# ===================== STREAK CALC =====================
def calculate_current_streak(scores, user_id):
    played = sorted(
        datetime.strptime(d, "%Y-%m-%d").date()
        for d, day in scores.items()
        if user_id in day
    )

    if not played:
        return 0

    today = datetime.now(UK_TZ).date()
    streak = 0
    day = today

    while day in played:
        streak += 1
        day -= timedelta(days=1)

    return streak

# ===================== CLEANUP =====================
def cleanup_old_scores(scores):
    cutoff = datetime.now(UK_TZ).date() - timedelta(days=CLEANUP_DAYS)
    return {
        d: v for d, v in scores.items()
        if datetime.strptime(d, "%Y-%m-%d").date() >= cutoff
    }

# ===================== DISCORD CLIENT =====================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

class MapTapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        daily_post.start()
        daily_scoreboard.start()
        weekly_roundup.start()
        await self.tree.sync()

client = MapTapBot()

def get_channel():
    return client.get_channel(MAPTAP_CHANNEL_ID)

# ===================== DAILY 11AM POST =====================
@tasks.loop(time=time(hour=11, minute=0, tzinfo=UK_TZ))
async def daily_post():
    ch = get_channel()
    if not ch:
        return

    await ch.send(
        "🗺️ **Daily MapTap is live!**\n"
        f"👉 {MAPTAP_URL}\n\n"
        "Post your results exactly as shared from the app ✈️\n"
        "_Scores over **1000** won’t be counted._"
    )

# ===================== SCORE PICKUP =====================
@client.event
async def on_message(message: discord.Message):
    if message.author.bot or message.channel.id != MAPTAP_CHANNEL_ID:
        return

    match = SCORE_REGEX.search(message.content)
    if not match:
        return

    score = int(match.group(1))
    if score > 1000:
        await message.add_reaction("❌")
        return

    scores, scores_sha = github_load_json(SCORES_PATH)
    users, users_sha = github_load_json(USERS_PATH)

    msg_time = message.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
    date_key = today_key(msg_time)
    user_id = str(message.author.id)

    scores.setdefault(date_key, {})
    existing = scores[date_key].get(user_id)

    user_stats = users.setdefault(user_id, {
        "total_points": 0,
        "days_played": 0,
        "best_streak": 0
    })

    if existing:
        user_stats["total_points"] -= existing["score"]
    else:
        user_stats["days_played"] += 1

    user_stats["total_points"] += score

    scores[date_key][user_id] = {
        "score": score,
        "updated_at": msg_time.isoformat()
    }

    current_streak = calculate_current_streak(scores, user_id)
    if current_streak > user_stats["best_streak"]:
        user_stats["best_streak"] = current_streak

    github_save_json(
        SCORES_PATH,
        scores,
        scores_sha,
        f"MapTap score update {date_key}"
    )

    github_save_json(
        USERS_PATH,
        users,
        users_sha,
        f"MapTap user stats update {user_id}"
    )

    await message.add_reaction("✅")

# ===================== DAILY SCOREBOARD (11PM) =====================
@tasks.loop(time=time(hour=23, minute=0, tzinfo=UK_TZ))
async def daily_scoreboard():
    ch = get_channel()
    if not ch:
        return

    scores, scores_sha = github_load_json(SCORES_PATH)
    date_key = today_key()

    today_scores = scores.get(date_key, {})

    if not today_scores:
        await ch.send(
            f"🗺️ **MapTap — Daily Scores**\n"
            f"*{pretty_date(date_key)}*\n\n😶 No scores today."
        )
        return

    sorted_scores = sorted(
        today_scores.items(),
        key=lambda x: x[1]["score"],
        reverse=True
    )

    lines = [
        f"{i}. <@{uid}> — **{entry['score']}**"
        for i, (uid, entry) in enumerate(sorted_scores, start=1)
    ]

    await ch.send(
        f"🗺️ **MapTap — Daily Scores**\n"
        f"*{pretty_date(date_key)}*\n\n"
        + "\n".join(lines) +
        f"\n\n✈️ Players today: **{len(sorted_scores)}**"
    )

    cleaned = cleanup_old_scores(scores)
    if cleaned != scores:
        github_save_json(
            SCORES_PATH,
            cleaned,
            scores_sha,
            "MapTap cleanup (69 days)"
        )

# ===================== WEEKLY ROUND-UP (SUN 23:05) =====================
@tasks.loop(time=time(hour=23, minute=5, tzinfo=UK_TZ))
async def weekly_roundup():
    now = datetime.now(UK_TZ)
    if now.weekday() != 6:
        return

    ch = get_channel()
    if not ch:
        return

    scores, _ = github_load_json(SCORES_PATH)

    monday = now.date() - timedelta(days=6)
    week_dates = [
        (monday + timedelta(days=i)).isoformat()
        for i in range(7)
    ]

    weekly = {}

    for d in week_dates:
        for uid, entry in scores.get(d, {}).items():
            weekly.setdefault(uid, {"total": 0, "days": 0})
            weekly[uid]["total"] += entry["score"]
            weekly[uid]["days"] += 1

    if not weekly:
        await ch.send("🗺️ **MapTap — Weekly Round-Up**\n\n😶 No scores this week.")
        return

    sorted_week = sorted(
        weekly.items(),
        key=lambda x: x[1]["total"],
        reverse=True
    )

    lines = [
        f"{i}. <@{uid}> — **{stats['total']} pts** ({stats['days']}/7 days)"
        for i, (uid, stats) in enumerate(sorted_week, start=1)
    ]

    await ch.send(
        "🗺️ **MapTap — Weekly Round-Up**\n"
        f"*Mon {monday.strftime('%d %b')} → Sun {now.strftime('%d %b')}*\n\n"
        + "\n".join(lines) +
        f"\n\n✈️ Weekly players: **{len(sorted_week)}**"
    )

# ===================== /MYMAPTAP =====================
@client.tree.command(name="mymaptap", description="View your MapTap stats")
async def mymaptap(interaction: discord.Interaction):
    users, _ = github_load_json(USERS_PATH)
    scores, _ = github_load_json(SCORES_PATH)

    user_id = str(interaction.user.id)
    stats = users.get(user_id)

    if not stats:
        await interaction.response.send_message(
            "You haven’t recorded any MapTap scores yet 🗺️",
            ephemeral=True
        )
        return

    current_streak = calculate_current_streak(scores, user_id)
    avg = round(stats["total_points"] / stats["days_played"])

    await interaction.response.send_message(
        "🗺️ **Your MapTap Stats**\n\n"
        f"• Total points (all-time): **{stats['total_points']}**\n"
        f"• Days played (all-time): **{stats['days_played']}**\n"
        f"• Average score: **{avg}**\n"
        f"• Current streak: 🔥 **{current_streak} days**\n"
        f"• Best streak (all-time): 🏆 **{stats['best_streak']} days**",
        ephemeral=True
    )

# ===================== RUN =====================
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("TOKEN missing")
    if not MAPTAP_CHANNEL_ID:
        raise RuntimeError("MAPTAP_CHANNEL_ID missing")
    if not GITHUB_TOKEN or not GITHUB_REPO:
        raise RuntimeError("GitHub env vars missing")

    Thread(target=run_web, daemon=True).start()
    client.run(TOKEN)