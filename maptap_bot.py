import discord
from discord.ext import tasks
from discord import app_commands

import os
import json
import re
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from flask import Flask
from threading import Thread

# ===================== CONFIG =====================
TOKEN = os.getenv("TOKEN")
MAPTAP_CHANNEL_ID = int(os.getenv("MAPTAP_CHANNEL_ID", "0"))

UK_TZ = ZoneInfo("Europe/London")

DATA_FILE = "maptap_scores.json"
USER_STATS_FILE = "maptap_users.json"

CLEANUP_DAYS = 69
MAPTAP_URL = "https://www.maptap.gg"

SCORE_REGEX = re.compile(r"Final score:\s*(\d+)", re.IGNORECASE)

# ===================== KEEP ALIVE (RENDER) =====================
app = Flask("maptap")

@app.route("/")
def home():
    return "MapTap bot running"

def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))

# ===================== JSON HELPERS =====================
def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def today_key(dt=None):
    if not dt:
        dt = datetime.now(UK_TZ)
    return dt.date().isoformat()

def pretty_date(date_key):
    return datetime.strptime(date_key, "%Y-%m-%d").strftime("%A %d %B")

def cleanup_old_scores(data):
    cutoff = datetime.now(UK_TZ).date() - timedelta(days=CLEANUP_DAYS)
    removed = False

    for date_key in list(data.keys()):
        date_obj = datetime.strptime(date_key, "%Y-%m-%d").date()
        if date_obj < cutoff:
            del data[date_key]
            removed = True

    if removed:
        save_json(DATA_FILE, data)

# ===================== STREAK CALC =====================
def calculate_streaks(data, user_id):
    played_dates = sorted(
        datetime.strptime(date, "%Y-%m-%d").date()
        for date, day_data in data.items()
        if user_id in day_data
    )

    if not played_dates:
        return 0, 0

    today = datetime.now(UK_TZ).date()
    current = 0
    day = today

    while day in played_dates:
        current += 1
        day -= timedelta(days=1)

    longest = 1
    streak = 1
    for i in range(1, len(played_dates)):
        if (played_dates[i] - played_dates[i - 1]).days == 1:
            streak += 1
            longest = max(longest, streak)
        else:
            streak = 1

    return current, longest

# ===================== DISCORD CLIENT =====================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

class MapTapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.data = load_json(DATA_FILE)
        self.user_stats = load_json(USER_STATS_FILE)

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
    if message.author.bot:
        return
    if message.channel.id != MAPTAP_CHANNEL_ID:
        return

    match = SCORE_REGEX.search(message.content)
    if not match:
        return

    score = int(match.group(1))
    if score > 1000:
        await message.add_reaction("❌")
        return

    msg_time = message.created_at.replace(
        tzinfo=ZoneInfo("UTC")
    ).astimezone(UK_TZ)

    date_key = today_key(msg_time)
    user_id = str(message.author.id)

    client.data.setdefault(date_key, {})
    existing = client.data[date_key].get(user_id)

    stats = client.user_stats.setdefault(user_id, {
        "total_points": 0,
        "days_played": 0,
        "best_streak": 0
    })

    if existing:
        stats["total_points"] -= existing["score"]
    else:
        stats["days_played"] += 1

    stats["total_points"] += score

    client.data[date_key][user_id] = {
        "score": score,
        "updated_at": msg_time.isoformat()
    }

    save_json(DATA_FILE, client.data)

    current_streak, _ = calculate_streaks(client.data, user_id)
    if current_streak > stats["best_streak"]:
        stats["best_streak"] = current_streak

    save_json(USER_STATS_FILE, client.user_stats)

    await message.add_reaction("✅")

# ===================== DAILY SCOREBOARD (11PM) =====================
@tasks.loop(time=time(hour=23, minute=0, tzinfo=UK_TZ))
async def daily_scoreboard():
    ch = get_channel()
    if not ch:
        return

    date_key = today_key()
    scores = client.data.get(date_key, {})

    if not scores:
        await ch.send(
            f"🗺️ **MapTap — Daily Scores**\n"
            f"*{pretty_date(date_key)}*\n\n"
            "😶 No scores today."
        )
        return

    sorted_scores = sorted(
        scores.items(),
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

    cleanup_old_scores(client.data)

# ===================== WEEKLY ROUND-UP (SUN 23:05) =====================
@tasks.loop(time=time(hour=23, minute=5, tzinfo=UK_TZ))
async def weekly_roundup():
    now = datetime.now(UK_TZ)
    if now.weekday() != 6:
        return

    ch = get_channel()
    if not ch:
        return

    monday = now.date() - timedelta(days=6)
    week_dates = [
        (monday + timedelta(days=i)).isoformat()
        for i in range(7)
    ]

    weekly = {}

    for date in week_dates:
        for uid, entry in client.data.get(date, {}).items():
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
    user_id = str(interaction.user.id)
    stats = client.user_stats.get(user_id)

    if not stats:
        await interaction.response.send_message(
            "You haven’t recorded any MapTap scores yet 🗺️",
            ephemeral=True
        )
        return

    current_streak, _ = calculate_streaks(client.data, user_id)
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
    if MAPTAP_CHANNEL_ID == 0:
        raise RuntimeError("MAPTAP_CHANNEL_ID missing")

    Thread(target=run_web, daemon=True).start()
    client.run(TOKEN)
