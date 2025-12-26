# =========================
# MapTap Companion Bot (FULL FILE)
# =========================

import os
import json
import re
import base64
import requests
import random
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
GITHUB_REPO = os.getenv("GITHUB_REPO")
RESET_PASSWORD = os.getenv("RESET_PASSWORD", "CHANGEME") # Set this in Render

SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")
SETTINGS_PATH = os.getenv("MAPTAP_SETTINGS_PATH", "data/maptap_settings.json")

MAPTAP_URL = os.getenv("MAPTAP_URL", "https://www.maptap.gg")
CLEANUP_DAYS = int(os.getenv("MAPTAP_CLEANUP_DAYS", "69"))
MAX_SCORE = int(os.getenv("MAPTAP_MAX_SCORE", "1000"))

SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)
ZERO_REGEX = re.compile(r"Final\s*score:\s*0(?!\d)", re.IGNORECASE)

# =====================================================
# MESSAGE VARIATIONS
# =====================================================
PB_MESSAGES = [
    "🚀 **New Personal Best!** <@{uid}> just smashed their record with a **{score}**!",
    "⭐ **Personal Best!** <@{uid}> is on fire with a new high of **{score}**!",
    "📈 Growth! <@{uid}> just set a new PB of **{score}**. Keep it up!",
    "🏆 A new record! <@{uid}> just beat their previous best with **{score}**!",
    "✨ Legend status! <@{uid}> just reached a new personal peak of **{score}**!",
    "🔥 Sizzling! <@{uid}> just updated their PB to **{score}**!",
    "🏅 Elite! <@{uid}> just clocked a personal best of **{score}**!",
    "💎 Solid work! <@{uid}> just hit a new PB of **{score}**!"
]

ROAST_MESSAGES = [
    "💀 0 points? <@{uid}>, did you even open the map?",
    "✈️ <@{uid}> just took a flight to nowhere. **Score: 0**.",
    "📉 <@{uid}>, that was... impressively bad. **0 points**.",
    "🔦 Can someone find <@{uid}>? They are clearly lost. **Score: 0**.",
    "🛑 Stop the count! <@{uid}> just posted a **0**.",
    "📉 <@{uid}> is currently a danger to himself and others in an aircraft. **Score: 0**.",
    "🤡 <@{uid}>, the ground is that way ↓. **Score: 0**.",
    "🙈 I’m blind! Oh wait, that’s just <@{uid}>'s score of **0**."
]

# =====================================================
# DEFAULT SETTINGS
# =====================================================
DEFAULT_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "channel_id": None,
    "daily_post_enabled": True,
    "daily_scoreboard_enabled": True,
    "weekly_roundup_enabled": True,
    "rivalry_enabled": True,
    "roasts_enabled": True,
    "admin_role_ids": [],
    "emojis": {
        "recorded": "🌏",
        "too_high": "❌",
        "rescan_ingested": "🔁"
    },
    "times": {
        "daily_post": "00:00",
        "daily_scoreboard": "23:30",
        "weekly_roundup": "23:45",
        "rivalry": "14:00"
    },
    "last_run": {
        "daily_post": None,
        "daily_scoreboard": None,
        "weekly_roundup": None,
        "rivalry": None
    }
}

HEADERS = {"Authorization": f"token {GITHUB_TOKEN}" if GITHUB_TOKEN else "", "Accept": "application/vnd.github.v3+json"}

# =====================================================
# GITHUB & UTILS
# =====================================================
def _gh_url(path: str) -> str: return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"

def github_load_json(path: str, default: Any) -> Tuple[Any, Optional[str]]:
    r = requests.get(_gh_url(path), headers=HEADERS, timeout=20)
    if r.status_code == 404: return default, None
    r.raise_for_status()
    payload = r.json()
    content = base64.b64decode(payload.get("content", "")).decode("utf-8") if payload.get("content") else ""
    return (json.loads(content), payload.get("sha")) if content.strip() else (default, payload.get("sha"))

def github_save_json(path: str, data: Any, sha: Optional[str], message: str) -> str:
    encoded = base64.b64encode(json.dumps(data, indent=2).encode("utf-8")).decode("utf-8")
    body = {"message": message, "content": encoded}
    if sha: body["sha"] = sha
    r = requests.put(_gh_url(path), headers=HEADERS, json=body, timeout=20)
    r.raise_for_status()
    return r.json().get("content", {}).get("sha", "")

def load_settings() -> Tuple[Dict[str, Any], Optional[str]]:
    settings, sha = github_load_json(SETTINGS_PATH, DEFAULT_SETTINGS.copy())
    merged = DEFAULT_SETTINGS.copy()
    if isinstance(settings, dict): merged.update(settings)
    return merged, sha

def save_settings(settings: Dict[str, Any], sha: Optional[str], message: str) -> Optional[str]:
    return github_save_json(SETTINGS_PATH, settings, sha, message)

def today_key(dt: Optional[datetime] = None) -> str:
    return (dt or datetime.now(UK_TZ)).date().isoformat()

# =====================================================
# RANKING LOGIC (AVERAGE BASED)
# =====================================================
def get_rankings(users: Dict[str, Any], min_days: int = 1, filter_uids: Optional[List[str]] = None) -> List[Dict]:
    """Sorts by Average, then Days Played as tiebreaker."""
    board = []
    for uid, data in users.items():
        if filter_uids and uid not in filter_uids: continue
        days = int(data.get("days_played", 0))
        if days < min_days: continue
        total = int(data.get("total_points", 0))
        avg = round(total / days) if days > 0 else 0
        board.append({"uid": uid, "avg": avg, "days": days, "total": total})
    
    board.sort(key=lambda x: (x["avg"], x["days"]), reverse=True)
    return board

def get_user_rank(uid: str, rankings: List[Dict]) -> Tuple[int, int]:
    for idx, entry in enumerate(rankings, 1):
        if entry["uid"] == uid: return idx, len(rankings)
    return 0, len(rankings)

# =====================================================
# MODALS & VIEWS
# =====================================================
class PasswordResetModal(discord.ui.Modal, title="Security Check"):
    password = discord.ui.TextInput(label="Enter Admin Reset Password", style=discord.TextStyle.short, required=True)
    
    async def on_submit(self, interaction: discord.Interaction):
        if self.password.value != RESET_PASSWORD:
            await interaction.response.send_message("❌ Incorrect password. Reset aborted.", ephemeral=True)
            return
        
        # Perform Reset
        empty_scores = {}
        empty_users = {}
        s_sha = github_load_json(SCORES_PATH, {})[1]
        u_sha = github_load_json(USERS_PATH, {})[1]
        
        github_save_json(SCORES_PATH, empty_scores, s_sha, f"RESET by {interaction.user}")
        github_save_json(USERS_PATH, empty_users, u_sha, f"RESET by {interaction.user}")
        
        await interaction.response.send_message(
            f"⚠️ **MapTap Reset** — Admin **{interaction.user.display_name}** has reset all server scores.",
            ephemeral=False
        )

class TimeSettingsModal(discord.ui.Modal, title="MapTap Times (UK)"):
    daily_post = discord.ui.TextInput(label="Daily Post (HH:MM)", placeholder="00:00", max_length=5)
    daily_board = discord.ui.TextInput(label="Daily Scoreboard (HH:MM)", placeholder="23:30", max_length=5)
    weekly = discord.ui.TextInput(label="Weekly Roundup (HH:MM)", placeholder="23:45", max_length=5)
    rivalry = discord.ui.TextInput(label="Rivalry Alert (HH:MM)", placeholder="14:00", max_length=5)

    def __init__(self, view_ref):
        super().__init__()
        self.view_ref = view_ref
        t = self.view_ref.settings.get("times", {})
        self.daily_post.default = t.get("daily_post", "00:00")
        self.daily_board.default = t.get("daily_scoreboard", "23:30")
        self.weekly.default = t.get("weekly_roundup", "23:45")
        self.rivalry.default = t.get("rivalry", "14:00")

    async def on_submit(self, interaction: discord.Interaction):
        try:
            for v in [self.daily_post.value, self.daily_board.value, self.weekly.value, self.rivalry.value]:
                datetime.strptime(v, "%H:%M")
        except:
            return await interaction.response.send_message("❌ Invalid format.", ephemeral=True)
        
        self.view_ref.settings["times"] = {
            "daily_post": self.daily_post.value,
            "daily_scoreboard": self.daily_board.value,
            "weekly_roundup": self.weekly.value,
            "rivalry": self.rivalry.value
        }
        await self.view_ref._save_refresh(interaction, "MapTap: update times")

class MapTapSettingsView(discord.ui.View):
    def __init__(self, settings, sha):
        super().__init__(timeout=300)
        self.settings, self.sha = settings, sha

    def _embed(self):
        c = self.settings.get('channel_id')
        r = self.settings.get('admin_role_ids', [])
        t = self.settings.get('times', {})
        e = discord.Embed(title="🗺️ MapTap Settings", color=0xF1C40F)
        e.description = f"**Channel:** <#{c}>\n**Roles:** {', '.join(f'<@&{rid}>' for rid in r) if r else 'Admin only'}"
        
        status = (f"Bot: {'✅' if self.settings.get('enabled') else '❌'}\n"
                  f"Daily Post: {'✅' if self.settings.get('daily_post_enabled') else '❌'}\n"
                  f"Daily Board: {'✅' if self.settings.get('daily_scoreboard_enabled') else '❌'}\n"
                  f"Weekly: {'✅' if self.settings.get('weekly_roundup_enabled') else '❌'}\n"
                  f"Roasts: {'✅' if self.settings.get('roasts_enabled') else '❌'}")
        
        times = f"Post: `{t.get('daily_post')}` | Board: `{t.get('daily_scoreboard')}` | Weekly: `{t.get('weekly_roundup')}`"
        e.add_field(name="🧭 System Status", value=status, inline=False)
        e.add_field(name="🕒 Times (UK)", value=times, inline=False)
        return e

    async def _save_refresh(self, interaction, msg):
        current, c_sha = load_settings()
        current.update(self.settings)
        new_sha = save_settings(current, c_sha, msg)
        self.settings, self.sha = current, new_sha or c_sha
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Set Channel")
    async def channel_select(self, interaction, select):
        self.settings["channel_id"] = select.values[0].id
        await self._save_refresh(interaction, "MapTap: channel")

    @discord.ui.button(label="Edit Times", style=discord.ButtonStyle.primary)
    async def btn_times(self, interaction, _): await interaction.response.send_modal(TimeSettingsModal(self))

    @discord.ui.button(label="Toggle Bot", style=discord.ButtonStyle.secondary)
    async def btn_bot(self, interaction, _):
        self.settings["enabled"] = not self.settings.get("enabled", True)
        await self._save_refresh(interaction, "MapTap: toggle bot")

    @discord.ui.button(label="Toggle Roasts", style=discord.ButtonStyle.secondary)
    async def btn_roasts(self, interaction, _):
        self.settings["roasts_enabled"] = not self.settings.get("roasts_enabled", True)
        await self._save_refresh(interaction, "MapTap: toggle roasts")

    @discord.ui.button(label="RESET SCORES", style=discord.ButtonStyle.danger)
    async def btn_reset(self, interaction, _):
        await interaction.response.send_modal(PasswordResetModal())

# =====================================================
# LEADERBOARD VIEW
# =====================================================
class LeaderboardDropdown(discord.ui.Select):
    def __init__(self, users, scores):
        options = [
            discord.SelectOption(label="This Week", description="Min 3 days played", value="week"),
            discord.SelectOption(label="This Month", description="Min 10 days played", value="month"),
            discord.SelectOption(label="All-Time", description="Min 10 days played", value="all")
        ]
        super().__init__(placeholder="Choose Timeframe", options=options)
        self.users, self.scores = users, scores

    async def callback(self, interaction: discord.Interaction):
        mode = self.values[0]
        now = datetime.now(UK_TZ)
        
        if mode == "week":
            min_days, title = 3, "This Week"
            start = (now - timedelta(days=now.weekday())).date()
            uids = {uid for d, data in self.scores.items() if datetime.fromisoformat(d).date() >= start for uid in data}
        elif mode == "month":
            min_days, title = 10, "This Month"
            uids = {uid for d, data in self.scores.items() if datetime.fromisoformat(d).month == now.month for uid in data}
        else:
            min_days, title, uids = 10, "All-Time", None

        ranks = get_rankings(self.users, min_days, list(uids) if uids else None)
        
        embed = discord.Embed(title=f"🗺️ MapTap Leaderboard - {title}", color=0x3498DB)
        if not ranks:
            embed.description = "No one has met the minimum day requirements yet!"
        else:
            lines = [f"{i}. <@{r['uid']}> — **Avg: {r['avg']}** ({r['days']} days)" for i, r in enumerate(ranks[:15], 1)]
            embed.description = "\n".join(lines)
        
        await interaction.response.edit_message(embed=embed)

class LeaderboardView(discord.ui.View):
    def __init__(self, users, scores):
        super().__init__(timeout=60)
        self.add_item(LeaderboardDropdown(users, scores))

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
        self.scheduler_tick.start()
        await self.tree.sync()

    @tasks.loop(minutes=1)
    async def scheduler_tick(self):
        s, sha = load_settings()
        if not s.get("enabled"): return
        now = datetime.now(UK_TZ)
        now_hm, today = now.strftime("%H:%M"), today_key(now)
        
        # Scheduling Logic
        t, lr = s.get("times", {}), s.get("last_run", {})
        if now_hm == t.get("daily_post") and lr.get("daily_post") != today:
            ch = self.get_channel(s.get("channel_id"))
            if ch: await ch.send(build_daily_prompt())
            s["last_run"]["daily_post"] = today
            save_settings(s, sha, "Daily Post")

client = MapTapBot()

# =====================================================
# COMMANDS
# =====================================================
@client.tree.command(name="mymaptap", description="View your personalized stats")
async def mymaptap(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})
    target = user or interaction.user
    uid = str(target.id)
    stats = users.get(uid)

    if not stats or int(stats.get("days_played", 0)) == 0:
        return await interaction.response.send_message("No scores found yet! Post a result to start tracking.")

    # Calculate Rankings
    all_ranks = get_rankings(users, 10)
    weekly_ranks = get_rankings(users, 3) # Simplified for context
    
    a_rank, a_total = get_user_rank(uid, all_ranks)
    w_rank, w_total = get_user_rank(uid, weekly_ranks)
    
    pb = stats.get("personal_best", {"score": 0, "date": "N/A"})
    avg = round(int(stats["total_points"]) / int(stats["days_played"]))
    
    embed = discord.Embed(title=f"🗺️ MapTap Stats — {target.display_name}", color=0xF1C40F)
    embed.add_field(name="📊 Server Rankings", value=f"• All-Time: **#{a_rank if a_rank else 'N/A'}** of {a_total}\n• This Week: **#{w_rank if w_rank else 'N/A'}** of {w_total}", inline=False)
    embed.add_field(name="⭐ Personal Records", value=f"• Best Score: **{pb['score']}** ({pb['date']})\n• Best Streak: 🔥 {stats.get('best_streak',0)} days\n• Current: 🔥 {calculate_current_streak(scores, uid)} days", inline=False)
    embed.add_field(name="📈 Overall Stats", value=f"• Total Points: {stats['total_points']}\n• Days Played: {stats['days_played']}\n• Average Score: **{avg}**", inline=False)
    
    await interaction.response.send_message(embed=embed)

@client.tree.command(name="leaderboard", description="View server rankings")
async def leaderboard(interaction: discord.Interaction):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})
    embed = discord.Embed(title="🗺️ MapTap Leaderboards", description="Select a timeframe below to see rankings (Based on Average Score).", color=0x3498DB)
    await interaction.response.send_message(embed=embed, view=LeaderboardView(users, scores))

@client.tree.command(name="maptapsettings", description="Admin settings")
async def settings(interaction: discord.Interaction):
    s, sha = load_settings()
    if not has_admin_access(interaction.user, s): return await interaction.response.send_message("No permission.", ephemeral=True)
    view = MapTapSettingsView(s, sha)
    await interaction.response.send_message(embed=view._embed(), view=view)

# =====================================================
# INGESTION & ROASTS
# =====================================================
@client.event
async def on_message(message: discord.Message):
    if message.author.bot: return
    s, _ = load_settings()
    if not s.get("enabled") or message.channel.id != s.get("channel_id"): return

    # Score Detection
    m = SCORE_REGEX.search(message.content or "")
    if not m: return
    
    score = int(m.group(1))
    if score > MAX_SCORE: return await message.add_reaction("❌")

    uid = str(message.author.id)
    now_uk = message.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(UK_TZ)
    date_key = today_key(now_uk)
    
    # Load Data
    scores, s_sha = github_load_json(SCORES_PATH, {})
    users, u_sha = github_load_json(USERS_PATH, {})
    
    day_bucket = scores.setdefault(date_key, {})
    stats = users.setdefault(uid, {"total_points": 0, "days_played": 0, "best_streak": 0, "personal_best": {"score": 0, "date": "N/A"}})
    
    old_pb = stats.get("personal_best", {}).get("score", 0)

    # Ingest
    if uid not in day_bucket: stats["days_played"] += 1
    else: stats["total_points"] -= int(day_bucket[uid]["score"])
    
    stats["total_points"] += score
    day_bucket[uid] = {"score": score, "time": now_uk.isoformat()}
    
    # Logic: Roast for 0
    if score == 0 and s.get("roasts_enabled"):
        await message.reply(random.choice(ROAST_MESSAGES).format(uid=uid))
    
    # Logic: PB Beat
    elif score > old_pb and stats["days_played"] > 1:
        await message.reply(random.choice(PB_MESSAGES).format(uid=uid, score=score))
        stats["personal_best"] = {"score": score, "date": date_key}
    elif stats["days_played"] == 1:
        stats["personal_best"] = {"score": score, "date": date_key}

    # Streak
    cur = calculate_current_streak(scores, uid)
    if cur > stats.get("best_streak", 0): stats["best_streak"] = cur

    github_save_json(SCORES_PATH, scores, s_sha, f"Score: {uid}")
    github_save_json(USERS_PATH, users, u_sha, f"Stats: {uid}")
    await message.add_reaction(s.get("emojis", {}).get("recorded", "🌏"))

# =====================================================
# HELPERS
# =====================================================
def has_admin_access(m, s): return m.guild_permissions.administrator or any(r.id in s.get("admin_role_ids", []) for r in m.roles)

def calculate_current_streak(scores, uid):
    dates = {d for d, day in scores.items() if uid in day}
    curr, count = datetime.now(UK_TZ).date(), 0
    while curr.isoformat() in dates:
        count += 1
        curr -= timedelta(days=1)
    return count

def build_daily_prompt():
    return f"🗺️ **Daily MapTap is live!**\n👉 {MAPTAP_URL}\n\nPost results exactly as shared to track scores! ✈️"

# =====================================================
# RESCAN (Updated for PB Logic)
# =====================================================
@client.tree.command(name="rescan", description="Sync data and fix PBs")
async def rescan(interaction: discord.Interaction, messages: int = 50):
    s, _ = load_settings()
    if not has_admin_access(interaction.user, s): return await interaction.response.send_message("No.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    
    scores, s_sha = github_load_json(SCORES_PATH, {})
    users, u_sha = github_load_json(USERS_PATH, {})
    
    # Fix PBs from existing JSON first
    for d, day in scores.items():
        for uid, entry in day.items():
            sc = int(entry["score"])
            u = users.setdefault(uid, {"total_points": 0, "days_played": 0, "best_streak": 0, "personal_best": {"score": 0, "date": "N/A"}})
            if sc > u["personal_best"]["score"]: u["personal_best"] = {"score": sc, "date": d}

    github_save_json(USERS_PATH, users, u_sha, "Rescan Sync")
    await interaction.followup.send("✅ Rescan & Data Sync Complete.")

# =====================================================
# RUN
# =====================================================
app = Flask("maptap")
@app.get("/")
def home(): return "MapTap Online"

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))).start()
    client.run(TOKEN)
