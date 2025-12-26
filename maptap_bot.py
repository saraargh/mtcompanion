# =========================
# MapTap Companion Bot (TOTAL RECALL EDITION)
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
# CONFIG & TIMEZONES
# =====================================================
UK_TZ = ZoneInfo("Europe/London")
TOKEN = os.getenv("TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")
RESET_PASSWORD = os.getenv("RESET_PASSWORD", "CHANGEME")

SCORES_PATH = os.getenv("MAPTAP_SCORES_PATH", "data/maptap_scores.json")
USERS_PATH = os.getenv("MAPTAP_USERS_PATH", "data/maptap_users.json")
SETTINGS_PATH = os.getenv("MAPTAP_SETTINGS_PATH", "data/maptap_settings.json")

MAPTAP_URL = os.getenv("MAPTAP_URL", "https://www.maptap.gg")
MAX_SCORE = 1000
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)

# =====================================================
# MESSAGE VARIATIONS
# =====================================================
PB_MESSAGES = [
    "🚀 **New Personal Best!** <@{uid}> smashed their record with **{score}**!",
    "⭐ **Personal Best!** <@{uid}> is on fire with a new high of **{score}**!",
    "📈 Growth! <@{uid}> just set a new PB of **{score}**!",
    "🏆 A new record! <@{uid}> beat their previous best with **{score}**!",
    "✨ Legend status! <@{uid}> reached a new peak of **{score}**!",
    "🔥 Sizzling! <@{uid}> updated their PB to **{score}**!",
    "🏅 Elite! <@{uid}> clocked a personal best of **{score}**!",
    "💎 Solid work! <@{uid}> hit a new PB of **{score}**!"
]

ROAST_MESSAGES = [
    "💀 0 points? <@{uid}>, did you even open the map?",
    "✈️ <@{uid}> just took a flight to nowhere. **Score: 0**.",
    "📉 <@{uid}>, that was impressively bad. **0 points**.",
    "🔦 Can someone find <@{uid}>? They are lost. **Score: 0**.",
    "🛑 Stop the count! <@{uid}> just posted a **0**.",
    "📉 <@{uid}> is a danger to aviation. **Score: 0**.",
    "🤡 <@{uid}>, the ground is that way ↓. **Score: 0**.",
    "🙈 I’m blind! Oh wait, that’s just <@{uid}>'s score of **0**."
]

# =====================================================
# GITHUB & SETTINGS HELPERS
# =====================================================
def _gh_url(path: str) -> str: return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}" if GITHUB_TOKEN else "", "Accept": "application/vnd.github.v3+json"}

def github_load_json(path: str, default: Any) -> Tuple[Any, Optional[str]]:
    r = requests.get(_gh_url(path), headers=HEADERS, timeout=20)
    if r.status_code == 404: return default, None
    r.raise_for_status()
    payload = r.json()
    content = base64.b64decode(payload.get("content", "")).decode("utf-8")
    return json.loads(content), payload.get("sha")

def github_save_json(path: str, data: Any, sha: Optional[str], message: str) -> str:
    encoded = base64.b64encode(json.dumps(data, indent=2).encode("utf-8")).decode("utf-8")
    body = {"message": message, "content": encoded}
    if sha: body["sha"] = sha
    r = requests.put(_gh_url(path), headers=HEADERS, json=body, timeout=20)
    r.raise_for_status()
    return r.json().get("content", {}).get("sha", "")

def load_settings():
    settings, sha = github_load_json(SETTINGS_PATH, {})
    defaults = {
        "enabled": True, "channel_id": None, "admin_role_ids": [],
        "daily_post_enabled": True, "daily_scoreboard_enabled": True,
        "weekly_roundup_enabled": True, "rivalry_enabled": True, "roasts_enabled": True,
        "times": {"daily_post": "00:00", "daily_scoreboard": "23:30", "weekly_roundup": "23:45", "rivalry": "14:00"},
        "last_run": {}
    }
    defaults.update(settings)
    return defaults, sha

# =====================================================
# RANKING & STREAK CALCULATIONS
# =====================================================
def get_rankings(users: Dict, min_days: int = 1, filter_uids: List = None):
    board = []
    for uid, data in users.items():
        if filter_uids and uid not in filter_uids: continue
        days = int(data.get("days_played", 0))
        if days < min_days: continue
        avg = round(int(data.get("total_points", 0)) / days) if days > 0 else 0
        board.append({"uid": uid, "avg": avg, "days": days})
    board.sort(key=lambda x: (x["avg"], x["days"]), reverse=True)
    return board

def calculate_current_streak(scores: Dict, user_id: str) -> int:
    played_dates = set()
    for date_key, day_data in scores.items():
        if user_id in day_data:
            try: played_dates.add(datetime.strptime(date_key, "%Y-%m-%d").date())
            except: pass
    if not played_dates: return 0
    day, streak = datetime.now(UK_TZ).date(), 0
    while day in played_dates:
        streak += 1
        day -= timedelta(days=1)
    return streak

# =====================================================
# MODALS & INTERFACE
# =====================================================
class AlertSettingsModal(discord.ui.Modal, title="Configure Notifications"):
    roasts = discord.ui.TextInput(label="Enable Roasts? (Yes/No)", max_length=3)
    daily = discord.ui.TextInput(label="Daily Scoreboard? (Yes/No)", max_length=3)
    weekly = discord.ui.TextInput(label="Weekly Roundup? (Yes/No)", max_length=3)
    rivalry = discord.ui.TextInput(label="Rivalry Alerts? (Yes/No)", max_length=3)

    def __init__(self, view_ref):
        super().__init__()
        self.view_ref = view_ref
        self.roasts.default = "Yes" if self.view_ref.settings.get("roasts_enabled") else "No"
        self.daily.default = "Yes" if self.view_ref.settings.get("daily_scoreboard_enabled") else "No"
        self.weekly.default = "Yes" if self.view_ref.settings.get("weekly_roundup_enabled") else "No"
        self.rivalry.default = "Yes" if self.view_ref.settings.get("rivalry_enabled") else "No"

    async def on_submit(self, interaction: discord.Interaction):
        self.view_ref.settings["roasts_enabled"] = self.roasts.value.lower() == "yes"
        self.view_ref.settings["daily_scoreboard_enabled"] = self.daily.value.lower() == "yes"
        self.view_ref.settings["weekly_roundup_enabled"] = self.weekly.value.lower() == "yes"
        self.view_ref.settings["rivalry_enabled"] = self.rivalry.value.lower() == "yes"
        new_sha = github_save_json(SETTINGS_PATH, self.view_ref.settings, self.view_ref.sha, "Updated Alerts")
        self.view_ref.sha = new_sha
        await interaction.response.edit_message(embed=self.view_ref._embed(), view=self.view_ref)

class PasswordResetModal(discord.ui.Modal, title="Security Check"):
    password = discord.ui.TextInput(label="Enter Admin Reset Password", required=True)
    async def on_submit(self, interaction: discord.Interaction):
        if self.password.value != RESET_PASSWORD: return await interaction.response.send_message("❌ Wrong password.", ephemeral=True)
        github_save_json(SCORES_PATH, {}, github_load_json(SCORES_PATH, {})[1], f"RESET by {interaction.user}")
        github_save_json(USERS_PATH, {}, github_load_json(USERS_PATH, {})[1], f"RESET by {interaction.user}")
        await interaction.response.send_message(f"⚠️ **MapTap Reset** — Admin **{interaction.user.display_name}** has cleared all scores.")

class MapTapSettingsView(discord.ui.View):
    def __init__(self, settings, sha):
        super().__init__(timeout=300)
        self.settings, self.sha = settings, sha

    def _embed(self):
        chan = f"<#{self.settings['channel_id']}>" if self.settings.get('channel_id') else "Not Set"
        roles = ", ".join(f"<@&{rid}>" for rid in self.settings.get('admin_role_ids', [])) or "Admins only"
        e = discord.Embed(title="🗺️ MapTap Settings", color=0xF1C40F)
        e.description = f"**Channel:** {chan}\n**Admin Roles:** {roles}"
        status = (f"Bot: {'✅' if self.settings['enabled'] else '❌'}\n"
                  f"Roasts: {'✅' if self.settings['roasts_enabled'] else '❌'}\n"
                  f"Weekly: {'✅' if self.settings['weekly_roundup_enabled'] else '❌'}\n"
                  f"Daily Board: {'✅' if self.settings['daily_scoreboard_enabled'] else '❌'}")
        e.add_field(name="🧭 Current Configuration", value=status)
        return e

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="MapTap Channel")
    async def c_select(self, interaction, select):
        self.settings["channel_id"] = select.values[0].id
        self.sha = github_save_json(SETTINGS_PATH, self.settings, self.sha, "Set Channel")
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Admin Roles", min_values=0, max_values=5)
    async def r_select(self, interaction, select):
        self.settings["admin_role_ids"] = [r.id for r in select.values]
        self.sha = github_save_json(SETTINGS_PATH, self.settings, self.sha, "Set Roles")
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="Configure Alerts", style=discord.ButtonStyle.primary)
    async def btn_alerts(self, interaction, _): await interaction.response.send_modal(AlertSettingsModal(self))

    @discord.ui.button(label="Toggle Bot", style=discord.ButtonStyle.secondary)
    async def btn_toggle(self, interaction, _):
        self.settings["enabled"] = not self.settings["enabled"]
        self.sha = github_save_json(SETTINGS_PATH, self.settings, self.sha, "Toggle Bot")
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="RESET SCORES", style=discord.ButtonStyle.danger)
    async def btn_reset(self, interaction, _): await interaction.response.send_modal(PasswordResetModal())

# =====================================================
# LEADERBOARD LOGIC
# =====================================================
class LeaderboardDropdown(discord.ui.Select):
    def __init__(self, users, scores):
        options = [
            discord.SelectOption(label="This Week", value="week", description="Min 3 days played"),
            discord.SelectOption(label="This Month", value="month", description="Min 10 days played"),
            discord.SelectOption(label="All-Time", value="all", description="Min 10 days played")
        ]
        super().__init__(placeholder="Select Timeframe", options=options)
        self.users, self.scores = users, scores

    async def callback(self, interaction: discord.Interaction):
        mode, now = self.values[0], datetime.now(UK_TZ)
        if mode == "week":
            min_d, title = 3, "This Week"
            start = (now - timedelta(days=now.weekday())).date()
            uids = {uid for d, data in self.scores.items() if datetime.fromisoformat(d).date() >= start for uid in data}
        elif mode == "month":
            min_d, title = 10, "This Month"
            uids = {uid for d, data in self.scores.items() if datetime.fromisoformat(d).month == now.month for uid in data}
        else: min_d, title, uids = 10, "All-Time", None

        ranks = get_rankings(self.users, min_d, list(uids) if uids else None)
        embed = discord.Embed(title=f"🗺️ MapTap - {title}", color=0x3498DB)
        lines = [f"{i}. <@{r['uid']}> — **Avg: {r['avg']}** ({r['days']} days)" for i, r in enumerate(ranks[:15], 1)]
        embed.description = "\n".join(lines) if lines else "No one qualifies yet."
        await interaction.response.edit_message(embed=embed)

class LeaderboardView(discord.ui.View):
    def __init__(self, users, scores):
        super().__init__(timeout=60)
        self.add_item(LeaderboardDropdown(users, scores))

# =====================================================
# MAIN BOT CLASS
# =====================================================
class MapTapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.all())
        self.tree = app_commands.CommandTree(self)
    async def setup_hook(self):
        self.scheduler_tick.start()
        await self.tree.sync()

    @tasks.loop(minutes=1)
    async def scheduler_tick(self):
        s, sha = load_settings()
        if not s.get("enabled"): return
        now = datetime.now(UK_TZ)
        now_hm, today = now.strftime("%H:%M"), now.date().isoformat()
        if now_hm == s["times"]["daily_post"] and s.get("daily_post_enabled") and s["last_run"].get("daily_post") != today:
            ch = self.get_channel(s.get("channel_id"))
            if ch: await ch.send(f"🗺️ **Daily MapTap is live!**\n👉 {MAPTAP_URL}\nPost results exactly as shared! ✈️")
            s["last_run"]["daily_post"] = today
            github_save_json(SETTINGS_PATH, s, sha, "Auto Post")

client = MapTapBot()

# =====================================================
# SLASH COMMANDS
# =====================================================
@client.tree.command(name="mymaptap", description="View your MapTap profile")
async def mymaptap(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})
    target = user or interaction.user
    uid = str(target.id)
    if uid not in users: return await interaction.response.send_message(f"No data for {target.display_name} yet!")
    
    stats = users[uid]
    ranks_at = get_rankings(users, 10)
    ranks_wk = get_rankings(users, 3)
    
    # Calculate rank position
    at_pos = next((i for i, r in enumerate(ranks_at, 1) if r['uid'] == uid), "N/A")
    wk_pos = next((i for i, r in enumerate(ranks_wk, 1) if r['uid'] == uid), "N/A")
    
    avg = round(int(stats["total_points"]) / int(stats["days_played"]))
    pb = stats.get("personal_best", {"score": 0, "date": "N/A"})
    
    embed = discord.Embed(title=f"🗺️ MapTap Stats — {target.display_name}", color=0xF1C40F)
    embed.add_field(name="📊 Server Rankings", value=f"• All-Time: **#{at_pos}**\n• This Week: **#{wk_pos}**", inline=False)
    embed.add_field(name="⭐ Personal Records", value=f"• Best Score: **{pb['score']}** ({pb['date']})\n• Best Streak: 🔥 {stats.get('best_streak',0)} days\n• Current Streak: 🔥 {calculate_current_streak(scores, uid)} days", inline=False)
    embed.add_field(name="📈 Overall Stats", value=f"• Total Points: {stats['total_points']}\n• Days Played: {stats['days_played']}\n• Average Score: **{avg}**", inline=False)
    await interaction.response.send_message(embed=embed)

@client.tree.command(name="leaderboard", description="View server leaderboards")
async def leaderboard(interaction: discord.Interaction):
    users, _ = github_load_json(USERS_PATH, {})
    scores, _ = github_load_json(SCORES_PATH, {})
    embed = discord.Embed(title="🗺️ MapTap Leaderboards", description="Select a timeframe below to view rankings.", color=0x3498DB)
    await interaction.response.send_message(embed=embed, view=LeaderboardView(users, scores))

@client.tree.command(name="maptapsettings", description="Admin only settings")
async def settings(interaction: discord.Interaction):
    s, sha = load_settings()
    if not (interaction.user.guild_permissions.administrator or any(r.id in s.get("admin_role_ids", []) for r in interaction.user.roles)):
        return await interaction.response.send_message("Access Denied.", ephemeral=True)
    await interaction.response.send_message(embed=MapTapSettingsView(s, sha)._embed(), view=MapTapSettingsView(s, sha))

# =====================================================
# MESSAGE LISTENER (INGESTION)
# =====================================================
@client.event
async def on_message(message: discord.Message):
    if message.author.bot: return
    s, _ = load_settings()
    if not s.get("enabled") or message.channel.id != s.get("channel_id"): return

    m = SCORE_REGEX.search(message.content or "")
    if not m: return
    
    score = int(m.group(1))
    if score > MAX_SCORE: return await message.add_reaction("❌")

    uid = str(message.author.id)
    scores, s_sha = github_load_json(SCORES_PATH, {})
    users, u_sha = github_load_json(USERS_PATH, {})
    
    date_key = datetime.now(UK_TZ).date().isoformat()
    day_bucket = scores.setdefault(date_key, {})
    stats = users.setdefault(uid, {"total_points": 0, "days_played": 0, "best_streak": 0, "personal_best": {"score": 0, "date": "N/A"}})
    
    old_pb = stats.get("personal_best", {}).get("score", 0)

    if uid not in day_bucket: stats["days_played"] += 1
    else: stats["total_points"] -= int(day_bucket[uid]["score"])
    
    stats["total_points"] += score
    day_bucket[uid] = {"score": score}
    
    # Notifications
    if score == 0 and s.get("roasts_enabled"):
        await message.reply(random.choice(ROAST_MESSAGES).format(uid=uid))
    elif score > old_pb:
        if stats["days_played"] > 1: await message.reply(random.choice(PB_MESSAGES).format(uid=uid, score=score))
        stats["personal_best"] = {"score": score, "date": date_key}

    # Streak check
    cur = calculate_current_streak(scores, uid)
    if cur > stats.get("best_streak", 0): stats["best_streak"] = cur

    github_save_json(SCORES_PATH, scores, s_sha, f"Score: {uid}")
    github_save_json(USERS_PATH, users, u_sha, f"Stats: {uid}")
    await message.add_reaction("🌏")

# =====================================================
# SERVER & START
# =====================================================
app = Flask("maptap")
@app.get("/")
def home(): return "MapTap Online"

if __name__ == "__main__":
    Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))).start()
    client.run(TOKEN)
