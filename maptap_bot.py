limport os
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
# CONFIGURATION & CONSTANTS
# =====================================================
UK_TZ = ZoneInfo("Europe/London")
TOKEN = os.getenv("TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")
RESET_PASSWORD = os.getenv("RESET_PASSWORD", "PILOT123")

SCORES_PATH = "data/maptap_scores.json"
USERS_PATH = "data/maptap_users.json"
SETTINGS_PATH = "data/maptap_settings.json"

MAPTAP_URL = "https://www.maptap.gg"
MAX_SCORE = 1000
SCORE_REGEX = re.compile(r"Final\s*score:\s*(\d+)", re.IGNORECASE)

PB_MESSAGES = [
    "🚀 **New Personal Best!** <@{uid}> smashed it with **{score}**!",
    "⭐ **PB ALERT!** <@{uid}> hit a new high of **{score}**!"
]
ROAST_MESSAGES = [
    "💀 0 points? <@{uid}>, did you even open the map?",
    "✈️ <@{uid}> took a flight to nowhere. **Score: 0**."
]

# =====================================================
# GITHUB DATA ARCHITECTURE
# =====================================================
def _gh_url(path): 
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}", 
    "Accept": "application/vnd.github.v3+json"
}

def github_load_json(path, default):
    r = requests.get(_gh_url(path), headers=HEADERS)
    if r.status_code == 404: 
        return default, None
    payload = r.json()
    content = base64.b64decode(payload["content"]).decode("utf-8")
    return json.loads(content), payload["sha"]

def github_save_json(path, data, sha, msg):
    encoded = base64.b64encode(json.dumps(data, indent=2).encode("utf-8")).decode("utf-8")
    body = {"message": msg, "content": encoded}
    if sha: 
        body["sha"] = sha
    r = requests.put(_gh_url(path), headers=HEADERS, json=body)
    return r.json().get("content", {}).get("sha", "")

def load_settings():
    s, sha = github_load_json(SETTINGS_PATH, {})
    defaults = {
        "enabled": True, 
        "channel_id": None, 
        "admin_role_ids": [],
        "daily_post_enabled": True, 
        "daily_scoreboard_enabled": True,
        "weekly_roundup_enabled": True, 
        "rivalry_enabled": True, 
        "roasts_enabled": True,
        "times": {"daily_post": "00:00", "daily_scoreboard": "23:30", "weekly_roundup": "23:45", "rivalry": "14:00"},
        "last_run": {}
    }
    defaults.update(s)
    return defaults, sha

# =====================================================
# MODALS (THE INTERACTIVE POP-UPS)
# =====================================================
class AlertSettingsModal(discord.ui.Modal, title="Configure Notifications"):
    def __init__(self, view):
        super().__init__()
        self.view = view
        self.roasts = discord.ui.TextInput(label="Enable Roasts? (Yes/No)", default="Yes" if view.settings["roasts_enabled"] else "No")
        self.daily = discord.ui.TextInput(label="Daily Scoreboard? (Yes/No)", default="Yes" if view.settings["daily_scoreboard_enabled"] else "No")
        self.weekly = discord.ui.TextInput(label="Weekly Roundup? (Yes/No)", default="Yes" if view.settings["weekly_roundup_enabled"] else "No")
        self.rivalry = discord.ui.TextInput(label="Rivalry Alerts? (Yes/No)", default="Yes" if view.settings["rivalry_enabled"] else "No")
        
        self.add_item(self.roasts)
        self.add_item(self.daily)
        self.add_item(self.weekly)
        self.add_item(self.rivalry)

    async def on_submit(self, interaction: discord.Interaction):
        self.view.settings["roasts_enabled"] = self.roasts.value.lower() == "yes"
        self.view.settings["daily_scoreboard_enabled"] = self.daily.value.lower() == "yes"
        self.view.settings["weekly_roundup_enabled"] = self.weekly.value.lower() == "yes"
        self.view.settings["rivalry_enabled"] = self.rivalry.value.lower() == "yes"
        
        self.view.sha = github_save_json(SETTINGS_PATH, self.view.settings, self.view.sha, "Updated Notification Settings")
        await interaction.response.edit_message(embed=self.view._embed(), view=self.view)

class ResetModal(discord.ui.Modal, title="⚠️ SYSTEM RESET"):
    password = discord.ui.TextInput(label="Enter Admin Password", required=True)
    async def on_submit(self, interaction: discord.Interaction):
        if self.password.value != RESET_PASSWORD:
            return await interaction.response.send_message("❌ Incorrect Password. Reset aborted.", ephemeral=True)
        
        github_save_json(SCORES_PATH, {}, github_load_json(SCORES_PATH, {})[1], "MANUAL RESET")
        github_save_json(USERS_PATH, {}, github_load_json(USERS_PATH, {})[1], "MANUAL RESET")
        await interaction.response.send_message(f"💣 **Database Cleared** by {interaction.user.mention}. All scores have been wiped.")

# =====================================================
# VIEWS (BUTTONS & DROPDOWNS)
# =====================================================
class SettingsView(discord.ui.View):
    def __init__(self, s, sha):
        super().__init__(timeout=None)
        self.settings = s
        self.sha = sha

    def _embed(self):
        e = discord.Embed(title="⚙️ MapTap Admin Settings", color=0xF1C40F)
        chan = f"<#{self.settings['channel_id']}>" if self.settings['channel_id'] else "Not Set"
        roles = ", ".join([f"<@&{r}>" for r in self.settings['admin_role_ids']]) or "None"
        e.description = f"**Channel:** {chan}\n**Admin Roles:** {roles}"
        
        status = (f"Roasts: {'✅' if self.settings['roasts_enabled'] else '❌'}\n"
                  f"Weekly Board: {'✅' if self.settings['weekly_roundup_enabled'] else '❌'}\n"
                  f"Daily Board: {'✅' if self.settings['daily_scoreboard_enabled'] else '❌'}")
        e.add_field(name="Alert Status", value=status)
        return e

    @discord.ui.select(cls=discord.ui.ChannelSelect, placeholder="Select MapTap Channel")
    async def channel_select(self, interaction, select):
        self.settings["channel_id"] = select.values[0].id
        self.sha = github_save_json(SETTINGS_PATH, self.settings, self.sha, "Update Channel")
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Select Admin Roles", min_values=0, max_values=5)
    async def role_select(self, interaction, select):
        self.settings["admin_role_ids"] = [r.id for r in select.values]
        self.sha = github_save_json(SETTINGS_PATH, self.settings, self.sha, "Update Roles")
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="Configure Notifications", style=discord.ButtonStyle.primary)
    async def notify_btn(self, interaction, _):
        await interaction.response.send_modal(AlertSettingsModal(self))

    @discord.ui.button(label="RESET SCORES", style=discord.ButtonStyle.danger)
    async def reset_btn(self, interaction, _):
        await interaction.response.send_modal(ResetModal())

# =====================================================
# COMMANDS: RESCAN & RANKING
# =====================================================
def get_rankings(users, min_days=1):
    board = []
    for uid, data in users.items():
        days = int(data.get("days_played", 0))
        if days < min_days: continue
        avg = round(int(data.get("total_points", 0)) / days) if days > 0 else 0
        board.append({"uid": uid, "avg": avg, "days": days})
    # Sort by Average first, then total days played as tiebreaker
    board.sort(key=lambda x: (x["avg"], x["days"]), reverse=True)
    return board

class LBView(discord.ui.View):
    def __init__(self, users):
        super().__init__()
        self.users = users

    @discord.ui.select(placeholder="Choose Timeframe", options=[
        discord.SelectOption(label="Weekly (Min 3 Days)", value="3"),
        discord.SelectOption(label="All-Time (Min 10 Days)", value="10")
    ])
    async def select_lb(self, interaction, select):
        min_d = int(select.values[0])
        ranks = get_rankings(self.users, min_d)
        embed = discord.Embed(title="🏆 MapTap Leaderboard", color=0x3498DB)
        desc = ""
        for i, r in enumerate(ranks[:15], 1):
            desc += f"{i}. <@{r['uid']}> — **Avg: {r['avg']}** ({r['days']} days)\n"
        embed.description = desc or "No users qualify for this board yet."
        await interaction.response.edit_message(embed=embed)

# =====================================================
# THE BOT CORE
# =====================================================
class MapTapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.all())
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        self.scheduler.start()
        await self.tree.sync()

    @tasks.loop(minutes=1)
    async def scheduler(self):
        s, sha = load_settings()
        now = datetime.now(UK_TZ).strftime("%H:%M")
        if now == s["times"]["daily_post"] and s.get("daily_post_enabled"):
            ch = self.get_channel(s["channel_id"])
            if ch: await ch.send(f"🗺️ **MapTap is Live!** Good luck pilots: {MAPTAP_URL}")

client = MapTapBot()

@client.tree.command(name="rescan", description="Rebuild database from message history")
async def rescan(interaction: discord.Interaction):
    s, _ = load_settings()
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("Admin only.", ephemeral=True)
    
    await interaction.response.defer()
    scores, users = {}, {}
    # Scan the last 1000 messages in the designated channel
    async for msg in interaction.channel.history(limit=1000):
        match = SCORE_REGEX.search(msg.content or "")
        if match:
            uid = str(msg.author.id)
            score = int(match.group(1))
            date = msg.created_at.astimezone(UK_TZ).date().isoformat()
            
            day_data = scores.setdefault(date, {})
            if uid not in day_data: # Only first score per day
                day_data[uid] = {"score": score}
                u_stats = users.setdefault(uid, {"total_points": 0, "days_played": 0, "personal_best": {"score": 0}})
                u_stats["total_points"] += score
                u_stats["days_played"] += 1
                if score > u_stats["personal_best"]["score"]:
                    u_stats["personal_best"] = {"score": score, "date": date}

    github_save_json(SCORES_PATH, scores, github_load_json(SCORES_PATH, {})[1], "Rescan Scores")
    github_save_json(USERS_PATH, users, github_load_json(USERS_PATH, {})[1], "Rescan Users")
    await interaction.followup.send("✅ **Rescan Complete.** Database rebuilt from history.")

@client.tree.command(name="mymaptap", description="View your profile")
async def mymaptap(interaction: discord.Interaction, user: discord.Member = None):
    u_data, _ = github_load_json(USERS_PATH, {})
    target = user or interaction.user
    uid = str(target.id)
    if uid not in u_data:
        return await interaction.response.send_message("No data found for this user.")
    
    stats = u_data[uid]
    avg = round(stats["total_points"] / stats["days_played"])
    embed = discord.Embed(title=f"📊 Stats: {target.display_name}", color=0xF1C40F)
    embed.add_field(name="Average", value=f"**{avg}**")
    embed.add_field(name="Days Played", value=str(stats["days_played"]))
    embed.add_field(name="Best Score", value=str(stats["personal_best"]["score"]))
    await interaction.response.send_message(embed=embed)

@client.tree.command(name="leaderboard", description="View rankings")
async def lb(interaction: discord.Interaction):
    u_data, _ = github_load_json(USERS_PATH, {})
    await interaction.response.send_message("Select Leaderboard Type:", view=LBView(u_data))

@client.tree.command(name="maptapsettings", description="Open Admin Panel")
async def m_set(interaction: discord.Interaction):
    s, sha = load_settings()
    await interaction.response.send_message(embed=SettingsView(s, sha)._embed(), view=SettingsView(s, sha))

@client.event
async def on_message(msg):
    if msg.author.bot: return
    s, _ = load_settings()
    if msg.channel.id != s.get("channel_id"): return
    
    match = SCORE_REGEX.search(msg.content or "")
    if match:
        score = int(match.group(1))
        scores, s_sha = github_load_json(SCORES_PATH, {})
        users, u_sha = github_load_json(USERS_PATH, {})
        uid = str(msg.author.id)
        today = datetime.now(UK_TZ).date().isoformat()
        
        day_bucket = scores.setdefault(today, {})
        if uid not in day_bucket:
            u_stats = users.setdefault(uid, {"total_points": 0, "days_played": 0, "personal_best": {"score": 0}})
            u_stats["total_points"] += score
            u_stats["days_played"] += 1
            day_bucket[uid] = {"score": score}
            
            if score == 0 and s["roasts_enabled"]:
                await msg.reply(random.choice(ROAST_MESSAGES).format(uid=uid))
            elif score > u_stats["personal_best"]["score"]:
                u_stats["personal_best"] = {"score": score, "date": today}
                await msg.add_reaction("⭐")
            
            github_save_json(SCORES_PATH, scores, s_sha, "Daily Score Ingest")
            github_save_json(USERS_PATH, users, u_sha, "User Stat Update")
            await msg.add_reaction("🌏")

# === FLASK KEEP-ALIVE ===
app = Flask(__name__)
@app.route('/')
def home(): return "Bot is Online"
def run(): app.run(host="0.0.0.0", port=10000)

if __name__ == "__main__":
    Thread(target=run).start()
    client.run(TOKEN)
