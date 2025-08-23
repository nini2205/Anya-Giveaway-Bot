# Nitro Giveaway Bot — Notion backend (with robust error handling)
# ---------------------------------------------------------------
# pip install -U discord.py notion-client
# Env vars:
#   BOT_TOKEN
#   NOTION_TOKEN
#   NOTION_DB_ID         # 32-char, no hyphens
#   LOG_CHANNEL_ID       # optional, numeric channel id
#   LOG_WEBHOOK_URL      # optional, Discord webhook URL (used if channel not set)
#   OWNER_ID             # optional, admin override

import os
import io
import csv
import discord
from discord import app_commands
from discord.ext import commands
from notion_client import Client as Notion
import aiohttp

print("🚀 Booting Anya giveaway bot...")

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

def _parse_channel_id(s: str | None):
    if not s:
        return None
    s = s.strip()
    return int(s) if s.isdigit() else None

LOG_CHANNEL_ID = _parse_channel_id(os.getenv("LOG_CHANNEL_ID"))
LOG_WEBHOOK_URL = os.getenv("LOG_WEBHOOK_URL", "").strip()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")

# Debug env check
print("🔎 Env check:")
print("  BOT_TOKEN set?", bool(BOT_TOKEN))
print("  NOTION_TOKEN set?", bool(NOTION_TOKEN))
print("  NOTION_DB_ID set?", bool(NOTION_DB_ID))
print("  LOG_CHANNEL_ID:", LOG_CHANNEL_ID)
print("  LOG_WEBHOOK_URL set?", bool(LOG_WEBHOOK_URL))
print("  OWNER_ID:", OWNER_ID)

if not BOT_TOKEN:
    raise SystemExit("❌ Missing BOT_TOKEN.")
if not NOTION_TOKEN:
    raise SystemExit("❌ Missing NOTION_TOKEN.")
if not NOTION_DB_ID:
    raise SystemExit("❌ Missing NOTION_DB_ID (32-char, no hyphens).")

# ---------------- Notion Store ----------------
# (… your NotionStore class stays the same …)

# ---------------- Bot Core ----------------
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        await tree.sync()
        print("✅ Slash commands synced")
    except Exception as e:
        print("⚠️ Slash sync error:", e)

# (… rest of your bot commands …)

if __name__ == "__main__":
    print("▶️ Running bot.run() now...")
    bot.run(BOT_TOKEN)
