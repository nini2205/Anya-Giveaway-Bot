# Nitro Giveaway Bot — Notion backend (small giveaways)
# -----------------------------------------------------
# Requirements:
#   pip install -U discord.py notion-client
#
# Env vars:
#   BOT_TOKEN       = your Discord bot token
#   OWNER_ID        = (optional) Discord user ID for admin override
#   LOG_CHANNEL_ID  = (optional) private channel ID for audit logs
#   NOTION_TOKEN    = secret Notion integration token
#   NOTION_DB_ID    = the target Notion database ID (32-char, no hyphens)
#
# Notion database setup (properties - name them exactly):
#   - Title:        "User ID" (Title)
#   - "Link"        (URL)
#   - "Redeemed"    (Checkbox)
#   - "Redeemed At" (Date)
#   - "DM Message ID" (Rich text)
#   - "Confirmed"   (Checkbox)
#   - "Added By"    (Rich text, optional)

import os
import io
import csv
import discord
from discord import app_commands
from discord.ext import commands
from notion_client import Client as Notion

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0"))
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")

if not NOTION_TOKEN or not NOTION_DB_ID:
    raise SystemExit("Missing NOTION_TOKEN or NOTION_DB_ID env vars.")

# ---------- Notion Store ----------

class NotionStore:
    def __init__(self, token: str, database_id: str):
        self.notion = Notion(auth=token)
        self.db_id = database_id

    def _page_to_row(self, page):
        props = page["properties"]
        uid = props["User ID"]["title"][0]["plain_text"] if props["User ID"]["title"] else ""
        link = props.get("Link", {}).get("url", None)
        redeemed = props.get("Redeemed", {}).get("checkbox", False)
        confirmed = props.get("Confirmed", {}).get("checkbox", False)
        redeemed_at = props.get("Redeemed At", {}).get("date", {}).get("start", None)
        dm_message_id = None
        if "DM Message ID" in props and props["DM Message ID"].get("rich_text"):
            dm_message_id = props["DM Message ID"]["rich_text"][0]["plain_text"]
        return {
            "page_id": page["id"],
            "user_id": uid,
            "link": link,
            "redeemed": redeemed,
            "redeemed_at": redeemed_at,
            "dm_message_id": dm_message_id,
            "confirmed": confirmed,
        }

    def get_page_by_user(self, user_id: int):
        res = self.notion.databases.query(
            database_id=self.db_id,
            filter={"property": "User ID", "title": {"equals": str(user_id)}},
            page_size=1,
        )
        return res.get("results", [None])[0]

    def upsert_winner(self, user_id: int, link: str, added_by: int | None = None):
        page = self.get_page_by_user(user_id)
        if page:
            props = {"Link": {"url": link}}
            if added_by:
                props["Added By"] = {"rich_text": [{"text": {"content": str(added_by)}}]}
            self.notion.pages.update(page_id=page["id"], properties=props)
        else:
            props = {
                "User ID": {"title": [{"text": {"content": str(user_id)}}]},
                "Link": {"url": link},
                "Redeemed": {"checkbox": False},
                "Confirmed": {"checkbox": False},
            }
            if added_by:
                props["Added By"] = {"rich_text": [{"text": {"content": str(added_by)}}]}
            self.notion.pages.create(parent={"database_id": self.db_id}, properties=props)

    def get_winner(self, user_id: int):
        page = self.get_page_by_user(user_id)
        return self._page_to_row(page) if page else None

    def mark_redeemed(self, user_id: int, dm_message_id: str | None = None):
        page = self.get_page_by_user(user_id)
        if not page: return
        props = {
            "Redeemed": {"checkbox": True},
            "Redeemed At": {"date": {"start": discord.utils.utcnow().isoformat()}},
        }
        if dm_message_id:
            props["DM Message ID"] = {"rich_text": [{"text": {"content": str(dm_message_id)}}]}
        self.notion.pages.update(page_id=page["id"], properties=props)

    def mark_confirmed(self, user_id: int):
        page = self.get_page_by_user(user_id)
        if not page: return
        self.notion.pages.update(page_id=page["id"], properties={"Confirmed": {"checkbox": True}})

    def list_remaining(self, limit=50):
        res = self.notion.databases.query(
            database_id=self.db_id,
            filter={"property": "Redeemed", "checkbox": {"equals": False}},
            page_size=limit,
        )
        return [self._page_to_row(p)["user_id"] for p in res.get("results", [])]

    def export_rows(self, limit=1000):
        res = self.notion.databases.query(database_id=self.db_id, page_size=min(limit, 100))
        rows = [self._page_to_row(p) for p in res.get("results", [])]
        return rows

store = NotionStore(NOTION_TOKEN, NOTION_DB_ID)

# ---------- Bot core ----------

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

def is_admin(interaction: discord.Interaction) -> bool:
    if OWNER_ID and interaction.user.id == OWNER_ID: return True
    if interaction.guild and interaction.user.guild_permissions.manage_guild: return True
    return False

async def log_audit(text: str):
    if not LOG_CHANNEL_ID: return
    ch = bot.get_channel(LOG_CHANNEL_ID)
    if ch:
        try: await ch.send(text)
        except: pass

# ---------- User DM commands ----------

@tree.command(name="claim", description="Claim your giveaway reward (DM only).")
async def claim(interaction: discord.Interaction):
    if interaction.guild is not None:
        return await interaction.response.send_message("Please DM me to use `/claim`.", ephemeral=True)

    row = store.get_winner(interaction.user.id)
    if not row:
        return await interaction.response.send_message("I couldn't find a prize for your account.", ephemeral=True)
    if row["redeemed"]:
        return await interaction.response.send_message("Your prize is already marked redeemed.", ephemeral=True)

    try:
        await interaction.response.send_message("🎉 Congrats! Here is your Nitro gift link:")
        msg = await interaction.followup.send(row["link"])
        store.mark_redeemed(interaction.user.id, getattr(msg, "id", None))
        await log_audit(f"✅ Sent Nitro link to {interaction.user} ({interaction.user.id})")
    except:
        await interaction.followup.send("Something went wrong sending your link.", ephemeral=True)

@tree.command(name="confirm", description="Confirm you redeemed your gift (DM only).")
async def confirm(interaction: discord.Interaction):
    if interaction.guild is not None:
        return await interaction.response.send_message("Please DM me to use this.", ephemeral=True)

    row = store.get_winner(interaction.user.id)
    if not row:
        return await interaction.response.send_message("You are not in my winners list.", ephemeral=True)
    if not row["redeemed"]:
        return await interaction.response.send_message("You need to run `/claim` first.", ephemeral=True)
    if row["confirmed"]:
        return await interaction.response.send_message("Already confirmed. Thank you!", ephemeral=True)

    store.mark_confirmed(interaction.user.id)
    await interaction.response.send_message("✅ Thanks for confirming!", ephemeral=True)
    await log_audit(f"📩 {interaction.user} ({interaction.user.id}) confirmed receipt.")

# ---------- Admin commands ----------

@tree.command(name="add_winner", description="(Admin) Add one winner + link.")
async def add_winner(interaction: discord.Interaction, user: discord.User, link: str):
    if not is_admin(interaction):
        return await interaction.response.send_message("No permission.", ephemeral=True)
    store.upsert_winner(user.id, link, interaction.user.id)
    await interaction.response.send_message(f"Added {user.mention}.", ephemeral=True)

@tree.command(name="list_remaining", description="(Admin) Show unclaimed winners.")
async def list_remaining(interaction: discord.Interaction):
    if not is_admin(interaction):
        return await interaction.response.send_message("No permission.", ephemeral=True)
    remaining = store.list_remaining()
    if not remaining:
        return await interaction.response.send_message("All winners redeemed 🎉", ephemeral=True)
    await interaction.response.send_message(f"Unredeemed: {', '.join(remaining)}", ephemeral=True)

@tree.command(name="status", description="(Admin) Check a winner's status.")
async def status(interaction: discord.Interaction, user: discord.User):
    if not is_admin(interaction):
        return await interaction.response.send_message("No permission.", ephemeral=True)
    row = store.get_winner(user.id)
    if not row:
        return await interaction.response.send_message("No record found.", ephemeral=True)
    state = "✅ Redeemed" if row["redeemed"] else "❌ Not redeemed"
    conf = "✅ Confirmed" if row["confirmed"] else "❌ Not confirmed"
    await interaction.response.send_message(
        f"**{user}**\nStatus: {state} | Confirmation: {conf}\nLink: ||{row['link']}||",
        ephemeral=True
    )

@tree.command(name="export_csv", description="(Admin) Export winners.")
async def export_csv(interaction: discord.Interaction):
    if not is_admin(interaction):
        return await interaction.response.send_message("No permission.", ephemeral=True)
    rows = store.export_rows()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["user_id","link","redeemed","redeemed_at","confirmed","dm_message_id"])
    for r in rows:
        writer.writerow([r["user_id"], r["link"], int(r["redeemed"]), r["redeemed_at"] or "", int(r["confirmed"]), r["dm_message_id"] or ""])
    buf.seek(0)
    await interaction.response.send_message(
        "Here’s your export.",
        file=discord.File(fp=io.BytesIO(buf.getvalue().encode("utf-8")), filename="winners.csv"),
        ephemeral=True
    )

# ---------- Events ----------

@bot.event
async def on_ready():
    try:
        await tree.sync()
    except Exception as e:
        print("Slash sync error:", e)
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("Missing BOT_TOKEN env var.")
    bot.run(BOT_TOKEN)
