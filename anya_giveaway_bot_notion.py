# Anya Nitro Giveaway Bot — Notion backend (robust & DM-ready)
# ------------------------------------------------------------
# Requirements:
#   pip install -U discord.py notion-client aiohttp
#
# Environment variables:
#   BOT_TOKEN          (required)
#   NOTION_TOKEN       (required)
#   NOTION_DB_ID       (required; 32 chars, no hyphens)
#   TEST_GUILD_ID      (optional; a server ID for instant command sync)
#   LOG_CHANNEL_ID     (optional; numeric channel ID for audit logs)
#   LOG_WEBHOOK_URL    (optional; Discord webhook URL for audit logs)
#   OWNER_ID           (optional; your Discord user ID for admin override)

import os
import io
import csv
import discord
from discord import app_commands
from discord.ext import commands
from notion_client import Client as Notion
import aiohttp

# ---------- Startup debug ----------
print("🚀 Booting Anya giveaway bot...")

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TEST_GUILD_ID = int(os.getenv("TEST_GUILD_ID", "0"))  # instant guild sync (optional)

def _parse_channel_id(s: str | None):
    if not s:
        return None
    s = s.strip()
    return int(s) if s.isdigit() else None

LOG_CHANNEL_ID = _parse_channel_id(os.getenv("LOG_CHANNEL_ID"))
LOG_WEBHOOK_URL = os.getenv("LOG_WEBHOOK_URL", "").strip()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")

print("🔎 Env check:")
print("  BOT_TOKEN set?", bool(BOT_TOKEN))
print("  NOTION_TOKEN set?", bool(NOTION_TOKEN))
print("  NOTION_DB_ID set?", bool(NOTION_DB_ID))
print("  TEST_GUILD_ID:", TEST_GUILD_ID)
print("  LOG_CHANNEL_ID:", LOG_CHANNEL_ID)
print("  LOG_WEBHOOK_URL set?", bool(LOG_WEBHOOK_URL))
print("  OWNER_ID:", OWNER_ID)

if not BOT_TOKEN:
    raise SystemExit("❌ Missing BOT_TOKEN.")
if not NOTION_TOKEN:
    raise SystemExit("❌ Missing NOTION_TOKEN.")
if not NOTION_DB_ID:
    raise SystemExit("❌ Missing NOTION_DB_ID (32-char, no hyphens).")

# ---------- Notion Store ----------
class NotionStore:
    """
    Safer Notion adapter. Expected properties:
      - Title (leftmost) named exactly 'User ID'
      - 'Link' (URL)
      - 'Redeemed' (Checkbox)
      - 'Redeemed At' (Date)
      - 'DM Message ID' (Rich text)
      - 'Confirmed' (Checkbox)
      - 'Added By' (Rich text) [optional]
    """
    def __init__(self, token: str, database_id: str):
        self.notion = Notion(auth=token)
        self.db_id = database_id

    @staticmethod
    def _get_title_text(props: dict, title_name: str) -> str:
        if title_name not in props:
            raise KeyError(
                f"Notion schema error: Title property '{title_name}' not found. "
                "Rename your leftmost Title column (📄) to 'User ID'."
            )
        title = props[title_name].get("title", [])
        if not title:
            return ""
        try:
            return title[0].get("plain_text", "") or title[0]["text"]["content"]
        except Exception:
            return ""

    def _page_to_row(self, page: dict) -> dict:
        props = page["properties"]
        try:
            uid = self._get_title_text(props, "User ID")
        except KeyError as e:
            print(f"[Notion ERROR] {e}")
            uid = "<MISSING TITLE>"

        link = props.get("Link", {}).get("url")
        redeemed = props.get("Redeemed", {}).get("checkbox", False)
        confirmed = props.get("Confirmed", {}).get("checkbox", False)
        redeemed_at = props.get("Redeemed At", {}).get("date", {}).get("start")
        dm_message_id = None
        if "DM Message ID" in props and props["DM Message ID"].get("rich_text"):
            dm_message_id = props["DM Message ID"]["rich_text"][0].get("plain_text")

        return {
            "page_id": page["id"],
            "user_id": uid,
            "link": link,
            "redeemed": redeemed,
            "redeemed_at": redeemed_at,
            "dm_message_id": dm_message_id,
            "confirmed": confirmed,
        }

    def get_page_by_user(self, user_id: int | str):
        try:
            res = self.notion.databases.query(
                database_id=self.db_id,
                filter={"property": "User ID", "title": {"equals": str(user_id)}},
                page_size=1,
            )
            results = res.get("results", [])
            return results[0] if results else None
        except Exception as e:
            print(f"[Notion ERROR] Query failed — check DB access and schema: {e}")
            raise

    def upsert_winner(self, user_id: int, link: str, added_by: int | None = None):
        props = {
            "User ID": {"title": [{"text": {"content": str(user_id)}}]},
            "Link": {"url": link},
            "Redeemed": {"checkbox": False},
            "Confirmed": {"checkbox": False},
        }
        if added_by:
            props["Added By"] = {"rich_text": [{"text": {"content": str(added_by)}}]}

        try:
            page = self.get_page_by_user(user_id)
            if page:
                self.notion.pages.update(page_id=page["id"], properties={"Link": {"url": link}})
                return page["id"]
            created = self.notion.pages.create(parent={"database_id": self.db_id}, properties=props)
            return created["id"]
        except KeyError as e:
            print(f"[Notion ERROR] {e}")
            raise
        except Exception as e:
            print(f"[Notion ERROR] Failed to upsert winner {user_id}: {e}")
            raise

    def get_winner(self, user_id: int):
        page = self.get_page_by_user(user_id)
        return self._page_to_row(page) if page else None

    def mark_redeemed(self, user_id: int, dm_message_id: str | None = None):
        page = self.get_page_by_user(user_id)
        if not page:
            print(f"[Notion WARN] mark_redeemed: user {user_id} not found.")
            return
        props = {
            "Redeemed": {"checkbox": True},
            "Redeemed At": {"date": {"start": discord.utils.utcnow().isoformat()}},
        }
        if dm_message_id:
            props["DM Message ID"] = {"rich_text": [{"text": {"content": str(dm_message_id)}}]}
        try:
            self.notion.pages.update(page_id=page["id"], properties=props)
        except Exception as e:
            print(f"[Notion ERROR] mark_redeemed failed for {user_id}: {e}")
            raise

    def mark_confirmed(self, user_id: int):
        page = self.get_page_by_user(user_id)
        if not page:
            print(f"[Notion WARN] mark_confirmed: user {user_id} not found.")
            return
        try:
            self.notion.pages.update(page_id=page["id"], properties={"Confirmed": {"checkbox": True}})
        except Exception as e:
            print(f"[Notion ERROR] mark_confirmed failed for {user_id}: {e}")
            raise

    def list_remaining(self, limit=50):
        try:
            res = self.notion.databases.query(
                database_id=self.db_id,
                filter={"property": "Redeemed", "checkbox": {"equals": False}},
                page_size=limit,
            )
            rows = [self._page_to_row(p) for p in res.get("results", [])]
            return [r["user_id"] for r in rows]
        except Exception as e:
            print(f"[Notion ERROR] list_remaining failed: {e}")
            return []

    def export_rows(self, limit=1000):
        rows = []
        try:
            res = self.notion.databases.query(database_id=self.db_id, page_size=min(limit, 100))
            rows.extend(self._page_to_row(p) for p in res.get("results", []))
            while res.get("has_more") and len(rows) < limit:
                res = self.notion.databases.query(
                    database_id=self.db_id,
                    start_cursor=res["next_cursor"],
                    page_size=min(limit - len(rows), 100),
                )
                rows.extend(self._page_to_row(p) for p in res.get("results", []))
        except Exception as e:
            print(f"[Notion ERROR] export_rows failed: {e}")
        return rows

store = NotionStore(NOTION_TOKEN, NOTION_DB_ID)

# ---------- Bot Core ----------
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

def is_admin(interaction: discord.Interaction) -> bool:
    if OWNER_ID and interaction.user.id == OWNER_ID:
        return True
    return bool(interaction.guild and interaction.user.guild_permissions.manage_guild)

async def log_audit(text: str):
    # Prefer channel logging when available
    if LOG_CHANNEL_ID:
        ch = bot.get_channel(LOG_CHANNEL_ID)
        if ch:
            try:
                await ch.send(text)
                return
            except Exception:
                pass
    # Fallback to webhook
    if LOG_WEBHOOK_URL:
        try:
            async with aiohttp.ClientSession() as sess:
                await sess.post(LOG_WEBHOOK_URL, json={"content": text}, timeout=10)
        except Exception:
            pass

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    try:
        if TEST_GUILD_ID:
            guild = discord.Object(id=TEST_GUILD_ID)
            tree.copy_global_to(guild=guild)
            await tree.sync(guild=guild)
            print(f"✅ Slash commands synced to guild {TEST_GUILD_ID} (instant)")
        # Always also sync globally so commands work in DMs / other servers
        await tree.sync()
        print("✅ Slash commands synced globally (DMs + all servers)")
    except Exception as e:
        print("⚠️ Slash sync error:", e)

# ---------- User DM Commands ----------
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@tree.command(name="claim", description="Claim your giveaway reward (DM only).")
async def claim(interaction: discord.Interaction):
    if interaction.guild is not None:
        return await interaction.response.send_message("Please DM me to use `/claim`.", ephemeral=True)

    row = store.get_winner(interaction.user.id)
    if not row:
        return await interaction.response.send_message(
            "I couldn't find a prize for your account. (Admin note: check Notion schema & permissions.)",
            ephemeral=True
        )
    if row["redeemed"]:
        return await interaction.response.send_message(
            "Your prize is already marked as redeemed. If you didn’t receive it, contact a moderator.",
            ephemeral=True
        )
    if not row["link"]:
        return await interaction.response.send_message(
            "A link isn’t stored for your record yet. Please contact a moderator.",
            ephemeral=True
        )

    try:
        await interaction.response.send_message("🎉 Congrats! Here is your Nitro gift link:")
        msg = await interaction.followup.send(row["link"])
        store.mark_redeemed(interaction.user.id, getattr(msg, "id", None))
        await log_audit(f"✅ Sent Nitro link to {interaction.user} ({interaction.user.id}) and marked redeemed.")
    except Exception as e:
        await interaction.followup.send("Something went wrong sending your link. Please try again or ping a moderator.", ephemeral=True)
        print(f"[CLAIM ERROR] {e}")

@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@tree.command(name="confirm", description="Confirm you redeemed your gift (DM only).")
async def confirm(interaction: discord.Interaction):
    if interaction.guild is not None:
        return await interaction.response.send_message("Please DM me to use this.", ephemeral=True)

    row = store.get_winner(interaction.user.id)
    if not row:
        return await interaction.response.send_message("You are not in my winners list.", ephemeral=True)
    if not row["redeemed"]:
        return await interaction.response.send_message("I haven’t marked your prize as redeemed yet. Please run `/claim` first.", ephemeral=True)
    if row["confirmed"]:
        return await interaction.response.send_message("Already marked as confirmed. Thank you!", ephemeral=True)

    try:
        store.mark_confirmed(interaction.user.id)
        await interaction.response.send_message("✅ Thanks! Your redemption has been confirmed.", ephemeral=True)
        await log_audit(f"📩 {interaction.user} ({interaction.user.id}) confirmed receipt of their gift.")
    except Exception as e:
        print(f"[CONFIRM ERROR] {e}")
        await interaction.response.send_message("Could not record your confirmation. Please contact a moderator.", ephemeral=True)

# ---------- Admin (Guild) Commands ----------
@tree.command(name="add_winner", description="(Admin) Add one winner + link.")
@app_commands.describe(user="The winner", link="One-time Nitro gift link")
async def add_winner(interaction: discord.Interaction, user: discord.User, link: str):
    if not is_admin(interaction):
        return await interaction.response.send_message("You don’t have permission.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        store.upsert_winner(user.id, link, interaction.user.id)
        await interaction.followup.send(f"Added {user.mention}. Have them DM `/claim`.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(
            "Failed to write to Notion. Ensure the leftmost Title column is named **User ID** and the integration has **Edit** access.",
            ephemeral=True
        )
        print(f"[ADD_WINNER ERROR] {e}")

@tree.command(name="list_remaining", description="(Admin) Show unclaimed winners.")
async def list_remaining(interaction: discord.Interaction):
    if not is_admin(interaction):
        return await interaction.response.send_message("You don’t have permission.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)
    remaining = store.list_remaining()
    if not remaining:
        return await interaction.followup.send("All winners redeemed 🎉", ephemeral=True)
    await interaction.followup.send(f"Unredeemed: {', '.join(remaining[:25])}", ephemeral=True)

@tree.command(name="status", description="(Admin) Check a winner's status.")
async def status(interaction: discord.Interaction, user: discord.User):
    if not is_admin(interaction):
        return await interaction.response.send_message("You don’t have permission.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)
    row = store.get_winner(user.id)
    if not row:
        return await interaction.followup.send(
            "No record found. (If you just added them, wait a few seconds and try again.)",
            ephemeral=True
        )
    state = "✅ Redeemed" if row["redeemed"] else "❌ Not redeemed"
    conf = "✅ Confirmed" if row["confirmed"] else "❌ Not confirmed"
    redeemed_at = row["redeemed_at"] or "—"
    dm_message_id = row["dm_message_id"] or "—"
    link = row["link"] or "—"
    await interaction.followup.send(
        f"**{user}**\nStatus: {state} | Confirmation: {conf}\nRedeemed at: {redeemed_at}\nDM message id: {dm_message_id}\nStored link: ||{link}||",
        ephemeral=True
    )

@tree.command(name="export_csv", description="(Admin) Export winners.")
async def export_csv(interaction: discord.Interaction):
    if not is_admin(interaction):
        return await interaction.response.send_message("You don’t have permission.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)
    rows = store.export_rows()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["user_id","link","redeemed","redeemed_at","confirmed","dm_message_id"])
    for r in rows:
        w.writerow([r["user_id"], r["link"] or "", int(r["redeemed"]), r["redeemed_at"] or "", int(r["confirmed"]), r["dm_message_id"] or ""])
    buf.seek(0)
    await interaction.followup.send(
        "Here’s your export.",
        file=discord.File(fp=io.BytesIO(buf.getvalue().encode("utf-8")), filename="winners.csv"),
        ephemeral=True
    )

# ---------- Global command error handler ----------
@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "Something went wrong processing that command. Check logs for details.",
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                "Something went wrong processing that command. Check logs for details.",
                ephemeral=True
            )
    except Exception:
        pass
    print("[APP CMD ERROR]", repr(error))

# ---------- Runner ----------
if __name__ == "__main__":
    print("▶️ Running bot.run() now...")
    bot.run(BOT_TOKEN)
