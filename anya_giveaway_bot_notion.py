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

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

print("Booting Anya bot…")
print("Env check:",
      "BOT_TOKEN set" if bool(BOT_TOKEN) else "BOT_TOKEN MISSING",
      "NOTION_TOKEN set" if bool(NOTION_TOKEN) else "NOTION_TOKEN MISSING",
      "NOTION_DB_ID set" if bool(NOTION_DB_ID) else "NOTION_DB_ID MISSING",
)

def _parse_channel_id(s: str | None):
    if not s:
        return None
    s = s.strip()
    return int(s) if s.isdigit() else None

LOG_CHANNEL_ID = _parse_channel_id(os.getenv("LOG_CHANNEL_ID"))
LOG_WEBHOOK_URL = os.getenv("LOG_WEBHOOK_URL", "").strip()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")

if not BOT_TOKEN:
    raise SystemExit("❌ Missing BOT_TOKEN.")
if not NOTION_TOKEN:
    raise SystemExit("❌ Missing NOTION_TOKEN.")
if not NOTION_DB_ID:
    raise SystemExit("❌ Missing NOTION_DB_ID (32-char, no hyphens).")

# ---------------- Notion Store ----------------

class NotionStore:
    """
    Safer Notion adapter. Provides helpful errors if the DB schema is wrong.
    Expected properties:
      - Title column named exactly 'User ID'  (Title)
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

    # ---------- helpers ----------
    @staticmethod
    def _get_title_text(props: dict, title_name: str) -> str:
        """
        Return the Title property's first text; raise a friendly error if missing/misnamed.
        """
        if title_name not in props:
            raise KeyError(
                f"Notion schema error: Title property '{title_name}' not found. "
                "Rename your leftmost Title column (📄) to 'User ID'."
            )
        title = props[title_name].get("title", [])
        if not title:
            # Return empty string; caller can decide how to handle.
            return ""
        # Defensive parsing
        try:
            return title[0].get("plain_text", "") or title[0]["text"]["content"]
        except Exception:
            return ""

    def _page_to_row(self, page: dict) -> dict:
        props = page["properties"]
        try:
            uid = self._get_title_text(props, "User ID")
        except KeyError as e:
            # Surface a readable error in logs and keep going
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

    # ---------- CRUD ----------
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
        """
        Create or update a row for user_id.
        Will raise helpful errors if the 'User ID' Title column is misconfigured.
        """
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
            # Likely misnamed Title column
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

