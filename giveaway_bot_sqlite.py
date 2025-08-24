# giveaway_bot_sqlite.py
# ---------------------------------------------------------------
# A single-file Discord giveaway bot using SQLite (async, safe for concurrency)
# Requirements:
#   pip install -U discord.py aiosqlite aiohttp
#
# Env:
#   BOT_TOKEN (required)
#   GUILD_ID, OWNER_ID, LOG_CHANNEL_ID, LOG_WEBHOOK_URL (optional)
#   DB_PATH (optional, default: giveaway.sqlite3) — on Railway use /mnt/data/giveaway.sqlite3
# ---------------------------------------------------------------

from __future__ import annotations
import os, sys, csv, asyncio
from pathlib import Path
from typing import Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands
import aiosqlite

# ------------------------------ Config ------------------------------
DB_PATH = Path(os.getenv("DB_PATH", "giveaway.sqlite3"))
BOT_TOKEN = os.getenv("BOT_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0"))
LOG_WEBHOOK_URL = os.getenv("LOG_WEBHOOK_URL")

# ------------------------------ Schema ------------------------------
SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
CREATE TABLE IF NOT EXISTS gift_links (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    code            TEXT UNIQUE NOT NULL,
    status          TEXT NOT NULL DEFAULT 'new',      -- 'new' | 'claimed' | 'disabled'
    claimed_by      TEXT,
    claimed_at      TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    notes           TEXT
);
CREATE TABLE IF NOT EXISTS winners (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         TEXT NOT NULL UNIQUE,
    username        TEXT,
    allow_multiple  INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS audit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL DEFAULT (datetime('now')),
    actor_user_id   TEXT,
    action          TEXT NOT NULL,
    metadata        TEXT
);
"""

# ------------------------------ DB Helpers ------------------------------
async def open_db() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(DB_PATH)
    await conn.executescript(SCHEMA)
    await conn.commit()
    return conn

async def add_links(conn: aiosqlite.Connection, codes: Sequence[str], actor_user_id: Optional[str] = None) -> int:
    added = 0
    await conn.execute("BEGIN")
    try:
        for c in codes:
            c = c.strip()
            if not c:
                continue
            try:
                await conn.execute("INSERT INTO gift_links (code, status) VALUES (?, 'new')", (c,))
                added += 1
            except aiosqlite.IntegrityError:
                pass  # duplicate
        await conn.execute(
            "INSERT INTO audit_log (actor_user_id, action, metadata) VALUES (?, 'ADD_LINK', ?)",
            (actor_user_id, f"count={added}")
        )
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise
    return added

async def add_winner(conn: aiosqlite.Connection, user_id: str, username: Optional[str] = None, allow_multiple: bool = False) -> bool:
    try:
        await conn.execute(
            "INSERT INTO winners (user_id, username, allow_multiple) VALUES (?,?,?)",
            (user_id, username, 1 if allow_multiple else 0)
        )
        await conn.execute(
            "INSERT INTO audit_log (actor_user_id, action, metadata) VALUES (?, 'ADD_WINNER', ?)",
            (user_id, f"username={username},allow_multiple={allow_multiple}")
        )
        await conn.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def is_winner(conn: aiosqlite.Connection, user_id: str) -> tuple[bool, bool]:
    async with conn.execute("SELECT allow_multiple FROM winners WHERE user_id = ?", (user_id,)) as cur:
        row = await cur.fetchone()
        if row is None:
            return (False, False)
        return (True, bool(row[0]))

async def user_claim_count(conn: aiosqlite.Connection, user_id: str) -> int:
    async with conn.execute("SELECT COUNT(*) FROM gift_links WHERE claimed_by = ? AND status = 'claimed'", (user_id,)) as cur:
        (cnt,) = await cur.fetchone()
        return int(cnt)

async def claim_one_link(conn: aiosqlite.Connection, user_id: str) -> Optional[str]:
    # Check eligibility
    is_win, allow_mult = await is_winner(conn, user_id)
    if not is_win:
        return None
    if not allow_mult and await user_claim_count(conn, user_id) > 0:
        return None

    # Atomic claim
    await conn.execute("BEGIN IMMEDIATE")
    try:
        async with conn.execute("SELECT id, code FROM gift_links WHERE status='new' ORDER BY id ASC LIMIT 1") as cur:
            row = await cur.fetchone()
            if row is None:
                await conn.execute("ROLLBACK")
                return None
            link_id, code = row

        result = await conn.execute(
            "UPDATE gift_links SET status='claimed', claimed_by=?, claimed_at=datetime('now') "
            "WHERE id=? AND status='new'",
            (user_id, link_id)
        )
        await conn.commit()
        if result.rowcount == 1:
            await conn.execute(
                "INSERT INTO audit_log (actor_user_id, action, metadata) VALUES (?, 'CLAIM', ?)",
                (user_id, f"link_id={link_id}")
            )
            await conn.commit()
            return code
        return None
    except Exception:
        await conn.rollback()
        raise

async def disable_link(conn: aiosqlite.Connection, code: str, actor_user_id: Optional[str] = None) -> bool:
    res = await conn.execute("UPDATE gift_links SET status='disabled' WHERE code = ?", (code,))
    await conn.commit()
    ok = (res.rowcount > 0)
    if ok:
        await conn.execute(
            "INSERT INTO audit_log (actor_user_id, action, metadata) VALUES (?, 'DISABLE_LINK', ?)",
            (actor_user_id, f"code={code}")
        )
        await conn.commit()
    return ok

async def stats(conn: aiosqlite.Connection) -> dict:
    async def one(q: str) -> int:
        async with conn.execute(q) as cur:
            (n,) = await cur.fetchone()
            return int(n)
    return {
        "total_links": await one("SELECT COUNT(*) FROM gift_links"),
        "available_links": await one("SELECT COUNT(*) FROM gift_links WHERE status='new'"),
        "claimed_links": await one("SELECT COUNT(*) FROM gift_links WHERE status='claimed'"),
        "disabled_links": await one("SELECT COUNT(*) FROM gift_links WHERE status='disabled'"),
        "winners": await one("SELECT COUNT(*) FROM winners"),
    }

# ------------------------------ Logging Helpers ------------------------------
async def log_event(bot: commands.Bot, message: str):
    if LOG_CHANNEL_ID:
        ch = bot.get_channel(LOG_CHANNEL_ID)
        if ch is not None:
            try:
                await ch.send(message); return
            except Exception:
                pass
    if LOG_WEBHOOK_URL:
        try:
            import aiohttp
            async with aiohttp.ClientSession() as sess:
                await sess.post(LOG_WEBHOOK_URL, json={"content": message}); return
        except Exception:
            pass
    print("[LOG]", message)

# ------------------------------ Discord Bot ------------------------------
intents = discord.Intents.none()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

db_conn: Optional[aiosqlite.Connection] = None

# Admin gate: allow server admins or OWNER_ID
def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if OWNER_ID and interaction.user.id == OWNER_ID:
            return True
        perms = getattr(interaction.user, "guild_permissions", None)
        return bool(perms and perms.administrator)
    return app_commands.check(predicate)

@bot.event
async def on_ready():
    global db_conn
    db_conn = await open_db()
    try:
        if GUILD_ID:
            guild_obj = discord.Object(id=GUILD_ID)
            # Purge old guild commands (e.g., legacy /add_winner signature) and
            # re-publish current globals to the guild for instant updates.
            tree.clear_commands(guild=guild_obj)
            tree.copy_global_to(guild=guild_obj)
            await tree.sync(guild=guild_obj)   # instant in that guild
        else:
            await tree.sync()                   # global sync (can take time)
    except Exception as e:
        await log_event(bot, f"Slash sync error: {e}")
    await log_event(bot, f"✅ Logged in as {bot.user} | DB: {DB_PATH}")

# -------- User: /claim --------
@tree.command(name="claim", description="Claim your Nitro link (if you're eligible).")
async def claim_cmd(interaction: discord.Interaction):
    assert db_conn is not None
    await interaction.response.defer(ephemeral=True)
    code = await claim_one_link(db_conn, str(interaction.user.id))
    if code is None:
        await interaction.followup.send("No link available or you're not eligible / already claimed.", ephemeral=True)
        return
    try:
        await interaction.user.send(f"🎁 Here is your Nitro gift link:\n{code}\n\nEnjoy!")
        await interaction.followup.send("Sent! Check your DMs. ✅", ephemeral=True)
        await log_event(bot, f"CLAIM: {interaction.user} ({interaction.user.id})")
    except discord.Forbidden:
        await interaction.followup.send("I couldn't DM you. Please open your DMs and run /claim again.", ephemeral=True)

# -------- Admin: /add_links --------
@tree.command(name="add_links", description="Admin: add one or more gift links (comma or newline separated).")
@admin_only()
async def add_links_cmd(interaction: discord.Interaction, links: str):
    assert db_conn is not None
    await interaction.response.defer(ephemeral=True)
    codes = [p.strip() for p in links.replace(",", "\n").splitlines() if p.strip()]
    added = await add_links(db_conn, codes, actor_user_id=str(interaction.user.id))
    await interaction.followup.send(f"Added {added} new link(s).", ephemeral=True)
    await log_event(bot, f"ADD_LINKS by {interaction.user} count={added}")

# -------- Admin: /add_links_file --------
@bot.tree.command(name="add_links_file", description="Admin: upload a text file with one code per line.")
@admin_only()
async def add_links_file(interaction: discord.Interaction, file: discord.Attachment):
    assert db_conn is not None
    await interaction.response.defer(ephemeral=True)
    data = await file.read()
    codes = [ln.strip() for ln in data.decode("utf-8", errors="ignore").splitlines() if ln.strip()]
    added = await add_links(db_conn, codes, actor_user_id=str(interaction.user.id))
    await interaction.followup.send(f"Added {added} link(s) from file.", ephemeral=True)
    await log_event(bot, f"ADD_LINKS_FILE by {interaction.user} count={added} file={file.filename}")

# -------- Admin: new /add_winner_v2 (user picker) --------
@bot.tree.command(name="add_winner_v2", description="Admin: register a winner (optionally allow multiple).")
@admin_only()
async def add_winner_v2(interaction: discord.Interaction, user: discord.User, allow_multiple: bool = False):
    assert db_conn is not None
    ok = await add_winner(db_conn, user_id=str(user.id), username=str(user), allow_multiple=allow_multiple)
    await interaction.response.send_message("Winner added." if ok else "Winner already exists.", ephemeral=True)
    await log_event(bot, f"ADD_WINNER by {interaction.user} target={user} ({user.id}) allow_multiple={allow_multiple}")

# -------- Admin: /disable_link --------
@tree.command(name="disable_link", description="Admin: disable a specific gift link code.")
@admin_only()
async def disable_link_cmd(interaction: discord.Interaction, code: str):
    assert db_conn is not None
    ok = await disable_link(db_conn, code, actor_user_id=str(interaction.user.id))
    await interaction.response.send_message("Disabled." if ok else "Code not found.", ephemeral=True)
    await log_event(bot, f"DISABLE_LINK by {interaction.user} code={code} ok={ok}")

# -------- Admin: /stats --------
@tree.command(name="stats", description="Admin: show giveaway stats.")
@admin_only()
async def stats_cmd(interaction: discord.Interaction):
    assert db_conn is not None
    s = await stats(db_conn)
    msg = (
        f"**Links**\n"
        f"- Total: {s['total_links']}\n"
        f"- Available: {s['available_links']}\n"
        f"- Claimed: {s['claimed_links']}\n"
        f"- Disabled: {s['disabled_links']}\n\n"
        f"**Winners**: {s['winners']}"
    )
    await interaction.response.send_message(msg, ephemeral=True)

# ------------------------------ CSV Import (CLI) ------------------------------
async def import_links_csv(path: str):
    conn = await open_db()
    codes: list[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row: continue
            codes.append(row[0])
    added = await add_links(conn, codes, actor_user_id="importer")
    print(f"Imported {added} links from {path}.")

async def import_winners_csv(path: str):
    conn = await open_db()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cnt = 0
        async with conn.execute("BEGIN"):
            for row in reader:
                user_id = (row.get("user_id") or "").strip()
                if not user_id: continue
                username = (row.get("username") or "").strip() or None
                allow_multiple = str(row.get("allow_multiple", "0")).strip().lower() in ("1","true","yes")
                await add_winner(conn, user_id, username, allow_multiple)
                cnt += 1
            await conn.commit()
    print(f"Imported ~{cnt} winners from {path}.")

# ------------------------------ Entrypoint ------------------------------
def main(argv: list[str]):
    # CLI:
    if len(argv) >= 3 and argv[1] == "--import-links":
        asyncio.run(import_links_csv(argv[2])); return
    if len(argv) >= 3 and argv[1] == "--import-winners":
        asyncio.run(import_winners_csv(argv[2])); return

    if not BOT_TOKEN:
        print("ERROR: BOT_TOKEN is not set."); sys.exit(1)

    # Normal run: discord.py manages its own event loop
    bot.run(BOT_TOKEN)

if __name__ == "__main__":
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
