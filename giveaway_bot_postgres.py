# giveaway_bot_postgres.py
# ---------------------------------------------------------------
# Discord giveaway bot backed by PostgreSQL (Railway Postgres + asyncpg)
# Env:
#   BOT_TOKEN (required)
#   DATABASE_URL (required)  e.g. postgresql://user:pass@host:port/db
#   GUILD_ID, OWNER_ID, LOG_CHANNEL_ID, LOG_WEBHOOK_URL (optional)
# ---------------------------------------------------------------

from __future__ import annotations
import os, sys, csv, asyncio
from typing import Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands
import asyncpg
from asyncpg import UniqueViolationError

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0"))
LOG_WEBHOOK_URL = os.getenv("LOG_WEBHOOK_URL")

# ------------------------------ SQL schema ------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS gift_links (
    id          SERIAL PRIMARY KEY,
    code        TEXT UNIQUE NOT NULL,
    status      TEXT NOT NULL DEFAULT 'new',   -- 'new' | 'claimed' | 'disabled'
    claimed_by  TEXT,
    claimed_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS winners (
    id              SERIAL PRIMARY KEY,
    user_id         TEXT NOT NULL UNIQUE,
    username        TEXT,
    allow_multiple  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id            SERIAL PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor_user_id TEXT,
    action        TEXT NOT NULL,
    metadata      TEXT
);
"""

# ------------------------------ DB layer (asyncpg) ------------------------------
pool: asyncpg.Pool | None = None

async def open_pool() -> asyncpg.Pool:
    assert DATABASE_URL, "DATABASE_URL is not set"
    p = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
    async with p.acquire() as conn:
        await conn.execute(SCHEMA)
    return p

async def add_links(codes: Sequence[str], actor_user_id: Optional[str]) -> int:
    assert pool
    added = 0
    async with pool.acquire() as conn, conn.transaction():
        for raw in codes:
            c = raw.strip()
            if not c:
                continue
            try:
                await conn.execute("INSERT INTO gift_links (code, status) VALUES ($1, 'new')", c)
                added += 1
            except UniqueViolationError:
                pass
        await conn.execute(
            "INSERT INTO audit_log (actor_user_id, action, metadata) VALUES ($1, 'ADD_LINK', $2)",
            actor_user_id, f"count={added}"
        )
    return added

async def add_winner(user_id: str, username: Optional[str], allow_multiple: bool) -> bool:
    assert pool
    async with pool.acquire() as conn, conn.transaction():
        try:
            await conn.execute(
                "INSERT INTO winners (user_id, username, allow_multiple) VALUES ($1,$2,$3)",
                user_id, username, allow_multiple
            )
            await conn.execute(
                "INSERT INTO audit_log (actor_user_id, action, metadata) VALUES ($1,'ADD_WINNER',$2)",
                user_id, f"username={username},allow_multiple={allow_multiple}"
            )
            return True
        except UniqueViolationError:
            return False

async def is_winner(user_id: str) -> tuple[bool, bool]:
    assert pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT allow_multiple FROM winners WHERE user_id=$1", user_id)
        if not row:
            return False, False
        return True, bool(row["allow_multiple"])

async def user_claim_count(user_id: str) -> int:
    assert pool
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM gift_links WHERE claimed_by=$1 AND status='claimed'",
            user_id
        )
        return int(n or 0)

async def claim_one_link(user_id: str) -> Optional[str]:
    """
    Atomically assign next 'new' code to this user. Returns code or None.
    """
    assert pool
    win, allow_mult = await is_winner(user_id)
    if not win:
        return None
    if not allow_mult and await user_claim_count(user_id) > 0:
        return None

    async with pool.acquire() as conn, conn.transaction():
        # lock a 'new' row
        row = await conn.fetchrow(
            "SELECT id, code FROM gift_links WHERE status='new' ORDER BY id ASC FOR UPDATE SKIP LOCKED LIMIT 1"
        )
        if not row:
            return None
        link_id, code = row["id"], row["code"]
        updated = await conn.execute(
            "UPDATE gift_links SET status='claimed', claimed_by=$1, claimed_at=NOW() WHERE id=$2 AND status='new'",
            user_id, link_id
        )
        if updated.endswith("1"):
            await conn.execute(
                "INSERT INTO audit_log (actor_user_id, action, metadata) VALUES ($1,'CLAIM',$2)",
                user_id, f"link_id={link_id}"
            )
            return str(code)
        return None

async def disable_link(code: str, actor_user_id: Optional[str]) -> bool:
    assert pool
    async with pool.acquire() as conn, conn.transaction():
        res = await conn.execute("UPDATE gift_links SET status='disabled' WHERE code=$1", code)
        ok = res.endswith("1")
        if ok:
            await conn.execute(
                "INSERT INTO audit_log (actor_user_id, action, metadata) VALUES ($1,'DISABLE_LINK',$2)",
                actor_user_id, f"code={code}"
            )
        return ok

async def stats() -> dict:
    assert pool
    async with pool.acquire() as conn:
        async def one(q: str) -> int:
            v = await conn.fetchval(q); return int(v or 0)
        return {
            "total_links": await one("SELECT COUNT(*) FROM gift_links"),
            "available_links": await one("SELECT COUNT(*) FROM gift_links WHERE status='new'"),
            "claimed_links": await one("SELECT COUNT(*) FROM gift_links WHERE status='claimed'"),
            "disabled_links": await one("SELECT COUNT(*) FROM gift_links WHERE status='disabled'"),
            "winners": await one("SELECT COUNT(*) FROM winners"),
        }

# ------------------------------ Discord bot ------------------------------
intents = discord.Intents.none()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if OWNER_ID and interaction.user.id == OWNER_ID:
            return True
        perms = getattr(interaction.user, "guild_permissions", None)
        return bool(perms and perms.administrator)
    return app_commands.check(predicate)

async def log_event(message: str):
    if LOG_CHANNEL_ID:
        ch = bot.get_channel(LOG_CHANNEL_ID)
        if ch:
            try:
                await ch.send(message); return
            except Exception:
                pass
    if LOG_WEBHOOK_URL:
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                await s.post(LOG_WEBHOOK_URL, json={"content": message}); return
        except Exception:
            pass
    print("[LOG]", message)

@bot.event
async def on_ready():
    global pool
    pool = await open_pool()
    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            tree.clear_commands(guild=guild)
            tree.copy_global_to(guild=guild)
            await tree.sync(guild=guild)
        else:
            await tree.sync()
    except Exception as e:
        await log_event(f"Slash sync error: {e}")
    await log_event(f"✅ Logged in as {bot.user} | Postgres ready")

# -------- User: /claim --------
@tree.command(name="claim", description="Claim your Nitro link (if you're eligible).")
async def claim_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    code = await claim_one_link(str(interaction.user.id))
    if not code:
        await interaction.followup.send("No link available or you're not eligible / already claimed.", ephemeral=True)
        return
    try:
        await interaction.user.send(f"🎁 Here is your Nitro gift link:\n{code}\n\nEnjoy!")
        await interaction.followup.send("Sent! Check your DMs. ✅", ephemeral=True)
        await log_event(f"CLAIM: {interaction.user} ({interaction.user.id})")
    except discord.Forbidden:
        await interaction.followup.send("I couldn't DM you. Please open your DMs and run /claim again.", ephemeral=True)

# -------- Admin: /add_links --------
@tree.command(name="add_links", description="Admin: add one or more gift links (comma or newline separated).")
@admin_only()
async def add_links_cmd(interaction: discord.Interaction, links: str):
    await interaction.response.defer(ephemeral=True)
    codes = [p.strip() for p in links.replace(",", "\n").splitlines() if p.strip()]
    added = await add_links(codes, actor_user_id=str(interaction.user.id))
    await interaction.followup.send(f"Added {added} new link(s).", ephemeral=True)
    await log_event(f"ADD_LINKS by {interaction.user} count={added}")

# -------- Admin: /add_links_file --------
@bot.tree.command(name="add_links_file", description="Admin: upload a text file with one code per line.")
@admin_only()
async def add_links_file(interaction: discord.Interaction, file: discord.Attachment):
    await interaction.response.defer(ephemeral=True)
    data = await file.read()
    codes = [ln.strip() for ln in data.decode("utf-8", errors="ignore").splitlines() if ln.strip()]
    added = await add_links(codes, actor_user_id=str(interaction.user.id))
    await interaction.followup.send(f"Added {added} link(s) from file.", ephemeral=True)
    await log_event(f"ADD_LINKS_FILE by {interaction.user} count={added} file={file.filename}")

# -------- Admin: /add_winner (user picker) --------
@bot.tree.command(name="add_winner", description="Admin: register a winner (optionally allow multiple).")
@admin_only()
async def add_winner_cmd(interaction: discord.Interaction, user: discord.User, allow_multiple: bool = False):
    ok = await add_winner(str(user.id), str(user), allow_multiple)
    await interaction.response.send_message("Winner added." if ok else "Winner already exists.", ephemeral=True)
    await log_event(f"ADD_WINNER by {interaction.user} target={user} ({user.id}) allow_multiple={allow_multiple}")

# -------- Admin: /disable_link --------
@tree.command(name="disable_link", description="Admin: disable a specific gift link code.")
@admin_only()
async def disable_link_cmd(interaction: discord.Interaction, code: str):
    ok = await disable_link(code, actor_user_id=str(interaction.user.id))
    await interaction.response.send_message("Disabled." if ok else "Code not found.", ephemeral=True)
    await log_event(f"DISABLE_LINK by {interaction.user} code={code} ok={ok}")

# -------- Admin: /stats --------
@tree.command(name="stats", description="Admin: show giveaway stats.")
@admin_only()
async def stats_cmd(interaction: discord.Interaction):
    s = await stats()
    msg = (
        f"**Links**\n"
        f"- Total: {s['total_links']}\n"
        f"- Available: {s['available_links']}\n"
        f"- Claimed: {s['claimed_links']}\n"
        f"- Disabled: {s['disabled_links']}\n\n"
        f"**Winners**: {s['winners']}"
    )
    await interaction.response.send_message(msg, ephemeral=True)

# ------------------------------ CSV Import (one-offs) ------------------------------
async def import_links_csv(path: str):
    global pool
    pool = await open_pool()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        codes = [row[0] for row in reader if row]
    added = await add_links(codes, actor_user_id="importer")
    print(f"Imported {added} links from {path}.")

async def import_winners_csv(path: str):
    global pool
    pool = await open_pool()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cnt = 0
        async with pool.acquire() as conn, conn.transaction():
            async def add(user_id, username, allow_multiple):
                try:
                    await conn.execute(
                        "INSERT INTO winners (user_id, username, allow_multiple) VALUES ($1,$2,$3)",
                        user_id, username, allow_multiple
                    )
                    return True
                except UniqueViolationError:
                    return False
            for row in reader:
                user_id = (row.get("user_id") or "").strip()
                if not user_id: continue
                username = (row.get("username") or "").strip() or None
                allow_multiple = str(row.get("allow_multiple", "0")).strip().lower() in ("1","true","yes")
                await add(user_id, username, allow_multiple)
                cnt += 1
    print(f"Imported ~{cnt} winners from {path}.")

# ------------------------------ Entrypoint ------------------------------
def main(argv: list[str]):
    if len(argv) >= 3 and argv[1] == "--import-links":
        asyncio.run(import_links_csv(argv[2])); return
    if len(argv) >= 3 and argv[1] == "--import-winners":
        asyncio.run(import_winners_csv(argv[2])); return

    if not BOT_TOKEN:
        print("ERROR: BOT_TOKEN is not set."); sys.exit(1)
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL is not set."); sys.exit(1)

    bot.run(BOT_TOKEN)

if __name__ == "__main__":
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass
