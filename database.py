"""
database.py — asyncpg-based database layer for Sheldon Bot.

Tables:
  users              — user_id, username, bio
  messages           — id, user_id, chat_id, text, timestamp  (last 100 per chat)
  chat_settings      — chat_id, message_count, reply_frequency
  image_gen_log      — id, date, count  (global daily DALL-E counter)
"""

import os
import asyncpg
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL: str = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/sheldon_bot"
)

_pool: asyncpg.Pool | None = None


# ─── Pool management ──────────────────────────────────────────────────────────

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


# ─── Schema ───────────────────────────────────────────────────────────────────

async def init_db() -> None:
    """Create all tables if they do not exist yet."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id   BIGINT PRIMARY KEY,
                username  TEXT    NOT NULL DEFAULT '',
                bio       TEXT    NOT NULL DEFAULT ''
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id        SERIAL    PRIMARY KEY,
                user_id   BIGINT    NOT NULL,
                chat_id   BIGINT    NOT NULL,
                text      TEXT      NOT NULL,
                timestamp TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_chat_ts
                ON messages (chat_id, timestamp DESC)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id          BIGINT PRIMARY KEY,
                message_count    INTEGER NOT NULL DEFAULT 0,
                reply_frequency  INTEGER NOT NULL DEFAULT 5
            )
        """)

        # Global daily DALL-E generation counter (one row per calendar date)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS image_gen_log (
                date   DATE    PRIMARY KEY,
                count  INTEGER NOT NULL DEFAULT 0
            )
        """)


# ─── Users CRUD ───────────────────────────────────────────────────────────────

async def upsert_user(user_id: int, username: str | None) -> None:
    """Insert or update a user record (preserves existing bio)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, username, bio)
            VALUES ($1, $2, '')
            ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username
        """, user_id, username or "")


async def get_user(user_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM users WHERE user_id = $1", user_id
        )


async def set_user_bio(user_id: int, bio: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET bio = $1 WHERE user_id = $2", bio, user_id
        )


# ─── Messages CRUD ────────────────────────────────────────────────────────────

async def save_message(user_id: int, chat_id: int, text: str) -> None:
    """Save a message and keep only the last 100 messages per chat."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO messages (user_id, chat_id, text, timestamp)
            VALUES ($1, $2, $3, $4)
        """, user_id, chat_id, text, datetime.utcnow())

        # Prune: keep only the newest 100 rows for this chat
        await conn.execute("""
            DELETE FROM messages
            WHERE chat_id = $1
              AND id NOT IN (
                  SELECT id FROM messages
                  WHERE chat_id = $1
                  ORDER BY timestamp DESC
                  LIMIT 100
              )
        """, chat_id)


async def get_recent_messages(chat_id: int, limit: int = 50) -> list[asyncpg.Record]:
    """Return the last *limit* messages for a chat, oldest-first."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT m.user_id,
                   m.text,
                   m.timestamp,
                   u.username,
                   u.bio
            FROM   messages m
            LEFT JOIN users u ON u.user_id = m.user_id
            WHERE  m.chat_id = $1
            ORDER  BY m.timestamp DESC
            LIMIT  $2
        """, chat_id, limit)
        return list(reversed(rows))          # chronological order


# ─── Chat settings CRUD ───────────────────────────────────────────────────────

async def _ensure_chat(chat_id: int, conn: asyncpg.Connection) -> None:
    await conn.execute("""
        INSERT INTO chat_settings (chat_id, message_count, reply_frequency)
        VALUES ($1, 0, 5)
        ON CONFLICT (chat_id) DO NOTHING
    """, chat_id)


async def ensure_chat_exists(chat_id: int) -> None:
    """Public wrapper — ensures a chat row exists without requiring a conn arg."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)


async def get_chat_settings(chat_id: int) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)
        return await conn.fetchrow(
            "SELECT * FROM chat_settings WHERE chat_id = $1", chat_id
        )


async def increment_and_get(chat_id: int) -> tuple[int, int]:
    """
    Atomically increment message_count and return (new_count, reply_frequency).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)
        row = await conn.fetchrow("""
            UPDATE chat_settings
            SET    message_count = message_count + 1
            WHERE  chat_id = $1
            RETURNING message_count, reply_frequency
        """, chat_id)
        return row["message_count"], row["reply_frequency"]


async def reset_message_count(chat_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE chat_settings SET message_count = 0 WHERE chat_id = $1",
            chat_id,
        )


async def increase_reply_frequency(chat_id: int, delta: int = 5) -> int:
    """Increase reply_frequency by *delta* and return the new value."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)
        row = await conn.fetchrow("""
            UPDATE chat_settings
            SET    reply_frequency = reply_frequency + $1
            WHERE  chat_id = $2
            RETURNING reply_frequency
        """, delta, chat_id)
        return row["reply_frequency"]


# ─── Image generation daily counter ──────────────────────────────────────────

IMAGE_GEN_DAILY_LIMIT = 10


async def get_image_gen_count_today() -> int:
    """Return how many images have been generated today (UTC date)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT count FROM image_gen_log WHERE date = CURRENT_DATE"
        )
        return row["count"] if row else 0


async def increment_image_gen_count() -> int:
    """Increment today's counter and return the new value."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO image_gen_log (date, count)
            VALUES (CURRENT_DATE, 1)
            ON CONFLICT (date) DO UPDATE
                SET count = image_gen_log.count + 1
            RETURNING count
        """)
        return row["count"]


async def image_gen_allowed() -> bool:
    """Return True if we haven't hit the daily limit yet."""
    return (await get_image_gen_count_today()) < IMAGE_GEN_DAILY_LIMIT
