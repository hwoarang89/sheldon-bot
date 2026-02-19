"""
database.py — asyncpg-based database layer for Sheldon Bot.

Tables:
  users              — user_id, username, bio
  messages           — id, user_id, chat_id, text, timestamp  (last 100 per chat)
  chat_settings      — chat_id, message_count, reply_frequency, last_activity
  chat_members       — chat_id, user_id  (known members for proactive pokes)
  image_gen_log      — date, count  (global daily DALL-E counter)
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
                chat_id            BIGINT    PRIMARY KEY,
                message_count      INTEGER   NOT NULL DEFAULT 0,
                reply_frequency    INTEGER   NOT NULL DEFAULT 5,
                humor_level        INTEGER   NOT NULL DEFAULT 5,
                last_activity      TIMESTAMP NOT NULL DEFAULT NOW(),
                next_poke_at       TIMESTAMP NOT NULL DEFAULT NOW(),
                max_response_lines INTEGER   NOT NULL DEFAULT 3
            )
        """)
        # Safe migrations for existing deployments
        for col, definition in [
            ("last_activity",      "TIMESTAMP NOT NULL DEFAULT NOW()"),
            ("next_poke_at",       "TIMESTAMP NOT NULL DEFAULT NOW()"),
            ("humor_level",        "INTEGER NOT NULL DEFAULT 5"),
            ("max_response_lines", "INTEGER NOT NULL DEFAULT 3"),
        ]:
            await conn.execute(f"""
                ALTER TABLE chat_settings
                    ADD COLUMN IF NOT EXISTS {col} {definition}
            """)

        # Known group members — used for proactive @mentions
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_members (
                chat_id  BIGINT NOT NULL,
                user_id  BIGINT NOT NULL,
                PRIMARY KEY (chat_id, user_id)
            )
        """)

        # Per-user ignore list: bot won't address this user until ignore_until
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_ignore (
                chat_id      BIGINT    NOT NULL,
                user_id      BIGINT    NOT NULL,
                ignore_until TIMESTAMP NOT NULL,
                PRIMARY KEY (chat_id, user_id)
            )
        """)

        # Global daily DALL-E generation counter (one row per calendar date)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS image_gen_log (
                date   DATE    PRIMARY KEY,
                count  INTEGER NOT NULL DEFAULT 0
            )
        """)

        # Banned words/phrases per chat
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS banned_phrases (
                id       SERIAL PRIMARY KEY,
                chat_id  BIGINT NOT NULL,
                phrase   TEXT   NOT NULL,
                added_by BIGINT NOT NULL,
                UNIQUE (chat_id, phrase)
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


# ─── Chat members (for proactive pokes) ──────────────────────────────────────

async def add_chat_member(chat_id: int, user_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_members (chat_id, user_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
        """, chat_id, user_id)


async def get_chat_members(chat_id: int) -> list[asyncpg.Record]:
    """Return all known (chat_id, user_id) pairs joined with users table."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT cm.user_id, u.username, u.bio
            FROM   chat_members cm
            JOIN   users u ON u.user_id = cm.user_id
            WHERE  cm.chat_id = $1
        """, chat_id)


async def get_members_without_bio(chat_id: int) -> list[asyncpg.Record]:
    """Return known members who have no bio yet."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT cm.user_id, u.username
            FROM   chat_members cm
            JOIN   users u ON u.user_id = cm.user_id
            WHERE  cm.chat_id = $1
              AND  (u.bio IS NULL OR u.bio = '')
        """, chat_id)


# ─── Last activity tracking ───────────────────────────────────────────────────

async def touch_last_activity(chat_id: int) -> None:
    """Update last_activity timestamp for a chat to now."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)
        await conn.execute("""
            UPDATE chat_settings SET last_activity = NOW() WHERE chat_id = $1
        """, chat_id)


async def get_silent_chats(silent_minutes: int = 60) -> list[asyncpg.Record]:
    """Return chats where nobody wrote for at least *silent_minutes* minutes."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT chat_id FROM chat_settings
            WHERE  last_activity < NOW() - ($1 * INTERVAL '1 minute')
        """, silent_minutes)


async def get_all_chat_ids() -> list[int]:
    """Return all known chat IDs."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id FROM chat_settings")
        return [r["chat_id"] for r in rows]


# ─── Chat settings: humor, response length, poke schedule ────────────────────

async def set_humor_level(chat_id: int, level: int) -> None:
    """Set humor level 1-10 (1=dry, 10=maximum jokes)."""
    level = max(1, min(10, level))
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)
        await conn.execute(
            "UPDATE chat_settings SET humor_level = $1 WHERE chat_id = $2",
            level, chat_id
        )


async def set_max_response_lines(chat_id: int, lines: int) -> None:
    """Set max response length in lines (1-10)."""
    lines = max(1, min(10, lines))
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)
        await conn.execute(
            "UPDATE chat_settings SET max_response_lines = $1 WHERE chat_id = $2",
            lines, chat_id
        )


async def set_next_poke_at(chat_id: int, minutes_from_now: int) -> None:
    """Schedule next proactive poke at now + N minutes."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)
        await conn.execute("""
            UPDATE chat_settings
            SET next_poke_at = NOW() + ($1 * INTERVAL '1 minute')
            WHERE chat_id = $2
        """, minutes_from_now, chat_id)


async def get_chats_due_for_poke() -> list[asyncpg.Record]:
    """Return chats where next_poke_at is in the past."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT chat_id, humor_level, max_response_lines
            FROM   chat_settings
            WHERE  next_poke_at <= NOW()
              AND  last_activity < next_poke_at
        """)


async def get_full_chat_settings(chat_id: int) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_chat(chat_id, conn)
        return await conn.fetchrow(
            "SELECT * FROM chat_settings WHERE chat_id = $1", chat_id
        )


# ─── Per-user ignore ──────────────────────────────────────────────────────────

async def set_user_ignore(chat_id: int, user_id: int, until_ts) -> None:
    """Tell the bot to not address this user until until_ts."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO user_ignore (chat_id, user_id, ignore_until)
            VALUES ($1, $2, $3)
            ON CONFLICT (chat_id, user_id) DO UPDATE
                SET ignore_until = EXCLUDED.ignore_until
        """, chat_id, user_id, until_ts)


async def is_user_ignored(chat_id: int, user_id: int) -> bool:
    """Return True if the bot should not proactively address this user right now."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT 1 FROM user_ignore
            WHERE  chat_id = $1 AND user_id = $2 AND ignore_until > NOW()
        """, chat_id, user_id)
        return row is not None


# ─── Banned phrases ───────────────────────────────────────────────────────────

async def add_banned_phrase(chat_id: int, phrase: str, added_by: int) -> bool:
    """Add a phrase to the ban list. Returns True if added, False if already exists."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO banned_phrases (chat_id, phrase, added_by)
                VALUES ($1, $2, $3)
                ON CONFLICT (chat_id, phrase) DO NOTHING
            """, chat_id, phrase.lower().strip(), added_by)
            return True
        except Exception:
            return False


async def remove_banned_phrase(chat_id: int, phrase: str) -> bool:
    """Remove a phrase from the ban list. Returns True if removed."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("""
            DELETE FROM banned_phrases
            WHERE chat_id = $1 AND phrase = $2
        """, chat_id, phrase.lower().strip())
        return result == "DELETE 1"


async def get_banned_phrases(chat_id: int) -> list[str]:
    """Return all banned phrases for a chat."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT phrase FROM banned_phrases WHERE chat_id = $1 ORDER BY id",
            chat_id
        )
        return [r["phrase"] for r in rows]


async def clear_banned_phrases(chat_id: int) -> int:
    """Remove all banned phrases for a chat. Returns count deleted."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM banned_phrases WHERE chat_id = $1", chat_id
        )
        return int(result.split()[-1])
