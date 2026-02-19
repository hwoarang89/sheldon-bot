"""
bot.py — Sheldon Cooper Telegram bot.

Stack: aiogram 3.x, asyncpg, openai (GPT-4o).

Environment variables required (.env):
  TELEGRAM_BOT_TOKEN
  OPENAI_API_KEY
  DATABASE_URL
"""

import logging
import os
import re

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, ChatMemberUpdated
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, JOIN_TRANSITION
from dotenv import load_dotenv
from openai import AsyncOpenAI

import database as db

load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set in .env")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set in .env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# ─── System prompt (hardcoded) ────────────────────────────────────────────────

SHELDON_SYSTEM_PROMPT = """Ты — Шелдон Купер. Суперинтеллектуальный, начитанный, слегка занудный участник чата.
Твой юмор тонкий, ты любишь иронично прожаривать участников на основе их хобби, но НИКОГДА не бываешь злым.

ЖЁСТКИЕ ЛИМИТЫ:
1. НИКАКИХ шуток про религию.
2. НИКАКИХ тем 18+ и пошлости.
3. НИКАКОГО мата.
4. Если обсуждают организационные вопросы, логистику, поездки — ОТКЛЮЧИ юмор, отвечай сухо по делу или молчи.

ОБУЧАЕМОСТЬ:
Если тебе пишут «пиши реже» или «плохая шутка» — покорно извинись в занудной манере и пообещай скорректировать алгоритмы.

Отвечай на том языке, на котором написано последнее сообщение. По умолчанию — русский."""

# ─── Phrases that trigger frequency increase ──────────────────────────────────

SLOW_DOWN_PATTERNS = re.compile(
    r"пиши\s+реже|заткнись|устал\s+от\s+тебя|хватит\s+писать|"
    r"помолчи|не\s+пиши|реже\s+пиши|замолчи",
    re.IGNORECASE,
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _build_chat_history(records: list) -> list[dict]:
    """Convert DB rows into OpenAI-compatible message dicts."""
    messages = []
    for row in records:
        username = row["username"] or f"user_{row['user_id']}"
        bio_note = f" (досье: {row['bio']})" if row["bio"] else ""
        messages.append({
            "role": "user",
            "content": f"{username}{bio_note}: {row['text']}",
        })
    return messages


async def _ask_sheldon(chat_id: int, trigger_text: str | None = None) -> str:
    """
    Build context from DB history and call GPT-4o.
    trigger_text — optional last message to append if it isn't saved yet.
    """
    history = await db.get_recent_messages(chat_id, limit=50)
    messages: list[dict] = [{"role": "system", "content": SHELDON_SYSTEM_PROMPT}]
    messages.extend(_build_chat_history(history))

    if trigger_text:
        messages.append({"role": "user", "content": trigger_text})

    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            max_tokens=512,
            temperature=0.85,
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("OpenAI error: %s", exc)
        return (
            "Произошла ошибка в моих нейронных цепях. "
            "Вероятно, это временный сбой квантового характера."
        )


async def _is_direct_mention(message: Message) -> bool:
    """Return True if the message is a reply to the bot or contains @bot_mention."""
    if message.reply_to_message and message.reply_to_message.from_user:
        if message.reply_to_message.from_user.id == (await bot.get_me()).id:
            return True
    if message.entities:
        bot_info = await bot.get_me()
        for entity in message.entities:
            if entity.type == "mention":
                mention_text = message.text[
                    entity.offset: entity.offset + entity.length
                ]
                if mention_text.lstrip("@").lower() == (bot_info.username or "").lower():
                    return True
    return False


# ─── Handlers ─────────────────────────────────────────────────────────────────

@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def on_new_member(event: ChatMemberUpdated):
    """Greet newcomers in Sheldon's style and ask for their bio."""
    new_user = event.new_chat_member.user
    if new_user.is_bot:
        return

    await db.upsert_user(new_user.id, new_user.username)
    await db.ensure_chat_exists(event.chat.id)

    first_name = new_user.first_name or "Незнакомец"
    greeting = (
        f"Оповещение: к нашему социальному эксперименту присоединился <b>{first_name}</b>. "
        f"Надеюсь, твой IQ выше среднего.\n\n"
        f"Для надлежащей каталогизации, {first_name}, прошу изложить краткое досье: "
        f"кто ты, чем занимаешься и каковы твои хобби? "
        f"Эта информация будет занесена в базу данных участников эксперимента."
    )
    await bot.send_message(event.chat.id, greeting)


@dp.message(F.text & F.chat.type.in_({"group", "supergroup"}))
async def on_group_message(message: Message):
    """
    Main handler for group text messages:
      1. Save user & message to DB.
      2. Check for 'slow down' phrases → increase frequency.
      3. Check for direct mention → always reply.
      4. Check counter → reply every N messages.
    """
    user = message.from_user
    if not user or user.is_bot:
        return

    text = message.text or ""
    chat_id = message.chat.id

    # Persist user and message
    await db.upsert_user(user.id, user.username)
    await db.save_message(user.id, chat_id, text)

    # ── Bio collection: if last bot message asked for bio ──────────────────
    # Simple heuristic: if the reply is to the bot's greeting we treat the
    # full message as bio. A production system would use FSM states.
    if message.reply_to_message and message.reply_to_message.from_user:
        bot_info = await bot.get_me()
        if (
            message.reply_to_message.from_user.id == bot_info.id
            and "досье" in (message.reply_to_message.text or "").lower()
        ):
            await db.set_user_bio(user.id, text)
            ack = (
                "Данные занесены в реестр. "
                "Должен признать, твоё досье обработано с максимальной точностью."
            )
            await message.reply(ack)
            return

    # ── Slow-down phrases ──────────────────────────────────────────────────
    if SLOW_DOWN_PATTERNS.search(text):
        new_freq = await db.increase_reply_frequency(chat_id, delta=5)
        apology = (
            "Приношу свои извинения. Я скорректирую алгоритм частоты ответов. "
            f"Новый интервал: каждые {new_freq} сообщений. "
            "Надеюсь, это соответствует социальным нормам вашей группы."
        )
        await message.reply(apology)
        return

    # ── Direct mention or reply to bot ────────────────────────────────────
    if await _is_direct_mention(message):
        reply_text = await _ask_sheldon(chat_id)
        await message.reply(reply_text)
        await db.reset_message_count(chat_id)
        return

    # ── Counter-based reply ───────────────────────────────────────────────
    count, frequency = await db.increment_and_get(chat_id)
    if count >= frequency:
        reply_text = await _ask_sheldon(chat_id)
        await message.reply(reply_text)
        await db.reset_message_count(chat_id)


@dp.message(F.text & F.chat.type == "private")
async def on_private_message(message: Message):
    """Handle private messages — always reply."""
    user = message.from_user
    if not user:
        return

    await db.upsert_user(user.id, user.username)
    await db.save_message(user.id, message.chat.id, message.text or "")

    reply_text = await _ask_sheldon(message.chat.id)
    await message.reply(reply_text)


@dp.message(Command("setbio"))
async def cmd_set_bio(message: Message):
    """
    /setbio <text>  — manually update your bio.
    Works in both private and group chats.
    """
    user = message.from_user
    if not user:
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply(
            "Использование: /setbio <ваше досье>\n"
            "Пример: /setbio Программист, люблю шахматы и астрофизику."
        )
        return

    bio_text = parts[1].strip()
    await db.upsert_user(user.id, user.username)
    await db.set_user_bio(user.id, bio_text)
    await message.reply(
        "Досье обновлено. Занесено в базу данных с отметкой 'актуально'. "
        "Ваши данные теперь помогут мне генерировать более точные иронические замечания."
    )


@dp.message(Command("frequency"))
async def cmd_frequency(message: Message):
    """
    /frequency <number>  — set reply frequency for this chat (admins only).
    """
    user = message.from_user
    if not user:
        return

    # Check admin rights
    member = await bot.get_chat_member(message.chat.id, user.id)
    if member.status not in ("administrator", "creator"):
        await message.reply("Только администраторы могут изменять частоту ответов.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.reply("Использование: /frequency <число>\nПример: /frequency 10")
        return

    new_freq = int(parts[1].strip())
    if new_freq < 1:
        await message.reply("Частота должна быть не менее 1.")
        return

    pool = await db.get_pool()
    async with pool.acquire() as conn:
        await db._ensure_chat(message.chat.id, conn)
        await conn.execute(
            "UPDATE chat_settings SET reply_frequency = $1 WHERE chat_id = $2",
            new_freq, message.chat.id,
        )
    await message.reply(
        f"Параметр reply_frequency установлен на {new_freq}. "
        "Алгоритм скорректирован. Вы можете быть спокойны."
    )


# ─── Startup / shutdown ───────────────────────────────────────────────────────

async def on_startup():
    logger.info("Initialising database …")
    await db.init_db()
    me = await bot.get_me()
    logger.info("Bot started: @%s", me.username)


async def on_shutdown():
    logger.info("Closing DB pool …")
    await db.close_pool()


# ─── Entry point ──────────────────────────────────────────────────────────────

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
