"""
bot.py — Sheldon Cooper Telegram bot.

Stack: aiogram 3.x, asyncpg, openai (GPT-4o + Whisper).

Environment variables required (.env):
  TELEGRAM_BOT_TOKEN
  OPENAI_API_KEY
  DATABASE_URL
"""

import base64
import io
import logging
import os
import re

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
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

bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# ─── System prompt ────────────────────────────────────────────────────────────

SHELDON_SYSTEM_PROMPT = """Ты — Шелдон Купер в Telegram-чате. Гений, сноб, но обаятельный зануда.

СТИЛЬ:
- Отвечай КОРОТКО — 1-3 предложения максимум. Никаких простыней текста.
- Юмор — твой главный инструмент. Шути ЧАСТО, остро, но без злобы.
- Иронизируй над хобби и занятиями участников — это твоя фишка.
- Используй снисходительный тон: ты умнее всех, но не агрессивен.
- Иногда вставляй "Базара нет", "Бинго!", "Как интересно... нет, погоди, совсем не интересно."
- Можешь процитировать науку или поп-культуру к месту.

ЖЁСТКИЕ ЛИМИТЫ:
1. НИКАКИХ шуток про религию.
2. НИКАКИХ тем 18+ и пошлости.
3. НИКАКОГО мата.
4. Организационные вопросы, логистика, поездки — отвечай сухо и по делу, без шуток.

ОБУЧАЕМОСТЬ:
Если пишут «пиши реже», «плохая шутка», «заткнись» — извинись занудно, пообещай «скорректировать алгоритмы».

Отвечай на языке последнего сообщения. По умолчанию — русский."""

# ─── Slow-down phrases ────────────────────────────────────────────────────────

SLOW_DOWN_PATTERNS = re.compile(
    r"пиши\s+реже|заткнись|устал\s+от\s+тебя|хватит\s+писать|"
    r"помолчи|не\s+пиши|реже\s+пиши|замолчи",
    re.IGNORECASE,
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _build_chat_history(records: list) -> list[dict]:
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
    """Call GPT-4o with chat history as context."""
    history = await db.get_recent_messages(chat_id, limit=50)
    messages: list[dict] = [{"role": "system", "content": SHELDON_SYSTEM_PROMPT}]
    messages.extend(_build_chat_history(history))

    if trigger_text:
        messages.append({"role": "user", "content": trigger_text})

    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            max_tokens=200,       # короче!
            temperature=0.95,     # чуть больше креативности
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("OpenAI error: %s", exc)
        return "Мои нейронные цепи дали сбой. Вероятно, виной тому квантовая флуктуация."


async def _ask_sheldon_about_image(chat_id: int, image_b64: str, caption: str | None) -> str:
    """Send image to GPT-4o Vision and get Sheldon's reaction."""
    history = await db.get_recent_messages(chat_id, limit=20)
    messages: list[dict] = [{"role": "system", "content": SHELDON_SYSTEM_PROMPT}]
    messages.extend(_build_chat_history(history))

    user_content: list = []
    if caption:
        user_content.append({"type": "text", "text": f"Участник прислал фото с подписью: {caption}"})
    else:
        user_content.append({"type": "text", "text": "Участник прислал фото. Прокомментируй."})

    user_content.append({
        "type": "image_url",
        "image_url": {"url": f"data:image/jpeg;base64,{image_b64}", "detail": "low"},
    })
    messages.append({"role": "user", "content": user_content})

    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            max_tokens=200,
            temperature=0.95,
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("OpenAI Vision error: %s", exc)
        return "Я попытался проанализировать это изображение, но мои фотонные рецепторы отказали."


async def _is_image_edit_request(caption: str) -> bool:
    """Ask GPT-4o whether the caption is a request to modify/redraw the image."""
    if not caption or len(caption.strip()) < 3:
        return False
    try:
        resp = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты определяешь намерение. "
                        "Если текст — просьба изменить, перерисовать, отредактировать изображение "
                        "(например: 'сделай фон космосом', 'добавь шляпу', 'в стиле аниме', "
                        "'убери человека', 'перекрась в синий') — ответь ТОЛЬКО словом YES. "
                        "Если это просто комментарий или вопрос — ответь ТОЛЬКО словом NO."
                    ),
                },
                {"role": "user", "content": caption},
            ],
            max_tokens=5,
            temperature=0,
        )
        answer = resp.choices[0].message.content.strip().upper()
        return answer.startswith("YES")
    except Exception:
        return False


async def _build_dalle_prompt(image_b64: str, edit_request: str) -> str:
    """Use GPT-4o Vision to describe the image and merge it with the edit request."""
    try:
        resp = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты составляешь промпт для DALL-E 3. "
                        "Детально опиши содержимое изображения, затем примени к описанию "
                        "следующее изменение от пользователя. "
                        "Верни ТОЛЬКО готовый промпт на английском языке, без пояснений."
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": f"Запрос на изменение: {edit_request}"},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_b64}",
                                "detail": "low",
                            },
                        },
                    ],
                },
            ],
            max_tokens=300,
            temperature=0.5,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("Prompt build error: %s", exc)
        return edit_request  # fallback — передаём запрос как есть


async def _generate_image(prompt: str) -> str | None:
    """Generate image via DALL-E 3, return URL or None on error."""
    try:
        resp = await openai_client.images.generate(
            model="dall-e-3",
            prompt=prompt,
            size="1024x1024",
            quality="standard",
            n=1,
        )
        return resp.data[0].url
    except Exception as exc:
        logger.error("DALL-E error: %s", exc)
        return None


async def _transcribe_voice(file_bytes: bytes, mime: str = "audio/ogg") -> str:
    """Transcribe voice message using OpenAI Whisper."""
    try:
        audio_file = io.BytesIO(file_bytes)
        audio_file.name = "voice.ogg"
        transcript = await openai_client.audio.transcriptions.create(
            model="whisper-1",
            file=audio_file,
            language="ru",
        )
        return transcript.text.strip()
    except Exception as exc:
        logger.error("Whisper error: %s", exc)
        return ""


async def _is_direct_mention(message: Message) -> bool:
    """Return True if the message is a reply to the bot or @mention."""
    if message.reply_to_message and message.reply_to_message.from_user:
        if message.reply_to_message.from_user.id == (await bot.get_me()).id:
            return True
    entities = message.entities or message.caption_entities or []
    if entities:
        bot_info = await bot.get_me()
        text = message.text or message.caption or ""
        for entity in entities:
            if entity.type == "mention":
                mention = text[entity.offset: entity.offset + entity.length]
                if mention.lstrip("@").lower() == (bot_info.username or "").lower():
                    return True
    return False


# ─── Handlers ─────────────────────────────────────────────────────────────────

@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def on_new_member(event: ChatMemberUpdated):
    """Greet newcomers in Sheldon's style."""
    new_user = event.new_chat_member.user
    if new_user.is_bot:
        return

    await db.upsert_user(new_user.id, new_user.username)
    await db.ensure_chat_exists(event.chat.id)

    first_name = new_user.first_name or "Незнакомец"
    greeting = (
        f"Оповещение: к нашему социальному эксперименту присоединился <b>{first_name}</b>. "
        f"Надеюсь, твой IQ выше среднего — хотя статистика не на твоей стороне.\n\n"
        f"Для каталогизации: кто ты, чем занимаешься, каковы хобби? "
        f"Данные будут занесены в базу для последующих иронических атак."
    )
    await bot.send_message(event.chat.id, greeting)


@dp.message(F.photo & F.chat.type.in_({"group", "supergroup", "private"}))
async def on_photo(message: Message):
    """
    Handle photos:
      - If caption is an edit/redraw request → generate via DALL-E 3 (max 10/day).
      - Otherwise → comment in Sheldon's style using GPT-4o Vision.
    """
    user = message.from_user
    if not user or user.is_bot:
        return

    chat_id = message.chat.id
    await db.upsert_user(user.id, user.username)
    await db.ensure_chat_exists(chat_id)

    caption = message.caption or ""
    if caption:
        await db.save_message(user.id, chat_id, f"[фото] {caption}")

    is_mention = await _is_direct_mention(message)

    # In groups respond only on mention or by counter
    if message.chat.type in ("group", "supergroup") and not is_mention:
        count, frequency = await db.increment_and_get(chat_id)
        if count < frequency:
            return
        await db.reset_message_count(chat_id)

    # Download photo once — needed for both paths
    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
    file_bytes = await bot.download_file(file.file_path)
    image_b64 = base64.b64encode(file_bytes.read()).decode("utf-8")

    # ── Detect if user wants image editing ────────────────────────────────
    wants_edit = await _is_image_edit_request(caption)

    if wants_edit:
        # Check daily limit
        if not await db.image_gen_allowed():
            used = await db.get_image_gen_count_today()
            await message.reply(
                f"🚫 Лимит исчерпан. За сегодня сгенерировано {used}/{db.IMAGE_GEN_DAILY_LIMIT} изображений. "
                "Мои вычислительные мощности не бесконечны — приходите завтра."
            )
            return

        # Notify chat that generation is in progress
        remaining = db.IMAGE_GEN_DAILY_LIMIT - await db.get_image_gen_count_today()
        wait_msg = await message.reply(
            f"🎨 Активирую нейросеть DALL-E 3... Обрабатываю запрос: <i>{caption}</i>\n"
            f"⏳ Генерация занимает ~15 секунд. Осталось попыток сегодня: {remaining - 1} из {db.IMAGE_GEN_DAILY_LIMIT}."
        )

        # Build rich prompt via Vision, then generate
        dalle_prompt = await _build_dalle_prompt(image_b64, caption)
        logger.info("DALL-E prompt: %s", dalle_prompt)

        image_url = await _generate_image(dalle_prompt)

        # Delete "waiting" message
        try:
            await wait_msg.delete()
        except Exception:
            pass

        if image_url:
            await db.increment_image_gen_count()
            count_now = await db.get_image_gen_count_today()
            await message.reply_photo(
                photo=image_url,
                caption=(
                    f"✅ Готово. Применил: «{caption}»\n"
                    f"Использовано сегодня: {count_now}/{db.IMAGE_GEN_DAILY_LIMIT}."
                ),
            )
        else:
            await message.reply(
                "Что-то пошло не так в моих нейросетевых цепях. "
                "DALL-E отказал в сотрудничестве. Попробуйте позже."
            )
    else:
        # Regular Vision comment
        reply_text = await _ask_sheldon_about_image(chat_id, image_b64, caption or None)
        await message.reply(reply_text)

    if is_mention:
        await db.reset_message_count(chat_id)


@dp.message(F.voice & F.chat.type.in_({"group", "supergroup", "private"}))
async def on_voice(message: Message):
    """Transcribe voice via Whisper, then respond as Sheldon."""
    user = message.from_user
    if not user or user.is_bot:
        return

    chat_id = message.chat.id
    await db.upsert_user(user.id, user.username)
    await db.ensure_chat_exists(chat_id)

    is_mention = await _is_direct_mention(message)

    # Download voice
    file = await bot.get_file(message.voice.file_id)
    file_bytes = await bot.download_file(file.file_path)
    audio_data = file_bytes.read()

    # Transcribe
    transcription = await _transcribe_voice(audio_data)
    if not transcription:
        if is_mention:
            await message.reply(
                "Я расслышал лишь белый шум. "
                "Возможно, твой микрофон столь же некомпетентен, как и твои аргументы."
            )
        return

    # Save as text message
    await db.save_message(user.id, chat_id, f"[голос]: {transcription}")
    logger.info("Voice transcribed: %s", transcription)

    # In groups — respond only on mention or counter
    if message.chat.type in ("group", "supergroup") and not is_mention:
        count, frequency = await db.increment_and_get(chat_id)
        if count < frequency:
            # At least confirm transcription silently (no reply)
            return
        await db.reset_message_count(chat_id)

    # Reply
    trigger = f"[Участник {user.first_name} сказал голосом]: {transcription}"
    reply_text = await _ask_sheldon(chat_id, trigger_text=trigger)
    await message.reply(f"🎙 <i>«{transcription}»</i>\n\n{reply_text}")

    if is_mention:
        await db.reset_message_count(chat_id)


@dp.message(F.text & F.chat.type.in_({"group", "supergroup"}))
async def on_group_message(message: Message):
    """Main handler for group text messages."""
    user = message.from_user
    if not user or user.is_bot:
        return

    text = message.text or ""
    chat_id = message.chat.id

    await db.upsert_user(user.id, user.username)
    await db.save_message(user.id, chat_id, text)

    # ── Bio collection ─────────────────────────────────────────────────────
    if message.reply_to_message and message.reply_to_message.from_user:
        bot_info = await bot.get_me()
        if (
            message.reply_to_message.from_user.id == bot_info.id
            and "досье" in (message.reply_to_message.text or "").lower()
        ):
            await db.set_user_bio(user.id, text)
            await message.reply(
                "Досье занесено. Теперь я знаю о тебе достаточно для качественных подколок. "
                "Добро пожаловать в базу данных."
            )
            return

    # ── Slow-down phrases ──────────────────────────────────────────────────
    if SLOW_DOWN_PATTERNS.search(text):
        new_freq = await db.increase_reply_frequency(chat_id, delta=5)
        await message.reply(
            f"Приношу извинения. Скорректирую алгоритм: буду отвечать раз в {new_freq} сообщений. "
            "Надеюсь, это удовлетворит ваши социальные потребности."
        )
        return

    # ── Direct mention ─────────────────────────────────────────────────────
    if await _is_direct_mention(message):
        reply_text = await _ask_sheldon(chat_id)
        await message.reply(reply_text)
        await db.reset_message_count(chat_id)
        return

    # ── Counter-based reply ────────────────────────────────────────────────
    count, frequency = await db.increment_and_get(chat_id)
    if count >= frequency:
        reply_text = await _ask_sheldon(chat_id)
        await message.reply(reply_text)
        await db.reset_message_count(chat_id)


@dp.message(F.text & (F.chat.type == "private"))
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
    """/setbio <text> — manually update your bio."""
    user = message.from_user
    if not user:
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply(
            "Использование: /setbio <ваше досье>\n"
            "Пример: /setbio Программист, люблю шахматы и квантовую физику."
        )
        return

    bio_text = parts[1].strip()
    await db.upsert_user(user.id, user.username)
    await db.set_user_bio(user.id, bio_text)
    await message.reply(
        "Досье обновлено. Теперь я знаю о тебе больше, чем тебе хотелось бы. "
        "Данные активированы для будущих атак иронией."
    )


@dp.message(Command("frequency"))
async def cmd_frequency(message: Message):
    """/frequency <N> — set reply frequency (admins only)."""
    user = message.from_user
    if not user:
        return

    member = await bot.get_chat_member(message.chat.id, user.id)
    if member.status not in ("administrator", "creator"):
        await message.reply("Только администраторы могут изменять частоту ответов.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.reply("Использование: /frequency <число>. Например: /frequency 10")
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
        f"Алгоритм скорректирован. Буду отвечать каждые {new_freq} сообщений. "
        "Можете расслабиться."
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
