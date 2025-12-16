# =========================
# VERY FIRST LINE (DEBUG)
# =========================
print("🔥 MAIN.PY STARTED 🔥")

# =========================
# IMPORTS
# =========================
import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from dotenv import load_dotenv

# =========================
# ENV
# =========================
load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")

if not API_TOKEN:
    raise RuntimeError("API_TOKEN not set")

# =========================
# LOGGING (!!! BEFORE USE)
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("timesheet-bot")
logger.info("✅ Logger initialized")

# =========================
# BOT SETUP
# =========================
bot = Bot(API_TOKEN)
dp = Dispatcher()

# =========================
# HANDLERS
# =========================
@dp.message(Command("start"))
async def start_handler(message: Message):
    logger.info(f"/start from {message.from_user.id}")
    await message.answer("✅ Бот запущен и отвечает")

@dp.message()
async def echo_handler(message: Message):
    logger.info(f"Message: {message.text}")
    await message.answer("Я получил сообщение 👍")

# =========================
# MAIN
# =========================
async def main():
    logger.info("🚀 Bot starting polling")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
