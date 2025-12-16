import os
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import asyncpg
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.filters import CommandStart

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ======================
# CONFIG
# ======================

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

ADMIN_IDS = [
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()
]

TZ = ZoneInfo("Europe/Amsterdam")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)
logger.info("🔥 MAIN.PY STARTED 🔥")

# ======================
# DB
# ======================

pool: asyncpg.Pool | None = None


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)

    async with pool.acquire() as c:
        await c.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                tg_id BIGINT PRIMARY KEY,
                role TEXT NOT NULL,
                approved BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )

        await c.execute(
            """
            CREATE TABLE IF NOT EXISTS shifts (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT REFERENCES users(tg_id),
                date DATE NOT NULL,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                hours NUMERIC
            );
            """
        )


# ======================
# KEYBOARDS (AIROGRAM 3)
# ======================

def main_kb(is_admin: bool) -> ReplyKeyboardMarkup:
    keyboard = [
        [
            KeyboardButton(text="✅ Пришел"),
            KeyboardButton(text="⛔ Ушел"),
        ],
        [
            KeyboardButton(text="🤒 Болел"),
            KeyboardButton(text="🏗 Объект"),
        ],
    ]

    if is_admin:
        keyboard.append(
            [KeyboardButton(text="📤 Табель")]
        )

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True
    )


# ======================
# BOT
# ======================

bot = Bot(API_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def start(msg: Message):
    async with pool.acquire() as c:
        user = await c.fetchrow(
            "SELECT * FROM users WHERE tg_id=$1",
            msg.from_user.id,
        )

        if not user:
            role = "admin" if msg.from_user.id in ADMIN_IDS else "employee"
            approved = msg.from_user.id in ADMIN_IDS

            await c.execute(
                """
                INSERT INTO users (tg_id, role, approved)
                VALUES ($1, $2, $3)
                """,
                msg.from_user.id,
                role,
                approved,
            )

            is_admin = role == "admin"
        else:
            is_admin = user["role"] == "admin"

    await msg.answer(
        "🏠 Главное меню",
        reply_markup=main_kb(is_admin)
    )


@dp.message(F.text == "✅ Пришел")
async def arrived(msg: Message):
    today = datetime.now(TZ).date()

    async with pool.acquire() as c:
        await c.execute(
            """
            INSERT INTO shifts (tg_id, date, start_time)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            msg.from_user.id,
            today,
            datetime.now(TZ),
        )

    await msg.answer("🟢 Приход зафиксирован")


@dp.message(F.text == "⛔ Ушел")
async def left(msg: Message):
    now = datetime.now(TZ)
    today = now.date()

    async with pool.acquire() as c:
        shift = await c.fetchrow(
            """
            SELECT * FROM shifts
            WHERE tg_id=$1 AND date=$2
            """,
            msg.from_user.id,
            today,
        )

        if not shift or not shift["start_time"]:
            await msg.answer("❌ Смена не найдена")
            return

        hours = min(
            8,
            round(((now - shift["start_time"]).total_seconds() / 3600) * 2) / 2
        )

        await c.execute(
            """
            UPDATE shifts
            SET end_time=$1, hours=$2
            WHERE id=$3
            """,
            now,
            hours,
            shift["id"],
        )

    await msg.answer(f"⏱ Отработано: {hours} ч.")


@dp.message(F.text == "🤒 Болел")
async def sick(msg: Message):
    today = datetime.now(TZ).date()

    async with pool.acquire() as c:
        await c.execute(
            """
            INSERT INTO shifts (tg_id, date, hours)
            VALUES ($1, $2, 0)
            ON CONFLICT DO NOTHING
            """,
            msg.from_user.id,
            today,
        )

    await msg.answer("🤒 Отмечено как больничный")


@dp.message(F.text == "📤 Табель")
async def report(msg: Message):
    async with pool.acquire() as c:
        user = await c.fetchrow(
            "SELECT role FROM users WHERE tg_id=$1",
            msg.from_user.id,
        )

        if not user or user["role"] != "admin":
            return

    await msg.answer("📊 Табель будет здесь (следующий шаг)")


# ======================
# SCHEDULER
# ======================

scheduler = AsyncIOScheduler(timezone=str(TZ))


def scheduler_job():
    logger.info("🕒 Scheduler tick")


# ======================
# MAIN
# ======================

async def main():
    await init_db()

    scheduler.add_job(scheduler_job, "interval", hours=1)
    scheduler.start()

    logger.info("🚀 Bot starting polling")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
