print("🔥 MAIN.PY STARTED 🔥")

import asyncio
import os
import math
import logging
from datetime import datetime, date
from calendar import monthrange
from zoneinfo import ZoneInfo

import asyncpg
from openpyxl import Workbook
from openpyxl.styles import Font

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, KeyboardButton, ReplyKeyboardMarkup,
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ================= CONFIG =================
load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x]
TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Amsterdam"))
GEO_REQUIRED = os.getenv("GEO_REQUIRED", "false").lower() == "true"

if not API_TOKEN or not DATABASE_URL:
    raise RuntimeError("ENV not set")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("timesheet")

bot = Bot(API_TOKEN)
dp = Dispatcher()

pool: asyncpg.Pool | None = None

# ================= DB =================
async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)

    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS users(
            tg_id BIGINT PRIMARY KEY,
            fio TEXT,
            role TEXT,
            approved BOOLEAN DEFAULT FALSE
        );
        """)

        await c.execute("""
        CREATE TABLE IF NOT EXISTS objects(
            id SERIAL PRIMARY KEY,
            name TEXT,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            radius INT
        );
        """)

        await c.execute("""
        CREATE TABLE IF NOT EXISTS shifts(
            tg_id BIGINT,
            day DATE,
            object_id INT,
            start_ts TIMESTAMPTZ,
            end_ts TIMESTAMPTZ,
            hours NUMERIC,
            status TEXT,
            PRIMARY KEY (tg_id, day)
        );
        """)

# ================= UTILS =================
def now(): return datetime.now(TZ)

def calc_hours(start, end):
    h = (end - start).total_seconds() / 3600
    return min(8, math.ceil(h))

# ================= KEYBOARDS =================
def main_kb(is_admin=False):
    kb = [
        ["✅ Пришел", "⛔ Ушел"],
        ["🤒 Болел", "🏗 Объект"]
    ]
    if is_admin:
        kb.append(["📤 Табель"])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# ================= START =================
@dp.message(Command("start"))
async def start(msg: Message):
    async with pool.acquire() as c:
        u = await c.fetchrow("SELECT * FROM users WHERE tg_id=$1", msg.from_user.id)
        if not u:
            await c.execute(
                "INSERT INTO users VALUES ($1,$2,$3,$4)",
                msg.from_user.id,
                msg.from_user.full_name,
                "admin" if msg.from_user.id in ADMIN_IDS else "user",
                msg.from_user.id in ADMIN_IDS
            )
            await msg.answer("⏳ Заявка отправлена админу")
            return

        if not u["approved"]:
            await msg.answer("⏳ Ожидай одобрения")
            return

        await msg.answer(
            "🏠 Главное меню",
            reply_markup=main_kb(u["role"] == "admin")
        )

# ================= WORK =================
@dp.message(F.text == "✅ Пришел")
async def arrived(msg: Message):
    async with pool.acquire() as c:
        await c.execute("""
        INSERT INTO shifts (tg_id, day, start_ts, status)
        VALUES ($1,$2,$3,'work')
        ON CONFLICT (tg_id, day)
        DO UPDATE SET start_ts=$3, status='work'
        """, msg.from_user.id, date.today(), now())
    await msg.answer("✅ Приход зафиксирован")

@dp.message(F.text == "⛔ Ушел")
async def left(msg: Message):
    async with pool.acquire() as c:
        s = await c.fetchrow(
            "SELECT start_ts FROM shifts WHERE tg_id=$1 AND day=$2",
            msg.from_user.id, date.today()
        )
        if not s:
            await msg.answer("❌ Нет прихода")
            return

        hours = calc_hours(s["start_ts"], now())
        await c.execute("""
        UPDATE shifts
        SET end_ts=$3, hours=$4
        WHERE tg_id=$1 AND day=$2
        """, msg.from_user.id, date.today(), now(), hours)

    await msg.answer(f"⛔ Уход. Часы: {hours}")

@dp.message(F.text == "🤒 Болел")
async def sick(msg: Message):
    async with pool.acquire() as c:
        await c.execute("""
        INSERT INTO shifts (tg_id, day, status)
        VALUES ($1,$2,'sick')
        ON CONFLICT (tg_id, day)
        DO UPDATE SET status='sick'
        """, msg.from_user.id, date.today())
    await msg.answer("🤒 Отмечено")

# ================= TIMESHEET =================
async def make_timesheet(bot: Bot):
    today = date.today()
    last = monthrange(today.year, today.month)[1]

    wb = Workbook()
    ws = wb.active

    ws.append(["ФИО"] + list(range(1, last+1)) + ["Часы", "Дни"])
    ws[1][0].font = Font(bold=True)

    async with pool.acquire() as c:
        users = await c.fetch("SELECT * FROM users WHERE approved=true")
        for u in users:
            row = [u["fio"]]
            total_h = 0
            days = 0

            for d in range(1, last+1):
                s = await c.fetchrow("""
                SELECT * FROM shifts
                WHERE tg_id=$1 AND day=$2
                """, u["tg_id"], date(today.year, today.month, d))

                if not s:
                    row.append("")
                elif s["status"] == "sick":
                    row.append("Б")
                else:
                    h = s["hours"] or ""
                    row.append(h)
                    if h:
                        total_h += h
                        days += 1

            row += [total_h, days]
            ws.append(row)

    path = "/tmp/tabel.xlsx"
    wb.save(path)

    for admin in ADMIN_IDS:
        await bot.send_document(admin, FSInputFile(path))

# ================= SCHEDULER =================
async def scheduler_job(bot: Bot):
    today = date.today()
    if today.day in (15, monthrange(today.year, today.month)[1]):
        await make_timesheet(bot)

# ================= MAIN =================
async def main():
    await init_db()

    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(scheduler_job, "cron", hour=18, args=[bot])
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
