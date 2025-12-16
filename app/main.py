import asyncio
import json
import logging
import os
import tempfile
from enum import Enum
from datetime import datetime, date
from calendar import monthrange
from typing import Dict, Any, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

# =========================
# ENV / CONFIG
# =========================

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
ADMIN_IDS = [467500951]

TZ = ZoneInfo("Europe/Amsterdam")
GEO_REQUIRED = os.getenv("GEO_REQUIRED", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("timesheet-bot")

# =========================
# ROLES
# =========================

class Role(str, Enum):
    ADMIN = "admin"
    EMPLOYEE = "employee"

# =========================
# STORAGE (JSON)
# =========================

USERS_FILE = "users.json"
users_lock = asyncio.Lock()

def load_users() -> Dict[str, Dict[str, Any]]:
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

async def save_users():
    async with users_lock:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)

users = load_users()

def ensure_user(tg_id: int) -> Dict[str, Any]:
    uid = str(tg_id)
    if uid not in users:
        users[uid] = {
            "fio": None,
            "phone": None,
            "position": None,
            "approved": tg_id in ADMIN_IDS,
            "role": Role.ADMIN if tg_id in ADMIN_IDS else Role.EMPLOYEE,
            "start": None,
            "geo_pending": False,
        }
    return users[uid]

def now() -> datetime:
    return datetime.now(TZ)

def calc_hours(start: datetime, end: datetime) -> float:
    h = (end - start).total_seconds() / 3600
    return min(8, round(h * 2) / 2)

# =========================
# PDF
# =========================

def make_pdf(path: str, title: str, rows: list):
    c = canvas.Canvas(path, pagesize=A4)
    w, h = A4

    y = h - 40
    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, y, title)
    y -= 30
    c.setFont("Helvetica", 10)

    for r in rows:
        c.drawString(40, y, " | ".join(map(str, r)))
        y -= 14
        if y < 40:
            c.showPage()
            y = h - 40

    c.save()

# =========================
# KEYBOARDS
# =========================

def main_kb(user):
    kb = [
        ["Пришел", "Ушел"],
        ["Мой статус"],
    ]
    if user["role"] == Role.ADMIN:
        kb.append(["📋 Сотрудники", "📊 Отчёт за день"])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

kb_phone = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отправить телефон", request_contact=True)]],
    resize_keyboard=True,
)

kb_geo = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отправить геолокацию", request_location=True)]],
    resize_keyboard=True,
)

# =========================
# FSM
# =========================

class AuthFSM(StatesGroup):
    fio = State()
    position = State()

# =========================
# ROUTER
# =========================

router = Router()

# =========================
# AUTH
# =========================

@router.message(Command("start"))
async def start_cmd(msg: Message, state: FSMContext):
    user = ensure_user(msg.from_user.id)
    await save_users()

    if user["approved"]:
        await msg.answer("✅ Доступ разрешён", reply_markup=main_kb(user))
    else:
        await msg.answer("Отправь номер телефона", reply_markup=kb_phone)

@router.message(F.contact)
async def get_phone(msg: Message, state: FSMContext):
    if msg.contact.user_id != msg.from_user.id:
        await msg.answer("❗ Отправь *свой* контакт кнопкой ниже", reply_markup=kb_phone)
        return

    user = ensure_user(msg.from_user.id)
    user["phone"] = msg.contact.phone_number
    await save_users()

    await state.set_state(AuthFSM.fio)
    await msg.answer("Введи ФИО")

@router.message(AuthFSM.fio)
async def get_fio(msg: Message, state: FSMContext):
    user = ensure_user(msg.from_user.id)
    user["fio"] = msg.text.strip()
    await state.set_state(AuthFSM.position)
    await msg.answer("Введи должность")

@router.message(AuthFSM.position)
async def get_position(msg: Message, state: FSMContext):
    user = ensure_user(msg.from_user.id)
    user["position"] = msg.text.strip()
    user["approved"] = False
    await save_users()
    await state.clear()

    for admin in ADMIN_IDS:
        await msg.bot.send_message(
            admin,
            f"🔔 Новая заявка\n\n{user['fio']}\n{user['position']}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="✅ Одобрить",
                        callback_data=f"approve:{msg.from_user.id}"
                    )
                ]]
            )
        )

    await msg.answer("⏳ Заявка отправлена, ожидай подтверждения")

@router.callback_query(F.data.startswith("approve:"))
async def approve(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return

    uid = cb.data.split(":")[1]
    if uid in users:
        users[uid]["approved"] = True
        await save_users()
        await cb.bot.send_message(int(uid), "✅ Доступ одобрен")

    await cb.answer("OK")

# =========================
# WORKDAY
# =========================

@router.message(F.text == "Пришел")
async def arrived(msg: Message):
    user = ensure_user(msg.from_user.id)
    if not user["approved"]:
        return

    user["start"] = now().isoformat()

    if GEO_REQUIRED:
        user["geo_pending"] = True
        await save_users()
        await msg.answer("Отправь геолокацию", reply_markup=kb_geo)
    else:
        user["geo_pending"] = False
        await save_users()
        await msg.answer("✅ Смена началась", reply_markup=main_kb(user))

@router.message(F.location)
async def got_geo(msg: Message):
    user = ensure_user(msg.from_user.id)

    # Лог для отладки
    logger.info(
        "GEO from %s | lat=%s lon=%s live=%s pending=%s",
        msg.from_user.id,
        msg.location.latitude,
        msg.location.longitude,
        msg.location.live_period,
        user.get("geo_pending"),
    )

    if not user.get("geo_pending"):
        await msg.answer("Геолокация получена, но сейчас она не требуется.")
        return

    user["geo_pending"] = False
    await save_users()

    await msg.answer("✅ Геолокация принята. Смена началась", reply_markup=main_kb(user))


# =========================
# REPORTS
# =========================

@router.message(F.text == "📋 Сотрудники")
async def employees(msg: Message):
    user = ensure_user(msg.from_user.id)
    if user["role"] != Role.ADMIN:
        return

    text = "👷 Сотрудники:\n\n"
    for u in users.values():
        if u["approved"]:
            status = "🟢" if u["start"] else "⚪️"
            text += f"{status} {u['fio']}\n"

    await msg.answer(text)

@router.message(F.text == "📊 Отчёт за день")
async def daily_report(msg: Message):
    today = date.today()
    rows = [
        [u["fio"], u["position"]]
        for u in users.values()
        if u["approved"]
    ]

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
        make_pdf(f.name, f"Отчёт за {today}", rows)
        await msg.answer_document(FSInputFile(f.name))

# =========================
# SCHEDULER
# =========================

async def scheduled_report(bot: Bot):
    today = now().date()
    last_day = monthrange(today.year, today.month)[1]

    if today.day in (15, last_day):
        for admin in ADMIN_IDS:
            await bot.send_message(admin, "📤 Автоотчёт готов")

# =========================
# MAIN
# =========================

async def main():
    if not API_TOKEN:
        raise RuntimeError("API_TOKEN not set")

    logger.info("Bot starting | GEO_REQUIRED=%s", GEO_REQUIRED)

    bot = Bot(API_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = AsyncIOScheduler(timezone=str(TZ))
    scheduler.add_job(scheduled_report, "cron", hour=18, args=[bot])
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
