import asyncpg
import asyncio
import json
import logging
import math
import os
import re
import tempfile
from enum import Enum
from calendar import monthrange
from datetime import datetime, date
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

import gspread
from google.oauth2.service_account import Credentials

from openpyxl import Workbook
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

import sentry_sdk

# =====================================================
# CONFIG / ENV / SENTRY
# =====================================================

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "service_account.json")
SHEET_NAME = os.getenv("SHEET_NAME", "Табель учета")
SENTRY_DSN = os.getenv("SENTRY_DSN")

ADMIN_IDS = [467500951]

TZ = ZoneInfo("Europe/Amsterdam")
GEO_REQUIRED = os.getenv("GEO_REQUIRED", "false") == "true"

if SENTRY_DSN:
    sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=1.0)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("timesheet-bot")

# =====================================================
# ROLES / ENUMS
# =====================================================

class Role(str, Enum):
    ADMIN = "admin"
    MANAGER = "manager"
    EMPLOYEE = "employee"

# =====================================================
# GLOBAL STATE + LOCKS
# =====================================================

USERS_FILE = "users.json"
OBJECTS_FILE = "objects.json"

users_lock = asyncio.Lock()
objects_lock = asyncio.Lock()

# =====================================================
# STORAGE (SAFE JSON)
# =====================================================

def load_json(path: str, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

async def save_json(path: str, data):
    lock = users_lock if "user" in path else objects_lock
    async with lock:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

users: Dict[str, Dict[str, Any]] = load_json(USERS_FILE, {})
objects: Dict[str, Dict[str, Any]] = load_json(OBJECTS_FILE, {})

def ensure_user(tg_id: int):
    uid = str(tg_id)
    if uid not in users:
        users[uid] = {
            "fio": None,
            "phone": None,
            "position": None,
            "approved": tg_id in ADMIN_IDS,
            "role": Role.ADMIN if tg_id in ADMIN_IDS else Role.EMPLOYEE,
            "object": None,
            "start": None,
            "geo_pending": False,
            "leave_geo_pending": False,
        }
    return users[uid]

def has_role(user, *roles):
    return user.get("role") in roles

def now():
    return datetime.now(TZ)

# =====================================================
# GOOGLE SHEETS (CACHE + BATCH)
# =====================================================

class SheetCache:
    headers: Dict[str, Dict[str, int]] = {}

def gs_client():
    if not os.path.exists(GOOGLE_CREDS_FILE):
        logger.warning("Google creds not found")
        return None
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

gc = gs_client()
sheet = None
sh = None

if gc:
    try:
        sh = gc.open(SHEET_NAME)
        sheet = sh.sheet1
    except Exception as e:
        logger.exception("Cannot open sheet: %s", e)

def get_col(ws, name: str) -> Optional[int]:
    if ws.title not in SheetCache.headers:
        header = ws.row_values(1)
        SheetCache.headers[ws.title] = {h: i + 1 for i, h in enumerate(header)}
    return SheetCache.headers[ws.title].get(name)

def batch_update(ws, updates):
    ws.spreadsheet.values_batch_update({
        "valueInputOption": "USER_ENTERED",
        "data": updates
    })

# =====================================================
# HOURS / TIME HELPERS
# =====================================================

def calc_hours(start: datetime, end: datetime) -> float:
    h = (end - start).total_seconds() / 3600
    return min(8, round(h * 2) / 2)  # 0.5 часа

# =====================================================
# PDF REPORTS
# =====================================================

def make_pdf(path: str, title: str, rows: list):
    c = canvas.Canvas(path, pagesize=A4)
    w, h = A4

    y = h - 40
    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, y, title)

    c.setFont("Helvetica", 10)
    y -= 30

    for r in rows:
        c.drawString(40, y, " | ".join(map(str, r)))
        y -= 14
        if y < 40:
            c.showPage()
            y = h - 40

    c.save()

# =====================================================
# KEYBOARDS
# =====================================================

def main_kb(user):
    kb = [
        ["Пришел", "Ушел"],
        ["Мой статус", "Ещё"],
    ]
    if has_role(user, Role.ADMIN, Role.MANAGER):
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

# =====================================================
# FSM
# =====================================================

class AuthFSM(StatesGroup):
    fio = State()
    position = State()

# =====================================================
# ROUTER
# =====================================================

router = Router()

# =====================================================
# AUTH
# =====================================================

@router.message(Command("start"))
async def start_cmd(msg: Message, state: FSMContext):
    user = ensure_user(msg.from_user.id)
    await save_json(USERS_FILE, users)

    if user["approved"]:
        await msg.answer("✅ Доступ разрешён", reply_markup=main_kb(user))
        return

    await msg.answer("Отправь телефон", reply_markup=kb_phone)

@router.message(F.contact)
async def get_phone(msg: Message, state: FSMContext):
    user = ensure_user(msg.from_user.id)
    user["phone"] = msg.contact.phone_number
    await save_json(USERS_FILE, users)

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

    await save_json(USERS_FILE, users)
    await state.clear()

    for admin in ADMIN_IDS:
        await msg.bot.send_message(
            admin,
            f"🔔 Заявка\n{user['fio']}\n{user['position']}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{msg.from_user.id}")
                    ]
                ]
            )
        )

    await msg.answer("⏳ Ожидай подтверждения")

@router.callback_query(F.data.startswith("approve:"))
async def approve(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return

    uid = cb.data.split(":")[1]
    if uid in users:
        users[uid]["approved"] = True
        await save_json(USERS_FILE, users)
        await cb.bot.send_message(int(uid), "✅ Доступ одобрен")
    await cb.answer("OK")

# =====================================================
# WORKDAY
# =====================================================

@router.message(F.text == "Пришел")
async def arrived(msg: Message):
    user = ensure_user(msg.from_user.id)
    if not user["approved"]:
        return

    user["start"] = now().isoformat()
    user["geo_pending"] = True
    await save_json(USERS_FILE, users)

    await msg.answer("Отправь геолокацию", reply_markup=kb_geo)

@router.message(F.location)
async def got_geo(msg: Message):
    user = ensure_user(msg.from_user.id)
    if user["geo_pending"]:
        user["geo_pending"] = False
        await save_json(USERS_FILE, users)
        await msg.answer("✅ Смена началась", reply_markup=main_kb(user))

@router.message(F.text == "Ушел")
async def left(msg: Message):
    user = ensure_user(msg.from_user.id)
    if not user.get("start"):
        await msg.answer("Ты не на смене")
        return

    start = datetime.fromisoformat(user["start"])
    end = now()
    hours = calc_hours(start, end)

    user["start"] = None
    await save_json(USERS_FILE, users)

    await msg.answer(f"✅ Записано {hours} ч", reply_markup=main_kb(user))

# =====================================================
# MANAGER PANEL
# =====================================================

@router.message(F.text == "📋 Сотрудники")
async def employees(msg: Message):
    user = ensure_user(msg.from_user.id)
    if not has_role(user, Role.ADMIN, Role.MANAGER):
        return

    text = "👷 Сотрудники:\n\n"
    for u in users.values():
        if u["role"] == Role.EMPLOYEE:
            status = "🟢" if u["start"] else "⚪️"
            text += f"{status} {u['fio']}\n"

    await msg.answer(text)

# =====================================================
# REPORTS
# =====================================================

@router.message(F.text == "📊 Отчёт за день")
async def daily_report(msg: Message):
    today = date.today()
    rows = [[u["fio"], u["position"]] for u in users.values() if u["approved"]]

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
        make_pdf(f.name, f"Отчёт за {today}", rows)
        await msg.answer_document(FSInputFile(f.name))

# =====================================================
# SCHEDULER
# =====================================================

async def scheduled_report(bot: Bot):
    today = now().date()
    last_day = monthrange(today.year, today.month)[1]
    if today.day in (15, last_day):
        for admin in ADMIN_IDS:
            await bot.send_message(admin, "📤 Автоотчёт готов")

# =====================================================
# MAIN
# =====================================================

async def main():
    if not API_TOKEN:
        raise RuntimeError("API_TOKEN not set")

    bot = Bot(API_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = AsyncIOScheduler(timezone=str(TZ))
    scheduler.add_job(scheduled_report, "cron", hour=18, args=[bot])
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
