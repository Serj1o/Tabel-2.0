#!/usr/bin/env python3
"""
Telegram бот для учета рабочего времени с геолокацией
Автоматическое заполнение табеля в Excel
Версия для Railway с PostgreSQL
"""

import asyncio
import os
import logging
from datetime import datetime, timedelta, date, time
from typing import Dict, Optional
import math
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

from geopy.distance import geodesic
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from geopy.distance import geodesic
import asyncpg
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Настройка
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "467500951").split(",")]
    DATABASE_URL = os.getenv("DATABASE_URL", "")
    GEO_REQUIRED = os.getenv("GEO_REQUIRED", "true").lower() == "true"
    LOCATION_RADIUS = int(os.getenv("LOCATION_RADIUS", "500"))
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    EMAIL_RECIPIENTS = [x.strip() for x in os.getenv("EMAIL_RECIPIENTS", "").split(",") if x.strip()]
    WORK_START_HOUR = 9
    MAX_WORK_HOURS = 8

if not Config.BOT_TOKEN or not Config.DATABASE_URL:
    raise ValueError("Проверьте BOT_TOKEN и DATABASE_URL в переменных окружения")

# Состояния
class Form(StatesGroup):
    waiting_for_location = State()
    waiting_for_object = State()
    waiting_for_employee_name = State()
    waiting_for_object_data = State()
    waiting_for_sick_reason = State()

# Основной класс бота
class WorkTimeBot:
    def __init__(self):
        self.bot = Bot(token=Config.BOT_TOKEN)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
        self.pool = None
        self.register_handlers()
    
    async def init_db(self):
        """Инициализация БД"""
        self.pool = await asyncpg.create_pool(Config.DATABASE_URL)
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS employees (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE,
                    full_name VARCHAR(255) NOT NULL,
                    is_admin BOOLEAN DEFAULT FALSE,
                    is_active BOOLEAN DEFAULT TRUE,
                    is_approved BOOLEAN DEFAULT FALSE
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS objects (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    address TEXT,
                    latitude DECIMAL(10, 8),
                    longitude DECIMAL(11, 8),
                    radius INTEGER DEFAULT 500
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS time_logs (
                    id SERIAL PRIMARY KEY,
                    employee_id INTEGER REFERENCES employees(id),
                    object_id INTEGER REFERENCES objects(id),
                    date DATE NOT NULL,
                    check_in TIMESTAMP,
                    check_out TIMESTAMP,
                    check_in_lat DECIMAL(10, 8),
                    check_in_lon DECIMAL(11, 8),
                    hours_worked DECIMAL(4, 2) DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'work',
                    notes TEXT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS access_requests (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    full_name VARCHAR(255),
                    status VARCHAR(20) DEFAULT 'pending'
                )
            ''')
            
            # Создаем администратора
            admin_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM employees WHERE telegram_id = $1)",
                Config.ADMIN_IDS[0]
            )
            
            if not admin_exists:
                await conn.execute('''
                    INSERT INTO employees (telegram_id, full_name, is_admin, is_approved)
                    VALUES ($1, $2, TRUE, TRUE)
                ''', Config.ADMIN_IDS[0], "Главный Администратор")
                
                # Тестовые объекты
                test_objects = [
                    ("Офис Москва", "ул. Тверская, 1", 55.7558, 37.6173, 200),
                    ("Склад Подольск", "Подольск, ул. Промышленная, 15", 55.4297, 37.5440, 300),
                    ("Удаленная работа", None, None, None, None),
                ]
                for obj in test_objects:
                    await conn.execute('''
                        INSERT INTO objects (name, address, latitude, longitude, radius)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT DO NOTHING
                    ''', *obj)
            
            logger.info("База данных инициализирована")
    
    def register_handlers(self):
        """Регистрация обработчиков"""
        commands = {
            "start": self.handle_start,
            "checkin": self.handle_checkin,
            "checkout": self.handle_checkout,
            "sick": self.handle_sick,
            "my_stats": self.handle_my_stats,
            "select_object": self.handle_select_object,
            "admin": self.handle_admin,
            "request_access": self.handle_request_access,
        }
        
        for cmd, handler in commands.items():
            self.dp.message.register(handler, Command(cmd))
        
        self.dp.message.register(self.handle_location, F.location)
        self.dp.message.register(self.process_text, Form.waiting_for_employee_name)
        self.dp.message.register(self.process_object_data, Form.waiting_for_object_data)
        self.dp.message.register(self.process_sick_reason, Form.waiting_for_sick_reason)
        self.dp.callback_query.register(self.handle_callback, F.data.startswith("obj_") | F.data.startswith("admin_") | F.data.startswith("approve_") | F.data.startswith("reject_"))
    
    # Вспомогательные методы
    async def check_access(self, user_id: int, need_admin: bool = False) -> bool:
        """Проверка доступа пользователя"""
        if not self.pool:
            return False
        
        async with self.pool.acquire() as conn:
            query = '''
                SELECT is_approved, is_admin FROM employees 
                WHERE telegram_id = $1 AND is_active = TRUE
            '''
            user = await conn.fetchrow(query, user_id)
            
            if not user:
                return False
            
            if need_admin:
                return user['is_admin'] and user['is_approved']
            
            return user['is_approved']
    
    # Основные обработчики
    async def handle_start(self, message: types.Message, state: FSMContext):
        """Обработка /start"""
        user_id = message.from_user.id
        
        async with self.pool.acquire() as conn:
            user = await conn.fetchrow('''
                SELECT full_name, is_admin, is_approved FROM employees 
                WHERE telegram_id = $1
            ''', user_id)
        
        if user and user['is_approved']:
            text = f"👋 Добро пожаловать, {user['full_name']}!\n\n"
            if user['is_admin']:
                text += "⚙️ Вы администратор. /admin - панель управления\n\n"
            text += "📋 Команды:\n/checkin - отметка прихода\n/checkout - отметка ухода\n/sick - больничный\n/my_stats - статистика\n/select_object - выбор объекта"
            await message.answer(text)
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📝 Запросить доступ", callback_data="request_access")
            ]])
            await message.answer("🔒 Это закрытая система. Запросите доступ:", reply_markup=keyboard)
    
    async def handle_request_access(self, message: types.Message, state: FSMContext):
        """Запрос доступа"""
        user_id = message.from_user.id
        
        async with self.pool.acquire() as conn:
            existing = await conn.fetchval('''
                SELECT status FROM access_requests WHERE telegram_id = $1 ORDER BY id DESC LIMIT 1
            ''', user_id)
            
            if existing == 'pending':
                await message.answer("⏳ Ваш запрос уже на рассмотрении.")
                return
        
        await message.answer("📝 Введите ваше ФИО для запроса доступа:")
        await state.set_state(Form.waiting_for_employee_name)
        await state.update_data(action="request_access", user_id=user_id)
    
    async def handle_checkin(self, message: types.Message, state: FSMContext):
        """Обработка /checkin"""
        user_id = message.from_user.id
        
        if not await self.check_access(user_id):
            await message.answer("❌ Доступ запрещен.")
            return
        
        today = date.today()
        async with self.pool.acquire() as conn:
            existing = await conn.fetchval('''
                SELECT check_in FROM time_logs 
                WHERE employee_id = (SELECT id FROM employees WHERE telegram_id = $1) AND date = $2
                LIMIT 1
            ''', user_id, today)
            
            if existing:
                await message.answer(f"✅ Вы уже отметили приход в {existing.strftime('%H:%M')}")
                return
        
        if Config.GEO_REQUIRED:
            keyboard = ReplyKeyboardMarkup(keyboard=[[
                KeyboardButton(text="📍 Отправить геолокацию", request_location=True)
            ]], resize_keyboard=True)
            await message.answer("📍 Отправьте геолокацию для отметки прихода:", reply_markup=keyboard)
            await state.set_state(Form.waiting_for_location)
            await state.update_data(action="checkin")
        else:
            await self.show_objects(message, state, "checkin")
    
    async def handle_checkout(self, message: types.Message, state: FSMContext):
        """Обработка /checkout"""
        user_id = message.from_user.id
        
        if not await self.check_access(user_id):
            await message.answer("❌ Доступ запрещен.")
            return
        
        today = date.today()
        async with self.pool.acquire() as conn:
            log = await conn.fetchrow('''
                SELECT id, check_in FROM time_logs 
                WHERE employee_id = (SELECT id FROM employees WHERE telegram_id = $1) AND date = $2
                AND check_out IS NULL
                LIMIT 1
            ''', user_id, today)
            
            if not log:
                await message.answer("❌ Сначала отметьте приход (/checkin)")
                return
        
        if Config.GEO_REQUIRED:
            keyboard = ReplyKeyboardMarkup(keyboard=[[
                KeyboardButton(text="📍 Отправить геолокацию", request_location=True)
            ]], resize_keyboard=True)
            await message.answer("📍 Отправьте геолокацию для отметки ухода:", reply_markup=keyboard)
            await state.set_state(Form.waiting_for_location)
            await state.update_data(action="checkout", log_id=log['id'])
        else:
            await self.process_checkout_simple(user_id, log['id'])
    
    async def handle_sick(self, message: types.Message, state: FSMContext):
        """Обработка /sick"""
        user_id = message.from_user.id
        
        if not await self.check_access(user_id):
            await message.answer("❌ Доступ запрещен.")
            return
        
        await message.answer("🏥 Введите причину больничного (необязательно):")
        await state.set_state(Form.waiting_for_sick_reason)
        await state.update_data(user_id=user_id)
    
    async def handle_my_stats(self, message: types.Message):
        """Обработка /my_stats"""
        user_id = message.from_user.id
        
        if not await self.check_access(user_id):
            await message.answer("❌ Доступ запрещен.")
            return
        
        today = date.today()
        month_start = date(today.year, today.month, 1)
        
        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT 
                    COUNT(*) as days_worked,
                    SUM(hours_worked) as total_hours,
                    SUM(CASE WHEN status = 'sick' THEN 1 ELSE 0 END) as sick_days
                FROM time_logs 
                WHERE employee_id = (SELECT id FROM employees WHERE telegram_id = $1)
                AND date >= $2 AND date <= $3
            ''', user_id, month_start, today)
            
            today_log = await conn.fetchrow('''
                SELECT check_in, check_out, hours_worked, status 
                FROM time_logs 
                WHERE employee_id = (SELECT id FROM employees WHERE telegram_id = $1) AND date = $2
                LIMIT 1
            ''', user_id, today)
        
        text = f"📊 Статистика за {today.strftime('%B %Y')}:\n\n"
        
        if stats:
            text += f"📅 Дней: {stats['days_worked'] or 0}\n"
            text += f"⏱️ Часов: {stats['total_hours'] or 0:.1f}\n"
            text += f"🤒 Больничных: {stats['sick_days'] or 0}\n"
        
        text += "\n📌 Сегодня:\n"
        if today_log:
            if today_log['status'] == 'sick':
                text += "🏥 Больничный\n"
            else:
                if today_log['check_in']:
                    text += f"↘️ Приход: {today_log['check_in'].strftime('%H:%M')}\n"
                if today_log['check_out']:
                    text += f"↗️ Уход: {today_log['check_out'].strftime('%H:%M')}\n"
                    text += f"⏱️ Часов: {today_log['hours_worked'] or 0:.1f}\n"
                else:
                    text += "↗️ Еще на работе\n"
        else:
            text += "📭 Нет отметок\n"
        
        await message.answer(text)
    
    async def handle_select_object(self, message: types.Message, state: FSMContext):
        """Обработка /select_object"""
        user_id = message.from_user.id
        
        if not await self.check_access(user_id):
            await message.answer("❌ Доступ запрещен.")
            return
        
        await self.show_objects(message, state, "select")
    
    async def handle_admin(self, message: types.Message):
        """Панель администратора"""
        user_id = message.from_user.id
        
        if not await self.check_access(user_id, need_admin=True):
            await message.answer("❌ Нет прав администратора.")
            return
        
        keyboard = InlineKeyboardBuilder()
        buttons = [
            ("👥 Запросы", "admin_requests"),
            ("📊 Табель", "admin_timesheet"),
            ("📧 Отправить", "admin_send"),
            ("👤 Сотрудники", "admin_employees"),
            ("🏗️ Объекты", "admin_objects"),
            ("📈 Статистика", "admin_stats"),
        ]
        
        for text, data in buttons:
            keyboard.button(text=text, callback_data=data)
        keyboard.adjust(2)
        
        await message.answer("⚙️ Панель администратора:", reply_markup=keyboard.as_markup())
    
    # Обработчики состояний
    async def process_text(self, message: types.Message, state: FSMContext):
        """Обработка текстовых сообщений"""
        data = await state.get_data()
        action = data.get("action")
        
        if action == "request_access":
            full_name = message.text.strip()
            user_id = data.get("user_id")
            
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO access_requests (telegram_id, full_name) VALUES ($1, $2)
                ''', user_id, full_name)
                
                # Уведомляем администраторов
                admins = await conn.fetch('SELECT telegram_id FROM employees WHERE is_admin = TRUE')
                for admin in admins:
                    try:
                        await self.bot.send_message(
                            admin['telegram_id'],
                            f"🆕 Новый запрос на доступ:\n👤 {full_name}\n🆔 {user_id}"
                        )
                    except:
                        pass
            
            await message.answer("✅ Запрос отправлен. Ожидайте подтверждения.")
            await state.clear()
    
    async def process_sick_reason(self, message: types.Message, state: FSMContext):
        """Обработка причины больничного"""
        data = await state.get_data()
        user_id = data.get("user_id")
        reason = message.text
        
        today = date.today()
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO time_logs (employee_id, date, status, notes)
                VALUES ((SELECT id FROM employees WHERE telegram_id = $1), $2, 'sick', $3)
            ''', user_id, today, reason)
        
        await message.answer(f"✅ Больничный отмечен\n📝 Причина: {reason if reason else 'не указана'}")
        await state.clear()
    
    async def process_object_data(self, message: types.Message, state: FSMContext):
        """Обработка данных объекта"""
        parts = message.text.strip().split("|")
        name = parts[0].strip()
        address = parts[1].strip() if len(parts) > 1 else None
        
        lat = lon = radius = None
        if len(parts) > 2: lat = float(parts[2]) if parts[2].strip() else None
        if len(parts) > 3: lon = float(parts[3]) if parts[3].strip() else None
        if len(parts) > 4: radius = int(parts[4]) if parts[4].strip() else 500
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO objects (name, address, latitude, longitude, radius)
                VALUES ($1, $2, $3, $4, $5)
            ''', name, address, lat, lon, radius)
        
        await message.answer(f"✅ Объект добавлен: {name}")
        await state.clear()
    
    # Обработчики геолокации и колбэков
    async def handle_location(self, message: types.Message, state: FSMContext):
        """Обработка геолокации"""
        data = await state.get_data()
        action = data.get("action")
        location = message.location
        
        # Находим ближайший объект
        async with self.pool.acquire() as conn:
            objects = await conn.fetch('SELECT id, name, latitude, longitude, radius FROM objects')
        
        nearest = None
        min_dist = float('inf')
        
        for obj in objects:
            if obj['latitude'] and obj['longitude']:
                dist = geodesic((location.latitude, location.longitude), 
                               (obj['latitude'], obj['longitude'])).meters
                if dist < min_dist:
                    min_dist = dist
                    nearest = obj
        
        if not nearest or min_dist > Config.LOCATION_RADIUS:
            await self.show_objects(message, state, action)
            await state.update_data(lat=location.latitude, lon=location.longitude)
            return
        
        if action == "checkin":
            await self.process_checkin_with_location(message.from_user.id, location, nearest, min_dist)
        elif action == "checkout":
            await self.process_checkout_with_location(data.get("log_id"), location, nearest, min_dist)
        
        await state.clear()
        await message.answer("✅ Местоположение подтверждено!", reply_markup=types.ReplyKeyboardRemove())
    
    async def handle_callback(self, callback: types.CallbackQuery, state: FSMContext):
        """Обработка callback-запросов"""
        data = callback.data
        
        if data.startswith("obj_"):
            await self.handle_object_selection(callback, state)
        elif data.startswith("approve_"):
            await self.handle_approval(callback)
        elif data.startswith("reject_"):
            await self.handle_rejection(callback)
        elif data == "admin_requests":
            await self.show_requests(callback)
        elif data == "admin_timesheet":
            await self.generate_timesheet(callback)
        elif data == "admin_send":
            await self.send_timesheet_email(callback)
        elif data == "admin_employees":
            await self.show_employees(callback)
        elif data == "admin_objects":
            await self.show_objects_admin(callback)
        elif data == "admin_stats":
            await self.show_stats(callback)
        elif data == "request_access":
            await callback.message.answer("📝 Введите ваше ФИО для запроса доступа:")
            await state.set_state(Form.waiting_for_employee_name)
            await state.update_data(action="request_access", user_id=callback.from_user.id)
        
        await callback.answer()
    
    # Методы обработки данных
    async def process_checkin_with_location(self, user_id: int, location, obj, distance: float):
        """Обработка прихода с геолокацией"""
        now = datetime.now()
        status = 'work'
        work_start = datetime.combine(now.date(), time(Config.WORK_START_HOUR, 0))
        if now > work_start + timedelta(minutes=15):
            status = 'late'
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO time_logs (employee_id, object_id, date, check_in, check_in_lat, check_in_lon, status)
                VALUES ((SELECT id FROM employees WHERE telegram_id = $1), $2, $3, $4, $5, $6, $7)
            ''', user_id, obj['id'], now.date(), now, location.latitude, location.longitude, status)
        
        await self.bot.send_message(
            user_id,
            f"✅ Приход отмечен!\n"
            f"⏰ {now.strftime('%H:%M')}\n"
            f"🏢 {obj['name']}\n"
            f"📍 {distance:.0f} м\n"
            f"{'⚠️ Опоздание!' if status == 'late' else ''}"
        )
    
    async def process_checkout_with_location(self, log_id: int, location, obj, distance: float):
        """Обработка ухода с геолокацией"""
        now = datetime.now()
        
        async with self.pool.acquire() as conn:
            # Получаем время прихода
            check_in = await conn.fetchval('SELECT check_in FROM time_logs WHERE id = $1', log_id)
            
            # Рассчитываем часы
            hours = (now - check_in).seconds / 3600
            hours = min(math.ceil(hours), Config.MAX_WORK_HOURS)
            
            # Обновляем запись
            await conn.execute('''
                UPDATE time_logs SET check_out = $1, check_out_lat = $2, check_out_lon = $3,
                hours_worked = $4, object_id = $5 WHERE id = $6
            ''', now, location.latitude, location.longitude, hours, obj['id'], log_id)
        
        await self.bot.send_message(
            (await self.pool.acquire()).fetchval('''
                SELECT telegram_id FROM employees WHERE id = (
                    SELECT employee_id FROM time_logs WHERE id = $1
                )
            ''', log_id),
            f"✅ Уход отмечен!\n⏰ {now.strftime('%H:%M')}\n🏢 {obj['name']}\n⏱️ {hours} ч."
        )
    
    async def process_checkout_simple(self, user_id: int, log_id: int):
        """Уход без геолокации"""
        now = datetime.now()
        
        async with self.pool.acquire() as conn:
            check_in = await conn.fetchval('SELECT check_in FROM time_logs WHERE id = $1', log_id)
            hours = (now - check_in).seconds / 3600
            hours = min(math.ceil(hours), Config.MAX_WORK_HOURS)
            
            await conn.execute('UPDATE time_logs SET check_out = $1, hours_worked = $2 WHERE id = $3', 
                             now, hours, log_id)
        
        await self.bot.send_message(user_id, f"✅ Уход отмечен в {now.strftime('%H:%M')}\n⏱️ {hours} ч.")
    
    async def show_objects(self, message: types.Message, state: FSMContext, action: str):
        """Показать список объектов"""
        async with self.pool.acquire() as conn:
            objects = await conn.fetch('SELECT id, name FROM objects ORDER BY name')
        
        keyboard = InlineKeyboardBuilder()
        for obj in objects:
            keyboard.button(text=obj['name'], callback_data=f"obj_{obj['id']}")
        keyboard.adjust(1)
        
        text = "🏢 Выберите объект:" if action == "select" else "🏢 Выберите объект для работы:"
        await message.answer(text, reply_markup=keyboard.as_markup())
        await state.set_state(Form.waiting_for_object)
        await state.update_data(action=action)
    
    async def handle_object_selection(self, callback: types.CallbackQuery, state: FSMContext):
        """Обработка выбора объекта"""
        obj_id = int(callback.data.split("_")[1])
        data = await state.get_data()
        action = data.get("action")
        user_id = callback.from_user.id
        
        async with self.pool.acquire() as conn:
            obj_name = await conn.fetchval('SELECT name FROM objects WHERE id = $1', obj_id)
        
        if action == "select":
            await callback.message.answer(f"✅ Объект '{obj_name}' выбран")
        
        elif action == "checkin":
            lat = data.get("lat")
            lon = data.get("lon")
            
            if lat and lon:
                distance = 0
                async with self.pool.acquire() as conn:
                    obj = await conn.fetchrow('SELECT latitude, longitude FROM objects WHERE id = $1', obj_id)
                    if obj['latitude']:
                        distance = geodesic((lat, lon), (obj['latitude'], obj['longitude'])).meters
                
                await self.process_checkin_with_location(user_id, 
                    types.Location(latitude=lat, longitude=lon), 
                    {'id': obj_id, 'name': obj_name}, distance)
            else:
                now = datetime.now()
                async with self.pool.acquire() as conn:
                    await conn.execute('''
                        INSERT INTO time_logs (employee_id, object_id, date, check_in)
                        VALUES ((SELECT id FROM employees WHERE telegram_id = $1), $2, $3, $4)
                    ''', user_id, obj_id, now.date(), now)
                
                await callback.message.answer(f"✅ Приход отмечен в {now.strftime('%H:%M')}\n🏢 {obj_name}")
        
        await state.clear()
    
    # Админ-методы
    async def show_requests(self, callback: types.CallbackQuery):
        """Показать запросы на доступ"""
        async with self.pool.acquire() as conn:
            requests = await conn.fetch('''
                SELECT id, telegram_id, full_name FROM access_requests 
                WHERE status = 'pending' ORDER BY id DESC
            ''')
        
        if not requests:
            await callback.message.answer("✅ Нет ожидающих запросов")
            return
        
        for req in requests:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_{req['id']}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{req['id']}")
            ]])
            await callback.message.answer(f"🆕 Запрос #{req['id']}\n👤 {req['full_name']}\n🆔 {req['telegram_id']}", 
                                         reply_markup=keyboard)
    
    async def handle_approval(self, callback: types.CallbackQuery):
        """Одобрить запрос"""
        req_id = int(callback.data.split("_")[1])
        
        async with self.pool.acquire() as conn:
            req = await conn.fetchrow('''
                SELECT telegram_id, full_name FROM access_requests WHERE id = $1
            ''', req_id)
            
            if not req:
                await callback.answer("Запрос не найден")
                return
            
            await conn.execute('''
                UPDATE access_requests SET status = 'approved' WHERE id = $1
            ''', req_id)
            
            await conn.execute('''
                INSERT INTO employees (telegram_id, full_name, is_approved)
                VALUES ($1, $2, TRUE)
                ON CONFLICT (telegram_id) DO UPDATE SET is_approved = TRUE
            ''', req['telegram_id'], req['full_name'])
        
        await callback.message.edit_text(f"✅ Запрос одобрен\n👤 {req['full_name']}")
        
        try:
            await self.bot.send_message(req['telegram_id'], 
                "🎉 Ваш запрос на доступ одобрен! Используйте /start для начала работы.")
        except:
            pass
    
    async def handle_rejection(self, callback: types.CallbackQuery):
        """Отклонить запрос"""
        req_id = int(callback.data.split("_")[1])
        
        async with self.pool.acquire() as conn:
            req = await conn.fetchrow('SELECT full_name FROM access_requests WHERE id = $1', req_id)
            await conn.execute("UPDATE access_requests SET status = 'rejected' WHERE id = $1", req_id)
        
        await callback.message.edit_text(f"❌ Запрос отклонен\n👤 {req['full_name']}")
    
    async def generate_timesheet(self, callback: types.CallbackQuery):
        """Сгенерировать табель"""
        try:
            excel_file = await self.create_excel_report()
            
            if excel_file:
                await self.bot.send_document(
                    chat_id=callback.from_user.id,
                    document=types.BufferedInputFile(
                        excel_file.getvalue(),
                        filename=f"Табель_{datetime.now().strftime('%Y_%m_%d')}.xlsx"
                    ),
                    caption="📊 Табель учета рабочего времени"
                )
            else:
                await callback.message.answer("❌ Ошибка формирования табеля")
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await callback.message.answer(f"❌ Ошибка: {str(e)}")
    
    async def create_excel_report(self):
        """Создание Excel отчета"""
        today = date.today()
        year, month = today.year, today.month
        
        # Получаем данные
        async with self.pool.acquire() as conn:
            employees = await conn.fetch('''
                SELECT id, full_name FROM employees 
                WHERE is_approved = TRUE AND is_active = TRUE 
                ORDER BY full_name
            ''')
            
            logs = await conn.fetch('''
                SELECT tl.employee_id, tl.date, tl.check_in, tl.check_out,
                       tl.hours_worked, tl.status, tl.check_in_lat, tl.check_in_lon,
                       o.name as object_name, e.full_name
                FROM time_logs tl
                JOIN employees e ON tl.employee_id = e.id
                LEFT JOIN objects o ON tl.object_id = o.id
                WHERE EXTRACT(YEAR FROM tl.date) = $1 AND EXTRACT(MONTH FROM tl.date) = $2
                ORDER BY e.full_name, tl.date
            ''', year, month)
        
        if not employees:
            return None
        
        # Создаем Excel
        wb = Workbook()
        
        # Лист 1: Табель
        ws1 = wb.active
        ws1.title = "Табель"
        ws1.append(["ФИО"] + [str(d) for d in range(1, 32)] + ["Итого часов", "Дней"])
        
        # Заполняем данные
        for emp in employees:
            row = [emp['full_name']] + [""] * 31 + [0, 0]
            total_hours = 0
            days = 0
            
            for log in logs:
                if log['employee_id'] == emp['id']:
                    day = log['date'].day
                    if log['status'] == 'sick':
                        row[day] = 'Б'
                    elif log['hours_worked']:
                        row[day] = f"{log['hours_worked']:.1f}"
                        total_hours += log['hours_worked']
                        days += 1
            
            row[32] = total_hours
            row[33] = days
            ws1.append(row)
        
        # Лист 2: TimeLog
        ws2 = wb.create_sheet("TimeLog")
        ws2.append(["ФИО", "Дата", "Приход", "Уход", "Часы", "Объект", "Координаты"])
        
        for log in logs:
            coords = ""
            if log['check_in_lat']:
                coords = f"{log['check_in_lat']:.4f}, {log['check_in_lon']:.4f}"
            
            ws2.append([
                log['full_name'],
                log['date'].strftime('%d.%m.%Y'),
                log['check_in'].strftime('%H:%M') if log['check_in'] else "",
                log['check_out'].strftime('%H:%M') if log['check_out'] else "",
                log['hours_worked'] or 0,
                log['object_name'] or "",
                coords
            ])
        
        # Сохраняем
        excel_file = io.BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)
        return excel_file
    
    async def send_timesheet_email(self, callback: types.CallbackQuery):
        """Отправка табеля по email"""
        if not Config.SMTP_USERNAME:
            await callback.message.answer("❌ SMTP не настроен")
            return
        
        await callback.message.answer("⏳ Отправляю табель...")
        
        try:
            excel_file = await self.create_excel_report()
            if not excel_file:
                await callback.message.answer("❌ Ошибка формирования табеля")
                return
            
            # Отправка email
            msg = MIMEMultipart()
            msg['From'] = Config.SMTP_USERNAME
            msg['To'] = ', '.join(Config.EMAIL_RECIPIENTS)
            msg['Subject'] = f"Табель {datetime.now().strftime('%B %Y')}"
            
            msg.attach(MIMEText("Табель учета рабочего времени во вложении", 'plain'))
            
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(excel_file.getvalue())
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', 
                                f'attachment; filename="Табель_{datetime.now().strftime("%Y_%m")}.xlsx"')
            msg.attach(attachment)
            
            with smtplib.SMTP(Config.SMTP_SERVER, Config.SMTP_PORT) as server:
                server.starttls()
                server.login(Config.SMTP_USERNAME, Config.SMTP_PASSWORD)
                server.send_message(msg)
            
            await callback.message.answer("✅ Табель отправлен по email")
        except Exception as e:
            logger.error(f"Ошибка отправки: {e}")
            await callback.message.answer(f"❌ Ошибка отправки: {str(e)}")
    
    async def show_employees(self, callback: types.CallbackQuery):
        """Показать список сотрудников"""
        async with self.pool.acquire() as conn:
            employees = await conn.fetch('''
                SELECT full_name, telegram_id, is_admin, 
                       (SELECT COUNT(*) FROM time_logs WHERE employee_id = employees.id 
                        AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)) as days_worked
                FROM employees WHERE is_approved = TRUE ORDER BY full_name
            ''')
        
        text = "👥 Сотрудники:\n\n"
        for emp in employees:
            role = "👑" if emp['is_admin'] else "👤"
            text += f"{role} {emp['full_name']}\nID: {emp['telegram_id']}, Дней: {emp['days_worked']}\n\n"
        
        await callback.message.answer(text)
    
    async def show_objects_admin(self, callback: types.CallbackQuery):
        """Показать объекты"""
        async with self.pool.acquire() as conn:
            objects = await conn.fetch('SELECT name, address, latitude, longitude FROM objects ORDER BY name')
        
        text = "🏗️ Объекты:\n\n"
        for obj in objects:
            text += f"🏢 {obj['name']}\n"
            if obj['address']:
                text += f"📍 {obj['address']}\n"
            if obj['latitude']:
                text += f"🌍 {obj['latitude']:.4f}, {obj['longitude']:.4f}\n"
            text += "\n"
        
        await callback.message.answer(text)
    
    async def show_stats(self, callback: types.CallbackQuery):
        """Показать статистику"""
        today = date.today()
        month_start = date(today.year, today.month, 1)
        
        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT 
                    COUNT(DISTINCT e.id) as total_employees,
                    COUNT(DISTINCT tl.employee_id) as worked_this_month,
                    SUM(tl.hours_worked) as total_hours,
                    AVG(tl.hours_worked) as avg_hours
                FROM employees e
                LEFT JOIN time_logs tl ON e.id = tl.employee_id 
                    AND tl.date >= $1 AND tl.date <= $2
                WHERE e.is_approved = TRUE AND e.is_active = TRUE
            ''', month_start, today)
            
            today_stats = await conn.fetchrow('''
                SELECT 
                    COUNT(DISTINCT employee_id) as worked_today,
                    SUM(hours_worked) as hours_today
                FROM time_logs WHERE date = $1
            ''', today)
        
        text = f"📈 Статистика за {today.strftime('%B %Y')}:\n\n"
        text += f"👥 Всего сотрудников: {stats['total_employees']}\n"
        text += f"✅ Работали в месяце: {stats['worked_this_month'] or 0}\n"
        text += f"⏱️ Всего часов: {stats['total_hours'] or 0:.1f}\n"
        text += f"📊 Среднее часов: {stats['avg_hours'] or 0:.1f}\n\n"
        text += f"📅 Сегодня:\n"
        text += f"✅ Работали: {today_stats['worked_today'] or 0}\n"
        text += f"⏱️ Часов: {today_stats['hours_today'] or 0:.1f}\n"
        
        await callback.message.answer(text)
    
    # Планировщик
    async def setup_scheduler(self):
        """Настройка планировщика задач"""
        self.scheduler.add_job(
            self.remind_checkout,
            CronTrigger(hour=19, minute=0),
            id="reminder"
        )
        self.scheduler.add_job(
            self.auto_checkout,
            CronTrigger(hour=23, minute=0),
            id="auto_checkout"
        )
        self.scheduler.start()
        logger.info("Планировщик запущен")
    
    async def remind_checkout(self):
        """Напоминание об уходе"""
        today = date.today()
        
        async with self.pool.acquire() as conn:
            employees = await conn.fetch('''
                SELECT e.telegram_id, e.full_name
                FROM time_logs tl
                JOIN employees e ON tl.employee_id = e.id
                WHERE tl.date = $1 AND tl.check_in IS NOT NULL AND tl.check_out IS NULL
                AND e.is_active = TRUE
            ''', today)
        
        for emp in employees:
            try:
                await self.bot.send_message(
                    emp['telegram_id'],
                    "🔔 Напоминание! Не забудьте отметить уход командой /checkout"
                )
            except:
                pass
    
    async def auto_checkout(self):
        """Автоматический уход в 23:00"""
        today = date.today()
        
        async with self.pool.acquire() as conn:
            logs = await conn.fetch('''
                SELECT tl.id, e.telegram_id, e.full_name, tl.check_in
                FROM time_logs tl
                JOIN employees e ON tl.employee_id = e.id
                WHERE tl.date = $1 AND tl.check_in IS NOT NULL AND tl.check_out IS NULL
            ''', today)
        
        now = datetime.now()
        for log in logs:
            try:
                hours = (now - log['check_in']).seconds / 3600
                hours = min(math.ceil(hours), Config.MAX_WORK_HOURS)
                
                async with self.pool.acquire() as conn:
                    await conn.execute('''
                        UPDATE time_logs SET check_out = $1, hours_worked = $2 WHERE id = $3
                    ''', now, hours, log['id'])
                
                await self.bot.send_message(
                    log['telegram_id'],
                    f"⏰ Автоматический уход в {now.strftime('%H:%M')}\n⏱️ Отработано: {hours} ч."
                )
            except:
                pass
    
    # Запуск
    async def run(self):
        """Запуск бота"""
        await self.init_db()
        await self.setup_scheduler()
        logger.info("Бот запущен")
        await self.dp.start_polling(self.bot)

# Точка входа
if __name__ == "__main__":
    bot = WorkTimeBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
