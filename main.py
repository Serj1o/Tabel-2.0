#!/usr/bin/env python3
"""
Telegram бот для учета рабочего времени с геолокацией
Автоматическое заполнение табеля в Excel и отправка по email
Версия для Railway с PostgreSQL и закрытым доступом
"""

import asyncio
import os
import logging
from datetime import datetime, timedelta, date, time
from typing import Dict, Tuple, Optional, List, Any
import math
import json
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter
from geopy.distance import geodesic
import asyncpg
from dotenv import load_dotenv

# Загрузка переменных окружения из Railway
load_dotenv()

# Для работы с Telegram ботом
try:
    from aiogram import Bot, Dispatcher, types, F
    from aiogram.filters import Command
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import (
        ReplyKeyboardMarkup, 
        KeyboardButton, 
        InlineKeyboardMarkup, 
        InlineKeyboardButton,
        ReplyKeyboardRemove,
        WebAppInfo
    )
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError as e:
    print("Установите необходимые библиотеки:")
    print("pip install aiogram apscheduler pandas openpyxl geopy python-dotenv asyncpg")
    raise e

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения Railway
class Config:
    # Токен бота (получить у @BotFather)
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    
    # ID администратора (467500951 как указано)
    ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "467500951").split(",")]
    
    # Настройки базы данных PostgreSQL (Railway предоставляет DATABASE_URL)
    DATABASE_URL = os.getenv("DATABASE_URL", "")
    
    # Требовать ли геолокацию (True/False)
    GEO_REQUIRED = os.getenv("GEO_REQUIRED", "true").lower() == "true"
    
    # Радиус для проверки геолокации (метры)
    LOCATION_RADIUS = int(os.getenv("LOCATION_RADIUS", "500"))
    
    # Настройки email для отправки табелей
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    EMAIL_RECIPIENTS = [x.strip() for x in os.getenv("EMAIL_RECIPIENTS", "").split(",") if x.strip()]
    
    # Рабочие часы
    WORK_START_HOUR = 9
    WORK_END_HOUR = 18
    MAX_WORK_HOURS = 8
    
    # Время напоминаний
    REMINDER_CHECKOUT_HOUR = 19  # Напоминание об отметке ухода
    DAILY_REPORT_HOUR = 20  # Ежедневный отчет администратору

# Проверка обязательных переменных
if not Config.BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")
if not Config.DATABASE_URL:
    raise ValueError("DATABASE_URL не установлен в переменных окружения")

# Состояния для FSM
class Form(StatesGroup):
    waiting_for_location = State()
    waiting_for_object = State()
    waiting_for_admin_action = State()
    waiting_for_employee_name = State()
    waiting_for_employee_tg_id = State()
    waiting_for_object_data = State()
    waiting_for_approval = State()
    waiting_for_sick_reason = State()

# Основной класс бота
class WorkTimeBot:
    def __init__(self):
        self.bot = Bot(token=Config.BOT_TOKEN)
        self.storage = MemoryStorage()
        self.dp = Dispatcher(storage=self.storage)
        self.scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
        
        # Подключение к БД будет установлено позже
        self.pool = None
        
        # Кэш для временных данных
        self.temp_data: Dict[int, Dict] = {}
        
        # Регистрация обработчиков
        self.register_handlers()
        
    async def init_database(self):
        """Инициализация базы данных PostgreSQL"""
        try:
            # Создаем пул соединений
            self.pool = await asyncpg.create_pool(
                Config.DATABASE_URL,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            
            async with self.pool.acquire() as conn:
                # Таблица сотрудников
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS employees (
                        id SERIAL PRIMARY KEY,
                        telegram_id BIGINT UNIQUE,
                        full_name VARCHAR(255) NOT NULL,
                        is_admin BOOLEAN DEFAULT FALSE,
                        is_active BOOLEAN DEFAULT TRUE,
                        is_approved BOOLEAN DEFAULT FALSE,
                        position VARCHAR(100),
                        phone VARCHAR(20),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        approved_at TIMESTAMP,
                        approved_by INTEGER REFERENCES employees(id)
                    )
                ''')
                
                # Таблица объектов
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS objects (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        address TEXT,
                        latitude DECIMAL(10, 8),
                        longitude DECIMAL(11, 8),
                        radius INTEGER DEFAULT 500,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Таблица рабочих отметок (TimeLog - детальная)
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
                        check_out_lat DECIMAL(10, 8),
                        check_out_lon DECIMAL(11, 8),
                        hours_worked DECIMAL(4, 2) DEFAULT 0,
                        status VARCHAR(20) DEFAULT 'work', -- work, sick, vacation, day_off, late
                        notes TEXT,
                        auto_checkout BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Таблица сводных данных по дням (для Excel)
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS work_days (
                        id SERIAL PRIMARY KEY,
                        employee_id INTEGER REFERENCES employees(id),
                        date DATE NOT NULL,
                        object_id INTEGER REFERENCES objects(id),
                        hours_worked DECIMAL(4, 2) DEFAULT 0,
                        status VARCHAR(20) DEFAULT 'work',
                        UNIQUE(employee_id, date)
                    )
                ''')
                
                # Таблица запросов на доступ
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS access_requests (
                        id SERIAL PRIMARY KEY,
                        telegram_id BIGINT NOT NULL,
                        full_name VARCHAR(255),
                        phone VARCHAR(20),
                        message TEXT,
                        status VARCHAR(20) DEFAULT 'pending', -- pending, approved, rejected
                        reviewed_by INTEGER REFERENCES employees(id),
                        reviewed_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Проверяем, есть ли администратор
                admin_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM employees WHERE telegram_id = $1)",
                    Config.ADMIN_IDS[0]
                )
                
                if not admin_exists:
                    # Создаем администратора
                    await conn.execute('''
                        INSERT INTO employees (telegram_id, full_name, is_admin, is_approved, is_active)
                        VALUES ($1, $2, TRUE, TRUE, TRUE)
                    ''', Config.ADMIN_IDS[0], "Главный Администратор")
                    
                    logger.info(f"Создан администратор с ID: {Config.ADMIN_IDS[0]}")
                
                # Создаем тестовые объекты, если их нет
                test_objects = [
                    ("Офис Москва", "ул. Тверская, 1", 55.7558, 37.6173, 200),
                    ("Склад Подольск", "Подольск, ул. Промышленная, 15", 55.4297, 37.5440, 300),
                    ("Строительный объект №1", "Московская область", 55.5807, 37.3928, 500),
                    ("Удаленная работа", None, None, None, None),
                ]
                
                for obj in test_objects:
                    exists = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM objects WHERE name = $1)",
                        obj[0]
                    )
                    if not exists:
                        await conn.execute('''
                            INSERT INTO objects (name, address, latitude, longitude, radius)
                            VALUES ($1, $2, $3, $4, $5)
                        ''', *obj)
                
                logger.info("База данных инициализирована")
                
        except Exception as e:
            logger.error(f"Ошибка инициализации БД: {e}")
            raise
    
    def register_handlers(self):
        """Регистрация обработчиков команд"""
        
        # Команды для всех пользователей
        @self.dp.message(Command("start"))
        async def cmd_start(message: types.Message, state: FSMContext):
            await self.handle_start(message, state)
        
        @self.dp.message(Command("request_access"))
        async def cmd_request_access(message: types.Message, state: FSMContext):
            await self.handle_request_access(message, state)
        
        @self.dp.message(Command("checkin"))
        async def cmd_checkin(message: types.Message, state: FSMContext):
            await self.handle_checkin(message, state)
        
        @self.dp.message(Command("checkout"))
        async def cmd_checkout(message: types.Message, state: FSMContext):
            await self.handle_checkout(message, state)
        
        @self.dp.message(Command("sick"))
        async def cmd_sick(message: types.Message, state: FSMContext):
            await self.handle_sick(message, state)
        
        @self.dp.message(Command("my_stats"))
        async def cmd_my_stats(message: types.Message):
            await self.handle_my_stats(message)
        
        @self.dp.message(Command("select_object"))
        async def cmd_select_object(message: types.Message, state: FSMContext):
            await self.handle_select_object(message, state)
        
        @self.dp.message(Command("my_logs"))
        async def cmd_my_logs(message: types.Message):
            await self.handle_my_logs(message)
        
        # Команды для администраторов
        @self.dp.message(Command("admin"))
        async def cmd_admin(message: types.Message):
            await self.handle_admin_panel(message)
        
        @self.dp.message(Command("send_timesheet"))
        async def cmd_send_timesheet(message: types.Message):
            await self.handle_send_timesheet(message)
        
        @self.dp.message(Command("add_employee"))
        async def cmd_add_employee(message: types.Message, state: FSMContext):
            await self.handle_add_employee(message, state)
        
        @self.dp.message(Command("add_object"))
        async def cmd_add_object(message: types.Message, state: FSMContext):
            await self.handle_add_object(message, state)
        
        @self.dp.message(Command("pending_requests"))
        async def cmd_pending_requests(message: types.Message):
            await self.handle_pending_requests(message)
        
        # Обработчики геолокации
        @self.dp.message(F.location)
        async def handle_location(message: types.Message, state: FSMContext):
            await self.process_location(message, state)
        
        # Обработчики текстовых сообщений для состояний
        @self.dp.message(Form.waiting_for_employee_name)
        async def process_employee_name(message: types.Message, state: FSMContext):
            await self.process_new_employee_name(message, state)
        
        @self.dp.message(Form.waiting_for_employee_tg_id)
        async def process_employee_tg_id(message: types.Message, state: FSMContext):
            await self.process_new_employee_tg_id(message, state)
        
        @self.dp.message(Form.waiting_for_object_data)
        async def process_object_data(message: types.Message, state: FSMContext):
            await self.process_new_object_data(message, state)
        
        @self.dp.message(Form.waiting_for_sick_reason)
        async def process_sick_reason(message: types.Message, state: FSMContext):
            await self.process_sick_reason_text(message, state)
        
        # Обработчики callback запросов
        @self.dp.callback_query(F.data.startswith("obj_"))
        async def handle_object_selection(callback: types.CallbackQuery, state: FSMContext):
            await self.process_object_selection(callback, state)
        
        @self.dp.callback_query(F.data.startswith("admin_"))
        async def handle_admin_action(callback: types.CallbackQuery):
            await self.process_admin_action(callback)
        
        @self.dp.callback_query(F.data.startswith("approve_"))
        async def handle_approval(callback: types.CallbackQuery):
            await self.process_approval(callback)
        
        @self.dp.callback_query(F.data.startswith("reject_"))
        async def handle_rejection(callback: types.CallbackQuery):
            await self.process_rejection(callback)
    
    async def setup_scheduler(self):
        """Настройка планировщика задач"""
        # Напоминание об отметке ухода каждый день в 19:00
        self.scheduler.add_job(
            self.send_checkout_reminders,
            CronTrigger(hour=Config.REMINDER_CHECKOUT_HOUR, minute=0),
            id="checkout_reminder"
        )
        
        # Ежедневный отчет администратору в 20:00
        self.scheduler.add_job(
            self.send_daily_report,
            CronTrigger(hour=Config.DAILY_REPORT_HOUR, minute=0),
            id="daily_report"
        )
        
        # Автоматический checkout в 23:00 для тех, кто не отметился
        self.scheduler.add_job(
            self.auto_checkout,
            CronTrigger(hour=23, minute=0),
            id="auto_checkout"
        )
        
        # Отправка табеля 15-го и последнего дня месяца
        self.scheduler.add_job(
            self.auto_send_timesheet,
            CronTrigger(day='15, last', hour=10, minute=0),
            id="auto_timesheet"
        )
        
        self.scheduler.start()
        logger.info("Планировщик задач запущен")
    
    # ===================== ОСНОВНЫЕ ОБРАБОТЧИКИ =====================
    
    async def handle_start(self, message: types.Message, state: FSMContext):
        """Обработка команды /start"""
        user_id = message.from_user.id
        
        # Проверяем, зарегистрирован ли пользователь и одобрен
        async with self.pool.acquire() as conn:
            employee = await conn.fetchrow('''
                SELECT id, full_name, is_admin, is_approved 
                FROM employees 
                WHERE telegram_id = $1 AND is_active = TRUE
            ''', user_id)
        
        if employee:
            if employee['is_approved']:
                # Пользователь одобрен
                welcome_text = f"👋 Добро пожаловать, {employee['full_name']}!\n\n"
                
                if employee['is_admin']:
                    welcome_text += "⚙️ Вы вошли как администратор.\n"
                    welcome_text += "Используйте /admin для управления системой.\n\n"
                
                welcome_text += (
                    "📋 Доступные команды:\n"
                    "/checkin - Отметка прихода на работу\n"
                    "/checkout - Отметка ухода с работы\n"
                    "/select_object - Выбор объекта работы\n"
                    "/sick - Отметка больничного\n"
                    "/my_stats - Моя статистика\n"
                    "/my_logs - Мои отметки времени\n"
                )
                
                await message.answer(welcome_text)
            else:
                await message.answer(
                    "⏳ Ваш аккаунт ожидает подтверждения администратором.\n"
                    "Мы уведомим вас, когда доступ будет предоставлен."
                )
        else:
            # Новый пользователь
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="📝 Запросить доступ",
                            callback_data="request_access"
                        )
                    ]
                ]
            )
            
            await message.answer(
                "🔒 Система учета рабочего времени\n\n"
                "Это закрытая система. Для получения доступа необходимо:\n"
                "1. Отправить запрос на доступ\n"
                "2. Дождаться подтверждения администратора\n"
                "3. После подтверждения вы сможете использовать все функции\n\n"
                "Запросить доступ:",
                reply_markup=keyboard
            )
    
    async def handle_request_access(self, message: types.Message, state: FSMContext):
        """Запрос доступа к системе"""
        user_id = message.from_user.id
        
        # Проверяем, не отправлял ли уже запрос
        async with self.pool.acquire() as conn:
            existing = await conn.fetchrow('''
                SELECT status FROM access_requests 
                WHERE telegram_id = $1 
                ORDER BY created_at DESC 
                LIMIT 1
            ''', user_id)
        
        if existing:
            status = existing['status']
            if status == 'pending':
                await message.answer("⏳ Ваш запрос уже ожидает рассмотрения.")
                return
            elif status == 'approved':
                await message.answer("✅ Ваш запрос уже одобрен. Используйте /start")
                return
            elif status == 'rejected':
                await message.answer("❌ Ваш предыдущий запрос был отклонен.")
        
        # Запрашиваем ФИО
        await message.answer(
            "📝 Запрос доступа к системе\n\n"
            "Введите ваше ФИО (полностью):",
            reply_markup=ReplyKeyboardRemove()
        )
        
        await state.set_state(Form.waiting_for_employee_name)
        await state.update_data(request_type="access", telegram_id=user_id)
    
    async def handle_checkin(self, message: types.Message, state: FSMContext):
        """Обработка команды /checkin"""
        user_id = message.from_user.id
        
        # Проверяем регистрацию и доступ
        if not await self.is_user_approved(user_id):
            await message.answer("❌ Доступ запрещен. Ваш аккаунт не подтвержден.")
            return
        
        # Проверяем, не отметился ли уже сегодня
        today = date.today()
        async with self.pool.acquire() as conn:
            existing = await conn.fetchrow('''
                SELECT id, check_in FROM time_logs 
                WHERE employee_id = (SELECT id FROM employees WHERE telegram_id = $1)
                AND date = $2
                ORDER BY check_in DESC 
                LIMIT 1
            ''', user_id, today)
        
        if existing and existing['check_in']:
            await message.answer("✅ Вы уже отметили приход сегодня!")
            
            # Показываем время прихода
            check_in_time = existing['check_in'].strftime('%H:%M')
            await message.answer(f"⏰ Время прихода: {check_in_time}")
            return
        
        if Config.GEO_REQUIRED:
            # Запрашиваем геолокацию
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [
                        KeyboardButton(
                            text="📍 Отправить геолокацию",
                            request_location=True
                        )
                    ],
                    [KeyboardButton(text="❌ Отмена")]
                ],
                resize_keyboard=True
            )
            
            await message.answer(
                "📍 Для отметки прихода необходимо отправить вашу геолокацию.\n"
                "Нажмите кнопку ниже:",
                reply_markup=keyboard
            )
        else:
            # Без геолокации - сразу выбор объекта
            await self.show_object_selection(message, state, "checkin")
        
        await state.set_state(Form.waiting_for_location)
        await state.update_data(action="checkin")
    
    async def handle_checkout(self, message: types.Message, state: FSMContext):
        """Обработка команды /checkout"""
        user_id = message.from_user.id
        
        if not await self.is_user_approved(user_id):
            await message.answer("❌ Доступ запрещен. Ваш аккаунт не подтвержден.")
            return
        
        # Получаем последнюю запись прихода за сегодня
        today = date.today()
        async with self.pool.acquire() as conn:
            log = await conn.fetchrow('''
                SELECT id, check_in, check_out, object_id 
                FROM time_logs 
                WHERE employee_id = (SELECT id FROM employees WHERE telegram_id = $1)
                AND date = $2
                ORDER BY check_in DESC 
                LIMIT 1
            ''', user_id, today)
        
        if not log:
            await message.answer("❌ Сначала отметьте приход (/checkin)!")
            return
        
        if log['check_out']:
            await message.answer("✅ Вы уже отметили уход сегодня!")
            
            # Показываем время ухода
            check_out_time = log['check_out'].strftime('%H:%M')
            await message.answer(f"⏰ Время ухода: {check_out_time}")
            return
        
        if Config.GEO_REQUIRED:
            # Запрашиваем геолокацию
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [
                        KeyboardButton(
                            text="📍 Отправить геолокацию",
                            request_location=True
                        )
                    ],
                    [KeyboardButton(text="❌ Отмена")]
                ],
                resize_keyboard=True
            )
            
            await message.answer(
                "📍 Для отметки ухода необходимо отправить вашу геолокацию.\n"
                "Нажмите кнопку ниже:",
                reply_markup=keyboard
            )
        else:
            # Без геолокации - сразу обработка ухода
            await self.process_checkout_without_geo(user_id, log['id'])
        
        await state.set_state(Form.waiting_for_location)
        await state.update_data(action="checkout", log_id=log['id'])
    
    async def handle_sick(self, message: types.Message, state: FSMContext):
        """Обработка команды /sick (больничный)"""
        user_id = message.from_user.id
        
        if not await self.is_user_approved(user_id):
            await message.answer("❌ Доступ запрещен. Ваш аккаунт не подтвержден.")
            return
        
        today = date.today()
        
        # Проверяем, нет ли уже отметки на сегодня
        async with self.pool.acquire() as conn:
            existing = await conn.fetchrow('''
                SELECT id FROM time_logs 
                WHERE employee_id = (SELECT id FROM employees WHERE telegram_id = $1)
                AND date = $2
            ''', user_id, today)
        
        if existing:
            await message.answer("❌ У вас уже есть отметка на сегодня. Сначала удалите её через администратора.")
            return
        
        await message.answer(
            "🏥 Отметка больничного\n\n"
            "Введите причину больничного (необязательно):",
            reply_markup=ReplyKeyboardRemove()
        )
        
        await state.set_state(Form.waiting_for_sick_reason)
        await state.update_data(telegram_id=user_id)
    
    async def handle_my_stats(self, message: types.Message):
        """Обработка команды /my_stats"""
        user_id = message.from_user.id
        
        if not await self.is_user_approved(user_id):
            await message.answer("❌ Доступ запрещен. Ваш аккаунт не подтвержден.")
            return
        
        # Получаем статистику за текущий месяц
        today = date.today()
        first_day = date(today.year, today.month, 1)
        
        async with self.pool.acquire() as conn:
            # Общая статистика
            stats = await conn.fetchrow('''
                SELECT 
                    COUNT(DISTINCT date) as days_worked,
                    SUM(hours_worked) as total_hours,
                    SUM(CASE WHEN status = 'sick' THEN 1 ELSE 0 END) as sick_days,
                    SUM(CASE WHEN status = 'late' THEN 1 ELSE 0 END) as late_days
                FROM time_logs 
                WHERE employee_id = (SELECT id FROM employees WHERE telegram_id = $1)
                AND date >= $2 AND date <= $3
            ''', user_id, first_day, today)
            
            # Статистика по объектам
            objects_stats = await conn.fetch('''
                SELECT o.name, 
                       COUNT(tl.id) as days_count,
                       SUM(tl.hours_worked) as total_hours
                FROM time_logs tl
                JOIN objects o ON tl.object_id = o.id
                WHERE tl.employee_id = (SELECT id FROM employees WHERE telegram_id = $1)
                AND tl.date >= $2 AND tl.date <= $3
                AND tl.status = 'work'
                GROUP BY o.id, o.name
                ORDER BY total_hours DESC
            ''', user_id, first_day, today)
            
            # Сегодняшняя запись
            today_log = await conn.fetchrow('''
                SELECT tl.check_in, tl.check_out, tl.hours_worked, 
                       tl.status, o.name as object_name,
                       tl.check_in_lat, tl.check_in_lon
                FROM time_logs tl
                LEFT JOIN objects o ON tl.object_id = o.id
                WHERE tl.employee_id = (SELECT id FROM employees WHERE telegram_id = $1)
                AND tl.date = $2
                ORDER BY tl.check_in DESC 
                LIMIT 1
            ''', user_id, today)
        
        # Формируем сообщение
        stats_text = f"📊 Ваша статистика за {today.strftime('%B %Y')}:\n\n"
        
        if stats:
            days_worked = stats['days_worked'] or 0
            total_hours = stats['total_hours'] or 0
            sick_days = stats['sick_days'] or 0
            late_days = stats['late_days'] or 0
            
            stats_text += f"📅 Отработано дней: {days_worked}\n"
            stats_text += f"⏱️ Всего часов: {total_hours:.1f}\n"
            stats_text += f"🤒 Дней на больничном: {sick_days}\n"
            if late_days > 0:
                stats_text += f"⏰ Опозданий: {late_days}\n"
        
        # Статистика по объектам
        if objects_stats:
            stats_text += "\n🏗️ По объектам:\n"
            for obj in objects_stats:
                stats_text += f"• {obj['name']}: {obj['days_count']} дн., {obj['total_hours']:.1f} ч.\n"
        
        # Сегодня
        stats_text += "\n📌 Сегодня:\n"
        
        if today_log:
            if today_log['status'] == 'sick':
                stats_text += "🏥 Статус: Больничный\n"
            else:
                object_name = today_log['object_name'] or "Не указан"
                stats_text += f"🏢 Объект: {object_name}\n"
                
                if today_log['check_in']:
                    check_in_time = today_log['check_in'].strftime('%H:%M')
                    stats_text += f"↘️ Приход: {check_in_time}\n"
                    
                    # Проверяем опоздание
                    check_in_dt = today_log['check_in']
                    work_start = datetime.combine(today, time(Config.WORK_START_HOUR, 0))
                    
                    if check_in_dt > work_start + timedelta(minutes=15):
                        stats_text += "⚠️ Вы опоздали!\n"
                
                if today_log['check_out']:
                    check_out_time = today_log['check_out'].strftime('%H:%M')
                    hours = today_log['hours_worked'] or 0
                    stats_text += f"↗️ Уход: {check_out_time}\n"
                    stats_text += f"⏱️ Отработано: {hours:.1f} ч.\n"
                else:
                    stats_text += "↗️ Уход: еще на работе\n"
                    
                    if today_log['check_in']:
                        # Считаем сколько уже отработано
                        check_in_dt = today_log['check_in']
                        now = datetime.now()
                        hours_passed = (now - check_in_dt).seconds / 3600
                        stats_text += f"⏱️ Прошло: {hours_passed:.1f} ч.\n"
        else:
            stats_text += "📭 Отметок нет\n"
        
        await message.answer(stats_text)
    
    async def handle_my_logs(self, message: types.Message):
        """Показать мои логи времени"""
        user_id = message.from_user.id
        
        if not await self.is_user_approved(user_id):
            await message.answer("❌ Доступ запрещен. Ваш аккаунт не подтвержден.")
            return
        
        # Получаем логи за последние 7 дней
        week_ago = date.today() - timedelta(days=7)
        
        async with self.pool.acquire() as conn:
            logs = await conn.fetch('''
                SELECT tl.date, tl.check_in, tl.check_out, 
                       tl.hours_worked, tl.status,
                       o.name as object_name,
                       tl.notes
                FROM time_logs tl
                LEFT JOIN objects o ON tl.object_id = o.id
                WHERE tl.employee_id = (SELECT id FROM employees WHERE telegram_id = $1)
                AND tl.date >= $2
                ORDER BY tl.date DESC, tl.check_in DESC
            ''', user_id, week_ago)
        
        if not logs:
            await message.answer("📭 У вас нет отметок за последнюю неделю.")
            return
        
        # Формируем сообщение
        logs_text = "📋 Ваши отметки за последние 7 дней:\n\n"
        
        current_date = None
        for log in logs:
            log_date = log['date']
            
            if current_date != log_date:
                current_date = log_date
                logs_text += f"\n📅 {log_date.strftime('%d.%m.%Y')}:\n"
            
            if log['status'] == 'sick':
                logs_text += "  🏥 Больничный"
                if log['notes']:
                    logs_text += f" ({log['notes']})"
                logs_text += "\n"
            else:
                object_name = log['object_name'] or "Не указан"
                
                if log['check_in']:
                    check_in = log['check_in'].strftime('%H:%M')
                    logs_text += f"  ↘️ {check_in}"
                else:
                    logs_text += "  ↘️ --:--"
                
                if log['check_out']:
                    check_out = log['check_out'].strftime('%H:%M')
                    logs_text += f" - ↗️ {check_out}"
                else:
                    logs_text += " - ↗️ --:--"
                
                hours = log['hours_worked'] or 0
                logs_text += f" ⏱️ {hours:.1f}ч. 🏢 {object_name}\n"
        
        # Если сообщение слишком длинное, делим на части
        if len(logs_text) > 4000:
            parts = [logs_text[i:i+4000] for i in range(0, len(logs_text), 4000)]
            for part in parts:
                await message.answer(part)
        else:
            await message.answer(logs_text)
    
    async def handle_select_object(self, message: types.Message, state: FSMContext):
        """Обработка команды /select_object"""
        user_id = message.from_user.id
        
        if not await self.is_user_approved(user_id):
            await message.answer("❌ Доступ запрещен. Ваш аккаунт не подтвержден.")
            return
        
        await self.show_object_selection(message, state, "select")
    
    async def handle_admin_panel(self, message: types.Message):
        """Панель администратора"""
        user_id = message.from_user.id
        
        if not await self.is_user_admin(user_id):
            await message.answer("❌ У вас нет прав администратора.")
            return
        
        # Создаем клавиатуру админ-панели
        keyboard = InlineKeyboardBuilder()
        
        keyboard.button(
            text="👥 Запросы на доступ",
            callback_data="admin_pending_requests"
        )
        keyboard.button(
            text="📊 Сформировать табель",
            callback_data="admin_generate_timesheet"
        )
        keyboard.button(
            text="📧 Отправить табель",
            callback_data="admin_send_timesheet"
        )
        keyboard.button(
            text="👤 Управление сотрудниками",
            callback_data="admin_manage_employees"
        )
        keyboard.button(
            text="🏗️ Управление объектами",
            callback_data="admin_manage_objects"
        )
        keyboard.button(
            text="📈 Статистика по всем",
            callback_data="admin_all_stats"
        )
        keyboard.button(
            text="⏱️ Все логи времени",
            callback_data="admin_all_logs"
        )
        keyboard.button(
            text="⚙️ Настройки",
            callback_data="admin_settings"
        )
        
        keyboard.adjust(2)
        
        await message.answer(
            "⚙️ Панель администратора\n\n"
            "Выберите действие:",
            reply_markup=keyboard.as_markup()
        )
    
    async def handle_pending_requests(self, message: types.Message):
        """Показать pending запросы"""
        user_id = message.from_user.id
        
        if not await self.is_user_admin(user_id):
            await message.answer("❌ У вас нет прав администратора.")
            return
        
        await self.show_pending_requests(message)
    
    async def handle_send_timesheet(self, message: types.Message):
        """Ручная отправка табеля"""
        user_id = message.from_user.id
        
        if not await self.is_user_admin(user_id):
            await message.answer("❌ У вас нет прав администратора.")
            return
        
        await message.answer("⏳ Формирую и отправляю табель...")
        
        try:
            success
