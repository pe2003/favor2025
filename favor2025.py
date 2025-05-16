import logging
import uuid
import qrcode
import io
import re
import asyncio
import time
import json
import os
from PIL import Image
from pyzbar.pyzbar import decode
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    filters,
    ContextTypes,
)
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Базовая директория
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Настройка логирования (без ротации, так как Render имеет эфемерную ФС)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler()  # Вывод логов в stdout для Render
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')
CHANNEL_ID = os.getenv('CHANNEL_ID')
ALLOWED_ADMIN_IDS = set(map(int, os.getenv('ALLOWED_ADMIN_IDS', '').split(','))) if os.getenv('ALLOWED_ADMIN_IDS') else set()
GOOGLE_SHEETS_KEY = os.getenv('GOOGLE_SHEETS_KEY')
ORGANIZER_CONTACT = os.getenv('ORGANIZER_CONTACT', '@Organizer')

# Проверка обязательных переменных
if not TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN не задан")
    raise ValueError("TELEGRAM_BOT_TOKEN не задан")
if not ADMIN_PASSWORD:
    logger.error("ADMIN_PASSWORD не задан")
    raise ValueError("ADMIN_PASSWORD не задан")
if not CHANNEL_ID:
    logger.error("CHANNEL_ID не задан")
    raise ValueError("CHANNEL_ID не задан")
if not GOOGLE_SHEETS_KEY:
    logger.error("GOOGLE_SHEETS_KEY не задан")
    raise ValueError("GOOGLE_SHEETS_KEY не задан")

# Путь к фото для команды /start
START_PHOTO_PATH = os.path.join(BASE_DIR, 'photo.jpg')

# Проверка существования и размера файла photo.jpg
MAX_PHOTO_SIZE_MB = 5  # Максимальный размер фото в МБ
if os.path.exists(START_PHOTO_PATH):
    photo_size_mb = os.path.getsize(START_PHOTO_PATH) / (1024 * 1024)
    if photo_size_mb > MAX_PHOTO_SIZE_MB:
        logger.warning(f"Файл photo.jpg слишком большой: {photo_size_mb:.2f} МБ. Максимум: {MAX_PHOTO_SIZE_MB} МБ.")
        START_PHOTO_PATH = None
else:
    logger.warning(f"Файл photo.jpg не найден по пути: {START_PHOTO_PATH}")
    START_PHOTO_PATH = None

# Путь к credentials.json
CREDENTIALS_PATH = os.path.join(BASE_DIR, 'credentials.json')
if not os.path.exists(CREDENTIALS_PATH):
    logger.error(f"Файл credentials.json не найден: {CREDENTIALS_PATH}")
    raise FileNotFoundError(f"Файл credentials.json не найден")

# Инициализация Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets']
worksheet = None
accommodation_worksheet = None

async def init_google_sheets(retries=3, backoff=2):
    global worksheet, accommodation_worksheet
    for attempt in range(retries):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_key(GOOGLE_SHEETS_KEY)
            try:
                worksheet = spreadsheet.worksheet('Лист1')
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title='Лист1', rows=100, cols=20)
                headers = ['registration_id', 'user_id', 'name', 'days', 'arrival_date', 'city', 'nick', 'phone', 'birth_date', 'gender', 'accommodation']
                worksheet.append_row(headers)
            try:
                accommodation_worksheet = spreadsheet.worksheet('Расселение')
            except gspread.exceptions.WorksheetNotFound:
                accommodation_worksheet = spreadsheet.add_worksheet(title='Расселение', rows=100, cols=10)
                headers = [f'Дом {i+1}' for i in range(10)]
                accommodation_worksheet.append_row(headers)
            logger.info("Google Sheets инициализирован успешно")
            return True
        except Exception as e:
            logger.error(f"Ошибка инициализации Google Sheets (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(backoff * (2 ** attempt))
            else:
                logger.error("Не удалось инициализировать Google Sheets")
                return False

# Функция для экранировки специальных символов в Markdown
def escape_markdown(text):
    if not isinstance(text, str):
        text = str(text)
    special_chars = r'([_*[\]()~`>#+\-=|{}.!])'
    return re.sub(special_chars, r'\\\1', text)

# Состояния для ConversationHandler
NAME, DAYS, ARRIVAL_DATE, CITY, PHONE, BIRTH_DATE, GENDER, ROOM, NOTIFICATION = range(9)

# Глобальные словари
user_data = {}
user_registration_ids = {}
registrations = {}
registered_users = set()
admin_users = set()
accommodation_initiated = set()
awaiting_notification = set()

stats = {
    'bot_opened': set(),
    'registered': set(),
    'checked_in': set()
}

room_assignments = {i+1: [] for i in range(10)}
user_room = {}

# Путь к файлу статистики
STATS_FILE = os.path.join(BASE_DIR, 'stats.json')

# Опции
days_options = [1, 2, 3, 4]
dates = ["03.07.2025", "04.07.2025", "05.07.2025", "06.07.2025"]

# Админская клавиатура
admin_keyboard = ReplyKeyboardMarkup([
    ["Статистика", "Очистить регистрации"],
    ["Разложить спать", "Отправить уведомление"],
    ["Выйти из админки"]
], resize_keyboard=True, one_time_keyboard=False)

# Проверка прав бота в канале
async def check_channel_permissions(context: ContextTypes.DEFAULT_TYPE):
    try:
        bot = context.bot
        chat_member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=bot.id)
        if chat_member.status not in ['administrator', 'creator']:
            logger.error(f"Бот не администратор канала {CHANNEL_ID}")
            return False
        if not chat_member.can_post_messages:
            logger.error(f"Бот не может отправлять сообщения в канал {CHANNEL_ID}")
            return False
        logger.info(f"Бот имеет права в канале {CHANNEL_ID}")
        return True
    except Exception as e:
        logger.error(f"Ошибка проверки прав: {e}")
        return False

# Уведомление админу
async def notify_admin(context, message, retries=3, backoff=2):
    escaped_message = escape_markdown(message)
    for attempt in range(retries):
        try:
            can_send = await check_channel_permissions(context)
            if not can_send:
                logger.error(f"Бот не может отправить уведомление: нет прав в канале {CHANNEL_ID}")
                return False
            await context.bot.send_message(chat_id=CHANNEL_ID, text=f"Ошибка бота: {escaped_message}", parse_mode='Markdown')
            logger.info(f"Уведомление отправлено: {message}")
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(backoff * (2 ** attempt))
            else:
                logger.error(f"Не удалось отправить уведомление: {e}")
                return False

# Динамическая клавиатура
def get_persistent_keyboard(user_id):
    keyboard = []
    first_row = []
    if user_id not in registered_users:
        first_row.append("Регистрация")
    else:
        if user_id in user_room:
            first_row.append("Отменить расселение")
        elif user_id in accommodation_initiated and user_id not in user_room:
            first_row.append("Расселить")
    if first_row:
        keyboard.append(first_row)
    keyboard.extend([
        ["Расписание", "Спикеры"],
        ["Место проведения", "Контакты"],
        ["QR Code"]
    ])
    logger.info(f"Generated keyboard for user_id={user_id}")
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

# Загрузка и сохранение данных
def load_registrations():
    global registrations, user_registration_ids, registered_users
    if worksheet is None:
        logger.error("Google Sheets не инициализирован")
        return
    retries = 3
    for attempt in range(retries):
        try:
            records = worksheet.get_all_records()
            logger.info(f"Получено {len(records)} записей")
            registrations.clear()
            user_registration_ids.clear()
            registered_users.clear()
            for record in records:
                registration_id = record['registration_id']
                user_id = int(record['user_id'])
                registrations[registration_id] = {
                    'name': record['name'],
                    'days': record['days'],
                    'arrival_date': record['arrival_date'],
                    'city': record['city'],
                    'nick': record['nick'],
                    'phone': record['phone'],
                    'birth_date': record['birth_date'],
                    'gender': record.get('gender', 'Не указан'),
                    'accommodation': record.get('accommodation', 'Нет')
                }
                user_registration_ids[user_id] = registration_id
                registered_users.add(user_id)
            logger.info(f"Registrations loaded: {len(registrations)}")
            return
        except Exception as e:
            logger.error(f"Ошибка загрузки регистраций (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 * (2 ** attempt))
            else:
                logger.error("Не удалось загрузить регистрации")

def save_registrations(context=None):
    if worksheet is None:
        logger.error("Google Sheets не инициализирован")
        if context:
            asyncio.create_task(notify_admin(context, "Google Sheets не инициализирован"))
        return
    retries = 3
    for attempt in range(retries):
        try:
            worksheet.clear()
            headers = ['registration_id', 'user_id', 'name', 'days', 'arrival_date', 'city', 'nick', 'phone', 'birth_date', 'gender', 'accommodation']
            worksheet.append_row(headers)
            for registration_id, data in registrations.items():
                user_id = next((uid for uid, rid in user_registration_ids.items() if rid == registration_id), None)
                if user_id is not None:
                    accommodation_status = "Да" if user_id in user_room else "Нет"
                    row = [
                        registration_id,
                        user_id,
                        data['name'],
                        data['days'],
                        data['arrival_date'],
                        data['city'],
                        data['nick'],
                        data['phone'],
                        data['birth_date'],
                        data['gender'],
                        accommodation_status
                    ]
                    worksheet.append_row(row)
            logger.info(f"Registrations saved: {len(registrations)}")
            return
        except Exception as e:
            logger.error(f"Ошибка сохранения регистраций (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 * (2 ** attempt))
            else:
                logger.error("Не удалось сохранить регистрации")
                if context:
                    asyncio.create_task(notify_admin(context, f"Ошибка сохранения регистраций: {e}"))

def load_accommodations():
    global room_assignments, user_room
    if accommodation_worksheet is None:
        logger.error("Google Sheets (Расселение) не инициализирован")
        return
    retries = 3
    for attempt in range(retries):
        try:
            records = accommodation_worksheet.get_all_values()
            if len(records) < 1:
                logger.info("Лист 'Расселение' пуст")
                return
            headers = records[0]
            room_assignments = {i+1: [] for i in range(10)}
            user_room = {}
            for row in records[1:]:
                for i, cell in enumerate(row):
                    if cell:
                        room_number = i + 1
                        if room_number <= 10 and len(room_assignments.get(room_number, [])) < 15:
                            room_assignments[room_number].append(cell)
                            for user_id, reg_id in user_registration_ids.items():
                                if registrations[reg_id]['name'] == cell:
                                    user_room[user_id] = room_number
                                    break
                        else:
                            logger.warning(f"Дом {room_number} превысил лимит: {cell}")
            logger.info(f"Accommodations loaded: {room_assignments}")
            return
        except Exception as e:
            logger.error(f"Ошибка загрузки расселения (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 * (2 ** attempt))
            else:
                logger.error("Не удалось загрузить расселение")

def save_accommodations(context=None):
    if accommodation_worksheet is None:
        logger.error("Google Sheets (Расселение) не инициализирован")
        if context:
            asyncio.create_task(notify_admin(context, "Google Sheets (Расселение) не инициализирован"))
        return
    retries = 3
    for attempt in range(retries):
        try:
            accommodation_worksheet.clear()
            headers = [f'Дом {i+1}' for i in range(10)]
            accommodation_worksheet.append_row(headers)
            max_rows = max(len(room_assignments.get(i+1, [])) for i in range(10)) + 1
            for row_idx in range(1, max_rows):
                row = []
                for col_idx in range(10):
                    if row_idx - 1 < len(room_assignments.get(col_idx + 1, [])):
                        row.append(room_assignments[col_idx + 1][row_idx - 1])
                    else:
                        row.append('')
                accommodation_worksheet.append_row(row)
            logger.info("Accommodations saved")
            return
        except Exception as e:
            logger.error(f"Ошибка сохранения расселения (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 * (2 ** attempt))
            else:
                logger.error("Не удалось сохранить расселение")
                if context:
                    asyncio.create_task(notify_admin(context, f"Ошибка сохранения расселения: {e}"))

def load_stats():
    global stats, admin_users, accommodation_initiated, awaiting_notification
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                stats = {k: set(v) for k, v in data.get('stats', {}).items()}
                admin_users = set(data.get('admin_users', []))
                accommodation_initiated = set(data.get('accommodation_initiated', []))
                awaiting_notification = set(data.get('awaiting_notification', []))
                logger.info(f"Stats loaded")
        else:
            logger.info("Stats file not found, initializing defaults")
            stats = {'bot_opened': set(), 'registered': set(), 'checked_in': set()}
            admin_users = set()
            accommodation_initiated = set()
            awaiting_notification = set()
    except Exception as e:
        logger.error(f"Error loading stats: {e}")
        stats = {'bot_opened': set(), 'registered': set(), 'checked_in': set()}
        admin_users = set()
        accommodation_initiated = set()
        awaiting_notification = set()

def save_stats(context=None):
    retries = 3
    for attempt in range(retries):
        try:
            with open(STATS_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'stats': {k: list(v) for k, v in stats.items()},
                    'admin_users': list(admin_users),
                    'accommodation_initiated': list(accommodation_initiated),
                    'awaiting_notification': list(awaiting_notification)
                }, f, ensure_ascii=False, indent=4)
            logger.info(f"Stats saved")
            return
        except Exception as e:
            logger.error(f"Error saving stats (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 * (2 ** attempt))
            else:
                logger.error("Не удалось сохранить статистику")
                if context:
                    asyncio.create_task(notify_admin(context, f"Ошибка сохранения статистики: {e}"))

# Инициализация
load_stats()

async def startup():
    await init_google_sheets()
    load_registrations()
    load_accommodations()

async def admin_login(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not context.args:
        await update.message.reply_text("Введите пароль: /admin <пароль>")
        return
    password = context.args[0]
    logger.info(f"Admin login attempt: user_id={user_id}")
    if password == ADMIN_PASSWORD and (not ALLOWED_ADMIN_IDS or user_id in ALLOWED_ADMIN_IDS):
        admin_users.add(user_id)
        save_stats(context)
        await update.message.reply_text("Вы авторизованы!", reply_markup=admin_keyboard)
    else:
        logger.info(f"Wrong password or unauthorized: user_id={user_id}")
        await update.message.reply_text("Неверный пароль или доступ запрещен.", reply_markup=get_persistent_keyboard(user_id))

async def handle_admin_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    text = update.message.text.strip()
    logger.info(f"Admin button: user_id={user_id}, text={text}")
    if user_id not in admin_users:
        await update.message.reply_text("Вы не авторизованы.", reply_markup=get_persistent_keyboard(user_id))
        return ConversationHandler.END
    if text == "Статистика":
        stats_message = (
            f"*Статистика:*\n"
            f"Открыли бота: {len(stats['bot_opened'])}\n"
            f"Зарегистрированы: {len(stats['registered'])}\n"
            f"Пришло: {len(stats['checked_in'])}\n"
            f"Расселение: {len(user_room)}"
        )
        await update.message.reply_text(stats_message, parse_mode='Markdown', reply_markup=admin_keyboard)
    elif text == "Очистить регистрации":
        keyboard = [
            [InlineKeyboardButton("Подтвердить", callback_data='confirm_clear')],
            [InlineKeyboardButton("Отмена", callback_data='cancel_clear')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Очистить все регистрации?", reply_markup=reply_markup)
    elif text == "Разложить спать":
        keyboard = [
            [InlineKeyboardButton("Подтвердить", callback_data='confirm_sleep')],
            [InlineKeyboardButton("Отмена", callback_data='cancel_sleep')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Начать процесс расселения?", reply_markup=reply_markup)
    elif text == "Отправить уведомление":
        context.user_data['awaiting_notification'] = True
        save_stats(context)
        await update.message.reply_text(
            "Введите текст уведомления:",
            reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True, one_time_keyboard=True)
        )
        return NOTIFICATION
    elif text == "Выйти из админки":
        admin_users.remove(user_id)
        context.user_data.pop('awaiting_notification', None)
        save_stats(context)
        await update.message.reply_text("Вы вышли из админки.", reply_markup=get_persistent_keyboard(user_id))
    return ConversationHandler.END

async def handle_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    logger.info(f"Handling notification: user_id={user_id}")
    if not context.user_data.get('awaiting_notification', False):
        await update.message.reply_text("Вы не инициировали уведомление.", reply_markup=admin_keyboard)
        return ConversationHandler.END
    text = update.message.text.strip()
    if text == "Отмена":
        context.user_data.pop('awaiting_notification', None)
        save_stats(context)
        await update.message.reply_text("Отправка отменена.", reply_markup=admin_keyboard)
        return ConversationHandler.END
    if not text:
        await update.message.reply_text("Текст не может быть пустым:", reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True, one_time_keyboard=True))
        return NOTIFICATION
    context.user_data.pop('awaiting_notification', None)
    save_stats(context)
    failed = 0
    sent_count = 0
    retries = 3
    users_to_notify = list(stats['bot_opened'])
    for uid in users_to_notify:
        for attempt in range(retries):
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"*Уведомление:*\n{escape_markdown(text)}",
                    parse_mode='Markdown',
                    reply_markup=get_persistent_keyboard(uid)
                )
                sent_count += 1
                await asyncio.sleep(0.05)
                break
            except Exception as e:
                logger.error(f"Ошибка отправки user_id={uid} (попытка {attempt+1}/{retries}): {e}")
                if attempt == retries - 1:
                    failed += 1
                await asyncio.sleep(2 * (2 ** attempt))
    logger.info(f"Notification: sent={sent_count}, failed={failed}")
    if failed > 0:
        await notify_admin(context, f"Не удалось отправить уведомление {failed} пользователям")
    await update.message.reply_text(f"Отправлено {sent_count} пользователям. Не удалось: {failed}.", reply_markup=admin_keyboard)
    return ConversationHandler.END

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    stats['bot_opened'].add(user_id)
    save_stats(context)
    logger.info(f"Start: user_id={user_id}")
    welcome_message = (
        "Молодежный заезд Восток 2025\n"
        "📅 Дата: 25.06.2025 - 01.07.2025\n"
        "🎯 Тема: Христос - мой краеугольный камень\n"
        "Место проведения - Бобруйск, Городок\n"
        "_❕Регистрация с 1 апреля по 1 июня❕_"
    )
    keyboard = admin_keyboard if user_id in admin_users else get_persistent_keyboard(user_id)
    retries = 3
    backoff = 2
    if START_PHOTO_PATH:
        for attempt in range(retries):
            try:
                with open(START_PHOTO_PATH, 'rb') as photo:
                    await update.message.reply_photo(
                        photo=photo,
                        caption=welcome_message,
                        reply_markup=keyboard,
                        parse_mode='Markdown'
                    )
                logger.info(f"Photo sent: user_id={user_id}")
                return ConversationHandler.END
            except Exception as e:
                logger.error(f"Ошибка отправки фото (попытка {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(backoff * (2 ** attempt))
                else:
                    await notify_admin(context, f"Ошибка отправки фото: {e}")
    await update.message.reply_text(welcome_message, reply_markup=keyboard, parse_mode='Markdown')
    return ConversationHandler.END

async def handle_persistent_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    logger.info(f"Persistent button: user_id={user_id}, text={text}")
    if user_id in admin_users:
        await update.message.reply_text("Вы в режиме админа.", reply_markup=admin_keyboard)
        return ConversationHandler.END
    if text == "Регистрация":
        if user_id in registered_users:
            keyboard = []
            if user_id in user_room:
                keyboard.append([InlineKeyboardButton("Отменить расселение", callback_data='cancel_accommodation_user')])
            elif user_id in accommodation_initiated and user_id not in user_room:
                keyboard.append([InlineKeyboardButton("Расселить", callback_data='request_accommodation')])
            keyboard.append([InlineKeyboardButton("QR Code", callback_data='show_qr')])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Вы уже зарегистрированы!", reply_markup=reply_markup)
            return ConversationHandler.END
        rules_message = (
            "*Правила Молодежного заезда Восток 2025:*\n"
            "1. Уважайте всех участников.\n"
            "2. Запрещено употребление алкоголя, курение, наркотики.\n"
            "3. Следуйте распорядку и указаниям организаторов.\n"
            "4. Соблюдайте чистоту.\n"
            "5. Участие только после регистрации и оплаты.\n"
        )
        keyboard = [[InlineKeyboardButton("Согласен", callback_data='agree')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(rules_message, reply_markup=reply_markup, parse_mode='Markdown')
        return ConversationHandler.END
    elif text == "Расписание":
        schedule_message = (
            "Распорядок дня:\n"
            "- 08:00 - Завтрак\n"
            "- 09:00 - Утреннее богослужение\n"
            "- 11:00 - Семинары\n"
            "- 13:00 - Обед\n"
            "- 14:00 - Свободное время\n"
            "- 17:00 - Вечернее богослужение\n"
            "- 19:00 - Ужин\n"
            "- 20:00 - Вечерняя программа"
        )
        await update.message.reply_text(schedule_message, reply_markup=get_persistent_keyboard(user_id))
    elif text == "Спикеры":
        speakers_message = (
            "Спикеры:\n"
            "- Иван Петров - пастор\n"
            "- Анна Смирнова - молодежный лидер\n"
            "- Сергей Ковалев - евангелист"
        )
        await update.message.reply_text(speakers_message, reply_markup=get_persistent_keyboard(user_id))
    elif text == "Место проведения":
        location_message = "Бобруйск, Городок. Подробности позже"
        await update.message.reply_text(location_message, reply_markup=get_persistent_keyboard(user_id))
    elif text == "Контакты":
        await update.message.reply_text(f"Свяжитесь: {ORGANIZER_CONTACT}", reply_markup=get_persistent_keyboard(user_id))
    elif text == "QR Code":
        registration_id = user_registration_ids.get(user_id)
        if registration_id:
            qr = qrcode.make(registration_id)
            img_byte_arr = io.BytesIO()
            qr.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            retries = 3
            for attempt in range(retries):
                try:
                    await update.message.reply_photo(
                        photo=img_byte_arr,
                        caption="Ваш QR-код для регистрации.",
                        reply_markup=get_persistent_keyboard(user_id)
                    )
                    return ConversationHandler.END
                except Exception as e:
                    logger.error(f"Ошибка QR (попытка {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 * (2 ** attempt))
                    else:
                        await notify_admin(context, f"Ошибка QR: {e}")
                        await update.message.reply_text("Не удалось отправить QR.", reply_markup=get_persistent_keyboard(user_id))
        else:
            await update.message.reply_text("Завершите регистрацию.", reply_markup=get_persistent_keyboard(user_id))
    elif text == "Отменить расселение":
        if user_id not in user_room or user_id not in registered_users:
            await update.message.reply_text("Вы не расселены.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        registration_id = user_registration_ids.get(user_id)
        if not registration_id:
            await update.message.reply_text("Ошибка: регистрация не найдена.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        user_name = registrations[registration_id]['name']
        room_number = user_room[user_id]
        if user_name in room_assignments.get(room_number, []):
            room_assignments[room_number].remove(user_name)
        del user_room[user_id]
        save_accommodations(context)
        save_stats(context)
        registrations[registration_id]['accommodation'] = 'Нет'
        save_registrations(context)
        await update.message.reply_text("Расселение отменено.", reply_markup=get_persistent_keyboard(user_id))
        return ConversationHandler.END
    elif text == "Расселить":
        if user_id not in registered_users:
            await update.message.reply_text("Зарегистрируйтесь.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        keyboard = [
            [InlineKeyboardButton("Да", callback_data='need_accommodation')],
            [InlineKeyboardButton("Нет", callback_data='no_accommodation')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Нужно место для ночлега?", reply_markup=reply_markup)
        return ConversationHandler.END
    return ConversationHandler.END

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    logger.info(f"Callback: user_id={user_id}, data={data}")
    if data == 'agree':
        await query.message.reply_text("Напишите ФИО:")
        return NAME
    elif data.startswith('days_'):
        days = int(data.split('_')[1])
        user_data[user_id] = user_data.get(user_id, {})
        user_data[user_id]['days'] = days
        keyboard = [[InlineKeyboardButton(date, callback_data=f'date_{date}')] for date in dates]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Выберите дату приезда:", reply_markup=reply_markup)
        return ARRIVAL_DATE
    elif data.startswith('date_'):
        date = data.split('_')[1]
        user_data[user_id]['arrival_date'] = date
        await query.message.reply_text("Из какого города?", reply_markup=ReplyKeyboardRemove())
        return CITY
    elif data.startswith('gender_'):
        gender = data.split('_')[1]
        user_data[user_id]['gender'] = gender
        registration_id = str(uuid.uuid4())
        data = user_data[user_id]
        registrations[registration_id] = {
            'name': data['name'],
            'days': data['days'],
            'arrival_date': data['arrival_date'],
            'city': data['city'],
            'nick': data['nick'],
            'phone': data['phone'],
            'birth_date': data['birth_date'],
            'gender': data['gender'],
            'accommodation': 'Нет'
        }
        stats['registered'].add(user_id)
        registered_users.add(user_id)
        user_registration_ids[user_id] = registration_id
        save_stats(context)
        save_registrations(context)
        confirmation_message = (
            "Регистрация успешна!\n"
            f"ФИО: {escape_markdown(data['name'])}\n"
            f"Дни: {data['days']}\n"
            f"Приезд: {data['arrival_date']}\n"
            f"Город: {escape_markdown(data['city'])}\n"
            f"Ник: {escape_markdown(data['nick'])}\n"
            f"Телефон: {escape_markdown(data['phone'])}\n"
            f"Рождение: {data['birth_date']}\n"
            f"Пол: {data['gender']}"
        )
        qr = qrcode.make(registration_id)
        img_byte_arr = io.BytesIO()
        qr.save(img_byte_arr, format='PNG')
        img_byte_arr.seek(0)
        channel_message = (
            "*Новая регистрация!*\n"
            f"ФИО: {escape_markdown(data['name'])}\n"
            f"Дни: {data['days']}\n"
            f"Приезд: {data['arrival_date']}\n"
            f"Город: {escape_markdown(data['city'])}\n"
            f"Ник: {escape_markdown(data.get('nick', 'Не указан'))}\n"
            f"Телефон: {escape_markdown(data.get('phone', 'Не указан'))}\n"
            f"Рождение: {data.get('birth_date', 'Не указана')}\n"
            f"Пол: {data.get('gender', 'Не указан')}"
        )
        retries = 3
        backoff = 2
        success = False
        for attempt in range(retries):
            try:
                can_send = await check_channel_permissions(context)
                if not can_send:
                    await notify_admin(context, f"Нет прав в канале {CHANNEL_ID}")
                    break
                await context.bot.send_message(chat_id=CHANNEL_ID, text=channel_message, parse_mode='Markdown')
                success = True
                break
            except Exception as e:
                logger.error(f"Ошибка канала (попытка {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(backoff * (2 ** attempt))
                else:
                    await notify_admin(context, f"Ошибка канала: {e}")
        for attempt in range(retries):
            try:
                await query.message.reply_photo(
                    photo=img_byte_arr,
                    caption=confirmation_message,
                    reply_markup=get_persistent_keyboard(user_id),
                    parse_mode='Markdown'
                )
                break
            except Exception as e:
                logger.error(f"Ошибка QR регистрации (попытка {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(backoff * (2 ** attempt))
                else:
                    await notify_admin(context, f"Ошибка QR регистрации: {e}")
                    await query.message.reply_text(confirmation_message, reply_markup=get_persistent_keyboard(user_id), parse_mode='Markdown')
        user_data.pop(user_id, None)
        return ConversationHandler.END
    elif data == 'confirm_clear':
        if user_id not in admin_users:
            await query.message.reply_text("Вы не админ.")
            return ConversationHandler.END
        stats['registered'].clear()
        stats['checked_in'].clear()
        registered_users.clear()
        registrations.clear()
        user_registration_ids.clear()
        room_assignments.clear()
        room_assignments.update({i+1: [] for i in range(10)})
        user_room.clear()
        accommodation_initiated.clear()
        context.user_data.pop('awaiting_notification', None)
        save_stats(context)
        save_registrations(context)
        save_accommodations(context)
        await query.message.edit_text("Данные очищены!", reply_markup=None)
        await query.message.reply_text("Выберите действие:", reply_markup=admin_keyboard)
        retries = 3
        for uid in stats['bot_opened']:
            for attempt in range(retries):
                try:
                    await context.bot.send_message(
                        chat_id=uid,
                        text="Регистрации очищены. Зарегистрируйтесь заново.",
                        reply_markup=get_persistent_keyboard(uid)
                    )
                    await asyncio.sleep(0.1)
                    break
                except Exception as e:
                    logger.error(f"Ошибка обновления клавиатуры user_id={uid} (попытка {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 * (2 ** attempt))
                    else:
                        await notify_admin(context, f"Ошибка обновления клавиатуры: {e}")
        return ConversationHandler.END
    elif data == 'cancel_clear':
        await query.message.edit_text("Очистка отменена.", reply_markup=None)
        await query.message.reply_text("Выберите действие:", reply_markup=admin_keyboard)
        return ConversationHandler.END
    elif data == 'confirm_sleep':
        if user_id not in admin_users:
            await query.message.reply_text("Вы не админ.")
            return ConversationHandler.END
        sent_count = 0
        keyboard = [
            [InlineKeyboardButton("Да", callback_data='need_accommodation')],
            [InlineKeyboardButton("Нет", callback_data='no_accommodation')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        retries = 3
        for uid in registered_users:
            for attempt in range(retries):
                try:
                    accommodation_initiated.add(uid)
                    await context.bot.send_message(
                        chat_id=uid,
                        text="Нужно место для ночлега?",
                        reply_markup=reply_markup
                    )
                    sent_count += 1
                    await asyncio.sleep(0.1)
                    break
                except Exception as e:
                    logger.error(f"Ошибка отправки user_id={uid} (попытка {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 * (2 ** attempt))
                    else:
                        await notify_admin(context, f"Ошибка расселения: {e}")
        save_stats(context)
        await query.message.edit_text(f"Расселение начато. Отправлено {sent_count} пользователям.", reply_markup=None)
        await query.message.reply_text("Выберите действие:", reply_markup=admin_keyboard)
        return ConversationHandler.END
    elif data == 'cancel_sleep':
        await query.message.edit_text("Расселение отменено.", reply_markup=None)
        await query.message.reply_text("Выберите действие:", reply_markup=admin_keyboard)
        return ConversationHandler.END
    elif data == 'need_accommodation':
        if user_id not in registered_users:
            await query.message.reply_text("Зарегистрируйтесь.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        registration_id = user_registration_ids.get(user_id)
        if not registration_id:
            await query.message.reply_text("Регистрация не найдена.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        gender = registrations[registration_id]['gender']
        keyboard = []
        row = []
        available_rooms = False
        if gender == "Мужской":
            rooms_range = range(1, 6)
        elif gender == "Женский":
            rooms_range = range(6, 11)
        else:
            await query.message.reply_text("Пол не указан.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        for room in rooms_range:
            if room not in room_assignments:
                room_assignments[room] = []
            occupied = len(room_assignments[room])
            if occupied < 15:
                row.append(InlineKeyboardButton(f"{room} дом ({occupied}/15)", callback_data=f'room_{room}'))
                available_rooms = True
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
        if row:
            keyboard.append(row)
        if not available_rooms:
            await query.message.reply_text("Все дома заняты.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text("Выберите дом:", reply_markup=reply_markup)
        return ROOM
    elif data == 'no_accommodation':
        await query.message.edit_text("Запаситесь спреями.", reply_markup=None)
        await query.message.reply_text("Вы отказались от расселения.", reply_markup=get_persistent_keyboard(user_id))
        return ConversationHandler.END
    elif data == 'request_accommodation':
        if user_id not in registered_users:
            await query.message.reply_text("Зарегистрируйтесь.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        keyboard = [
            [InlineKeyboardButton("Да", callback_data='need_accommodation')],
            [InlineKeyboardButton("Нет", callback_data='no_accommodation')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Нужно место для ночлега?", reply_markup=reply_markup)
        return ConversationHandler.END
    elif data.startswith('room_'):
        try:
            room_number = int(data.split('_')[1])
            if room_number not in range(1, 11):
                await query.message.reply_text("Недопустимый дом.", reply_markup=get_persistent_keyboard(user_id))
                return ConversationHandler.END
            registration_id = user_registration_ids.get(user_id)
            if not registration_id:
                await query.message.reply_text("Регистрация не найдена.", reply_markup=get_persistent_keyboard(user_id))
                return ConversationHandler.END
            gender = registrations[registration_id]['gender']
            if (gender == "Мужской" and room_number > 5) or (gender == "Женский" and room_number < 6):
                await query.message.reply_text("Дом недоступен.", reply_markup=get_persistent_keyboard(user_id))
                return ConversationHandler.END
            if room_number not in room_assignments:
                room_assignments[room_number] = []
            occupied = len(room_assignments[room_number])
            if occupied >= 15:
                await query.message.reply_text("Дом занят.", reply_markup=get_persistent_keyboard(user_id))
                return ConversationHandler.END
            user_name = registrations[registration_id]['name']
            for r in range(1, 11):
                if user_name in room_assignments.get(r, []):
                    room_assignments[r].remove(user_name)
            room_assignments[room_number].append(user_name)
            user_room[user_id] = room_number
            save_accommodations(context)
            data = registrations[registration_id]
            data['accommodation'] = 'Да'
            save_registrations(context)
            await query.message.edit_text(f"Вы забронировали дом {room_number}.", parse_mode='Markdown')
            response = (
                "*Ваше место:*\n"
                f"ФИО: {escape_markdown(data['name'])}\n"
                f"Дни: {data['days']}\n"
                f"Приезд: {data['arrival_date']}\n"
                f"Город: {escape_markdown(data['city'])}\n"
                f"Ник: {escape_markdown(data.get('nick', 'Не указан'))}\n"
                f"Телефон: {escape_markdown(data.get('phone', 'Не указан'))}\n"
                f"Рождение: {data.get('birth_date', 'Не указана')}\n"
                f"Пол: {data.get('gender', 'Не указан')}\n"
                f"Дом: {room_number}"
            )
            qr = qrcode.make(registration_id)
            img_byte_arr = io.BytesIO()
            qr.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            retries = 3
            for attempt in range(retries):
                try:
                    await query.message.reply_photo(
                        photo=img_byte_arr,
                        caption=response,
                        parse_mode='Markdown',
                        reply_markup=get_persistent_keyboard(user_id)
                    )
                    break
                except Exception as e:
                    logger.error(f"Ошибка QR расселения (попытка {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 * (2 ** attempt))
                    else:
                        await notify_admin(context, f"Ошибка QR расселения: {e}")
                        await query.message.reply_text(response, reply_markup=get_persistent_keyboard(user_id), parse_mode='Markdown')
            await query.message.reply_text("Можно отменить расселение.", reply_markup=get_persistent_keyboard(user_id))
        except Exception as e:
            logger.error(f"Ошибка выбора дома: {e}")
            await notify_admin(context, f"Ошибка дома user_id={user_id}: {e}")
            await query.message.reply_text("Ошибка выбора дома.", reply_markup=get_persistent_keyboard(user_id))
        return ConversationHandler.END
    elif data == 'cancel_accommodation_user':
        if user_id not in user_room or user_id not in registered_users:
            await query.message.reply_text("Вы не расселены.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        registration_id = user_registration_ids.get(user_id)
        if not registration_id:
            await query.message.reply_text("Регистрация не найдена.", reply_markup=get_persistent_keyboard(user_id))
            return ConversationHandler.END
        user_name = registrations[registration_id]['name']
        room_number = user_room[user_id]
        if user_name in room_assignments.get(room_number, []):
            room_assignments[room_number].remove(user_name)
        del user_room[user_id]
        save_accommodations(context)
        save_stats(context)
        registrations[registration_id]['accommodation'] = 'Нет'
        save_registrations(context)
        await query.message.edit_text("Расселение отменено.", reply_markup=get_persistent_keyboard(user_id))
        return ConversationHandler.END
    elif data == 'show_qr':
        registration_id = user_registration_ids.get(user_id)
        if registration_id:
            qr = qrcode.make(registration_id)
            img_byte_arr = io.BytesIO()
            qr.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            retries = 3
            for attempt in range(retries):
                try:
                    await query.message.reply_photo(
                        photo=img_byte_arr,
                        caption="Ваш QR-код.",
                        reply_markup=get_persistent_keyboard(user_id)
                    )
                    break
                except Exception as e:
                    logger.error(f"Ошибка QR (попытка {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 * (2 ** attempt))
                    else:
                        await notify_admin(context, f"Ошибка QR: {e}")
                        await query.message.reply_text("Не удалось отправить QR.", reply_markup=get_persistent_keyboard(user_id))
        else:
            await query.message.reply_text("Завершите регистрацию.", reply_markup=get_persistent_keyboard(user_id))
        return ConversationHandler.END
    return ConversationHandler.END

async def name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name_text = update.message.text.strip()
    logger.info(f"Name: user_id={user_id}, name={name_text}")
    if not name_text or len(name_text.split()) < 2:
        await update.message.reply_text("Введите полное ФИО:")
        return NAME
    user_data[user_id] = {'name': name_text}
    keyboard = [
        [InlineKeyboardButton(f"{days} день: {days*10}$", callback_data=f'days_{days}') for days in [1, 2]],
        [InlineKeyboardButton(f"{days} дня: {days*10}$", callback_data=f'days_{days}') for days in [3, 4]]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("На сколько дней?", reply_markup=reply_markup)
    return DAYS

async def city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    city_text = update.message.text.strip()
    logger.info(f"City: user_id={user_id}, city={city_text}")
    if not city_text or len(city_text) < 2:
        await update.message.reply_text("Введите город:")
        return CITY
    user_data[user_id]['city'] = city_text
    username = update.effective_user.username or "Не указан"
    user_data[user_id]['nick'] = username
    keyboard = [[KeyboardButton("Поделиться контактом", request_contact=True)]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("Укажите телефон:", reply_markup=reply_markup)
    return PHONE

async def phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if update.message.contact:
        phone_number = update.message.contact.phone_number
    else:
        phone_number = update.message.text.strip()
    logger.info(f"Phone: user_id={user_id}, phone={phone_number}")
    if not re.match(r"^\+?\d{10,15}$", phone_number):
        await update.message.reply_text("Введите корректный телефон:")
        return PHONE
    user_data[user_id]['phone'] = phone_number
    await update.message.reply_text("Дата рождения (ДД.ММ.ГГГГ):", reply_markup=ReplyKeyboardRemove())
    return BIRTH_DATE

async def birth_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    birth_date_text = update.message.text.strip()
    logger.info(f"Birth date: user_id={user_id}, birth_date={birth_date_text}")
    if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", birth_date_text):
        await update.message.reply_text("Введите дату ДД.ММ.ГГГГ:")
        return BIRTH_DATE
    try:
        day, month, year = map(int, birth_date_text.split('.'))
        if not (1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2025):
            raise ValueError
    except ValueError:
        await update.message.reply_text("Некорректная дата:")
        return BIRTH_DATE
    user_data[user_id]['birth_date'] = birth_date_text
    keyboard = [
        [InlineKeyboardButton("Мужской", callback_data='gender_Мужской')],
        [InlineKeyboardButton("Женский", callback_data='gender_Женский')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await update.message.reply_text("Выберите пол:", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ошибка клавиатуры пола: {e}")
        await notify_admin(context, f"Ошибка клавиатуры пола: {e}")
        await update.message.reply_text("Ошибка. Попробуйте снова.", reply_markup=ReplyKeyboardRemove())
        return BIRTH_DATE
    return GENDER

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.info(f"Cancel: user_id={user_id}")
    keyboard = admin_keyboard if user_id in admin_users else get_persistent_keyboard(user_id)
    context.user_data.pop('awaiting_notification', None)
    save_stats(context)
    await update.message.reply_text("Действие отменено.", reply_markup=keyboard)
    user_data.pop(user_id, None)
    return ConversationHandler.END

async def check_qr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in admin_users:
        await update.message.reply_text("Вы не админ.")
        return
    if not context.args:
        await update.message.reply_text("Пример: /check_qr <ID>")
        return
    registration_id = context.args[0]
    if registration_id in registrations:
        data = registrations[registration_id]
        user_id = next((uid for uid, rid in user_registration_ids.items() if rid == registration_id), None)
        accommodation_status = "Да" if user_id in user_room else "Нет"
        room_number = user_room.get(user_id, "Не выбрано")
        accommodation_text = f"Расселение: {room_number} Дом" if accommodation_status == "Да" else "Расселение: Не надо"
        response = (
            "*Регистрация найдена!*\n"
            f"ФИО: {escape_markdown(data['name'])}\n"
            f"Дни: {data['days']}\n"
            f"Приезд: {data['arrival_date']}\n"
            f"Город: {escape_markdown(data['city'])}\n"
            f"Ник: {escape_markdown(data.get('nick', 'Не указан'))}\n"
            f"Телефон: {escape_markdown(data.get('phone', 'Не указан'))}\n"
            f"Рождение: {data.get('birth_date', 'Не указана')}\n"
            f"Пол: {data.get('gender', 'Не указан')}\n"
            f"{accommodation_text}\n"
            "Участник зарегистрирован."
        )
        retries = 3
        for attempt in range(retries):
            try:
                records = worksheet.get_all_records()
                row_idx = None
                for idx, record in enumerate(records):
                    if record['registration_id'] == registration_id:
                        row_idx = idx + 2
                        break
                if row_idx:
                    worksheet.format(f"A{row_idx}:K{row_idx}", {
                        "backgroundColor": {"red": 0.678, "green": 1.0, "blue": 0.678}
                    })
                else:
                    response += "\nОшибка: строка не найдена."
                break
            except Exception as e:
                logger.error(f"Ошибка форматирования (попытка {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 * (2 ** attempt))
                else:
                    await notify_admin(context, f"Ошибка форматирования: {e}")
                    response += f"\nОшибка форматирования: {e}"
    else:
        response = "Регистрация не найдена."
    await update.message.reply_text(response, parse_mode='Markdown', reply_markup=admin_keyboard)

async def scan_qr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in admin_users:
        await update.message.reply_text("Вы не админ.")
        return
    photo = update.message.photo[-1]
    photo_file = await photo.get_file()
    photo_bytes = await photo_file.download_as_bytearray()
    img = Image.open(io.BytesIO(photo_bytes))
    decoded_objects = decode(img)
    if decoded_objects:
        registration_id = decoded_objects[0].data.decode('utf-8')
        if registration_id in registrations:
            data = registrations[registration_id]
            user_id = next((uid for uid, rid in user_registration_ids.items() if rid == registration_id), None)
            accommodation_status = "Да" if user_id in user_room else "Нет"
            room_number = user_room.get(user_id, "Не выбрано")
            accommodation_text = f"Расселение: {room_number} Дом" if accommodation_status == "Да" else "Расселение: Не надо"
            stats['checked_in'].add(registration_id)
            save_stats(context)
            response = (
                "*Регистрация найдена!*\n"
                f"ФИО: {escape_markdown(data['name'])}\n"
                f"Дни: {data['days']}\n"
                f"Приезд: {data['arrival_date']}\n"
                f"Город: {escape_markdown(data['city'])}\n"
                f"Ник: {escape_markdown(data.get('nick', 'Не указан'))}\n"
                f"Телефон: {escape_markdown(data.get('phone', 'Не указан'))}\n"
                f"Рождение: {data.get('birth_date', 'Не указана')}\n"
                f"Пол: {data.get('gender', 'Не указан')}\n"
                f"{accommodation_text}\n"
                "Участник зарегистрирован."
            )
            retries = 3
            for attempt in range(retries):
                try:
                    records = worksheet.get_all_records()
                    row_idx = None
                    for idx, record in enumerate(records):
                        if record['registration_id'] == registration_id:
                            row_idx = idx + 2
                            break
                    if row_idx:
                        worksheet.format(f"A{row_idx}:K{row_idx}", {
                            "backgroundColor": {"red": 0.678, "green": 1.0, "blue": 0.678}
                        })
                    else:
                        response += "\nОшибка: строка не найдена."
                    break
                except Exception as e:
                    logger.error(f"Ошибка форматирования (попытка {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 * (2 ** attempt))
                    else:
                        await notify_admin(context, f"Ошибка форматирования: {e}")
                        response += f"\nОшибка форматирования: {e}"
            channel_message = (
                "*Новая регистрация!*\n"
                f"ФИО: {escape_markdown(data['name'])}\n"
                f"Дни: {data['days']}\n"
                f"Приезд: {data['arrival_date']}\n"
                f"Город: {escape_markdown(data['city'])}\n"
                f"Ник: {escape_markdown(data.get('nick', 'Не указан'))}\n"
                f"Телефон: {escape_markdown(data.get('phone', 'Не указан'))}\n"
                f"Рождение: {data.get('birth_date', 'Не указана')}\n"
                f"Пол: {data.get('gender', 'Не указан')}"
            )
            success = False
            for attempt in range(retries):
                try:
                    can_send = await check_channel_permissions(context)
                    if not can_send:
                        await notify_admin(context, f"Нет прав в канале {CHANNEL_ID}")
                        break
                    await context.bot.send_message(chat_id=CHANNEL_ID, text=channel_message, parse_mode='Markdown')
                    success = True
                    break
                except Exception as e:
                    logger.error(f"Ошибка канала (попытка {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 * (2 ** attempt))
                    else:
                        await notify_admin(context, f"Ошибка канала: {e}")
                        response += f"\nОшибка канала: {e}"
        else:
            response = "Регистрация не найдена."
    else:
        response = "Не удалось прочитать QR."
    await update.message.reply_text(response, parse_mode='Markdown', reply_markup=admin_keyboard)

def update_accommodation_status(user_id, context=None):
    if worksheet is None:
        logger.error("Google Sheets не инициализирован")
        if context:
            asyncio.create_task(notify_admin(context, "Google Sheets не инициализирован"))
        return
    retries = 3
    for attempt in range(retries):
        try:
            records = worksheet.get_all_records()
            for idx, record in enumerate(records):
                if record['user_id'] == str(user_id):
                    cell_list = worksheet.row_values(idx + 1)
                    cell_list[-1] = "Да" if user_id in user_room else "Нет"
                    worksheet.update(f'A{idx+1}', [cell_list])
                    logger.info(f"Status updated: user_id={user_id}")
                    return
            logger.warning(f"User_id {user_id} not found")
            return
        except Exception as e:
            logger.error(f"Ошибка статуса (попытка {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 * (2 ** attempt))
            else:
                logger.error("Не удалось обновить статус")
                if context:
                    asyncio.create_task(notify_admin(context, f"Ошибка статуса user_id={user_id}: {e}"))

async def main():
    application = Application.builder().token(TOKEN).build()
    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(button_callback, pattern='^(agree|confirm_clear|cancel_clear|confirm_sleep|cancel_sleep|need_accommodation|no_accommodation|room_[1-9]|room_10|cancel_accommodation_user|request_accommodation|show_qr|gender_Мужской|gender_Женский)$'),
            MessageHandler(filters.Text(["Отправить уведомление"]) & ~filters.COMMAND, handle_admin_buttons),
            MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Text(["Отмена"]), handle_notification)
        ],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, name)],
            DAYS: [CallbackQueryHandler(button_callback, pattern='^days_[1-4]$')],
            ARRIVAL_DATE: [CallbackQueryHandler(button_callback, pattern='^date_(03\\.07\\.2025|04\\.07\\.2025|05\\.07\\.2025|06\\.07\\.2025)$')],
            CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, city)],
            PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, phone),
                MessageHandler(filters.CONTACT, phone)
            ],
            BIRTH_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, birth_date)],
            GENDER: [CallbackQueryHandler(button_callback, pattern='^gender_(Мужской|Женский)$')],
            ROOM: [CallbackQueryHandler(button_callback, pattern='^room_[1-9]|room_10$')],
            NOTIFICATION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_notification),
                MessageHandler(filters.Text(["Отмена"]), cancel)
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )
    admin_buttons = ["Статистика", "Очистить регистрации", "Разложить спать", "Отправить уведомление", "Выйти из админки"]
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_login))
    application.add_handler(MessageHandler(filters.Text(admin_buttons) & ~filters.COMMAND, handle_admin_buttons))
    application.add_handler(MessageHandler(filters.Text(["Регистрация", "Расписание", "Спикеры", "Место проведения", "Контакты", "QR Code", "Отменить расселение", "Расселить"]) & ~filters.COMMAND, handle_persistent_buttons))
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("check_qr", check_qr))
    application.add_handler(MessageHandler(filters.PHOTO, scan_qr))
    await startup()
    await application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    asyncio.run(main())
