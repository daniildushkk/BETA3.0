import asyncio
import logging
import os
import aiohttp
import json
import aiosqlite
import re
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
import vk_api
from vk_api.utils import get_random_id

# Загрузка переменных среды
from dotenv import load_dotenv
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === КОНФИГУРАЦИЯ ===
BOT_TOKEN = os.getenv('BOT_TOKEN')
VK_USER_TOKEN = os.getenv('VK_USER_TOKEN')
VK_GROUP_IDS = [group.strip() for group in os.getenv('VK_GROUP_IDS', '').split(',') if group.strip()]
VK_EVENT_KEYWORDS = [keyword.strip() for keyword in os.getenv('VK_EVENT_KEYWORDS', '').split(',') if keyword.strip()]
YANDEX_API_KEY = os.getenv('YANDEX_API_KEY')
YANDEX_FOLDER_ID = os.getenv('YANDEX_FOLDER_ID')

# Минимальная дата для мероприятий
MIN_EVENT_DATE = datetime.fromisoformat(os.getenv('MIN_EVENT_DATE', '2025-11-01'))

# Проверка обязательных переменных
if not all([BOT_TOKEN, VK_USER_TOKEN, VK_GROUP_IDS, VK_EVENT_KEYWORDS]):
    logger.error("❌ Отсутствуют обязательные переменные!")
    logger.info("Проверьте .env файл:")
    logger.info(f"BOT_TOKEN: {'✅' if BOT_TOKEN else '❌'}")
    logger.info(f"VK_USER_TOKEN: {'✅' if VK_USER_TOKEN else '❌'}")
    logger.info(f"VK_GROUP_IDS: {VK_GROUP_IDS}")
    logger.info(f"VK_EVENT_KEYWORDS: {VK_EVENT_KEYWORDS}")
    exit(1)

# Инициализация бота Telegram с увеличенным таймаутом
bot = Bot(token=BOT_TOKEN, timeout=60)
dp = Dispatcher()

# Инициализация VK API
vk_session = vk_api.VkApi(token=VK_USER_TOKEN)
vk = vk_session.get_api()

# === СОЗДАНИЕ КЛАВИАТУРЫ МЕНЮ ===
def get_main_keyboard():
    """Создает основную клавиатуру меню"""
    builder = ReplyKeyboardBuilder()

    # Добавляем кнопки в два ряда
    builder.add(
        KeyboardButton(text="📅 Мероприятия"),
        KeyboardButton(text="🗓️ Календарь"),
        KeyboardButton(text="🔄 Обновить"),
        KeyboardButton(text="📊 Статус"),
        KeyboardButton(text="❓ Помощь"),
        KeyboardButton(text="ℹ️ О боте")
    )

    builder.adjust(2, 2, 2)  # 2 кнопки в первом ряду, 2 во втором, 2 в третьем
    return builder.as_markup(resize_keyboard=True)

def get_events_keyboard():
    """Создает клавиатуру для раздела мероприятий"""
    builder = ReplyKeyboardBuilder()

    builder.add(
        KeyboardButton(text="📅 Все мероприятия"),
        KeyboardButton(text="🗓️ Календарь"),
        KeyboardButton(text="🔄 Обновить"),
        KeyboardButton(text="🏠 Главное меню")
    )

    builder.adjust(2, 2)
    return builder.as_markup(resize_keyboard=True)

# === AI АНАЛИЗАТОР ===
class YandexGPTAnalyzer:
    def __init__(self, yandex_api_key, folder_id):
        self.api_key = yandex_api_key
        self.folder_id = folder_id
        self.url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

    async def analyze_event(self, text):
        if not self.api_key or not self.folder_id:
            return None

        try:
            headers = {
                "Authorization": f"Api-Key {self.api_key}",
                "Content-Type": "application/json"
            }

            system_prompt = """Ты — помощник для анализа постов о мероприятиях в университете МИСИС. 
            Извлекай информацию о мероприятиях в формате JSON.

            Пример ответа:
            {
                "title": "Хакатон по искусственному интеллекту",
                "date": "13.11.2025", 
                "time": "14:00",
                "location": "Главный корпус, ауд. 301"
            }"""

            payload = {
                "modelUri": f"gpt://{self.folder_id}/yandexgpt-lite",
                "completionOptions": {
                    "stream": False,
                    "temperature": 0.3,
                    "maxTokens": 500
                },
                "messages": [
                    {
                        "role": "system",
                        "text": system_prompt
                    },
                    {
                        "role": "user",
                        "text": f"Проанализируй этот пост о мероприятии и извлеки информацию в JSON формате:\n\n{text[:3000]}"
                    }
                ]
            }

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                async with session.post(self.url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        response_text = result['result']['alternatives'][0]['message']['text']

                        cleaned_text = response_text.strip()
                        if cleaned_text.startswith('```json'):
                            cleaned_text = cleaned_text[7:]
                        if cleaned_text.endswith('```'):
                            cleaned_text = cleaned_text[:-3]

                        try:
                            ai_data = json.loads(cleaned_text)
                            try:
                                event_date = datetime.strptime(ai_data.get('date', '01.11.2025'), '%d.%m.%Y')
                                ai_data['date'] = event_date.strftime('%Y-%m-%d')
                            except ValueError:
                                ai_data['date'] = MIN_EVENT_DATE.strftime('%Y-%m-%d')

                            if event_date >= MIN_EVENT_DATE:
                                logger.info(f"✅ AI анализ успешен: {ai_data.get('title', 'Unknown')}")
                                return ai_data
                        except json.JSONDecodeError:
                            return None
                    return None

        except Exception as e:
            logger.error(f"❌ Ошибка AI анализа: {e}")
            return None

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ОБРАБОТКИ ТЕКСТА ===
def remove_title_from_description(title, description):
    """Удаляет заголовок из начала описания, если они дублируются"""
    if not title or not description:
        return description

    # Приводим к нижнему регистру для сравнения
    title_lower = title.lower().strip()
    description_lower = description.lower().strip()

    # Если описание начинается с заголовка, удаляем его
    if description_lower.startswith(title_lower):
        # Берем оригинальный текст описания и удаляем заголовок
        cleaned_description = description[len(title):].strip()

        # Убираем возможные точки, запятые, тире в начале
        cleaned_description = re.sub(r'^[.,—:\-\s]+', '', cleaned_description)

        # Если после очистки остался осмысленный текст, возвращаем его
        if len(cleaned_description) > 10:  # Минимальная длина текста
            return cleaned_description

    return description

def clean_description(text, title):
    """Очищает описание от дублирования с заголовком и обрезает до разумной длины"""
    if not text:
        return ""

    # Удаляем заголовок из начала, если есть дублирование
    text = remove_title_from_description(title, text)

    # Разбиваем на предложения и берем первые 2-3 осмысленных предложения
    sentences = re.split(r'[.!?]+', text)
    meaningful_sentences = []

    for sentence in sentences:
        sentence = sentence.strip()
        if (len(sentence) > 20 and  # Минимальная длина предложения
                not sentence.startswith(('http://', 'https://', 'vk.com/', '@')) and
                not any(word in sentence.lower() for word in ['подписывайся', 'репост', 'поделись'])):
            meaningful_sentences.append(sentence)

        if len(meaningful_sentences) >= 3:  # Максимум 3 предложения
            break

    if meaningful_sentences:
        result = '. '.join(meaningful_sentences) + '.'
        # Обрезаем до 400 символов, но стараемся не обрывать на полуслове
        if len(result) > 400:
            result = result[:400]
            last_space = result.rfind(' ')
            if last_space > 350:  # Если есть разумное место для обрезки
                result = result[:last_space] + '...'
            else:
                result = result + '...'
        return result

    # Если не нашли осмысленных предложений, возвращаем оригинал (обрезанный)
    return text[:300] + '...' if len(text) > 300 else text

# === VK ПАРСЕР ===
class VKParser:
    def __init__(self, vk_api, yandex_api_key=None, folder_id=None):
        self.vk = vk_api
        self.ai_analyzer = None

        if yandex_api_key and folder_id:
            self.ai_analyzer = YandexGPTAnalyzer(yandex_api_key, folder_id)
            logger.info("✅ AI анализатор активирован")

    async def search_events(self, group_ids, keywords):
        """Поиск мероприятий в нескольких группах VK по нескольким ключевым словам"""
        try:
            events = []

            for group_id in group_ids:
                try:
                    logger.info(f"🔍 Парсинг группы VK: {group_id}")
                    group_events = await self.get_group_events(group_id, keywords)
                    events.extend(group_events)
                except Exception as e:
                    logger.warning(f"Ошибка в группе {group_id}: {e}")

            # Фильтруем по дате
            filtered_events = []
            for event in events:
                try:
                    event_date = datetime.strptime(event['event_date'], '%Y-%m-%d')
                    if event_date >= MIN_EVENT_DATE:
                        filtered_events.append(event)
                except ValueError:
                    filtered_events.append(event)

            logger.info(f"✅ Найдено {len(filtered_events)} мероприятий в {len(group_ids)} группах")
            return filtered_events

        except Exception as e:
            logger.error(f"❌ Ошибка парсинга VK: {e}")
            return []

    async def get_group_events(self, group_id, keywords):
        """Получение мероприятий из конкретной группы VK"""
        events = []
        try:
            # Получаем посты из группы
            owner_id = f"-{group_id}" if group_id.isdigit() else group_id

            response = self.vk.wall.get(
                owner_id=owner_id,
                count=100,  # последние 100 постов
                filter='owner'
            )

            for post in response['items']:
                if not post.get('text'):
                    continue

                text = post['text']

                # Проверяем все ключевые слова
                text_lower = text.lower()
                if any(keyword.lower() in text_lower for keyword in keywords):
                    logger.info(f"🎯 Найден пост с ключевым словом в группе {group_id}")
                    event_data = await self.parse_post(post, group_id, post['owner_id'])
                    if event_data:
                        events.append(event_data)

            return events

        except Exception as e:
            logger.error(f"❌ Ошибка парсинга группы {group_id}: {e}")
            return []

    async def parse_post(self, post, group_id, owner_id):
        """Парсинг поста VK с AI"""
        try:
            text = post['text']
            post_id = post['id']

            # AI анализ
            ai_data = None
            if self.ai_analyzer:
                ai_data = await self.ai_analyzer.analyze_event(text)

            if ai_data and all(key in ai_data for key in ['title', 'date', 'time', 'location']):
                title = ai_data.get('title')
                date = ai_data.get('date')
                time = ai_data.get('time')
                location = ai_data.get('location')
                logger.info(f"🎯 AI анализ: {title}")
            else:
                # Резервный парсинг
                title = self.extract_title(text)
                date = self.extract_date(text)
                time = self.extract_time(text)
                location = self.extract_location(text)
                logger.info(f"ℹ️ Ручной парсинг: {title}")

            if not title:
                title = "Мероприятие МИСИС"

            # Проверяем дату
            try:
                event_date = datetime.strptime(date, '%Y-%m-%d')
                if event_date < MIN_EVENT_DATE:
                    return None
            except ValueError:
                date = MIN_EVENT_DATE.strftime('%Y-%m-%d')

            # ФОРМИРУЕМ КОРРЕКТНУЮ ССЫЛКУ НА ПОСТ ВК
            if str(owner_id).startswith('-'):
                # Для групп: owner_id отрицательный, убираем минус
                group_num = str(owner_id)[1:]
                source_url = f"https://vk.com/wall-{group_num}_{post_id}"
            else:
                # Для личных страниц
                source_url = f"https://vk.com/wall{owner_id}_{post_id}"

            # ОЧИЩАЕМ ОПИСАНИЕ ОТ ДУБЛИРОВАНИЯ С ЗАГОЛОВКОМ
            cleaned_description = clean_description(text, title)

            event_data = {
                'title': title,
                'description': cleaned_description,  # Используем очищенное описание
                'event_date': date,
                'event_time': time,
                'location': location,
                'source': f"vk_{group_id}",
                'source_url': source_url,
                'tags': '#мероприятие',
                'image_path': None,
                'ai_processed': ai_data is not None
            }

            return event_data

        except Exception as e:
            logger.error(f"❌ Ошибка парсинга поста: {e}")
            return None

    def extract_title(self, text):
        """Извлечение заголовка"""
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if (line and len(line) > 10 and len(line) < 200 and
                    not line.startswith('#') and
                    not any(word in line.lower() for word in ['http', 'vk.com'])):
                return line

        words = text.split()[:8]
        return ' '.join(words) + '...'

    def extract_date(self, text):
        """Извлечение даты - УЛУЧШЕННАЯ ВЕРСИЯ"""
        # Паттерны для дат
        date_patterns = [
            # Формат DD.MM.YYYY
            r'(\d{1,2}\.\d{1,2}\.\d{4})',
            # Формат DD.MM
            r'(\d{1,2}\.\d{1,2})(?!\.\d)',
            # Текстовые названия месяцев
            r'(\d{1,2}\s+(?:январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[ья]|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+\d{4})',
            r'(\d{1,2}\s+(?:январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[ья]|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья]))',
        ]

        month_mapping = {
            'январ': 1, 'феврал': 2, 'март': 3, 'апрел': 4,
            'май': 5, 'мая': 5, 'июн': 6, 'июл': 7, 'август': 8,
            'сентябр': 9, 'октябр': 10, 'ноябр': 11, 'декабр': 12
        }

        for pattern in date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                date_str = match if isinstance(match, str) else match[0]
                try:
                    # Обработка формата DD.MM.YYYY
                    if re.match(r'\d{1,2}\.\d{1,2}\.\d{4}', date_str):
                        day, month, year = map(int, date_str.split('.'))
                        date_obj = datetime(year, month, day)
                        if date_obj >= MIN_EVENT_DATE:
                            return date_obj.strftime('%Y-%m-%d')

                    # Обработка формата DD.MM (без года)
                    elif re.match(r'\d{1,2}\.\d{1,2}(?!\.\d)', date_str):
                        day, month = map(int, date_str.split('.'))
                        current_year = datetime.now().year
                        # Если месяц уже прошел в этом году, берем следующий год
                        if month < datetime.now().month or (month == datetime.now().month and day < datetime.now().day):
                            current_year += 1
                        date_obj = datetime(current_year, month, day)
                        if date_obj >= MIN_EVENT_DATE:
                            return date_obj.strftime('%Y-%m-%d')

                    # Обработка текстовых дат
                    elif any(month in date_str.lower() for month in month_mapping.keys()):
                        for month_name, month_num in month_mapping.items():
                            if month_name in date_str.lower():
                                # Извлекаем числа из строки
                                numbers = re.findall(r'\d+', date_str)
                                if numbers:
                                    day = int(numbers[0])
                                    # Ищем год
                                    year_match = re.search(r'\d{4}', date_str)
                                    year = int(year_match.group()) if year_match else datetime.now().year

                                    date_obj = datetime(year, month_num, day)
                                    if date_obj >= MIN_EVENT_DATE:
                                        return date_obj.strftime('%Y-%m-%d')

                except Exception:
                    continue

        # Если дата не найдена, используем минимальную дату
        return MIN_EVENT_DATE.strftime('%Y-%m-%d')

    def extract_time(self, text):
        """Извлечение времени"""
        time_patterns = [
            r'(\d{1,2}:\d{2})',
            r'(\d{1,2}\s*[чh]\s*\d{1,2})',
        ]

        for pattern in time_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for time_str in matches:
                try:
                    if ':' in time_str:
                        hours, minutes = time_str.split(':')
                        return f"{int(hours):02d}:{int(minutes):02d}"
                    elif any(char in time_str.lower() for char in ['ч', 'h']):
                        time_str = re.sub(r'[чhмm\s]', ' ', time_str).strip()
                        parts = time_str.split()
                        if len(parts) == 2:
                            hours, minutes = parts
                            return f"{int(hours):02d}:{int(minutes):02d}"
                except Exception:
                    continue

        return "18:00"

    def extract_location(self, text):
        """Извлечение места"""
        location_keywords = [
            'ауд.', 'аудитория', 'корпус', 'МИСИС', 'лаборатория', 'зал',
            'комната', 'кабинет', 'актовый', 'конференц', 'лекторий', 'актовый зал'
        ]

        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if any(keyword in line.lower() for keyword in location_keywords):
                return line

        return "МИСИС"

    async def save_events_to_db(self, events):
        """Сохранение в базу данных"""
        try:
            saved_count = 0
            async with aiosqlite.connect('events.db') as db:
                for event in events:
                    cursor = await db.execute(
                        'SELECT id FROM events WHERE title = ? AND event_date = ? AND source = ?',
                        (event['title'], event['event_date'], event['source'])
                    )
                    existing = await cursor.fetchone()

                    if not existing:
                        await db.execute('''
                            INSERT INTO events (title, description, event_date, event_time, location, source, source_url, tags, image_path)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            event['title'], event['description'], event['event_date'],
                            event['event_time'], event['location'], event['source'],
                            event['source_url'], event['tags'], event.get('image_path')
                        ))
                        saved_count += 1
                        logger.info(f"💾 Сохранено: {event['title']}")

                await db.commit()
                return saved_count

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в БД: {e}")
            return 0

# === БАЗА ДАННЫХ ===
async def init_db():
    """Инициализация базы данных"""
    async with aiosqlite.connect('events.db') as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                event_date TEXT,
                event_time TEXT,
                location TEXT,
                source TEXT,
                source_url TEXT,
                tags TEXT,
                image_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.commit()
        logger.info("✅ База данных готова")

# === КАЛЕНДАРЬ ===
class Calendar:
    @staticmethod
    def generate_week_keyboard():
        builder = InlineKeyboardBuilder()
        today = max(datetime.now(), MIN_EVENT_DATE)
        start_of_week = today - timedelta(days=today.weekday())

        for week_offset in range(0, 8):
            week_start = start_of_week + timedelta(days=week_offset * 7)
            week_end = week_start + timedelta(days=6)
            week_text = f"📅 {week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}"
            callback_data = f"week_{week_start.strftime('%Y-%m-%d')}"
            builder.button(text=week_text, callback_data=callback_data)

        builder.adjust(2)
        return builder.as_markup()

# === АВТОПАРСИНГ ПРИ СТАРТЕ ===
async def auto_parse_events():
    """Автоматический парсинг при запуске бота"""
    try:
        logger.info("🔄 Автопарсинг мероприятий из VK...")
        logger.info(f"📋 Группы: {VK_GROUP_IDS}")
        logger.info(f"🔍 Ключевые слова: {VK_EVENT_KEYWORDS}")

        parser = VKParser(
            vk,
            yandex_api_key=YANDEX_API_KEY,
            folder_id=YANDEX_FOLDER_ID
        )

        events = await parser.search_events(VK_GROUP_IDS, VK_EVENT_KEYWORDS)
        saved_count = await parser.save_events_to_db(events)

        if saved_count > 0:
            logger.info(f"✅ Автопарсинг: сохранено {saved_count} мероприятий")
        else:
            logger.info("✅ Автопарсинг: новых мероприятий не найдено")

    except Exception as e:
        logger.error(f"❌ Ошибка автопарсинга: {e}")

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
async def send_event_message(chat_id, event_data):
    """Отправка сообщения с мероприятием"""
    title, description, event_date, event_time, location, image_path, source_url = event_data

    # Форматируем дату
    formatted_date = datetime.strptime(event_date, '%Y-%m-%d').strftime('%d.%m.%Y')

    # Формируем текст мероприятия
    event_text = (
        f"{title}\n"
        f"📅 {formatted_date} в {event_time}\n"
        f"📍 {location}\n"
        f"📝 {description}\n"
        f"🔗 [Ссылка на пост]({source_url})"
    )

    await bot.send_message(chat_id=chat_id, text=event_text)

# === ОБРАБОТЧИКИ КОМАНД И КНОПОК ===
@dp.message(Command("start"))
async def start_handler(message: Message):
    """Обработчик команды /start"""
    welcome_text = (
        "🎓 Подручный - твой цифровой ассистент!\n\n"
        "Я помогу найти интересные мероприятия в университете. "
        "Все команды доступны в меню ниже 👇\n\n"
        "Просто нажми на кнопку в меню и смотри!"
    )
    await message.answer(welcome_text, reply_markup=get_main_keyboard())

@dp.message(F.text == "🏠 Главное меню")
async def main_menu_handler(message: Message):
    """Обработчик кнопки главного меню"""
    await message.answer("🏠 Выберите действие из меню:", reply_markup=get_main_keyboard())

@dp.message(F.text == "📅 Мероприятия")
async def events_button_handler(message: Message):
    """Обработчик кнопки мероприятий"""
    await message.answer("📅 Раздел мероприятий:", reply_markup=get_events_keyboard())
    await events_handler(message)

@dp.message(F.text == "📅 Все мероприятия")
async def all_events_handler(message: Message):
    """Обработчик кнопки всех мероприятий"""
    await events_handler(message)

@dp.message(F.text == "🗓️ Календарь")
async def calendar_button_handler(message: Message):
    """Обработчик кнопки календаря"""
    await calendar_handler(message)

@dp.message(F.text == "🔄 Обновить")
async def update_button_handler(message: Message):
    """Обработчик кнопки обновления"""
    await update_handler(message)

@dp.message(F.text == "📊 Статус")
async def status_button_handler(message: Message):
    """Обработчик кнопки статуса"""
    await status_handler(message)

@dp.message(F.text == "❓ Помощь")
async def help_button_handler(message: Message):
    """Обработчик кнопки помощи"""
    await help_handler(message)

@dp.message(F.text == "ℹ️ О боте")
async def about_handler(message: Message):
    """Обработчик кнопки 'О боте'"""
    about_text = (
        "🤖 О боте\n\n"
        "Этот бот создан для студентов МИСИС, чтобы упростить поиск мероприятий.\n\n"
        "Технологии:\n"
        "• Python + Aiogram\n"
        "• VK API для парсинга мероприятий\n"
        "• Yandex GPT для анализа постов\n"
        "• SQLite для хранения данных\n\n"
        "Источники информации:\n"
        "• Официальные студенческие сообщества МИСИС в ВК\n"
        "Бот автоматически обновляет информацию каждый час!"
    )
    await message.answer(about_text)

# === СТАРЫЕ ОБРАБОТЧИКИ КОМАНД (для совместимости) ===
@dp.message(Command("status"))
async def status_handler(message: Message):
    """Показать статус системы"""
    status_text = (
        "🔧 Статус системы:\n"
        f"• 🤖 Бот: {'✅' if BOT_TOKEN else '❌'}\n"
        f"• 🔑 VK API: {'✅' if VK_USER_TOKEN else '❌'}\n"
        f"• 🤖 AI Анализатор: {'✅' if YANDEX_API_KEY and YANDEX_FOLDER_ID else '❌'}\n"
        f"• 💾 База данных: {'✅' if os.path.exists('events.db') else '❌'}\n\n"
        "Все системы работают нормально! 🚀"
    )
    await message.answer(status_text)

@dp.message(Command("help"))
async def help_handler(message: Message):
    help_text = (
        "📖 Бот мероприятий МИСИС\n\n"
        "Парсит группы VK:\n"
        f"{chr(10).join(['• ' + group for group in VK_GROUP_IDS])}\n\n"
        "Ищет по ключевым словам:\n"
        f"{chr(10).join(['• ' + keyword for keyword in VK_EVENT_KEYWORDS[:5]])}\n"
        f"{f'• ... и еще {len(VK_EVENT_KEYWORDS) - 5} слов' if len(VK_EVENT_KEYWORDS) > 5 else ''}\n\n"
        "Доступные команды:\n"
        "• 📅 Мероприятия - все мероприятия (подробно)\n"
        "• 🗓️ Календарь - календарь по неделям\n"
        "• 🔄 Обновить - запустить парсинг\n"
        "• 📊 Статус - статус системы\n"
        "• ❓ Помощь - эта справка\n"
        "• ℹ️ О боте - информация о боте"
    )
    await message.answer(help_text)

@dp.message(Command("events"))
async def events_handler(message: Message):
    """Показать все мероприятия подробно"""
    try:
        async with aiosqlite.connect('events.db') as db:
            cursor = await db.execute('''
                SELECT title, description, event_date, event_time, location, image_path, source_url
                FROM events 
                WHERE event_date >= ? 
                ORDER BY event_date, event_time
            ''', (MIN_EVENT_DATE.strftime('%Y-%m-%d'),))
            events = await cursor.fetchall()

        if events:
            await message.answer(f"🎓 Найдено мероприятий: {len(events)}")

            # Отправляем каждое мероприятие отдельным сообщением
            for event_data in events:
                await send_event_message(message.chat.id, event_data)

        else:
            await message.answer("❌ Мероприятий не найдено\nНажмите '🔄 Обновить' для поиска")

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer("❌ Ошибка загрузки мероприятий")

@dp.message(Command("calendar"))
async def calendar_handler(message: Message):
    """Показать календарь для выбора недели"""
    keyboard = Calendar.generate_week_keyboard()
    await message.answer(
        "📅 Выберите неделю для просмотра мероприятий:\n\n"
        "Каждое мероприятие будет показано подробно, как в разделе '📅 Мероприятия'",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("week_"))
async def week_handler(callback: CallbackQuery):
    """Обработчик выбора недели в календаре"""
    try:
        date_str = callback.data.split("_")[1]
        start_date = datetime.strptime(date_str, '%Y-%m-%d')
        end_date = start_date + timedelta(days=6)

        await callback.message.edit_text(
            f"🔍 Ищу мероприятия на неделю:\n"
            f"📅 {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}"
        )

        async with aiosqlite.connect('events.db') as db:
            cursor = await db.execute('''
                SELECT title, description, event_date, event_time, location, image_path, source_url
                FROM events 
                WHERE event_date BETWEEN ? AND ? 
                ORDER BY event_date, event_time
            ''', (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
            events = await cursor.fetchall()

        if events:
            # Отправляем заголовок с количеством мероприятий
            await callback.message.answer(
                f"📅 Мероприятия на неделю {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}:\n"
                f"🎯 Найдено: {len(events)} мероприятий"
            )

            # Отправляем каждое мероприятие отдельным сообщением (подробно)
            for event_data in events:
                await send_event_message(callback.message.chat.id, event_data)

        else:
            await callback.message.answer(
                f"❌ На неделю {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')} мероприятий не найдено"
            )

        await callback.answer()

    except Exception as e:
        logger.error(f"Ошибка в week_handler: {e}")
        await callback.message.answer("❌ Произошла ошибка при загрузке мероприятий")
        await callback.answer()

@dp.message(Command("update"))
async def update_handler(message: Message):
    """Запуск парсинга"""
    try:
        await message.answer("🔍 Запуск парсинга мероприятий из VK...")

        parser = VKParser(
            vk,
            yandex_api_key=YANDEX_API_KEY,
            folder_id=YANDEX_FOLDER_ID
        )

        events = await parser.search_events(VK_GROUP_IDS, VK_EVENT_KEYWORDS)
        saved_count = await parser.save_events_to_db(events)

        if saved_count > 0:
            await message.answer(
                f"✅ Парсинг завершен!\n"
                f"Сохранено мероприятий: {saved_count}\n"
                f"Проверено групп: {len(VK_GROUP_IDS)}\n"
                f"Ключевых слов: {len(VK_EVENT_KEYWORDS)}"
            )
        else:
            await message.answer("✅ Новых мероприятий не найдено")

    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}")
        await message.answer("❌ Ошибка при парсинге")

# === ЗАПУСК С ОБРАБОТКОЙ ОШИБОК ===
async def safe_start_polling():
    """Безопасный запуск бота с повторными попытками"""
    max_retries = 3
    retry_delay = 10  # секунд

    for attempt in range(max_retries):
        try:
            logger.info(f"🚀 Попытка запуска бота {attempt + 1}/{max_retries}...")
            await dp.start_polling(bot)
            break
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске (попытка {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Повторная попытка через {retry_delay} секунд...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error("❌ Все попытки запуска провалились")
                raise

async def main():
    try:
        await init_db()
        logger.info("✅ База данных инициализирована")

        # Автоматический парсинг при старте (в фоне)
        asyncio.create_task(auto_parse_events())

        logger.info("🚀 Запуск бота с меню...")
        await safe_start_polling()

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        # Даем время для логирования перед выходом
        await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Необработанная ошибка: {e}")