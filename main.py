import asyncio
import logging
import os
import aiohttp
import json
import aiosqlite
import re
import random
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
import vk_api
from vk_api.utils import get_random_id
from googletrans import Translator

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

# === СИСТЕМА ПЕРЕВОДОВ ===
class TranslationService:
    def __init__(self):
        self.translations = {
            'ru': {
                # Кнопки
                'calendar': "🗓️ Календарь",
                'update': "🔄 Обновить",
                'status': "📊 Статус",
                'help': "❓ Помощь",
                'about': "ℹ️ О боте",
                'main_menu': "🏠 Главное меню",
                'events': "📅 Мероприятия",
                'all_events': "📅 Все мероприятия",
                'language': "🌍 Язык",

                # Сообщения
                'welcome': "🎓 Подручный - твой цифровой ассистент!\n\nЯ помогу найти интересные мероприятия в университете. Все команды доступны в меню ниже 👇\n\nПросто нажми на кнопку в меню и смотри!",
                'choose_action': "🏠 Выберите действие из меню:",
                'events_section': "📅 Раздел мероприятий:",
                'about_text': "🤖 О боте\n\nЭтот бот создан для студентов МИСИС, чтобы упростить поиск мероприятий.\n\nТехнологии:\n• Python + Aiogram\n• VK API для парсинга мероприятий\n• Yandex GPT для анализа постов\n• SQLite для хранения данных\n\nИсточники информации:\n• Официальные студенческие сообщества МИСИС в ВК\nБот автоматически обновляет информацию каждый час!",
                'status_text': "🔧 Статус системы:\n• 🤖 Бот: {bot_status}\n• 🔑 VK API: {vk_status}\n• 🤖 AI Анализатор: {ai_status}\n• 💾 База данных: {db_status}\n\nВсе системы работают нормально! 🚀",
                'help_text': "📖 Бот мероприятий МИСИС\n\nПарсит группы VK:\n{groups}\n\nИщет по ключевым словам:\n{keywords}\n\nДоступные команды:\n• 📅 Мероприятия - все мероприятия (подробно)\n• 🗓️ Календарь - календарь по неделям\n• 🔄 Обновить - запустить парсинг\n• 📊 Статус - статус системы\n• ❓ Помощь - эта справка\n• ℹ️ О боте - информация о боте\n• 🌍 Язык - сменить язык",
                'parsing_started': "🔍 Запуск парсинга мероприятий из VK...",
                'parsing_completed': "✅ Парсинг завершен!\nСохранено мероприятий: {saved_count}\nПроверено групп: {groups_count}\nКлючевых слов: {keywords_count}",
                'no_new_events': "✅ Новых мероприятий не найдено",
                'parsing_error': "❌ Ошибка при парсинге",
                'events_found': "🎓 Найдено мероприятий: {count}",
                'no_events': "❌ Мероприятий не найдено\nНажмите '🔄 Обновить' для поиска",
                'loading_error': "❌ Ошибка загрузки мероприятий",
                'calendar_choose': "📅 Выберите неделю для просмотра мероприятий:\n\nКаждое мероприятие будет показано подробно, как в разделе '📅 Мероприятия'",
                'week_events': "📅 Мероприятия на неделю {start_date} - {end_date}:\n🎯 Найдено: {count} мероприятий",
                'no_week_events': "❌ На неделю {start_date} - {end_date} мероприятий не найдено",
                'searching_week': "🔍 Ищу мероприятия на неделю:\n📅 {start_date} - {end_date}",
                'choose_language': "🌍 Выберите язык / Choose language:",
                'language_changed': "✅ Язык изменен на русский!",

                # Формат мероприятия
                'event_format': "{title}\n📅 {date} в {time}\n📍 {location}\n📝 {description}\n🔗 [Ссылка на пост]({url})",

                # Статусы
                'yes': '✅',
                'no': '❌'
            },
            'en': {
                # Кнопки
                'calendar': "🗓️ Calendar",
                'update': "🔄 Update",
                'status': "📊 Status",
                'help': "❓ Help",
                'about': "ℹ️ About",
                'main_menu': "🏠 Main Menu",
                'events': "📅 Events",
                'all_events': "📅 All Events",
                'language': "🌍 Language",

                # Сообщения
                'welcome': "🎓 Assistant - your digital helper!\n\nI'll help you find interesting events at the university. All commands are available in the menu below 👇\n\nJust click a button in the menu and see!",
                'choose_action': "🏠 Choose an action from the menu:",
                'events_section': "📅 Events section:",
                'about_text': "🤖 About the Bot\n\nThis bot was created for MISIS students to simplify event search.\n\nTechnologies:\n• Python + Aiogram\n• VK API for event parsing\n• Yandex GPT for post analysis\n• SQLite for data storage\n\nInformation sources:\n• Official MISIS student communities in VK\nThe bot automatically updates information every hour!",
                'status_text': "🔧 System status:\n• 🤖 Bot: {bot_status}\n• 🔑 VK API: {vk_status}\n• 🤖 AI Analyzer: {ai_status}\n• 💾 Database: {db_status}\n\nAll systems are working normally! 🚀",
                'help_text': "📖 MISIS Events Bot\n\nParses VK groups:\n{groups}\n\nSearches by keywords:\n{keywords}\n\nAvailable commands:\n• 📅 Events - all events (detailed)\n• 🗓️ Calendar - weekly calendar\n• 🔄 Update - start parsing\n• 📊 Status - system status\n• ❓ Help - this help\n• ℹ️ About - bot information\n• 🌍 Language - change language",
                'parsing_started': "🔍 Starting event parsing from VK...",
                'parsing_completed': "✅ Parsing completed!\nSaved events: {saved_count}\nChecked groups: {groups_count}\nKeywords: {keywords_count}",
                'no_new_events': "✅ No new events found",
                'parsing_error': "❌ Parsing error",
                'events_found': "🎓 Events found: {count}",
                'no_events': "❌ No events found\nPress '🔄 Update' to search",
                'loading_error': "❌ Error loading events",
                'calendar_choose': "📅 Choose a week to view events:\n\nEach event will be shown in detail, as in the '📅 Events' section",
                'week_events': "📅 Events for week {start_date} - {end_date}:\n🎯 Found: {count} events",
                'no_week_events': "❌ No events found for week {start_date} - {end_date}",
                'searching_week': "🔍 Searching for events for the week:\n📅 {start_date} - {end_date}",
                'choose_language': "🌍 Выберите язык / Choose language:",
                'language_changed': "✅ Language changed to English!",

                # Формат мероприятия
                'event_format': "{title}\n📅 {date} at {time}\n📍 {location}\n📝 {description}\n🔗 [Post link]({url})",

                # Статусы
                'yes': '✅',
                'no': '❌'
            }
        }

    def get_text(self, key: str, lang: str = 'ru', **kwargs) -> str:
        """Получить переведенный текст"""
        text = self.translations.get(lang, self.translations['ru']).get(key, key)
        return text.format(**kwargs) if kwargs else text

# Инициализация сервиса переводов
translator = TranslationService()

# === УМНЫЙ ПЕРЕВОДЧИК С КЭШИРОВАНИЕМ ===
class SmartTranslator:
    def __init__(self):
        self.translator = Translator()
        self.translation_cache = {}
        self.cache_file = 'translation_cache.json'
        self.load_cache()
        logger.info("✅ Умный переводчик с кэшированием инициализирован")

    def load_cache(self):
        """Загрузка кэша переводов из файла"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.translation_cache = json.load(f)
                logger.info(f"✅ Загружено {len(self.translation_cache)} кэшированных переводов")
        except Exception as e:
            logger.warning(f"Не удалось загрузить кэш: {e}")
            self.translation_cache = {}

    def save_cache(self):
        """Сохранение кэша переводов в файл"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.translation_cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Не удалось сохранить кэш: {e}")

    async def translate_text(self, text: str, target_lang: str = 'en') -> str:
        """Умный перевод с кэшированием"""
        if target_lang == 'ru' or not text.strip():
            return text

        # Создаем ключ для кэша
        cache_key = f"{text}_{target_lang}"

        # Проверяем кэш
        if cache_key in self.translation_cache:
            return self.translation_cache[cache_key]

        try:
            # Добавляем небольшую случайную задержку от 0.1 до 0.5 секунд
            await asyncio.sleep(random.uniform(0.1, 0.5))

            # Выполняем перевод
            translated = self.translator.translate(text, dest=target_lang)

            result = translated.text if translated and hasattr(translated, 'text') else text

            # Сохраняем в кэш
            self.translation_cache[cache_key] = result
            self.save_cache()

            return result

        except Exception as e:
            logger.warning(f"Ошибка перевода: {e}")
            # Сохраняем оригинальный текст в кэш, чтобы не пытаться переводить снова
            self.translation_cache[cache_key] = text
            self.save_cache()
            return text

# Инициализация умного переводчика
text_translator = SmartTranslator()

# === СИСТЕМА ЯЗЫКОВ ===
async def get_user_language(user_id: int) -> str:
    """Получить язык пользователя из БД"""
    try:
        async with aiosqlite.connect('events.db') as db:
            cursor = await db.execute(
                'SELECT language FROM user_settings WHERE user_id = ?',
                (user_id,)
            )
            result = await cursor.fetchone()
            return result[0] if result else 'ru'
    except Exception:
        return 'ru'

async def set_user_language(user_id: int, language: str):
    """Установить язык пользователя в БД"""
    try:
        async with aiosqlite.connect('events.db') as db:
            await db.execute(
                '''INSERT OR REPLACE INTO user_settings (user_id, language) 
                   VALUES (?, ?)''',
                (user_id, language)
            )
            await db.commit()
    except Exception as e:
        logger.error(f"Error saving language: {e}")

# === КЛАВИАТУРЫ С ПОДДЕРЖКОЙ ЯЗЫКОВ ===
def get_language_keyboard():
    """Клавиатура выбора языка"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🇷🇺 Русский", callback_data="lang_ru")
    builder.button(text="🇬🇧 English", callback_data="lang_en")
    return builder.as_markup()

def get_main_keyboard(lang: str = 'ru'):
    """Создает основную клавиатуру меню с учетом языка"""
    builder = ReplyKeyboardBuilder()

    builder.add(
        KeyboardButton(text=translator.get_text('events', lang)),
        KeyboardButton(text=translator.get_text('calendar', lang)),
        KeyboardButton(text=translator.get_text('update', lang)),
        KeyboardButton(text=translator.get_text('status', lang)),
        KeyboardButton(text=translator.get_text('help', lang)),
        KeyboardButton(text=translator.get_text('about', lang)),
        KeyboardButton(text=translator.get_text('language', lang))
    )

    builder.adjust(2, 2, 2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_events_keyboard(lang: str = 'ru'):
    """Создает клавиатуру для раздела мероприятий с учетом языка"""
    builder = ReplyKeyboardBuilder()

    builder.add(
        KeyboardButton(text=translator.get_text('events', lang)),
        KeyboardButton(text=translator.get_text('calendar', lang)),
        KeyboardButton(text=translator.get_text('update', lang)),
        KeyboardButton(text=translator.get_text('main_menu', lang))
    )

    builder.adjust(2, 2)
    return builder.as_markup(resize_keyboard=True)

# === AI АНАЛИЗАТОР С ПОДДЕРЖКОЙ ПЕРЕВОДА ===
class YandexGPTAnalyzer:
    def __init__(self, yandex_api_key, folder_id):
        self.api_key = yandex_api_key
        self.folder_id = folder_id
        self.url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

    async def analyze_event(self, text, target_lang='ru'):
        if not self.api_key or not self.folder_id:
            return None

        try:
            headers = {
                "Authorization": f"Api-Key {self.api_key}",
                "Content-Type": "application/json"
            }

            # Выбираем промпт в зависимости от языка
            if target_lang == 'en':
                system_prompt = """You are an assistant for analyzing posts about events at MISIS University. 
                Extract event information in JSON format.

                Example response:
                {
                    "title": "AI Hackathon",
                    "date": "11.13.2025", 
                    "time": "14:00",
                    "location": "Main building, room 301"
                }"""
            else:
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
                                # Обрабатываем дату в зависимости от языка
                                if target_lang == 'en':
                                    event_date = datetime.strptime(ai_data.get('date', '11.01.2025'), '%m.%d.%Y')
                                else:
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

# === VK ПАРСЕР С ПОДДЕРЖКОЙ ПЕРЕВОДА ===
class VKParser:
    def __init__(self, vk_api, yandex_api_key=None, folder_id=None):
        self.vk = vk_api
        self.ai_analyzer = None

        if yandex_api_key and folder_id:
            self.ai_analyzer = YandexGPTAnalyzer(yandex_api_key, folder_id)
            logger.info("✅ AI анализатор активирован")

    async def search_events(self, group_ids, keywords, target_lang='ru'):
        """Поиск мероприятий с поддержкой языка"""
        try:
            events = []

            for group_id in group_ids:
                try:
                    logger.info(f"🔍 Парсинг группы VK: {group_id}")
                    group_events = await self.get_group_events(group_id, keywords, target_lang)
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

    async def get_group_events(self, group_id, keywords, target_lang='ru'):
        """Получение мероприятий из конкретной группы VK"""
        events = []
        try:
            owner_id = f"-{group_id}" if group_id.isdigit() else group_id

            response = self.vk.wall.get(
                owner_id=owner_id,
                count=100,
                filter='owner'
            )

            for post in response['items']:
                if not post.get('text'):
                    continue

                text = post['text']
                text_lower = text.lower()
                if any(keyword.lower() in text_lower for keyword in keywords):
                    logger.info(f"🎯 Найден пост с ключевым словом в группе {group_id}")
                    event_data = await self.parse_post(post, group_id, post['owner_id'], target_lang)
                    if event_data:
                        events.append(event_data)

            return events

        except Exception as e:
            logger.error(f"❌ Ошибка парсинга группы {group_id}: {e}")
            return []

    async def parse_post(self, post, group_id, owner_id, target_lang='ru'):
        """Парсинг поста VK с умным переводом"""
        try:
            text = post['text']
            post_id = post['id']

            # AI анализ
            ai_data = None
            if self.ai_analyzer:
                ai_data = await self.ai_analyzer.analyze_event(text, target_lang)

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
                title = "Мероприятие МИСИС" if target_lang == 'ru' else "MISIS Event"

            # Проверяем дату
            try:
                event_date = datetime.strptime(date, '%Y-%m-%d')
                if event_date < MIN_EVENT_DATE:
                    return None
            except ValueError:
                date = MIN_EVENT_DATE.strftime('%Y-%m-%d')

            # Формируем ссылку
            if str(owner_id).startswith('-'):
                group_num = str(owner_id)[1:]
                source_url = f"https://vk.com/wall-{group_num}_{post_id}"
            else:
                source_url = f"https://vk.com/wall{owner_id}_{post_id}"

            # Очищаем описание
            cleaned_description = clean_description(text, title)

            # УМНЫЙ ПЕРЕВОД: переводим только если нужно и используем кэш
            if target_lang == 'en':
                # Создаем задачи для параллельного перевода
                translate_tasks = [
                    text_translator.translate_text(title, 'en'),
                    text_translator.translate_text(cleaned_description, 'en'),
                    text_translator.translate_text(location, 'en')
                ]

                # Выполняем все переводы параллельно
                translated_texts = await asyncio.gather(*translate_tasks, return_exceptions=True)

                # Обрабатываем результаты
                title = translated_texts[0] if not isinstance(translated_texts[0], Exception) else title
                cleaned_description = translated_texts[1] if not isinstance(translated_texts[1], Exception) else cleaned_description
                location = translated_texts[2] if not isinstance(translated_texts[2], Exception) else location

            event_data = {
                'title': title,
                'description': cleaned_description,
                'event_date': date,
                'event_time': time,
                'location': location,
                'source': f"vk_{group_id}",
                'source_url': source_url,
                'tags': '#event' if target_lang == 'en' else '#мероприятие',
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
        """Извлечение даты"""
        date_patterns = [
            r'(\d{1,2}\.\d{1,2}\.\d{4})',
            r'(\d{1,2}\.\d{1,2})(?!\.\d)',
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
                    if re.match(r'\d{1,2}\.\d{1,2}\.\d{4}', date_str):
                        day, month, year = map(int, date_str.split('.'))
                        date_obj = datetime(year, month, day)
                        if date_obj >= MIN_EVENT_DATE:
                            return date_obj.strftime('%Y-%m-%d')
                    elif re.match(r'\d{1,2}\.\d{1,2}(?!\.\d)', date_str):
                        day, month = map(int, date_str.split('.'))
                        current_year = datetime.now().year
                        if month < datetime.now().month or (month == datetime.now().month and day < datetime.now().day):
                            current_year += 1
                        date_obj = datetime(current_year, month, day)
                        if date_obj >= MIN_EVENT_DATE:
                            return date_obj.strftime('%Y-%m-%d')
                    elif any(month in date_str.lower() for month in month_mapping.keys()):
                        for month_name, month_num in month_mapping.items():
                            if month_name in date_str.lower():
                                numbers = re.findall(r'\d+', date_str)
                                if numbers:
                                    day = int(numbers[0])
                                    year_match = re.search(r'\d{4}', date_str)
                                    year = int(year_match.group()) if year_match else datetime.now().year
                                    date_obj = datetime(year, month_num, day)
                                    if date_obj >= MIN_EVENT_DATE:
                                        return date_obj.strftime('%Y-%m-%d')
                except Exception:
                    continue

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

    async def save_events_to_db(self, events, language='ru'):
        """Сохранение в базу данных с указанием языка"""
        try:
            saved_count = 0
            async with aiosqlite.connect('events.db') as db:
                for event in events:
                    # Теперь проверяем по source и языку
                    cursor = await db.execute(
                        'SELECT id FROM events WHERE source = ? AND event_date = ? AND language = ?',
                        (event['source'], event['event_date'], language)
                    )
                    existing = await cursor.fetchone()

                    if not existing:
                        await db.execute('''
                            INSERT INTO events (title, description, event_date, event_time, location, source, source_url, tags, image_path, language)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            event['title'], event['description'], event['event_date'],
                            event['event_time'], event['location'], event['source'],
                            event['source_url'], event['tags'], event.get('image_path'), language
                        ))
                        saved_count += 1
                        logger.info(f"💾 Сохранено ({language}): {event['title']}")

                await db.commit()
                return saved_count

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в БД: {e}")
            return 0

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
def remove_title_from_description(title, description):
    """Удаляет заголовок из начала описания"""
    if not title or not description:
        return description

    title_lower = title.lower().strip()
    description_lower = description.lower().strip()

    if description_lower.startswith(title_lower):
        cleaned_description = description[len(title):].strip()
        cleaned_description = re.sub(r'^[.,—:\-\s]+', '', cleaned_description)
        if len(cleaned_description) > 10:
            return cleaned_description

    return description

def clean_description(text, title):
    """Очищает описание от дублирования с заголовком"""
    if not text:
        return ""

    text = remove_title_from_description(title, text)
    sentences = re.split(r'[.!?]+', text)
    meaningful_sentences = []

    for sentence in sentences:
        sentence = sentence.strip()
        if (len(sentence) > 20 and
                not sentence.startswith(('http://', 'https://', 'vk.com/', '@')) and
                not any(word in sentence.lower() for word in ['подписывайся', 'репост', 'поделись'])):
            meaningful_sentences.append(sentence)
        if len(meaningful_sentences) >= 3:
            break

    if meaningful_sentences:
        result = '. '.join(meaningful_sentences) + '.'
        if len(result) > 400:
            result = result[:400]
            last_space = result.rfind(' ')
            if last_space > 350:
                result = result[:last_space] + '...'
            else:
                result = result + '...'
        return result

    return text[:300] + '...' if len(text) > 300 else text

# === БАЗА ДАННЫХ С НАСТРОЙКАМИ ПОЛЬЗОВАТЕЛЕЙ ===
async def init_db():
    """Инициализация базы данных с таблицей настроек"""
    async with aiosqlite.connect('events.db') as db:
        # Таблица мероприятий
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
                language TEXT NOT NULL DEFAULT 'ru',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица настроек пользователей
        await db.execute('''
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                language TEXT NOT NULL DEFAULT 'ru',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await db.commit()
        logger.info("✅ База данных готова")

async def migrate_db():
    """Миграция базы данных для добавления поля language"""
    try:
        async with aiosqlite.connect('events.db') as db:
            # Проверяем, есть ли столбец language
            cursor = await db.execute("PRAGMA table_info(events)")
            columns = await cursor.fetchall()
            column_names = [column[1] for column in columns]

            if 'language' not in column_names:
                logger.info("🔄 Добавляем поле language в таблицу events...")
                await db.execute('ALTER TABLE events ADD COLUMN language TEXT NOT NULL DEFAULT "ru"')
                await db.commit()
                logger.info("✅ Миграция базы данных завершена")
            else:
                logger.info("✅ База данных уже актуальна")

    except Exception as e:
        logger.error(f"❌ Ошибка миграции БД: {e}")

# === КАЛЕНДАРЬ ===
class Calendar:
    @staticmethod
    def generate_week_keyboard(lang='ru'):
        builder = InlineKeyboardBuilder()
        today = max(datetime.now(), MIN_EVENT_DATE)
        start_of_week = today - timedelta(days=today.weekday())

        for week_offset in range(0, 8):
            week_start = start_of_week + timedelta(days=week_offset * 7)
            week_end = week_start + timedelta(days=6)

            if lang == 'en':
                week_text = f"📅 {week_start.strftime('%m/%d')} - {week_end.strftime('%m/%d')}"
            else:
                week_text = f"📅 {week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}"

            callback_data = f"week_{week_start.strftime('%Y-%m-%d')}"
            builder.button(text=week_text, callback_data=callback_data)

        builder.adjust(2)
        return builder.as_markup()

# === ОБРАБОТЧИКИ КОМАНД ===
@dp.message(Command("start"))
async def start_handler(message: Message):
    """Обработчик команды /start с выбором языка"""
    user_id = message.from_user.id

    # Всегда показываем выбор языка при старте
    welcome_text = translator.get_text('choose_language', 'ru')
    await message.answer(welcome_text, reply_markup=get_language_keyboard())

@dp.callback_query(F.data.startswith("lang_"))
async def language_callback_handler(callback: CallbackQuery):
    """Обработчик выбора языка"""
    lang = callback.data.split("_")[1]  # 'ru' или 'en'
    user_id = callback.from_user.id

    await set_user_language(user_id, lang)

    # Показываем сообщение об успешной смене языка
    lang_text = translator.get_text('language_changed', lang)
    await callback.message.edit_text(lang_text)

    # Показываем основное меню
    welcome_text = translator.get_text('welcome', lang)
    await callback.message.answer(welcome_text, reply_markup=get_main_keyboard(lang))
    await callback.answer()

@dp.message(F.text == "🌍 Язык")
@dp.message(F.text == "🌍 Language")
async def language_button_handler(message: Message):
    """Обработчик кнопки смены языка"""
    await message.answer(
        translator.get_text('choose_language', await get_user_language(message.from_user.id)),
        reply_markup=get_language_keyboard()
    )

# Обработчики кнопок меню с поддержкой языка
@dp.message(F.text == "🏠 Главное меню")
@dp.message(F.text == "🏠 Main Menu")
async def main_menu_handler(message: Message):
    """Обработчик кнопки главного меню"""
    user_id = message.from_user.id
    lang = await get_user_language(user_id)
    await message.answer(
        translator.get_text('choose_action', lang),
        reply_markup=get_main_keyboard(lang)
    )

@dp.message(F.text == "📅 Мероприятия")
@dp.message(F.text == "📅 Events")
async def events_button_handler(message: Message):
    """Обработчик кнопки мероприятий"""
    user_id = message.from_user.id
    lang = await get_user_language(user_id)
    await message.answer(
        translator.get_text('events_section', lang),
        reply_markup=get_events_keyboard(lang)
    )
    await events_handler(message)

@dp.message(F.text == "🗓️ Календарь")
@dp.message(F.text == "🗓️ Calendar")
async def calendar_button_handler(message: Message):
    """Обработчик кнопки календаря"""
    await calendar_handler(message)

@dp.message(F.text == "🔄 Обновить")
@dp.message(F.text == "🔄 Update")
async def update_button_handler(message: Message):
    """Обработчик кнопки обновления"""
    await update_handler(message)

@dp.message(F.text == "📊 Статус")
@dp.message(F.text == "📊 Status")
async def status_button_handler(message: Message):
    """Обработчик кнопки статуса"""
    await status_handler(message)

@dp.message(F.text == "❓ Помощь")
@dp.message(F.text == "❓ Help")
async def help_button_handler(message: Message):
    """Обработчик кнопки помощи"""
    await help_handler(message)

@dp.message(F.text == "ℹ️ О боте")
@dp.message(F.text == "ℹ️ About")
async def about_handler(message: Message):
    """Обработчик кнопки 'О боте'"""
    user_id = message.from_user.id
    lang = await get_user_language(user_id)
    about_text = translator.get_text('about_text', lang)
    await message.answer(about_text)

# === ОСНОВНЫЕ ОБРАБОТЧИКИ С ПОДДЕРЖКОЙ ЯЗЫКА ===
async def send_event_message(chat_id, event_data, lang='ru'):
    """Отправка сообщения с мероприятием с учетом языка"""
    title, description, event_date, event_time, location, image_path, source_url = event_data

    # Форматируем дату в зависимости от языка
    if lang == 'en':
        formatted_date = datetime.strptime(event_date, '%Y-%m-%d').strftime('%m/%d/%Y')
    else:
        formatted_date = datetime.strptime(event_date, '%Y-%m-%d').strftime('%d.%m.%Y')

    # Формируем текст мероприятия
    event_text = translator.get_text('event_format', lang).format(
        title=title,
        date=formatted_date,
        time=event_time,
        location=location,
        description=description,
        url=source_url
    )

    await bot.send_message(chat_id=chat_id, text=event_text, parse_mode='Markdown')

@dp.message(Command("status"))
async def status_handler(message: Message):
    """Показать статус системы"""
    user_id = message.from_user.id
    lang = await get_user_language(user_id)

    status_text = translator.get_text('status_text', lang,
                                      bot_status=translator.get_text('yes', lang),
                                      vk_status=translator.get_text('yes', lang),
                                      ai_status=translator.get_text('yes', lang) if YANDEX_API_KEY and YANDEX_FOLDER_ID else translator.get_text('no', lang),
                                      db_status=translator.get_text('yes', lang) if os.path.exists('events.db') else translator.get_text('no', lang)
                                      )
    await message.answer(status_text)

@dp.message(Command("help"))
async def help_handler(message: Message):
    user_id = message.from_user.id
    lang = await get_user_language(user_id)

    groups_text = '\n'.join([f"• {group}" for group in VK_GROUP_IDS])
    keywords_text = '\n'.join([f"• {keyword}" for keyword in VK_EVENT_KEYWORDS[:5]])
    if len(VK_EVENT_KEYWORDS) > 5:
        keywords_text += f"\n• ... и еще {len(VK_EVENT_KEYWORDS)-5} слов"

    help_text = translator.get_text('help_text', lang,
                                    groups=groups_text,
                                    keywords=keywords_text
                                    )
    await message.answer(help_text)

@dp.message(Command("events"))
async def events_handler(message: Message):
    """Показать все мероприятия подробно ТОЛЬКО на языке пользователя"""
    user_id = message.from_user.id
    lang = await get_user_language(user_id)

    try:
        async with aiosqlite.connect('events.db') as db:
            cursor = await db.execute('''
                SELECT title, description, event_date, event_time, location, image_path, source_url
                FROM events 
                WHERE event_date >= ? AND language = ?
                ORDER BY event_date, event_time
            ''', (MIN_EVENT_DATE.strftime('%Y-%m-%d'), lang))
            events = await cursor.fetchall()

        if events:
            await message.answer(translator.get_text('events_found', lang, count=len(events)))

            for event_data in events:
                await send_event_message(message.chat.id, event_data, lang)

        else:
            await message.answer(translator.get_text('no_events', lang))

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer(translator.get_text('loading_error', lang))

@dp.message(Command("calendar"))
async def calendar_handler(message: Message):
    """Показать календарь для выбора недели"""
    user_id = message.from_user.id
    lang = await get_user_language(user_id)

    keyboard = Calendar.generate_week_keyboard(lang)
    await message.answer(
        translator.get_text('calendar_choose', lang),
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("week_"))
async def week_handler(callback: CallbackQuery):
    """Обработчик выбора недели в календаре - ТОЛЬКО на языке пользователя"""
    user_id = callback.from_user.id
    lang = await get_user_language(user_id)

    try:
        date_str = callback.data.split("_")[1]
        start_date = datetime.strptime(date_str, '%Y-%m-%d')
        end_date = start_date + timedelta(days=6)

        # Форматируем даты для отображения
        if lang == 'en':
            start_date_str = start_date.strftime('%m/%d/%Y')
            end_date_str = end_date.strftime('%m/%d/%Y')
        else:
            start_date_str = start_date.strftime('%d.%m.%Y')
            end_date_str = end_date.strftime('%d.%m.%Y')

        await callback.message.edit_text(
            translator.get_text('searching_week', lang,
                                start_date=start_date_str,
                                end_date=end_date_str
                                )
        )

        async with aiosqlite.connect('events.db') as db:
            cursor = await db.execute('''
                SELECT title, description, event_date, event_time, location, image_path, source_url
                FROM events 
                WHERE event_date BETWEEN ? AND ? AND language = ?
                ORDER BY event_date, event_time
            ''', (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), lang))
            events = await cursor.fetchall()

        if events:
            await callback.message.answer(
                translator.get_text('week_events', lang,
                                    start_date=start_date_str,
                                    end_date=end_date_str,
                                    count=len(events)
                                    )
            )

            for event_data in events:
                await send_event_message(callback.message.chat.id, event_data, lang)

        else:
            await callback.message.answer(
                translator.get_text('no_week_events', lang,
                                    start_date=start_date_str,
                                    end_date=end_date_str
                                    )
            )

        await callback.answer()

    except Exception as e:
        logger.error(f"Ошибка в week_handler: {e}")
        await callback.message.answer(translator.get_text('loading_error', lang))
        await callback.answer()

@dp.message(Command("update"))
async def update_handler(message: Message):
    """Запуск парсинга на языке пользователя"""
    user_id = message.from_user.id
    lang = await get_user_language(user_id)

    try:
        await message.answer(translator.get_text('parsing_started', lang))

        parser = VKParser(
            vk,
            yandex_api_key=YANDEX_API_KEY,
            folder_id=YANDEX_FOLDER_ID
        )

        # Парсим на языке пользователя
        events = await parser.search_events(VK_GROUP_IDS, VK_EVENT_KEYWORDS, lang)
        saved_count = await parser.save_events_to_db(events, lang)

        if saved_count > 0:
            await message.answer(
                translator.get_text('parsing_completed', lang,
                                    saved_count=saved_count,
                                    groups_count=len(VK_GROUP_IDS),
                                    keywords_count=len(VK_EVENT_KEYWORDS)
                                    )
            )
        else:
            await message.answer(translator.get_text('no_new_events', lang))

    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}")
        await message.answer(translator.get_text('parsing_error', lang))

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

        # Парсим на обоих языках для начального наполнения базы
        events_ru = await parser.search_events(VK_GROUP_IDS, VK_EVENT_KEYWORDS, 'ru')
        saved_count_ru = await parser.save_events_to_db(events_ru, 'ru')

        # Также парсим на английском
        events_en = await parser.search_events(VK_GROUP_IDS, VK_EVENT_KEYWORDS, 'en')
        saved_count_en = await parser.save_events_to_db(events_en, 'en')

        if saved_count_ru > 0 or saved_count_en > 0:
            logger.info(f"✅ Автопарсинг: сохранено {saved_count_ru} мероприятий на русском и {saved_count_en} на английском")
        else:
            logger.info("✅ Автопарсинг: новых мероприятий не найдено")

    except Exception as e:
        logger.error(f"❌ Ошибка автопарсинга: {e}")

# === ЗАПУСК С ОБРАБОТКОЙ ОШИБОК ===
async def safe_start_polling():
    """Безопасный запуск бота с повторными попытками"""
    max_retries = 3
    retry_delay = 10

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
        await migrate_db()  # Добавляем миграцию
        logger.info("✅ База данных инициализирована")

        # Автоматический парсинг при старте (в фоне)
        asyncio.create_task(auto_parse_events())

        logger.info("🚀 Запуск бота с мультиязычной поддержкой...")
        await safe_start_polling()

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Необработанная ошибка: {e}")