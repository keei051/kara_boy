import os
import asyncio
import json
import re
from datetime import datetime
from urllib.parse import quote, urlparse, parse_qs
from loguru import logger
import aiohttp
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import inspect
from functools import wraps

# Настройка логгера
logger.add("bot.log", rotation="1 MB")
logger.info("🚀 Бот запускается")

# Получение токенов
BOT_TOKEN = os.getenv("BOT_TOKEN")
VK_TOKEN = os.getenv("VK_API_TOKEN")

if not BOT_TOKEN or not VK_TOKEN:
    logger.error("Токены не установлены")
    raise ValueError("BOT_TOKEN и VK_TOKEN должны быть установлены")

# Инициализация бота
bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

# Класс состояний
class LinkForm(StatesGroup):
    waiting_for_link = State()
    waiting_for_title = State()
    waiting_for_stats_date = State()

# Класс для работы с JSON
class JsonStorage:
    def __init__(self, file_name=os.getenv("LINKS_PATH", "links.json")):
        self.file_name = file_name
        self.data = self._load_data()

    def _load_data(self):
        try:
            with open(self.file_name, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            logger.info("Файл links.json не найден, создаётся новый")
            return {}

    def _save_data(self):
        try:
            with open(self.file_name, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка записи JSON: {e}")
            raise

    def get_user_links(self, user_id):
        return self.data.get(str(user_id), [])

    def add_link(self, user_id, link_data):
        uid = str(user_id)
        self.data.setdefault(uid, [])
        if len(self.data[uid]) >= 50:
            self.data[uid].pop(0)
        self.data[uid].append(link_data)
        self._save_data()

storage = JsonStorage()

# Очистка URL для безопасного логирования
def sanitize_url(url):
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    sensitive_params = ['token', 'password', 'key']
    for param in sensitive_params:
        if param in query_params:
            query_params[param] = ['[REDACTED]']
    query = "&".join(f"{k}={v[0]}" for k, v in query_params.items()) if query_params else ""
    return parsed._replace(query=query).geturl()

# Проверка валидности URL
async def is_valid_url(url):
    sanitized_url = sanitize_url(url)
    if not re.match(r'^https?://[^\s]+$', url):
        logger.error(f"Недействительный URL: {sanitized_url}")
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, timeout=5) as r:
                if r.status in (200, 301, 302):
                    return True
                elif r.status == 429:
                    logger.warning(f"Слишком много запросов для {sanitized_url}")
                    return False
                return False
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка проверки URL {sanitized_url}: {e}")
        return False
    except asyncio.TimeoutError:
        logger.error(f"Таймаут при проверке URL {sanitized_url}")
        return False

# Функция сокращения ссылки через VK API
async def shorten_link_vk(url):
    sanitized_url = sanitize_url(url)
    if not await is_valid_url(url):
        return None, "Недействительный или недоступный URL"
    encoded_url = quote(url, safe='')
    if len(encoded_url) > 8000:
        logger.error(f"URL слишком длинный: {sanitized_url}")
        return None, "URL слишком длинный для сокращения"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.vk.com/method/utils.getShortLink?url={encoded_url}&v=5.199&access_token={VK_TOKEN}") as resp:
                data = await resp.json()
                if 'response' in data and 'short_url' in data['response']:
                    return data['response']['short_url'], ""
                logger.error(f"Ошибка VK API для {sanitized_url}: {data.get('error', 'Неизвестная ошибка')}")
                return None, "Ошибка VK API"
    except Exception as e:
        logger.error(f"Ошибка при сокращении ссылки {sanitized_url}: {e}")
        return None, "Не удалось сократить ссылку"

# Функция получения статистики по ссылке
async def get_link_stats(key, date_from=None, date_to=None):
    params = {"access_token": VK_TOKEN, "key": key, "v": "5.199", "interval": "day", "extended": 1}
    if date_from and date_to:
        params["date_from"] = date_from
        params["date_to"] = date_to
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.vk.com/method/utils.getLinkStats", params=params) as resp:
                data = await resp.json()
                total = 0
                if 'response' in data and 'stats' in data['response']:
                    for day in data['response']['stats']:
                        total += day.get("views", 0)
                return {"views": total}
    except Exception as e:
        logger.error(f"Ошибка получения статистики для ключа {key}: {e}")
        return {"views": 0}

# Создание клавиатуры
def make_kb(buttons, row=2):
    return InlineKeyboardMarkup(inline_keyboard=[buttons[i:i+row] for i in range(0, len(buttons), row)])

# Главное меню
def get_main_menu():
    return make_kb([
        InlineKeyboardButton(text="🔗 Сократить ссылку", callback_data="add_link"),
        InlineKeyboardButton(text="📊 Статистика переходов", callback_data="stats"),
        InlineKeyboardButton(text="📋 Мои ссылки", callback_data="list_links"),
    ])

# Клавиатура отмены
cancel_kb = make_kb([InlineKeyboardButton(text="🚫 Отмена", callback_data="cancel")])

# Декоратор обработки ошибок
def handle_error(handler):
    @wraps(handler)
    async def wrapper(*args, **kwargs):
        try:
            sig = inspect.signature(handler)
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
            return await handler(*args, **filtered_kwargs)
        except Exception as e:
            logger.error(f"Ошибка в {handler.__name__}: {e}")
            text = f"❌ Ошибка: {str(e)[:50]}"
            reply = get_main_menu()
            if isinstance(args[0], types.CallbackQuery):
                await args[0].message.edit_text(text, reply_markup=reply)
                await args[0].answer()
            elif isinstance(args[0], types.Message):
                await args[0].answer(text, reply_markup=reply)
    return wrapper

# Обработчики
@router.message(Command("start"))
@handle_error
async def cmd_start(message: types.Message, state: FSMContext):
    logger.info(f"Получена команда /start от пользователя {message.from_user.id}")
    await state.clear()
    await message.answer(
        "✨ Добро пожаловать!\nВы можете:\n🔗 Сокращать ссылки\n📊 Смотреть статистику\n📋 Хранить ссылки",
        reply_markup=get_main_menu()
    )

@router.message(Command("links"))
@handle_error
async def cmd_links(message: types.Message, state: FSMContext):
    logger.info(f"Получена команда /links от пользователя {message.from_user.id}")
    await state.clear()
    uid = str(message.from_user.id)
    links = storage.get_user_links(uid)
    if not links:
        await message.answer("📋 У вас нет сохранённых ссылок", reply_markup=get_main_menu())
        return
    text = "📋 Ваши ссылки:\n\n"
    for link in links:
        link_text = f"🔗 {link['title']}:\n{link['short']}\nСоздано: {link['created'][:19]}\n\n"
        if len(text) + len(link_text) > 4000:
            await message.answer(text, reply_markup=get_main_menu())
            text = "📋 Ваши ссылки (продолжение):\n\n"
        text += link_text
    await message.answer(text, reply_markup=get_main_menu())

@router.message(Command("help"))
@handle_error
async def cmd_help(message: types.Message, state: FSMContext):
    logger.info(f"Получена команда /help от пользователя {message.from_user.id}")
    await state.clear()
    await message.answer(
        "ℹ️ Помощь по боту:\n\n"
        "/start — начать работу\n"
        "/links — показать сохранённые ссылки\n"
        "/help — показать эту справку\n"
        "🔗 Используйте кнопки для сокращения ссылок, просмотра статистики и управления ссылками",
        reply_markup=get_main_menu()
    )

@router.callback_query(lambda c: c.data == "cancel")
@handle_error
async def cancel_action(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("✅ Отменено", reply_markup=get_main_menu())
    await cb.answer()

@router.callback_query(lambda c: c.data == "add_link")
@handle_error
async def add_link(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "🔗 Введите ссылку (http://... или https://...):",
        reply_markup=cancel_kb
    )
    await state.set_state(LinkForm.waiting_for_link)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_link))
@handle_error
async def process_link(message: types.Message, state: FSMContext):
    url = message.text.strip()
    if not await is_valid_url(url):
        await message.answer("❌ Неверный или недоступный URL. Попробуйте снова (пример: https://example.com):", reply_markup=cancel_kb)
        return
    loading_msg = await message.answer('⏳ Сокращаю...')
    short_url, error_msg = await shorten_link_vk(url)
    await loading_msg.delete()
    if not short_url:
        await message.answer(f"❌ {error_msg}", reply_markup=cancel_kb)
        return
    await state.update_data(original=url, short=short_url)
    await message.answer("📝 Введите название для ссылки (до 100 символов):", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_title)

@router.message(StateFilter(LinkForm.waiting_for_title))
@handle_error
async def process_title(message: types.Message, state: FSMContext):
    title = message.text.strip()[:100]
    if not title:
        await message.answer("❌ Название не может быть пустым:", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    uid = str(message.from_user.id)
    link_data = {
        "title": title,
        "short": data['short'],
        "original": data['original'],
        "created": datetime.now().isoformat()
    }
    storage.add_link(uid, link_data)
    await message.answer(
        f"✅ Ссылка сохранена:\n<b>{title}</b>\n{data['short']}",
        parse_mode="HTML",
        reply_markup=get_main_menu()
    )
    await state.clear()

@router.callback_query(lambda c: c.data == "stats")
@handle_error
async def stats_menu(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "📅 Введите даты (ГГГГ-ММ-ДД ГГГГ-ММ-ДД, например, 2025-06-01 2025-06-30):",
        reply_markup=cancel_kb
    )
    await state.set_state(LinkForm.waiting_for_stats_date)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_stats_date))
@handle_error
async def process_stats_date(message: types.Message, state: FSMContext):
    dates = message.text.strip().split()
    if len(dates) != 2 or not all(re.match(r"\d{4}-\d{2}-\d{2}", d) for d in dates):
        await message.answer("❌ Неверный формат. Пример: 2025-06-01 2025-06-30", reply_markup=cancel_kb)
        return
    date_from, date_to = dates
    try:
        date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
        date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")
        if date_from_dt > datetime.now():
            await message.answer("❌ Начальная дата не может быть в будущем", reply_markup=cancel_kb)
            return
        if date_to_dt < date_from_dt:
            await message.answer("❌ Конечная дата не может быть раньше начальной", reply_markup=cancel_kb)
            return
    except ValueError:
        await message.answer("❌ Неверные даты. Используйте формат ГГГГ-ММ-ДД и реальные даты", reply_markup=cancel_kb)
        return
    uid = str(message.from_user.id)
    links = storage.get_user_links(uid)
    if not links:
        await message.answer("📋 У вас нет ссылок", reply_markup=get_main_menu())
        await state.clear()
        return
    loading_msg = await message.answer('⏳ Загружаем...')
    semaphore = asyncio.Semaphore(5)  # Ограничение на 5 одновременных запросов
    async def limited_get_stats(link, date_from, date_to, semaphore):
        async with semaphore:
            return await get_link_stats(link['short'].split('/')[-1], date_from, date_to)
    stats = await asyncio.gather(
        *(limited_get_stats(link, date_from, date_to, semaphore) for link in links)
    )
    text = f"📊 Статистика переходов за {date_from}—{date_to}\n\n"
    total_views = 0
    for i, link in enumerate(links):
        views = stats[i]['views']
        total_views += views
        text += f"🔗 {link['title']}: {views} просмотров\n"
    text += f"\n👁 Всего: {total_views} просмотров"
    await loading_msg.delete()
    await message.answer(text, reply_markup=get_main_menu())
    await state.clear()

@router.callback_query(lambda c: c.data == "list_links")
@handle_error
async def list_links(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    uid = str(cb.from_user.id)
    links = storage.get_user_links(uid)
    if not links:
        await cb.message.edit_text("📋 У вас нет сохранённых ссылок", reply_markup=get_main_menu())
        await cb.answer()
        return
    text = "📋 Ваши ссылки:\n\n"
    for link in links:
        link_text = f"🔗 {link['title']}:\n{link['short']}\nСоздано: {link['created'][:19]}\n\n"
        if len(text) + len(link_text) > 4000:
            await cb.message.answer(text, reply_markup=get_main_menu())
            text = "📋 Ваши ссылки (продолжение):\n\n"
        text += link_text
    await cb.message.edit_text(text, reply_markup=get_main_menu())
    await cb.answer()

async def main():
    logger.info("Запуск бота...")
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info(f"Webhook успешно удалён с попытки {attempt + 1}")
            dp.include_router(router)
            logger.info("Начинаем polling")
            await dp.start_polling(bot, polling_timeout=20, handle_as_tasks=False)
            break
        except Exception as e:
            logger.error(f"Ошибка бота (попытка {attempt + 1}/{max_attempts}): {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(5)
            else:
                logger.error("Превышено количество попыток")
                raise
        finally:
            logger.info("Закрытие сессии бота")
            await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
