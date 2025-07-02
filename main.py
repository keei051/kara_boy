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
import threading

# Настройка логгера
logger.add("bot.log", rotation="1 MB", encoding="utf-8")
logger.info(f"🚀 Бот запускается в {datetime.now().strftime('%I:%M %p %Z, %d %B %Y')}")

# Токены (оставлены в коде, как указано)
BOT_TOKEN = "7735071651:AAHVN_ZjYJ2NZRIzJXtvDfRIPUcZhPBqUEo"
VK_TOKEN = "4ccacfc94ccacfc94ccacfc9024fffb48c44cca4ccacfc924a94e533627dc4bbeb3ee97"

if not BOT_TOKEN or not VK_TOKEN:
    logger.error("Токены не установлены")
    raise ValueError("BOT_TOKEN и VK_TOKEN должны быть установлены")

# Блокировка для JSON
json_lock = threading.Lock()

# Инициализация бота и диспетчера
bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

# Класс состояний
class LinkForm(StatesGroup):
    waiting_for_link = State()
    waiting_for_title = State()
    waiting_for_link_action = State()
    waiting_for_rename = State()

# Класс для работы с JSON
class JsonStorage:
    """Хранилище ссылок в JSON-файле с потокобезопасной записью."""
    def __init__(self, file_name="links.json"):
        self.file_name = file_name
        self.data = self._load_data()

    def _load_data(self):
        try:
            with open(self.file_name, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (UnicodeDecodeError, FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Ошибка загрузки JSON: {e}")
            return {}

    def _save_data(self):
        with json_lock:
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
        with json_lock:
            self.data.setdefault(uid, [])
            # Проверка на дубли
            if any(link['original'] == link_data['original'] for link in self.data[uid]):
                logger.info(f"Попытка добавить дублирующую ссылку для {uid}: {link_data['original']}")
                return False
            if len(self.data[uid]) >= 50:
                removed_link = self.data[uid].pop(0)
                logger.info(f"Удалена старая ссылка для {uid}: {removed_link['title']}")
            self.data[uid].append(link_data)
            self._save_data()
            return True

    def delete_link(self, user_id, link_index):
        uid = str(user_id)
        with json_lock:
            if uid in self.data and 0 <= link_index < len(self.data[uid]):
                self.data[uid].pop(link_index)
                self._save_data()
                return True
        return False

    def rename_link(self, user_id, link_index, new_title):
        uid = str(user_id)
        with json_lock:
            if uid in self.data and 0 <= link_index < len(self.data[uid]):
                self.data[uid][link_index]["title"] = new_title[:100]
                self._save_data()
                return True
        return False

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
async def is_valid_url(url, session):
    sanitized_url = sanitize_url(url)
    if not re.match(r'^https?://[^\s]+$', url):
        logger.error(f"Недействительный URL: {sanitized_url}")
        return False
    try:
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
async def shorten_link_vk(url, session):
    sanitized_url = sanitize_url(url)
    if len(url) > 2048:
        logger.error(f"URL слишком длинный: {sanitized_url}")
        return None, None, "URL превышает допустимую длину (2048 символов)"
    if not await is_valid_url(url, session):
        return None, None, "Недействительный или недоступный URL"
    encoded_url = quote(url, safe='')
    try:
        async with session.get(
            f"https://api.vk.com/method/utils.getShortLink?url={encoded_url}&v=5.199&access_token={VK_TOKEN}",
            timeout=5
        ) as resp:
            if resp.status != 200:
                logger.error(f"VK API вернул статус {resp.status} для {sanitized_url}")
                return None, None, "Ошибка сервера VK"
            data = await resp.json()
            if not isinstance(data, dict):
                logger.error(f"Некорректный формат ответа VK API для {sanitized_url}")
                return None, None, "Некорректный ответ VK API"
            if 'response' in data and 'short_url' in data['response']:
                return data['response']['short_url'], data['response']['key'], ""
            logger.error(f"Ошибка VK API для {sanitized_url}: {data.get('error', 'Неизвестная ошибка')}")
            return None, None, "Ошибка VK API"
    except Exception as e:
        logger.error(f"Ошибка при сокращении ссылки {sanitized_url}: {e}")
        return None, None, "Не удалось сократить ссылку"

# Функция получения статистики по ссылке
async def get_link_stats(key, session):
    params = {"access_token": VK_TOKEN, "key": key, "v": "5.199", "interval": "day", "extended": 1}
    try:
        async with session.get(
            "https://api.vk.com/method/utils.getLinkStats",
            params=params,
            timeout=5
        ) as resp:
            if resp.status != 200:
                logger.error(f"VK API вернул статус {resp.status} для ключа {key}")
                return {"views": 0, "countries": {}}
            data = await resp.json()
            stats = {"views": 0, "countries": {}}
            if 'response' in data and 'stats' in data['response']:
                for day in data['response']['stats']:
                    stats["views"] += day.get("views", 0)
                    country_id = day.get("country")
                    if country_id:
                        stats["countries"][country_id] = stats["countries"].get(country_id, 0) + day.get("views", 0)
            return stats
    except Exception as e:
        logger.error(f"Ошибка получения статистики для ключа {key}: {e}")
        return {"views": 0, "countries": {}}

# Получение названий стран
async def get_country_name(country_id, session):
    try:
        async with session.get(
            f"https://api.vk.com/method/database.getCountriesById?country_ids={country_id}&v=5.199&access_token={VK_TOKEN}",
            timeout=5
        ) as resp:
            if resp.status != 200:
                logger.error(f"VK API вернул статус {resp.status} для country_id {country_id}")
                return 'Неизвестная страна'
            data = await resp.json()
            if 'response' in data and data['response']:
                return data['response'][0].get('name', 'Неизвестная страна')
            return 'Неизвестная страна'
    except Exception as e:
        logger.error(f"Ошибка получения названия страны {country_id}: {e}")
        return 'Неизвестная страна'

# Создание клавиатуры
def make_kb(buttons, row=2):
    return InlineKeyboardMarkup(inline_keyboard=[buttons[i:i+row] for i in range(0, len(buttons), row)])

# Главное меню
def get_main_menu():
    return make_kb([
        InlineKeyboardButton(text="🔗 Сократить ссылку", callback_data="add_link"),
        InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
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
            logger.error(f"Ошибка в {handler.__name__}: {str(type(e).__name__)} - {str(e)[:100]}")
            text = "❌ Произошла ошибка. Пожалуйста, попробуйте позже или обратитесь к администратору."
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
    start_message = "✨ Добро пожаловать!\nВы можете:\n🔗 Сократить ссылки\n📊 Смотреть статистику"
    await message.answer(start_message, reply_markup=get_main_menu())

@router.message(Command("help"))
@handle_error
async def cmd_help(message: types.Message, state: FSMContext):
    logger.info(f"Получена команда /help от пользователя {message.from_user.id}")
    await state.clear()
    await message.answer(
        "ℹ️ Помощь по боту:\n\n"
        "/start — начать работу\n"
        "/help — показать эту справку\n"
        "🔗 Используйте кнопки для сокращения ссылок и просмотра статистики",
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
        "🔗 Введите ссылку (http:// или https://):",
        reply_markup=cancel_kb
    )
    await state.set_state(LinkForm.waiting_for_link)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_link))
@handle_error
async def process_link(message: types.Message, state: FSMContext, session: aiohttp.ClientSession):
    url = message.text.strip()
    if not await is_valid_url(url, session):
        await message.answer(
            "❌ Неверный URL. Убедитесь, что он начинается с http:// или https:// и доступен.\nПример: https://example.com",
            reply_markup=cancel_kb
        )
        return
    loading_msg = await message.answer('⏳ Сокращаю...')
    short_url, key, error_msg = await shorten_link_vk(url, session)
    await loading_msg.delete()
    if not short_url:
        await message.answer(f"❌ {error_msg}", reply_markup=cancel_kb)
        return
    await state.update_data(original=url, short=short_url, key=key)
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
        "key": data['key'],
        "created": datetime.now().isoformat()
    }
    if not storage.add_link(uid, link_data):
        await message.answer("❌ Эта ссылка уже сохранена.", reply_markup=get_main_menu())
        await state.clear()
        return
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
    uid = str(cb.from_user.id)
    links = storage.get_user_links(uid)
    if not links:
        await cb.message.edit_text("📋 У вас нет сохранённых ссылок", reply_markup=get_main_menu())
        await cb.answer()
        return
    buttons = [
        InlineKeyboardButton(text=f"{link['title']} ({link['short']})", callback_data=f"link_stats:{i}")
        for i, link in enumerate(links)
    ]
    buttons.append(InlineKeyboardButton(text="🚫 Отмена", callback_data="cancel"))
    kb = make_kb(buttons, row=1)
    await cb.message.edit_text("📊 Выберите ссылку для просмотра статистики:", reply_markup=kb)
    await state.set_state(LinkForm.waiting_for_link_action)
    await cb.answer()

@router.callback_query(lambda c: c.data.startswith("link_stats:"))
@handle_error
async def show_link_stats(cb: types.CallbackQuery, state: FSMContext, session: aiohttp.ClientSession):
    link_index = int(cb.data.split(":")[1])
    uid = str(cb.from_user.id)
    links = storage.get_user_links(uid)
    if not (0 <= link_index < len(links)):
        await cb.message.edit_text("❌ Ссылка не найдена", reply_markup=get_main_menu())
        await state.clear()
        await cb.answer()
        return
    link = links[link_index]
    loading_msg = await cb.message.edit_text('⏳ Загружаем статистику...')
    stats = await get_link_stats(link['key'], session)
    text = f"📊 Статистика для '{link['title']}'\n\n"
    text += f"🔗 Короткая ссылка: {link['short']}\n"
    text += f"🌐 Оригинальная ссылка: {link['original']}\n"
    text += f"👁 Переходы: {stats['views']}\n"
    if stats['countries']:
        text += "\n🌍 Геолокация (по странам):\n"
        for country_id, views in stats['countries'].items():
            country_name = await get_country_name(country_id, session)
            text += f"{country_name}: {views} переходов\n"
    else:
        text += "\n🌍 Геолокация: данные отсутствуют\n"
    buttons = [
        InlineKeyboardButton(text="⬅ Назад к списку", callback_data="stats"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete_link:{link_index}"),
        InlineKeyboardButton(text="✏ Переименовать", callback_data=f"rename_link:{link_index}")
    ]
    kb = make_kb(buttons, row=1)
    await loading_msg.delete()
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(lambda c: c.data.startswith("delete_link:"))
@handle_error
async def delete_link(cb: types.CallbackQuery, state: FSMContext):
    link_index = int(cb.data.split(":")[1])
    uid = str(cb.from_user.id)
    if storage.delete_link(uid, link_index):
        await cb.message.edit_text("✅ Ссылка удалена", reply_markup=get_main_menu())
    else:
        await cb.message.edit_text("❌ Ошибка удаления ссылки", reply_markup=get_main_menu())
    await state.clear()
    await cb.answer()

@router.callback_query(lambda c: c.data.startswith("rename_link:"))
@handle_error
async def rename_link(cb: types.CallbackQuery, state: FSMContext):
    link_index = int(cb.data.split(":")[1])
    uid = str(cb.from_user.id)
    links = storage.get_user_links(uid)
    if not (0 <= link_index < len(links)):
        await cb.message.edit_text("❌ Ссылка не найдена", reply_markup=get_main_menu())
        await state.clear()
        await cb.answer()
        return
    await state.update_data(link_index=link_index)
    await cb.message.edit_text("✏ Введите новое название для ссылки (до 100 символов):", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_rename)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_rename))
@handle_error
async def process_rename(message: types.Message, state: FSMContext):
    new_title = message.text.strip()[:100]
    if not new_title:
        await message.answer("❌ Название не может быть пустым:", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    link_index = data.get("link_index")
    uid = str(message.from_user.id)
    if storage.rename_link(uid, link_index, new_title):
        links = storage.get_user_links(uid)
        link = links[link_index]
        await message.answer(
            f"✅ Ссылка переименована:\n<b>{new_title}</b>\n{link['short']}",
            parse_mode="HTML",
            reply_markup=get_main_menu()
        )
    else:
        await message.answer("❌ Ошибка переименования ссылки", reply_markup=get_main_menu())
    await state.clear()

# Запуск
async def main():
    logger.info("Запуск бота...")
    async with aiohttp.ClientSession() as session:
        bot.session = session  # Сохраняем сессию в объекте бота
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
                logger.error(f"Ошибка бота (попытка {attempt + 1}/{max_attempts}): {str(type(e).__name__)} - {str(e)[:100]}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(5)
                else:
                    logger.error("Превышено количество попыток")
                    raise

if __name__ == "__main__":
    import sys
    logger.info(f"Кодировка stdout: {sys.stdout.encoding}")
    asyncio.run(main())
