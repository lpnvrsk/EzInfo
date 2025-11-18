from bs4 import BeautifulSoup
from datetime import datetime, date
import os
import re
import sqlite3
import time
from tqdm import tqdm
import threading
from requests import Session
import signal
import sys
import random
import logging

# ==================== КОНФИГУРАЦИЯ ====================
PLAYTIME_ONLY = False  # Если True - парсит только по времени игры, если False - также по имени
COOKIES_FILE = 'cookies.md'  # Файл с cookies для авторизации

# Настройки задержек между запросами
ENABLE_DELAYS = False  # Включить ли задержки между запросами
DELAY_SECONDS = 0.5    # Фиксированная задержка в секундах (если ENABLE_DELAYS=True и RANDOM_DELAY=False)
RANDOM_DELAY = False   # Использовать случайные задержки вместо фиксированных
MIN_DELAY = 0.3        # Минимальная случайная задержка в секундах (если RANDOM_DELAY=True)
MAX_DELAY = 1.2        # Максимальная случайная задержка в секундах (если RANDOM_DELAY=True)

PLAYTIME_URL = "https://ezwow.org/index.php?app=isengard&module=core&tab=armory&section=characters&realm=1&sort%5Bkey%5D=playtime&sort%5Border%5D=desc&st="
NAME_URL = "https://ezwow.org/index.php?app=isengard&module=core&tab=armory&section=characters&realm=1&sort%5Bkey%5D=name&sort%5Border%5D=desc&st="
LAST_PAGE_URL = 'https://ezwow.org/index.php?app=isengard&module=core&tab=armory&section=characters&realm=1&sort%5Bkey%5D=playtime&sort%5Border%5D=desc&st=9999999999999999999'

CLASS_TRANSLATION = {
    'Hunter (Охотник)': 'Hunter',
    'Druid (Друид)': 'Druid', 
    'Paladin (Паладин)': 'Paladin',
    'Shaman (Шаман)': 'Shaman',
    'Mage (Маг)': 'Mage',
    'Warrior (Воин)': 'Warrior',
    'Priest (Жрец)': 'Priest',
    'Rogue (Разбойник)': 'Rogue',
    'Death knight (Рыцарь смерти)': 'Death Knight',
    'Warlock (Чернокнижник)': 'Warlock'
}

RACE_TRANSLATION = {
    'Дренеи': 'Draenei',
    'Ночные эльфы': 'Night Elf', 
    'Кровавые эльфы': 'Blood Elf',
    'Орки': 'Orc',
    'Люди': 'Human',
    'Нежить': 'Undead',
    'Таурены': 'Tauren',
    'Тролли': 'Troll',
    'Дворфы': 'Dwarf',
    'Гномы': 'Gnome'
}

CONFIG = {
    'timeout': 30,
    'max_attempts': 3,
    'retry_delay': 2,
    'logs_folder': 'LOGS',
    'bases_folder': 'BASES'
}

download_active = True

# ==================== СИСТЕМА ЛОГГИРОВАНИЯ ====================

class ThreadSafeLogger:
    def __init__(self):
        self.logger = None
        self.lock = threading.Lock()
        
    def setup(self, log_file):
        """Настройка логгера"""
        self.logger = logging.getLogger('parser')
        self.logger.setLevel(logging.INFO)
        
        # Форматтер
        formatter = logging.Formatter(
            '%(asctime)s [%(threadName)s]: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Файловый обработчик
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # Консольный обработчик
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Очистка существующих обработчиков и добавление новых
        self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Запрет распространения на корневой логгер
        self.logger.propagate = False
    
    def log(self, message):
        """Потокобезопасное логирование"""
        if self.logger:
            with self.lock:
                self.logger.info(message)

# Глобальный экземпляр логгера
logger = ThreadSafeLogger()

# ==================== РАБОТА С БАЗАМИ ДАННЫХ ====================

def init_technical_db():
    """Инициализация технической базы данных"""
    os.makedirs(CONFIG['bases_folder'], exist_ok=True)
    db_filename = f"{CONFIG['bases_folder']}/tech_base_{datetime.now().strftime('%y%m%d_%H%M')}.db"
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS playtime_data (
        playtime_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ez_id INTEGER,
        forum_name TEXT,
        name TEXT,
        level INTEGER,
        gs INTEGER,
        ilvl INTEGER,
        class TEXT,
        race TEXT,
        guild TEXT,
        kills INTEGER,
        ap INTEGER,
        pers_online BOOLEAN,
        forum_online BOOLEAN,
        page_number INTEGER,
        UNIQUE(ez_id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS name_data (
        ez_id INTEGER PRIMARY KEY,
        forum_name TEXT,
        name TEXT,
        level INTEGER,
        gs INTEGER,
        ilvl INTEGER,
        class TEXT,
        race TEXT,
        guild TEXT,
        kills INTEGER,
        ap INTEGER,
        pers_online BOOLEAN,
        forum_online BOOLEAN,
        page_number INTEGER
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scan_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_type TEXT UNIQUE,
        last_processed_page INTEGER DEFAULT 0,
        total_pages INTEGER DEFAULT 0,
        characters_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'active',
        last_update TEXT
    )
    """)
    
    conn.commit()
    conn.close()
    logger.log(f"Техническая база инициализирована: {db_filename}")
    return db_filename

def init_final_db():
    """Инициализация финальной базы данных"""
    db_filename = f"{CONFIG['bases_folder']}/ezbase_final_{datetime.now().strftime('%y%m%d_%H%M')}.db"
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS characters (
        ez_id INTEGER PRIMARY KEY,
        forum_name TEXT,
        name TEXT,
        level INTEGER,
        gs INTEGER,
        ilvl INTEGER,
        class TEXT,
        race TEXT,
        guild TEXT,
        kills INTEGER,
        ap INTEGER,
        pers_online BOOLEAN,
        forum_online BOOLEAN,
        source TEXT,
        scan_date TEXT,
        playtime INTEGER
    )
    """)
    
    conn.commit()
    conn.close()
    logger.log(f"Финальная база инициализирована: {db_filename}")
    return db_filename

def save_scan_progress(db_filename, data_type, last_page, total_pages, char_count, status):
    """Сохранение прогресса сканирования"""
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()
        cursor.execute("""
        INSERT OR REPLACE INTO scan_progress
        (data_type, last_processed_page, total_pages, characters_count, status, last_update)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (data_type, last_page, total_pages, char_count, status, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.log(f"Ошибка сохранения прогресса: {str(e)}")

def get_scan_progress(db_filename, data_type):
    """Получение прогресса сканирования"""
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()
        cursor.execute("""
        SELECT last_processed_page, total_pages, characters_count, status
        FROM scan_progress WHERE data_type = ?
        """, (data_type,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'last_page': result[0],
                'total_pages': result[1],
                'char_count': result[2],
                'status': result[3]
            }
        return {'last_page': 0, 'total_pages': 0, 'char_count': 0, 'status': 'active'}
    except Exception as e:
        logger.log(f"Ошибка получения прогресса: {str(e)}")
        return {'last_page': 0, 'total_pages': 0, 'char_count': 0, 'status': 'active'}

def save_characters_batch(db_filename, data_type, characters_data, page_number):
    """Сохранение пачки персонажей в техническую базу"""
    if not characters_data:
        return 0
    
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()
        saved_count = 0
        
        if data_type == "playtime":
            for char_data in characters_data:
                try:
                    char_data_with_page = char_data + (page_number,)
                    cursor.execute("""
                    INSERT OR REPLACE INTO playtime_data
                    (ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, page_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, char_data_with_page)
                    saved_count += 1
                except Exception as e:
                    logger.log(f"Ошибка сохранения персонажа {char_data[0]}: {str(e)}")
        else:
            for char_data in characters_data:
                try:
                    char_data_with_page = char_data + (page_number,)
                    cursor.execute("""
                    INSERT OR REPLACE INTO name_data
                    (ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, page_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, char_data_with_page)
                    saved_count += 1
                except Exception as e:
                    logger.log(f"Ошибка сохранения персонажа {char_data[0]}: {str(e)}")
        
        conn.commit()
        conn.close()
        return saved_count
    except Exception as e:
        logger.log(f"Ошибка сохранения батча: {str(e)}")
        return 0

def merge_databases(tech_db, final_db):
    """Объединение данных из технической базы в финальную"""
    logger.log("Начинаем объединение данных в финальную базу...")
    
    try:
        tech_conn = sqlite3.connect(tech_db)
        final_conn = sqlite3.connect(final_db)
        tech_cursor = tech_conn.cursor()
        final_cursor = final_conn.cursor()
        
        # Получаем статистику
        tech_cursor.execute("SELECT COUNT(*) FROM playtime_data")
        playtime_count = tech_cursor.fetchone()[0]
        tech_cursor.execute("SELECT COUNT(*) FROM name_data")
        name_count = tech_cursor.fetchone()[0]
        
        logger.log(f"Данные для объединения: Playtime - {playtime_count}, Name - {name_count}")
        
        if playtime_count == 0 and name_count == 0:
            logger.log("ОШИБКА: В технической базе нет данных для объединения!")
            return 0
        
        scan_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_inserted = 0
        
        # Вставка данных из Playtime
        if playtime_count > 0:
            tech_cursor.execute("""
            SELECT ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, playtime_id
            FROM playtime_data
            """)
            playtime_chars = tech_cursor.fetchall()
            
            for char in playtime_chars:
                try:
                    ez_id, forum_name, name, level, gs, ilvl, class_, race, guild, kills, ap, pers_online, forum_online, playtime_id = char
                    final_cursor.execute("""
                    INSERT OR REPLACE INTO characters
                    (ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, source, scan_date, playtime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (ez_id, forum_name, name, level, gs, ilvl, class_, race, guild, kills, ap, pers_online, forum_online, 'playtime', scan_date, playtime_id))
                    total_inserted += 1
                except Exception as e:
                    logger.log(f"Ошибка вставки персонажа {char[0]} из playtime: {str(e)}")
            
            logger.log(f"Добавлено {len(playtime_chars)} персонажей из playtime_data")
        
        # Вставка данных из Name
        if name_count > 0:
            tech_cursor.execute("""
            SELECT ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online
            FROM name_data
            """)
            name_chars = tech_cursor.fetchall()
            name_inserted = 0
            
            for char in name_chars:
                try:
                    final_cursor.execute("SELECT ez_id FROM characters WHERE ez_id = ?", (char[0],))
                    if not final_cursor.fetchone():
                        final_cursor.execute("""
                        INSERT INTO characters
                        (ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, source, scan_date, playtime)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, char + ('name', scan_date, None))
                        total_inserted += 1
                        name_inserted += 1
                except Exception as e:
                    logger.log(f"Ошибка вставки персонажа {char[0]} из name: {str(e)}")
            
            logger.log(f"Добавлено {name_inserted} персонажей из name_data")
        
        # Финальная статистика
        final_cursor.execute("SELECT COUNT(*) FROM characters")
        total_final = final_cursor.fetchone()[0]
        
        final_conn.commit()
        tech_conn.close()
        final_conn.close()
        
        logger.log("=" * 50)
        logger.log("РЕЗУЛЬТАТЫ ОБЪЕДИНЕНИЯ:")
        logger.log(f"Всего в технической базе: Playtime={playtime_count}, Name={name_count}")
        logger.log(f"Итого в финальной базе: {total_final} персонажей")
        logger.log("=" * 50)
        
        return total_final
    except Exception as e:
        logger.log(f"Ошибка при объединении баз: {str(e)}")
        import traceback
        logger.log(f"Трассировка: {traceback.format_exc()}")
        return 0

# ==================== ПАРСИНГ ДАННЫХ ====================

def clean_text(element):
    """Очистка текста от лишних пробелов"""
    return re.sub(r'\s+', '', element.get_text(strip=True)) if element else ''

def translate_class(class_name):
    """Перевод названия класса с русского на английский"""
    return CLASS_TRANSLATION.get(class_name, class_name)

def translate_race(race_name):
    """Перевод названия расы с русского на английский"""
    return RACE_TRANSLATION.get(race_name, race_name)

def parse_character(character):
    """Парсинг данных одного персонажа из HTML"""
    try:
        name_tag = character.find('td').find('a')
        if not name_tag:
            return None
        
        ez_id = str(name_tag).split('character=')[1].split('">')[0]
        race_icon = character.find('img', class_='character-icon character-race')
        class_icon = character.find('img', class_='character-icon character-class')
        
        original_race = race_icon['title'] if race_icon else ''
        original_class = class_icon['title'] if class_icon else ''
        race = translate_race(original_race)
        class_ = translate_class(original_class)
        
        guild_tag = character.find('span', class_='guild-name')
        td_tags = character.find_all('td', class_='short')
        
        # Проверка онлайн статуса
        character_icons = character.find('span', class_='character-icons')
        pers_online = bool(
            character_icons and
            character_icons.find('span', class_='online') and
            character_icons.find('span', class_='online').find('img', title='В сети')
        )
        
        # Проверка онлайн статуса форумного аккаунта
        member_span = character.find('span', class_='member')
        forum_acc_online = bool(
            member_span and
            member_span.find('span', class_='online') and
            member_span.find('span', class_='online').find('img', title='В сети')
        )
        
        return (
            int(ez_id),
            clean_text(character.find('span', class_='member')),
            name_tag.text.strip(),
            int(clean_text(td_tags[0]) or 0) if len(td_tags) > 0 else 0,
            int(clean_text(td_tags[3]) or 0) if len(td_tags) > 3 else 0,
            int(clean_text(td_tags[2]) or 0) if len(td_tags) > 2 else 0,
            class_,
            race,
            guild_tag.get_text(strip=True) if guild_tag else '',
            int(clean_text(td_tags[1]) or 0) if len(td_tags) > 1 else 0,
            int(clean_text(td_tags[4]) or 0) if len(td_tags) > 4 else 0,
            pers_online,
            forum_acc_online
        )
    except Exception as e:
        logger.log(f"Ошибка парсинга персонажа: {str(e)}")
        return None

def parse_html_content(html_content):
    """Парсинг HTML контента и извлечение персонажей"""
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        characters = soup.find_all('tr', class_='character')
        parsed_characters = []
        
        for character in characters:
            char_data = parse_character(character)
            if char_data:
                parsed_characters.append(char_data)
                
        return parsed_characters, len(characters)
    except Exception as e:
        logger.log(f"Ошибка парсинга HTML: {str(e)}")
        return [], 0

# ==================== СКАЧИВАНИЕ И ОБРАБОТКА ====================

def load_cookies_from_file(filename):
    """Загрузка cookies из файла"""
    cookies = {}
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read().strip()
            parts = content.split('\n\n', 1)
            if len(parts) > 0:
                cookies_section = parts[0]
                lines = cookies_section.split('\n')
                if len(lines) >= 2:
                    cookies_line = lines[1].strip()
                    cookie_pairs = cookies_line.split(';')
                    for pair in cookie_pairs:
                        pair = pair.strip()
                        if pair and '=' in pair:
                            name, value = pair.split('=', 1)
                            cookies[name.strip()] = value.strip()
    except FileNotFoundError:
        logger.log(f"Файл cookies '{filename}' не найден")
    return cookies

def initialize_session(cookies_dict):
    """Инициализация HTTP-сессии с cookies"""
    session = Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value)
    return session

def get_last_page(session):
    """Определение общего количества страниц для сканирования"""
    try:
        response = session.get(LAST_PAGE_URL, timeout=CONFIG['timeout'])
        last_st = int(response.url.split('&st=')[1])
        last_page = (last_st // 20) * 20
        total_pages = (last_st // 20) + 1
        logger.log(f"Определена последняя страница: {last_page} (всего страниц: {total_pages})")
        return last_page, total_pages
    except Exception as e:
        logger.log(f"Ошибка получения последней страницы: {str(e)}")
        return 0, 0

def download_page_with_retry(session, url, page_number, data_type):
    """Скачивание страницы с повторными попытками"""
    for attempt in range(CONFIG['max_attempts']):
        try:
            response = session.get(f"{url}{page_number}", timeout=CONFIG['timeout'])
            if response.status_code == 200:
                return response
            else:
                logger.log(f"Поток {data_type}: ошибка {response.status_code} на странице {page_number}, попытка {attempt+1}")
        except Exception as e:
            logger.log(f"Поток {data_type}: ошибка соединения на странице {page_number}, попытка {attempt+1}: {str(e)}")
        
        if attempt < CONFIG['max_attempts'] - 1:
            time.sleep(CONFIG['retry_delay'])
    
    logger.log(f"Поток {data_type}: не удалось скачать страницу {page_number} после {CONFIG['max_attempts']} попыток")
    return None

def get_delay():
    """Получение задержки в зависимости от настроек"""
    if RANDOM_DELAY:
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        return delay
    elif ENABLE_DELAYS:
        return DELAY_SECONDS
    else:
        return 0

def download_and_process_thread(base_url, data_type, session, tech_db, progress_data):
    """Поток для скачивания и обработки данных"""
    global download_active
    
    logger.log(f"Запуск потока {data_type}...")
    progress = get_scan_progress(tech_db, data_type)
    start_page = progress['last_page']
    total_characters = progress['char_count']
    last_page, total_pages = progress_data['last_page'], progress_data['total_pages']
    
    if progress['status'] == 'completed':
        logger.log(f"Поток {data_type} уже завершен ранее")
        return
    
    logger.log(f"Поток {data_type} начинается со страницы {start_page}")
    
    pbar = tqdm(
        total=total_pages,
        initial=start_page // 20,
        desc=f"{data_type:>8}",
        position=0 if data_type == "playtime" else 1,
        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
    )
    
    current_page = start_page
    while current_page <= last_page and download_active:
        response = download_page_with_retry(session, base_url, current_page, data_type)
        if not response:
            logger.log(f"Поток {data_type}: КРИТИЧЕСКАЯ ОШИБКА - не удалось скачать страницу {current_page}")
            save_scan_progress(tech_db, data_type, current_page, total_pages, total_characters, 'error')
            break
        
        characters, char_count = parse_html_content(response.content)
        if char_count == 0:
            logger.log(f"Поток {data_type}: КРИТИЧЕСКАЯ ОШИБКА - страница {current_page} не содержит персонажей!")
            save_scan_progress(tech_db, data_type, current_page, total_pages, total_characters, 'error')
            break
        
        saved_count = save_characters_batch(tech_db, data_type, characters, current_page)
        if saved_count > 0:
            total_characters += saved_count
        
        save_scan_progress(tech_db, data_type, current_page + 20, total_pages, total_characters, 'active')
        current_page += 20
        pbar.update(1)
        
        delay = get_delay()
        if delay > 0:
            time.sleep(delay)
    
    if current_page > last_page:
        status = 'completed'
        logger.log(f"Поток {data_type} УСПЕШНО ЗАВЕРШЕН")
    else:
        status = 'stopped' if download_active else 'interrupted'
        logger.log(f"Поток {data_type} ОСТАНОВЛЕН")
    
    save_scan_progress(tech_db, data_type, min(current_page, last_page), total_pages, total_characters, status)
    pbar.close()

def signal_handler(sig, frame):
    """Обработчик сигнала прерывания"""
    global download_active
    logger.log("Получен сигнал прерывания, завершаем работу...")
    download_active = False

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================

def main():
    """Основная функция"""
    global download_active
    
    signal.signal(signal.SIGINT, signal_handler)
    start_time = time.time()
    
    # Создаем папки
    os.makedirs(CONFIG['logs_folder'], exist_ok=True)
    os.makedirs(CONFIG['bases_folder'], exist_ok=True)
    
    # Настройка логгера
    log_filename = f"{CONFIG['logs_folder']}/direct_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    logger.setup(log_filename)
    
    logger.log('=' * 60)
    logger.log('ЗАПУСК ПРЯМОГО ПАРСЕРА (ПАМЯТЬ → БАЗА)')
    logger.log(f'Дата: {date.today().strftime("%Y.%m.%d")}')
    logger.log(f'Режим PLAYTIME_ONLY: {"ДА" if PLAYTIME_ONLY else "НЕТ"}')
    logger.log('=' * 60)
    
    try:
        # Инициализация баз данных
        tech_db = init_technical_db()
        final_db = init_final_db()
        
        # Загрузка cookies
        cookies_dict = load_cookies_from_file(COOKIES_FILE)
        if not cookies_dict:
            logger.log("ОШИБКА: Не удалось загрузить cookies!")
            return
        
        # Инициализация сессий
        session1 = initialize_session(cookies_dict)
        session2 = initialize_session(cookies_dict) if not PLAYTIME_ONLY else None
        
        # Определение последней страницы
        logger.log("Определение количества страниц...")
        last_page, total_pages = get_last_page(session1)
        if last_page == 0:
            logger.log("ОШИБКА: Не удалось определить количество страниц!")
            return
        
        progress_data = {'last_page': last_page, 'total_pages': total_pages}
        
        # Запуск потоков
        threads = []
        playtime_thread = threading.Thread(
            target=download_and_process_thread,
            args=(PLAYTIME_URL, "playtime", session1, tech_db, progress_data),
            name="Playtime_Thread"
        )
        threads.append(playtime_thread)
        
        if not PLAYTIME_ONLY:
            name_thread = threading.Thread(
                target=download_and_process_thread,
                args=(NAME_URL, "name", session2, tech_db, progress_data),
                name="Name_Thread"
            )
            threads.append(name_thread)
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Объединение данных
        logger.log("\n" + "=" * 60)
        logger.log("НАЧИНАЕМ ОБЪЕДИНЕНИЕ ДАННЫХ")
        logger.log("=" * 60)
        
        total_final = merge_databases(tech_db, final_db)
        
        # Итоговая статистика
        total_duration = time.time() - start_time
        minutes = int(total_duration // 60)
        seconds = int(total_duration % 60)
        
        logger.log('=' * 60)
        logger.log('РАБОТА ЗАВЕРШЕНА')
        logger.log(f'Общее время: {minutes:02d}:{seconds:02d}')
        logger.log(f'Итоговых персонажей: {total_final}')
        logger.log(f'Техническая база: {tech_db}')
        logger.log(f'Финальная база: {final_db}')
        logger.log('=' * 60)
        
    except Exception as e:
        logger.log(f'КРИТИЧЕСКАЯ ОШИБКА: {str(e)}')
        import traceback
        logger.log(traceback.format_exc())

if __name__ == "__main__":
    main()
