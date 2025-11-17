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

# ==================== КОНФИГУРАЦИЯ ====================
PLAYTIME_ONLY = False
COOKIES_FILE = 'cookies.md'
ENABLE_DELAYS = False
DELAY_SECONDS = 0.5
RANDOM_DELAY = False  # Новая опция - случайные задержки
MIN_DELAY = 0.3       # Минимальная задержка
MAX_DELAY = 1.2       # Максимальная задержка

# Базовые URL для парсинга
PLAYTIME_URL = "https://ezwow.org/index.php?app=isengard&module=core&tab=armory&section=characters&realm=1&sort%5Bkey%5D=playtime&sort%5Border%5D=desc&st="
NAME_URL = "https://ezwow.org/index.php?app=isengard&module=core&tab=armory&section=characters&realm=1&sort%5Bkey%5D=name&sort%5Border%5D=desc&st="

LAST_PAGE_URL = 'https://ezwow.org/index.php?app=isengard&module=core&tab=armory&section=characters&realm=1&sort%5Bkey%5D=playtime&sort%5Border%5D=desc&st=9999999999999999999'

# Словари для преобразования классов и рас (полное поле -> английское название)
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

# Настройки
CONFIG = {
    'timeout': 30,
    'max_attempts': 3,
    'retry_delay': 2,
    'logs_folder': 'LOGS',
    'bases_folder': 'BASES'
}

# Глобальные переменные
log_file = None
log_lock = threading.Lock()
download_active = True

# ==================== СИСТЕМА ЛОГГИРОВАНИЯ ====================

def logger(message, display=True):
    """Потокобезопасная запись сообщений в лог-файл и вывод в консоль"""
    global log_file, log_lock
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    thread_name = threading.current_thread().name
    log_message = f"{now} [{thread_name}]: {message}"
    
    with log_lock:
        if display:
            print(log_message)
        if log_file:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(log_message + "\n")

# ==================== РАБОТА С БАЗАМИ ДАННЫХ ====================

def init_technical_db():
    """Инициализация технической базы данных"""
    os.makedirs(CONFIG['bases_folder'], exist_ok=True)
    db_filename = f"{CONFIG['bases_folder']}/tech_base_{datetime.now().strftime('%y%m%d_%H%M')}.db"
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    
    # Таблица для данных из Playtime с playtime_id
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
    
    # Таблица для данных из Name
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
    
    # Таблица прогресса сканирования
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
    logger(f"Техническая база инициализирована: {db_filename}")
    return db_filename

def init_final_db():
    """Инициализация финальной базы данных с полем playtime"""
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
    logger(f"Финальная база инициализирована: {db_filename}")
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
        logger(f"Ошибка сохранения прогресса: {str(e)}")

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
        else:
            return {'last_page': 0, 'total_pages': 0, 'char_count': 0, 'status': 'active'}
    except Exception as e:
        logger(f"Ошибка получения прогресса: {str(e)}")
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
            # Для playtime_data используем автоматическое присвоение playtime_id
            for char_data in characters_data:
                try:
                    # Добавляем page_number к данным персонажа (без playtime_id - он autoincrement)
                    char_data_with_page = char_data + (page_number,)
                    cursor.execute("""
                    INSERT OR REPLACE INTO playtime_data 
                    (ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, page_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, char_data_with_page)
                    saved_count += 1
                except Exception as e:
                    logger(f"Ошибка сохранения персонажа {char_data[0]}: {str(e)}", display=False)
        else:
            # Для name_data сохраняем как раньше
            table_name = "name_data"
            for char_data in characters_data:
                try:
                    char_data_with_page = char_data + (page_number,)
                    cursor.execute(f"""
                    INSERT OR REPLACE INTO {table_name} 
                    (ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, page_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, char_data_with_page)
                    saved_count += 1
                except Exception as e:
                    logger(f"Ошибка сохранения персонажа {char_data[0]}: {str(e)}", display=False)
        
        conn.commit()
        conn.close()
        return saved_count
    except Exception as e:
        logger(f"Ошибка сохранения батча: {str(e)}")
        return 0

def check_technical_db_data(tech_db):
    """Проверка данных в технической базе"""
    logger("Проверка данных в технической базе...")
    try:
        conn = sqlite3.connect(tech_db)
        cursor = conn.cursor()
        
        # Проверяем таблицу playtime_data
        cursor.execute("SELECT COUNT(*) FROM playtime_data")
        playtime_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM name_data") 
        name_count = cursor.fetchone()[0]
        
        logger(f"В технической базе: Playtime - {playtime_count}, Name - {name_count}")
        
        # Проверяем несколько записей из каждой таблицы
        if playtime_count > 0:
            cursor.execute("SELECT playtime_id, ez_id, name, level, class, race FROM playtime_data LIMIT 3")
            playtime_samples = cursor.fetchall()
            logger(f"Примеры из playtime_data: {playtime_samples}")
        
        if name_count > 0:
            cursor.execute("SELECT ez_id, name, level, class, race FROM name_data LIMIT 3")
            name_samples = cursor.fetchall()
            logger(f"Примеры из name_data: {name_samples}")
        
        conn.close()
        
        return playtime_count > 0 or name_count > 0
        
    except Exception as e:
        logger(f"Ошибка проверки технической базы: {str(e)}")
        return False

def merge_databases(tech_db, final_db):
    """Объединение данных из технической базы в финальную - ОБНОВЛЕННАЯ ВЕРСИЯ С PLAYTIME"""
    logger("Начинаем объединение данных в финальную базу...")
    
    try:
        tech_conn = sqlite3.connect(tech_db)
        final_conn = sqlite3.connect(final_db)
        tech_cursor = tech_conn.cursor()
        final_cursor = final_conn.cursor()
        
        # Получаем статистику из технической базы
        tech_cursor.execute("SELECT COUNT(*) FROM playtime_data")
        playtime_count = tech_cursor.fetchone()[0]
        
        tech_cursor.execute("SELECT COUNT(*) FROM name_data")
        name_count = tech_cursor.fetchone()[0]
        
        logger(f"Данные для объединения: Playtime - {playtime_count}, Name - {name_count}")
        
        # Проверяем, есть ли данные в технической базе
        if playtime_count == 0 and name_count == 0:
            logger("ОШИБКА: В технической базе нет данных для объединения!")
            return 0
        
        scan_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_inserted = 0
        
        # ВСТАВЛЯЕМ ДАННЫЕ ИЗ PLAYTIME (с playtime_id)
        if playtime_count > 0:
            logger("Вставка данных из таблицы playtime_data...")
            tech_cursor.execute("""
            SELECT ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, playtime_id
            FROM playtime_data
            """)
            
            playtime_chars = tech_cursor.fetchall()
            for char in playtime_chars:
                try:
                    # Извлекаем playtime_id и остальные данные
                    ez_id, forum_name, name, level, gs, ilvl, class_, race, guild, kills, ap, pers_online, forum_online, playtime_id = char
                    
                    final_cursor.execute("""
                    INSERT OR REPLACE INTO characters 
                    (ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, source, scan_date, playtime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (ez_id, forum_name, name, level, gs, ilvl, class_, race, guild, kills, ap, pers_online, forum_online, 'playtime', scan_date, playtime_id))
                    total_inserted += 1
                except Exception as e:
                    logger(f"Ошибка вставки персонажа {char[0]} из playtime: {str(e)}", display=False)
            
            logger(f"Добавлено {len(playtime_chars)} персонажей из playtime_data")
        
        # ВСТАВЛЯЕМ ДАННЫЕ ИЗ NAME (с playtime = NULL)
        if name_count > 0:
            logger("Вставка данных из таблицы name_data...")
            tech_cursor.execute("""
            SELECT ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online
            FROM name_data
            """)
            
            name_chars = tech_cursor.fetchall()
            name_inserted = 0
            
            for char in name_chars:
                try:
                    # Проверяем, есть ли уже такой персонаж в финальной базе
                    final_cursor.execute("SELECT ez_id FROM characters WHERE ez_id = ?", (char[0],))
                    if not final_cursor.fetchone():
                        # Вставляем с playtime = NULL
                        final_cursor.execute("""
                        INSERT INTO characters 
                        (ez_id, forum_name, name, level, gs, ilvl, class, race, guild, kills, ap, pers_online, forum_online, source, scan_date, playtime)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, char + ('name', scan_date, None))
                        total_inserted += 1
                        name_inserted += 1
                except Exception as e:
                    logger(f"Ошибка вставки персонажа {char[0]} из name: {str(e)}", display=False)
            
            logger(f"Добавлено {name_inserted} персонажей из name_data")
        
        # Получаем финальную статистику
        final_cursor.execute("SELECT COUNT(*) FROM characters")
        total_final = final_cursor.fetchone()[0]
        
        final_cursor.execute("SELECT COUNT(*) FROM characters WHERE source = 'playtime'")
        total_playtime_final = final_cursor.fetchone()[0]
        
        final_cursor.execute("SELECT COUNT(*) FROM characters WHERE source = 'name'")
        total_name_final = final_cursor.fetchone()[0]
        
        # Статистика по playtime
        final_cursor.execute("SELECT COUNT(*) FROM characters WHERE playtime IS NOT NULL")
        total_with_playtime = final_cursor.fetchone()[0]
        
        final_cursor.execute("SELECT MIN(playtime), MAX(playtime) FROM characters WHERE playtime IS NOT NULL")
        playtime_range = final_cursor.fetchone()
        
        # Проверяем уникальность playtime
        final_cursor.execute("""
        SELECT playtime, COUNT(*) 
        FROM characters 
        WHERE playtime IS NOT NULL 
        GROUP BY playtime 
        HAVING COUNT(*) > 1
        """)
        duplicate_playtimes = final_cursor.fetchall()
        
        final_conn.commit()
        tech_conn.close()
        final_conn.close()
        
        logger("=" * 50)
        logger("РЕЗУЛЬТАТЫ ОБЪЕДИНЕНИЯ:")
        logger(f"Всего в технической базе: Playtime={playtime_count}, Name={name_count}")
        logger(f"Добавлено в финальную базу: {total_inserted} персонажей")
        logger(f"Итого в финальной базе: {total_final} персонажей")
        logger(f"Распределение: Playtime={total_playtime_final}, Name={total_name_final}")
        logger(f"С playtime: {total_with_playtime}, Без playtime: {total_final - total_with_playtime}")
        if playtime_range[0] is not None:
            logger(f"Диапазон playtime: {playtime_range[0]} - {playtime_range[1]}")
        
        if duplicate_playtimes:
            logger(f"ПРЕДУПРЕЖДЕНИЕ: Найдено {len(duplicate_playtimes)} дубликатов playtime!")
            for dup in duplicate_playtimes[:5]:  # Показываем первые 5 дубликатов
                logger(f"  playtime {dup[0]} встречается {dup[1]} раз")
        
        logger("=" * 50)
        
        return total_final
        
    except Exception as e:
        logger(f"Ошибка при объединении баз: {str(e)}")
        import traceback
        logger(f"Трассировка: {traceback.format_exc()}")
        return 0

# ==================== ПАРСИНГ ДАННЫХ ====================

def clean_text(element):
    """Очистка текста от лишних пробелов"""
    return re.sub(r'\s+', '', element.get_text(strip=True)) if element else ''

def translate_class(class_name):
    """Перевод названия класса с русского на английский (полное поле -> английское название)"""
    if not class_name:
        return ''
    return CLASS_TRANSLATION.get(class_name, class_name)

def translate_race(race_name):
    """Перевод названия расы с русского на английский (полное поле -> английское название)"""
    if not race_name:
        return ''
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
        
        # Получаем оригинальные значения и преобразуем их
        original_race = race_icon['title'] if race_icon else ''
        original_class = class_icon['title'] if class_icon else ''
        
        race = translate_race(original_race)
        class_ = translate_class(original_class)
        
        guild_tag = character.find('span', class_='guild-name')
        td_tags = character.find_all('td', class_='short')
        
        # Проверка онлайн статуса персонажа
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
            clean_text(character.find('span', class_='member')),  # Аккаунт
            name_tag.text.strip(),                                # Имя
            int(clean_text(td_tags[0]) or 0) if len(td_tags) > 0 else 0,  # Уровень
            int(clean_text(td_tags[3]) or 0) if len(td_tags) > 3 else 0,  # GS
            int(clean_text(td_tags[2]) or 0) if len(td_tags) > 2 else 0,  # iLVL
            class_,                                               # Класс
            race,                                                 # Раса
            guild_tag.get_text(strip=True) if guild_tag else '',  # Гильдия
            int(clean_text(td_tags[1]) or 0) if len(td_tags) > 1 else 0,  # Убийства
            int(clean_text(td_tags[4]) or 0) if len(td_tags) > 4 else 0,  # AP
            pers_online,                                          # Персонаж онлайн
            forum_acc_online                                      # Форум онлайн
        )
    except Exception as e:
        logger(f"Ошибка парсинга персонажа: {str(e)}", display=False)
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
        logger(f"Ошибка парсинга HTML: {str(e)}")
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
        logger(f"Файл cookies '{filename}' не найден")
    return cookies

def initialize_session(cookies_dict):
    """Инициализация HTTP-сессии с cookies"""
    session = Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value)
    return session

def get_last_page(session):
    """Определение общего количества страниц для сканирования через запрос максимальной страницы"""
    try:
        response = session.get(LAST_PAGE_URL, timeout=CONFIG['timeout'])
        # Извлекаем номер последней страницы из URL ответа
        last_st = int(response.url.split('&st=')[1])
        last_page = (last_st // 20) * 20  # Округляем до ближайшего кратного 20
        total_pages = (last_st // 20) + 1
        logger(f"Определена последняя страница: {last_page} (всего страниц: {total_pages})")
        return last_page, total_pages
    except Exception as e:
        logger(f"Ошибка получения последней страницы: {str(e)}")
        return 0, 0

def download_page_with_retry(session, url, page_number, data_type):
    """Скачивание страницы с повторными попытками"""
    for attempt in range(CONFIG['max_attempts']):
        try:
            response = session.get(f"{url}{page_number}", timeout=CONFIG['timeout'])
            
            if response.status_code == 200:
                return response
            else:
                logger(f"Поток {data_type}: ошибка {response.status_code} на странице {page_number}, попытка {attempt+1}")
                
        except Exception as e:
            logger(f"Поток {data_type}: ошибка соединения на странице {page_number}, попытка {attempt+1}: {str(e)}")
        
        if attempt < CONFIG['max_attempts'] - 1:
            time.sleep(CONFIG['retry_delay'])
    
    logger(f"Поток {data_type}: не удалось скачать страницу {page_number} после {CONFIG['max_attempts']} попыток")
    return None

def get_delay():
    """Получение задержки в зависимости от настроек"""
    if RANDOM_DELAY:
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        logger(f"Случайная задержка: {delay:.2f} сек", display=False)
        return delay
    elif ENABLE_DELAYS:
        return DELAY_SECONDS
    else:
        return 0

def download_and_process_thread(base_url, data_type, session, tech_db, progress_data):
    """Поток для скачивания и обработки данных"""
    global download_active
    
    logger(f"Запуск потока {data_type}...")
    
    # Получаем прогресс
    progress = get_scan_progress(tech_db, data_type)
    start_page = progress['last_page']
    total_characters = progress['char_count']
    last_page, total_pages = progress_data['last_page'], progress_data['total_pages']
    
    if progress['status'] == 'completed':
        logger(f"Поток {data_type} уже завершен ранее")
        return
    
    logger(f"Поток {data_type} начинается со страницы {start_page}")
    
    # Прогресс-бар
    pbar = tqdm(
        total=total_pages,
        initial=start_page // 20,
        desc=f"{data_type:>8}",
        position=0 if data_type == "playtime" else 1,
        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
    )
    
    current_page = start_page
    
    while current_page <= last_page and download_active:
        # Скачиваем страницу с повторными попытками
        response = download_page_with_retry(session, base_url, current_page, data_type)
        
        if not response:
            # Не удалось скачать страницу после всех попыток
            logger(f"Поток {data_type}: КРИТИЧЕСКАЯ ОШИБКА - не удалось скачать страницу {current_page}")
            save_scan_progress(tech_db, data_type, current_page, total_pages, total_characters, 'error')
            break
        
        # Парсим страницу
        characters, char_count = parse_html_content(response.content)
        
        if char_count == 0:
            # ПУСТАЯ СТРАНИЦА - КРИТИЧЕСКАЯ ОШИБКА
            logger(f"Поток {data_type}: КРИТИЧЕСКАЯ ОШИБКА - страница {current_page} не содержит персонажей!")
            logger("ВОЗМОЖНЫЕ ПРИЧИНЫ:")
            logger("1. Неверные cookies")
            logger("2. Сервер блокирует запросы")
            logger("3. Проблемы с подключением к серверу")
            save_scan_progress(tech_db, data_type, current_page, total_pages, total_characters, 'error')
            break
        
        # Сохраняем персонажей в базу
        saved_count = save_characters_batch(tech_db, data_type, characters, current_page)
        
        if saved_count > 0:
            total_characters += saved_count
            logger(f"Поток {data_type}: стр {current_page} -> {saved_count} перс", display=False)
        
        # Сохраняем прогресс после КАЖДОЙ страницы
        save_scan_progress(tech_db, data_type, current_page + 20, total_pages, total_characters, 'active')
        
        current_page += 20
        pbar.update(1)
        
        # Пауза между запросами (если включено)
        delay = get_delay()
        if delay > 0:
            time.sleep(delay)
    
    # Завершаем поток
    if current_page > last_page:
        status = 'completed'
        logger(f"Поток {data_type} УСПЕШНО ЗАВЕРШЕН")
    else:
        status = 'stopped' if download_active else 'interrupted'
        logger(f"Поток {data_type} ОСТАНОВЛЕН")
    
    save_scan_progress(tech_db, data_type, min(current_page, last_page), total_pages, total_characters, status)
    pbar.close()

def signal_handler(sig, frame):
    """Обработчик сигнала прерывания"""
    global download_active
    logger("Получен сигнал прерывания, завершаем работу...")
    download_active = False

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================

def main():
    """Основная функция"""
    global log_file, download_active
    
    # Настройка обработчика прерывания
    signal.signal(signal.SIGINT, signal_handler)
    
    start_time = time.time()
    
    # Создаем папки
    os.makedirs(CONFIG['logs_folder'], exist_ok=True)
    os.makedirs(CONFIG['bases_folder'], exist_ok=True)
    
    # Настройка логгера
    log_filename = f"{CONFIG['logs_folder']}/direct_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_file = log_filename
    
    logger('=' * 60)
    logger('ЗАПУСК ПРЯМОГО ПАРСЕРА (ПАМЯТЬ → БАЗА)')
    logger(f'Дата: {date.today().strftime("%Y.%m.%d")}')
    logger(f'Режим PLAYTIME_ONLY: {"ДА" if PLAYTIME_ONLY else "НЕТ"}')
    logger(f'Фиксированные паузы: {"ДА" if ENABLE_DELAYS else "НЕТ"}')
    if ENABLE_DELAYS:
        logger(f'Длительность паузы: {DELAY_SECONDS} сек')
    logger(f'Случайные паузы: {"ДА" if RANDOM_DELAY else "НЕТ"}')
    if RANDOM_DELAY:
        logger(f'Диапазон пауз: {MIN_DELAY} - {MAX_DELAY} сек')
    logger('=' * 60)
    
    try:
        # Инициализация баз данных
        tech_db = init_technical_db()
        final_db = init_final_db()
        
        # Загрузка cookies
        cookies_dict = load_cookies_from_file(COOKIES_FILE)
        if not cookies_dict:
            logger("ОШИБКА: Не удалось загрузить cookies!")
            return
        
        # Инициализация сессий
        session1 = initialize_session(cookies_dict)
        session2 = initialize_session(cookies_dict) if not PLAYTIME_ONLY else None
        
        # Определение последней страницы
        logger("Определение количества страниц...")
        last_page, total_pages = get_last_page(session1)
        
        if last_page == 0:
            logger("ОШИБКА: Не удалось определить количество страниц!")
            return
        
        progress_data = {'last_page': last_page, 'total_pages': total_pages}
        
        # Запуск потоков
        threads = []
        
        # Playtime поток
        playtime_thread = threading.Thread(
            target=download_and_process_thread,
            args=(PLAYTIME_URL, "playtime", session1, tech_db, progress_data),
            name="Playtime_Thread"
        )
        threads.append(playtime_thread)
        
        # Name поток (если нужно)
        if not PLAYTIME_ONLY:
            name_thread = threading.Thread(
                target=download_and_process_thread,
                args=(NAME_URL, "name", session2, tech_db, progress_data),
                name="Name_Thread"
            )
            threads.append(name_thread)
        
        # Запуск потоков
        for thread in threads:
            thread.start()
        
        # Ожидание завершения потоков
        for thread in threads:
            thread.join()
        
        # Проверяем данные перед объединением
        logger("\n" + "=" * 60)
        logger("ПРОВЕРКА ДАННЫХ ПЕРЕД ОБЪЕДИНЕНИЕМ")
        logger("=" * 60)
        
        has_data = check_technical_db_data(tech_db)
        
        if not has_data:
            logger("ПРЕДУПРЕЖДЕНИЕ: В технической базе нет данных!")
            logger("Возможные причины:")
            logger("1. Проблемы с cookies")
            logger("2. Сервер блокирует запросы") 
            logger("3. Изменилась структура HTML страниц")
            logger("4. Проблемы с подключением к интернету")
            return
        
        # Объединение данных
        logger("\n" + "=" * 60)
        logger("НАЧИНАЕМ ОБЪЕДИНЕНИЕ ДАННЫХ")
        logger("=" * 60)
        
        total_final = merge_databases(tech_db, final_db)
        
        # Итоговая статистика
        total_duration = time.time() - start_time
        minutes = int(total_duration // 60)
        seconds = int(total_duration % 60)
        
        logger('=' * 60)
        logger('РАБОТА ЗАВЕРШЕНА')
        logger(f'Общее время: {minutes:02d}:{seconds:02d}')
        logger(f'Итоговых персонажей: {total_final}')
        logger(f'Техническая база: {tech_db}')
        logger(f'Финальная база: {final_db}')
        logger('=' * 60)
        
    except Exception as e:
        logger(f'КРИТИЧЕСКАЯ ОШИБКА: {str(e)}')
        import traceback
        logger(traceback.format_exc())

if __name__ == "__main__":
    main()