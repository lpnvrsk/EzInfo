import sqlite3
import os
import shutil
from datetime import datetime

# Конфигурационная переменная для пути к папке интерфейса WoW
WoW_InterfaceFolderPath = r"C:\GAMES\Isengard_WotLK_335a\Interface\AddOns\EzInfo"  # Укажите путь, например: "C:\Isengard_WotLK_335a\Interface\AddOns\EzInfo"

# Глобальная переменная для файла лога
log_file = None

def setup_logging():
    """Настраивает логирование в файл с временной меткой"""
    global log_file
    logs_dir = "LOGS"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Создаем имя файла с текущей датой и временем
    current_time = datetime.now().strftime("%d.%m.%Y-%H.%M.%S")
    log_filename = f"DataInfuser_{current_time}.txt"  # Изменено на .txt
    log_filepath = os.path.join(logs_dir, log_filename)
    
    try:
        # Открываем файл для записи (используем 'a' для добавления, чтобы не потерять данные при ошибках)
        log_file = open(log_filepath, 'a', encoding='utf-8')
        
        # Пишем начальную информацию
        log_file.write(f"Логирование начато: {log_filepath}\n")
        log_file.write(f"Время запуска: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
        log_file.write("-" * 50 + "\n")
        log_file.flush()
        log_message(f"Лог-файл создан: {log_filepath}")
    except Exception as e:
        print(f"Ошибка создания лог-файла: {e}")

def log_message(message):
    """Записывает сообщение в лог файл и выводит в консоль"""
    global log_file
    print(message)  # Выводим в консоль
    if log_file:
        try:
            log_file.write(message + "\n")
            log_file.flush()  # Принудительно записываем в файл
        except Exception as e:
            print(f"Ошибка записи в лог: {e}")

def close_logging():
    """Закрывает файл лога"""
    global log_file
    if log_file:
        try:
            log_file.write("-" * 50 + "\n")
            log_file.write(f"Логирование завершено: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
            log_file.close()
            log_message(f"Лог-файл закрыт")
        except Exception as e:
            print(f"Ошибка закрытия лог-файла: {e}")
        finally:
            log_file = None

# Таблицы для преобразования классов и рас
CLASSES = {
    "Hunter": 0,
    "Warlock": 1,
    "Priest": 2,
    "Paladin": 3,
    "Mage": 4,
    "Rogue": 5,
    "Druid": 6,
    "Shaman": 7,
    "Warrior": 8,
    "Death Knight": 9,
}

RACES = {
    "Human": 0,
    "Dwarf": 1,
    "Night Elf": 2,
    "Gnome": 3,
    "Draenei": 4,
    "Orc": 5,
    "Undead": 6,
    "Tauren": 7,
    "Troll": 8,
    "Blood Elf": 9,
}

# Функция для определения цвета GS
def get_gs_color(gs_value):
    """Возвращает цветовой код для значения GS"""
    if not gs_value or gs_value == 0:
        return "FFFAFAFA"  # белый для пустых значений
    elif gs_value < 1000:
        return "FFFAFAFA"  # белый
    elif gs_value < 2000:
        return "FFA1FA4F"  # зеленый
    elif gs_value < 3000:
        return "FF5763FB"  # синий
    elif gs_value < 4000:
        return "FF8A2BE2"  # фиолетовый
    elif gs_value < 5000:
        return "FFFF8C00"  # ярко-оранжевый
    elif gs_value < 6000:
        return "FFFF4500"  # красно-оранжевый
    elif gs_value < 6200:
        return "FFff8484"  # темно-красный
    else:
        return "FFFF1493"  # глубокий розовый

def find_database_file():
    """Находит последний файл базы данных в каталоге BASES или текущем каталоге"""
    bases_dir = "BASES"
    # Ищем в каталоге BASES
    if os.path.exists(bases_dir) and os.path.isdir(bases_dir):
        db_files = []
        for file in os.listdir(bases_dir):
            if file.startswith('ezbase_') and file.endswith('.db'):
                file_path = os.path.join(bases_dir, file)
                db_files.append((file_path, os.path.getmtime(file_path)))
        if db_files:
            # Сортируем по дате изменения (последний первый)
            db_files.sort(key=lambda x: x[1], reverse=True)
            log_message(f"Найдена база в BASES: {os.path.basename(db_files[0][0])}")
            return db_files[0][0]
    # Ищем в текущем каталоге
    for file in os.listdir('.'):
        if file.startswith('ezbase_') and file.endswith('.db'):
            log_message(f"Найдена база в текущем каталоге: {file}")
            return file
    return None

def generate_database_code(database_dict):
    """Генерирует LUA код базы данных с разбивкой по строкам"""
    lines = []
    lines.append("{")
    # Сортируем по forum_name для сохранения алфавитного порядка
    sorted_items = sorted(database_dict.items(), key=lambda x: x[0])
    account_lines = []
    for forum_name, chars_list in sorted_items:
        # Экранируем специальные символы в forum_name
        escaped_forum_name = forum_name.replace('"', '\\"').replace("'", "\\'")
        # Создаем список всех персонажей для этого аккаунта
        char_lines = []
        for char_data in chars_list:
            name, lvl, gs, race, guild, class_num, gs_color = char_data
            # Экранируем специальные символы в имени и гильдии
            escaped_name = name.replace('"', '\\"').replace("'", "\\'") if name else ""
            escaped_guild = guild.replace('"', '\\"').replace("'", "\\'") if guild else ""
            char_line = f'{{"{escaped_name}",{lvl},{gs},{race},"{escaped_guild}",{class_num},"{gs_color}"}}'
            char_lines.append(char_line)
        # Объединяем персонажей в строки, разбивая если слишком длинные
        current_line = ""
        account_parts = []
        for char_line in char_lines:
            # Если добавление следующего персонажа превысит 4000 символов, начинаем новую строку
            if len(current_line) + len(char_line) + 2 > 4000 and current_line:
                account_parts.append(current_line.rstrip(','))
                current_line = char_line + ","
            else:
                current_line += char_line + ","
        # Добавляем последнюю часть
        if current_line:
            account_parts.append(current_line.rstrip(','))
        # Формируем окончательный код для аккаунта
        if len(account_parts) == 1:
            account_line = f'["{escaped_forum_name}"] = {{{account_parts[0]}}}'
        else:
            account_line = f'["{escaped_forum_name}"] = {{\n    '
            account_line += ',\n    '.join(account_parts)
            account_line += '\n  }'
        account_lines.append(account_line)
    # Объединяем все аккаунты с переносами строк
    lines.append(',\n  '.join(account_lines))
    lines.append("}")
    return '\n'.join(lines)

def generate_addon_with_database():
    """Основная функция генерации аддона"""
    log_message("Запуск генерации аддона...")
    # Находим базу данных автоматически
    db_path = find_database_file()
    if not db_path:
        log_message("Ошибка: Файл базы данных не найден!")
        log_message("Убедитесь, что файл с именем ezbase_*.db находится в папке BASES или в текущей папке.")
        return
    log_message(f"Используется база данных: {db_path}")
    # Получаем дату изменения файла как дату сборки
    build_time = datetime.fromtimestamp(os.path.getmtime(db_path)).strftime('%d.%m.%Y %H:%M')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Получаем общее количество записей
    cursor.execute("SELECT COUNT(*) FROM characters")
    total_records = cursor.fetchone()[0]
    log_message(f"Всего записей в базе: {total_records}")

    # Получаем количество уникальных аккаунтов и гильдий
    cursor.execute("SELECT COUNT(DISTINCT forum_name) FROM characters WHERE forum_name != ''")
    total_accounts = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT guild) FROM characters WHERE guild != ''")
    total_guilds = cursor.fetchone()[0]
    log_message(f"Уникальных аккаунтов: {total_accounts}")
    log_message(f"Уникальных гильдий: {total_guilds}")

    # Получаем все записи, группируем по аккаунту и сортируем по GS внутри каждого аккаунта
    cursor.execute("""
        SELECT forum_name, name, level, gs, class, guild, race
        FROM characters
        ORDER BY forum_name, gs DESC
    """)
    rows = cursor.fetchall()

    # Создаем словарь для хранения данных
    database_dict = {}
    unknown_classes = set()  # Для отслеживания неизвестных классов
    for row in rows:
        forum_name, name, level, gs, class_str, guild, race_str = row
        # Обрабатываем пустой forum_name
        if not forum_name or forum_name.strip() == "":
            forum_name = "NOFORUMNAME"
        # Преобразуем class из строки в число
        class_num = CLASSES.get(class_str, 0)
        # Если класс не найден, попробуем найти игнорируя регистр
        if class_num == 0 and class_str:
            for class_key, class_id in CLASSES.items():
                if class_str.lower() == class_key.lower():
                    class_num = class_id
                    break
        # Если все еще не нашли, добавляем в список неизвестных
        if class_num == 0 and class_str:
            unknown_classes.add(class_str)
            # log_message(f"Предупреждение: Неизвестный класс '{class_str}' для персонажа '{name}'")
        # Преобразуем race из строки в число
        race_num = RACES.get(race_str, 0)
        # Если раса не найдена, попробуем найти по ключу в нижнем регистре
        if race_num == 0 and race_str:
            for race_key, race_id in RACES.items():
                if race_str.lower() == race_key.lower():
                    race_num = race_id
                    break
        # Определяем цвет GS
        gs_color = get_gs_color(gs)

        # Создаем запись персонажа: [name, level, gs, race, guild, class, gs_color]
        char_data = [name, level, int(gs) if gs else 0, race_num, guild, class_num, gs_color]
        # Добавляем персонажа в соответствующую группу forum_name
        if forum_name not in database_dict:
            database_dict[forum_name] = []
        database_dict[forum_name].append(char_data)

    # Выводим статистику по неизвестным классам
    if unknown_classes:
        log_message(f"\nПредупреждение: Найдены неизвестные классы: {unknown_classes}")
        log_message("Пожалуйста, добавьте их в словарь CLASSES в скрипте")

    # Генерируем код базы данных
    log_message("Генерация LUA кода базы данных...")
    db_code = generate_database_code(database_dict)

    # Генерируем основной файл аддона со встроенной базой
    log_message("Создание файла аддона...")
    
    # Формируем LUA код с правильным экранированием
    main_code = f'''-- EzInfo Addon
-- Локальные ссылки для оптимизации
local strlower, format, GetTime, UnitName, UnitIsPlayer, UnitExists = strlower, string.format, GetTime, UnitName, UnitIsPlayer, UnitExists
local CreateFrame, print, ipairs, pairs, table_sort = CreateFrame, print, ipairs, pairs, table.sort

-- Таблицы цветов и рас
local CLASSES = {{
    [0] = "ffabd473", -- Охотник - зеленый
    [1] = "ff8788ee", -- Чернокнижник - фиолетовый
    [2] = "ffffffff", -- Жрец - белый
    [3] = "fff58cba", -- Паладин - розовый
    [4] = "ff3fc7eb", -- Маг - голубой
    [5] = "fffff569", -- Разбойник - желтый
    [6] = "ffff7d0a", -- Друид - оранжевый
    [7] = "ff0070de", -- Шаман - синий
    [8] = "ffc79c6e", -- Воин - коричневый
    [9] = "ffc41f3b", -- Рыцарь смерти - красный
    [10] = "ff00ff00", -- Светло-зеленый (для выделений и гильдий)
    [11] = "ffff1919", -- Красный (Орда)
    [12] = "ff3399ff", -- Синий (Альянс)
    [13] = "ff00bfff"  -- Голубой (основной текст)
}}

local RACES = {{
    [0] = "Человек",
    [1] = "Дворф",
    [2] = "Ночной эльф",
    [3] = "Гном",
    [4] = "Дреней",
    [5] = "Орк",
    [6] = "Нежить",
    [7] = "Таурен",
    [8] = "Тролль",
    [9] = "Кровавый эльф"
}}

-- Метаданные базы
local DB_SOURCE = "{os.path.basename(db_path)}"
local DB_BUILD_TIME = "{build_time}"
local DB_TOTAL_CHARS = {total_records}
local DB_TOTAL_ACCOUNTS = {total_accounts}
local DB_TOTAL_GUILDS = {total_guilds}

-- База данных персонажей (встроенная)
local DB = {db_code}

-- Настройки (автопоиск по умолчанию ВЫКЛЮЧЕН)
local EzInfo_Config = {{
    autoTargetMode = false    -- автопоиск для /qq ВЫКЛЮЧЕН по умолчанию
}}

-- Оптимизированные функции
local function TEXT_COLOR(TEXT, INDEX)
    return ("|c%s%s|r"):format(CLASSES[INDEX], TEXT)
end

local function TEXT_CHARACTER(CHARACTER)
    local name = TEXT_COLOR(CHARACTER[1], CHARACTER[6]) -- Имя цветом класса (индекс 6)
    local level = TEXT_COLOR("["..CHARACTER[2].."]", 13) -- Уровень голубым
    local gs = "|c"..CHARACTER[7]..CHARACTER[3].." GS|r" -- GS цветом из базы
    local raceColor = CHARACTER[4] >= 5 and 11 or 12 -- Цвет фракции для расы
    local race = TEXT_COLOR(RACES[CHARACTER[4]] or "Неизвестно", raceColor) -- Раса цветом фракции
    local guild = CHARACTER[5] ~= "" and TEXT_COLOR(" <"..CHARACTER[5]..">", 10) or "" -- Гильдия светло-зеленым
    print(level.." |Hplayer:"..CHARACTER[1].."|h"..name.."|h "..gs.." "..race..guild)
end

local function PRINT_ARRAY(CHARACTERS, ACCOUNT)
    local COUNTER = 0
    print(TEXT_COLOR("Все персонажи аккаунта: ", 13) .. TEXT_COLOR(ACCOUNT, 10))
    for i = 1, #CHARACTERS do
        TEXT_CHARACTER(CHARACTERS[i])
        COUNTER = COUNTER + 1
    end
    print(TEXT_COLOR("Найдено персонажей: ", 13) .. TEXT_COLOR(COUNTER, 10))
end

local function FIND_CHARACTER_DATA(FIND)
    local nameLower = strlower(FIND)
    -- Сначала ищем по имени персонажа
    for account, characters in pairs(DB) do
        for i = 1, #characters do
            local character = characters[i]
            if character and character[1] and strlower(character[1]) == nameLower then
                return characters, account
            end
        end
    end
    -- Если не нашли по имени персонажа, ищем по имени аккаунта
    for account, characters in pairs(DB) do
        if strlower(account) == nameLower then
            return characters, account
        end
    end
    return nil
end

-- Основная функция поиска
local function FIND_CHARACTER(FIND)
    local startTime = GetTime()
    local characters, account = FIND_CHARACTER_DATA(FIND)
    return characters, account, GetTime() - startTime
end

-- Автопоиск по цели (работает только если включен)
local function SEARCH_TARGET()
    if not EzInfo_Config.autoTargetMode then return end

    local targetName = UnitName("target")
    if not targetName or not UnitIsPlayer("target") then return end

    local characters, account, searchTime = FIND_CHARACTER(targetName)
    if characters and account then
        print("|cFFc200c2АВТОПОИСК:|r " .. TEXT_COLOR(targetName, 10))
        PRINT_ARRAY(characters, account)
        print(TEXT_COLOR("Автопоиск выполнен за ", 13) .. TEXT_COLOR(format("%.3f", searchTime) .. " сек", 10))
    else
        print("|cFFc200c2АВТОПОИСК:|r " .. TEXT_COLOR(targetName, 10) .. TEXT_COLOR(" не найден", 13))
    end
end

-- Обработчик команд
SLASH_EZINFO1, SLASH_EZINFO2 = '/qq', '/ezinfo'

SlashCmdList["EZINFO"] = function(MESSAGE)
    -- Обработка команд автопоиска
    if MESSAGE == "on" then
        EzInfo_Config.autoTargetMode = true
        print(TEXT_COLOR("EzInfo: Автопоиск ", 13) .. TEXT_COLOR("ВКЛЮЧЕН", 10))
        if UnitExists("target") and UnitIsPlayer("target") then
            SEARCH_TARGET()
        end
        return
    elseif MESSAGE == "off" then
        EzInfo_Config.autoTargetMode = false
        print(TEXT_COLOR("EzInfo: Автопоиск ", 13) .. TEXT_COLOR("ВЫКЛЮЧЕН", 10))
        return
    elseif MESSAGE == "status" then
        local status = EzInfo_Config.autoTargetMode and TEXT_COLOR("ВКЛЮЧЕН", 10) or TEXT_COLOR("ВЫКЛЮЧЕН", 10)
        print(TEXT_COLOR("EzInfo: Автопоиск: ", 13) .. status)
        return
    elseif MESSAGE == "memory" then
        local memory = collectgarbage("count")
        print(TEXT_COLOR("Использование памяти LUA: ", 13) .. TEXT_COLOR(format("%.2f MB", memory/1024), 10))
        return
    end

    -- Поиск персонажа
    local FIND = MESSAGE ~= "" and MESSAGE or UnitName("target")
    if not FIND or FIND == "" then
        -- Показываем справку и информацию о базе
        print(TEXT_COLOR("EzInfo", 11))
        print(TEXT_COLOR("База персонажей", 10))
        print(TEXT_COLOR("На основе программ от Border, Jyn (janeblower)", 13))
        print(TEXT_COLOR("Использование: /qq <имя_персонажа>", 10))
        print(TEXT_COLOR("Или выберите цель и введите /qq", 10))
        print(TEXT_COLOR("Так же: /qq <имя_аккаунта>", 10))
        print(TEXT_COLOR("Дополнительные команды:", 10))
        print(TEXT_COLOR("/qq on - включить автопоиск по цели", 12))
        print(TEXT_COLOR("/qq off - выключить автопоиск по цели", 12))
        print(TEXT_COLOR("/qq status - показать статус автопоиска", 12))
        print(TEXT_COLOR("/qq memory - показать использование памяти", 12))
        -- Показываем информацию о базе
        print(TEXT_COLOR("Файл базы: " .. DB_SOURCE .. " от " .. DB_BUILD_TIME, 13))
        print(TEXT_COLOR("Персонажи: " .. DB_TOTAL_CHARS .. ", Аккаунты: " .. DB_TOTAL_ACCOUNTS .. ", Гильдии: " .. DB_TOTAL_GUILDS, 10))
        return
    end

    print(TEXT_COLOR("EzInfo", 10))

    local characters, account, searchTime = FIND_CHARACTER(FIND)
    print(TEXT_COLOR("Поиск выполнен за ", 13) .. TEXT_COLOR(format("%.3f", searchTime) .. " сек", 10))

    if characters and account then
        PRINT_ARRAY(characters, account)
    else
        print(TEXT_COLOR("Персонаж или аккаунт '", 13)..TEXT_COLOR(FIND, 10)..TEXT_COLOR("' не найден", 13))
    end
end

-- Обработчик смены цели (автопоиск работает только если включен)
local frame = CreateFrame("Frame")
frame:RegisterEvent("PLAYER_TARGET_CHANGED")
frame:SetScript("OnEvent", SEARCH_TARGET)

-- Сообщение о загрузке аддона
local loadFrame = CreateFrame("Frame")
loadFrame:RegisterEvent("ADDON_LOADED")
loadFrame:SetScript("OnEvent", function(self, event, addonName)
    if addonName == "EzInfo" then
        print(TEXT_COLOR("EzInfo загружен. ", 13) .. TEXT_COLOR("/qq", 10) .. TEXT_COLOR(" для поиска", 13))
        print(TEXT_COLOR("Автопоиск по цели: ", 13) ..
              (EzInfo_Config.autoTargetMode and TEXT_COLOR("ВКЛЮЧЕН", 10) or TEXT_COLOR("ВЫКЛЮЧЕН", 11)))
        -- Показываем информацию о базе
        print(TEXT_COLOR("Файл базы: " .. DB_SOURCE .. " от " .. DB_BUILD_TIME, 13))
        print(TEXT_COLOR("Персонажи: " .. DB_TOTAL_CHARS .. ", Аккаунты: " .. DB_TOTAL_ACCOUNTS .. ", Гильдии: " .. DB_TOTAL_GUILDS, 10))
    end
end)
'''

    # Сохраняем основной файл аддона в текущей директории
    with open('EzInfo.lua', 'w', encoding='utf-8') as f:
        f.write(main_code)
    log_message(f"Файл аддона сохранен как EzInfo.lua")
    
    # Копируем файл в папку интерфейса WoW, если путь указан
    if WoW_InterfaceFolderPath and WoW_InterfaceFolderPath.strip():
        try:
            destination_path = os.path.join(WoW_InterfaceFolderPath, 'EzInfo.lua')
            shutil.copy2('EzInfo.lua', destination_path)
            log_message(f"Файл аддона скопирован в: {destination_path}")
        except Exception as e:
            log_message(f"Ошибка при копировании файла в {WoW_InterfaceFolderPath}: {e}")
    
    log_message(f"Генерация аддона завершена!")
    log_message(f"Статистика базы:")
    log_message(f"  Персонажи: {total_records}")
    log_message(f"  Аккаунты: {total_accounts}")
    log_message(f"  Гильдии: {total_guilds}")
    
    if unknown_classes:
        log_message(f"Предупреждение: Обнаружены неизвестные классы: {unknown_classes}")
    
    conn.close()

if __name__ == "__main__":
    try:
        setup_logging()
        generate_addon_with_database()
    except Exception as e:
        log_message(f"Произошла ошибка: {e}")
        import traceback
        log_message(f"Трассировка ошибки: {traceback.format_exc()}")
    finally:
        close_logging()
