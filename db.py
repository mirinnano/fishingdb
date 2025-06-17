import sqlite3
import logging
import json
from datetime import datetime

def get_connection():
    return sqlite3.connect('fishing_data.db')

def create_tables():
    with get_connection() as conn:
        # 漁獲結果テーブル
        conn.execute('''CREATE TABLE IF NOT EXISTS fishing_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        location TEXT NOT NULL,
                        fish_type TEXT NOT NULL,
                        quantity INTEGER,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
        
        # 気象データテーブル
        conn.execute('''CREATE TABLE IF NOT EXISTS weather_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL UNIQUE,
                        min_temperature REAL,
                        max_temperature REAL,
                        precipitation_pct REAL,
                        wave_height REAL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
        
        # 潮位データテーブル（新規追加）
        conn.execute('''CREATE TABLE IF NOT EXISTS tide_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL UNIQUE,
                        high_tide_1 TIME,
                        high_tide_1_height REAL,
                        high_tide_2 TIME,
                        high_tide_2_height REAL,
                        low_tide_1 TIME,
                        low_tide_1_height REAL,
                        low_tide_2 TIME,
                        low_tide_2_height REAL,
                        sunrise TIME,
                        sunset TIME,
                        moonrise TIME,
                        moonset TIME,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
        
        logging.info("テーブルが正常に作成されました")

def insert_weather_data(data):
    # 気象データ挿入処理（潮位データ対応版）
    with get_connection() as conn:
        conn.execute('''INSERT INTO weather_data 
                        (date, min_temperature, max_temperature, precipitation_pct, wave_height)
                        VALUES (?, ?, ?, ?, ?)''',
                    (data['date'], 
                     data['temperature'].get('min'),
                     data['temperature'].get('max'),
                     data.get('precipitation'),
                     data.get('wave_height')))
        
        # 潮位データ挿入（新規追加）
        if 'tide' in data:
            conn.execute('''INSERT INTO tide_data (
                            date, high_tide_1, high_tide_1_height, high_tide_2, high_tide_2_height,
                            low_tide_1, low_tide_1_height, low_tide_2, low_tide_2_height,
                            sunrise, sunset, moonrise, moonset)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                        (data['date'],
                         data['tide'].get('high_tide_1_time'),
                         data['tide'].get('high_tide_1_height'),
                         data['tide'].get('high_tide_2_time'),
                         data['tide'].get('high_tide_2_height'),
                         data['tide'].get('low_tide_1_time'),
                         data['tide'].get('low_tide_1_height'),
                         data['tide'].get('low_tide_2_time'),
                         data['tide'].get('low_tide_2_height'),
                         data['tide'].get('sunrise'),
                         data['tide'].get('sunset'),
                         data['tide'].get('moonrise'),
                         data['tide'].get('moonset')))
        conn.commit()

    # JSON保存処理（潮位データ対応版）
    json_entry = {
        "date": data['date'],
        "weather_metrics": {
            "min_temperature": data['temperature'].get('min'),
            "max_temperature": data['temperature'].get('max'),
            "precipitation_pct": data.get('precipitation'),
            "wave_height": data.get('wave_height')
        },
        "tide_metrics": data.get('tide', {}),
        "collection_time": datetime.now().isoformat()
    }

    try:
        with open('weather_data.json', 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []

    existing_data.append(json_entry)
    
    with open('weather_data.json', 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)

def check_duplicate(date, table_name):
    with get_connection() as conn:
        cursor = conn.execute(f'SELECT COUNT(*) FROM {table_name} WHERE date = ?', (date,))
        return cursor.fetchone()[0] > 0

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_tables()
