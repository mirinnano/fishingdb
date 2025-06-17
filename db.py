# db.py
import sqlite3
import logging
import json

# --- データベース接続 ---
def get_connection():
    return sqlite3.connect('fishing_data.db')

# --- テーブル作成 ---
def create_tables():
    with get_connection() as conn:
        logging.info("データベーステーブルを準備中...")

        # 1. 釣果結果テーブル
        conn.execute('''
        CREATE TABLE IF NOT EXISTS fishing_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            prefecture TEXT NOT NULL,
            shop_name TEXT NOT NULL,
            fish_name TEXT NOT NULL,
            details TEXT,
            crawled_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(report_date, shop_name, fish_name)
        )''')

        # 2. 従来テーブル（JSON格納）
        conn.execute('''
        CREATE TABLE IF NOT EXISTS daily_conditions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            min_temp REAL,
            max_temp REAL,
            precipitation REAL,
            wave_height REAL,
            tide_json TEXT,
            crawled_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')

        # 3. AI 向け平坦化テーブル
        conn.execute('''
        CREATE TABLE IF NOT EXISTS daily_conditions_flat (
            date TEXT PRIMARY KEY,
            min_temp REAL,
            max_temp REAL,
            precipitation REAL,
            wave_height REAL,
            high_tide_1_time TEXT,
            high_tide_1_height REAL,
            high_tide_2_time TEXT,
            high_tide_2_height REAL,
            low_tide_1_time TEXT,
            low_tide_1_height REAL,
            low_tide_2_time TEXT,
            low_tide_2_height REAL,
            sun_rise TEXT,
            sun_set TEXT,
            moon_age REAL,
            moon_rise TEXT,
            moon_set TEXT
        )''')

        logging.info("テーブルの準備が完了しました。")

# --- データ挿入 ---
def insert_daily_conditions(data):
    """従来の JSON 格納テーブルに挿入"""
    with get_connection() as conn:
        cursor = conn.cursor()
        tide_json_string = json.dumps(data['tide'], ensure_ascii=False) if data.get('tide') else None
        w = data['weather']
        cursor.execute('''
        INSERT OR IGNORE INTO daily_conditions (
            date, min_temp, max_temp, precipitation, wave_height, tide_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            data['date'],
            w.get('min_temp'),
            w.get('max_temp'),
            w.get('precipitation'),
            w.get('wave_height'),
            tide_json_string
        ))
        if cursor.rowcount:
            logging.info(f"[{data['date']}] の気象・潮位データを登録しました。")
        else:
            logging.info(f"[{data['date']}] の気象・潮位データは既に登録済みです。")

def insert_daily_conditions_flat(data):
    """平坦化テーブルに挿入"""
    with get_connection() as conn:
        cursor = conn.cursor()
        ht = data['tide']['high_tides']
        lt = data['tide']['low_tides']
        sun = data['tide']['sun']
        mn = data['tide']['moon']
        w = data['weather']
        cursor.execute('''
        INSERT OR IGNORE INTO daily_conditions_flat (
            date, min_temp, max_temp, precipitation, wave_height,
            high_tide_1_time, high_tide_1_height, high_tide_2_time, high_tide_2_height,
            low_tide_1_time, low_tide_1_height, low_tide_2_time, low_tide_2_height,
            sun_rise, sun_set, moon_age, moon_rise, moon_set
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['date'], w.get('min_temp'), w.get('max_temp'),
            w.get('precipitation'), w.get('wave_height'),
            ht[0]['time'] if len(ht)>0 else None,
            ht[0]['height_cm'] if len(ht)>0 else None,
            ht[1]['time'] if len(ht)>1 else None,
            ht[1]['height_cm'] if len(ht)>1 else None,
            lt[0]['time'] if len(lt)>0 else None,
            lt[0]['height_cm'] if len(lt)>0 else None,
            lt[1]['time'] if len(lt)>1 else None,
            lt[1]['height_cm'] if len(lt)>1 else None,
            sun.get('rise'), sun.get('set'),
            float(mn.get('age')) if mn.get('age') else None,
            mn.get('rise'), mn.get('set')
        ))
        if cursor.rowcount:
            logging.info(f"[{data['date']}] の平坦化データを登録しました。")
        else:
            logging.info(f"[{data['date']}] の平坦化データは既に登録済みです。")

def insert_fishing_results(results_list):
    """釣果データをDBに挿入"""
    with get_connection() as conn:
        inserted = 0
        for r in results_list:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT OR IGNORE INTO fishing_results
              (report_date, prefecture, shop_name, fish_name, details)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                r['report_date'], r['prefecture'],
                r['shop_name'], r['fish_name'], r['details']
            ))
            if cursor.rowcount:
                inserted += 1
        conn.commit()
        logging.info(f"{inserted} 件の新しい釣果データを保存しました。")
