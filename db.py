# db.py
# データベースの構造定義と、データの挿入・重複チェックを担当します。
# 潮汐情報をJSONと平坦化テーブルの両方で保存し、分析のしやすさを向上させます。
import sqlite3
import logging
import json

def get_connection():
    """データベースへの接続を取得する"""
    return sqlite3.connect('fishing_data.db')

def create_tables():
    """必要なテーブルをすべて作成する"""
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

        # 3. 【AI学習用】平坦化テーブル
        conn.execute('''
        CREATE TABLE IF NOT EXISTS daily_conditions_flat (
            date TEXT PRIMARY KEY,
            min_temp REAL,
            max_temp REAL,
            precipitation REAL,
            wave_height REAL,
            tide_name TEXT,
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

def insert_daily_conditions(data):
    """従来の JSON 格納テーブルと、AI用の平坦化テーブルの両方にデータを挿入する"""
    with get_connection() as conn:
        # --- 従来のJSONテーブルへの挿入 ---
        cursor = conn.cursor()
        tide_json_string = json.dumps(data.get('tide', {}), ensure_ascii=False) if data.get('tide') else None
        w = data.get('weather', {})
        cursor.execute('''
        INSERT OR IGNORE INTO daily_conditions (
            date, min_temp, max_temp, precipitation, wave_height, tide_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            data.get('date'), w.get('min_temp'), w.get('max_temp'),
            w.get('precipitation'), w.get('wave_height'), tide_json_string
        ))
        if cursor.rowcount > 0:
            logging.info(f"[{data.get('date')}] のJSONデータを登録しました。")
        else:
            logging.info(f"[{data.get('date')}] のJSONデータは既に登録済みです。")

        # --- AI用平坦化テーブルへの挿入 ---
        if data.get('tide'):
            tide = data['tide']
            ht = tide.get('high_tides', [])
            lt = tide.get('low_tides', [])
            sun = tide.get('sun', {})
            mn = tide.get('moon', {})
            
            cursor.execute('''
            INSERT OR IGNORE INTO daily_conditions_flat (
                date, min_temp, max_temp, precipitation, wave_height, tide_name,
                high_tide_1_time, high_tide_1_height, high_tide_2_time, high_tide_2_height,
                low_tide_1_time, low_tide_1_height, low_tide_2_time, low_tide_2_height,
                sun_rise, sun_set, moon_age, moon_rise, moon_set
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['date'], w.get('min_temp'), w.get('max_temp'),
                w.get('precipitation'), w.get('wave_height'), tide.get('tide_name'),
                ht[0].get('time') if len(ht) > 0 else None,
                ht[0].get('height_cm') if len(ht) > 0 else None,
                ht[1].get('time') if len(ht) > 1 else None,
                ht[1].get('height_cm') if len(ht) > 1 else None,
                lt[0].get('time') if len(lt) > 0 else None,
                lt[0].get('height_cm') if len(lt) > 0 else None,
                lt[1].get('time') if len(lt) > 1 else None,
                lt[1].get('height_cm') if len(lt) > 1 else None,
                sun.get('rise'), sun.get('set'),
                float(mn.get('age')) if mn.get('age') else None,
                mn.get('rise'), mn.get('set')
            ))
            if cursor.rowcount > 0:
                logging.info(f"[{data.get('date')}] の平坦化データを登録しました。")

def insert_fishing_results(results_list):
    """釣果データをDBに挿入"""
    with get_connection() as conn:
        inserted_count = 0
        for result in results_list:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT OR IGNORE INTO fishing_results (report_date, prefecture, shop_name, fish_name, details)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                result['report_date'], result['prefecture'], result['shop_name'],
                result['fish_name'], result['details']
            ))
            if cursor.rowcount > 0:
                inserted_count += 1
                logging.debug(f"  -> [釣果登録]: {json.dumps(result, ensure_ascii=False)}")
        
        conn.commit()
        if inserted_count > 0:
            logging.info(f"{inserted_count}件の新しい釣果データをDBに保存しました。")
