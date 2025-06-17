import requests
import logging
from datetime import datetime, timedelta
import re
from bs4 import BeautifulSoup
import json

# db.pyから必要な関数をインポート
from db import insert_daily_conditions, insert_fishing_results

# --- 初期設定 ---
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/91.0.4472.124 Safari/537.36'
    )
}

# --- 気象・潮位データ取得 ---
def get_marine_and_tide_data():
    """神奈川県の気象データと潮位データを取得し、統合してDBに保存する"""
    logging.info("気象・潮位データの取得を開始...")
    try:
        # 1. 気象庁から気象データを取得
        weather_api_url = 'https://www.jma.go.jp/bosai/forecast/data/forecast/140000.json'
        weather_res = requests.get(weather_api_url, headers=HEADERS, timeout=10)
        weather_res.raise_for_status()
        weather_json = weather_res.json()

        # 2. tide736から潮位データを取得
        today = datetime.now()
        tide_api_url = (
            f'https://api.tide736.net/get_tide.php?pc=14&hc=16&rg=week'
            f'&yr={today.year}&mn={today.month}&dy={today.day}'
        )
        tide_res = requests.get(tide_api_url, headers=HEADERS, timeout=10)
        tide_res.raise_for_status()

        try:
            tide_json = tide_res.json()
        except json.JSONDecodeError:
            logging.error("Tide APIの応答がJSON形式ではありません。レスポンス内容:")
            logging.error(tide_res.text)
            tide_json = {}

        # --- データの整形 ---
        date_for_db = today.strftime('%Y-%m-%d')

        # 気象データの整形
        weather_data = {
            'wave_height': 0.0,
            'precipitation': 0.0,
            'max_temp': 0.0,
            'min_temp': 0.0
        }

        if not weather_json or not isinstance(weather_json, list):
            logging.warning("気象庁APIから空または不正なデータが返されました。")
        else:
            time_series = weather_json[0].get('timeSeries', [])

            # ── 波浪情報 (waves) は time_series[0] にある
            if len(time_series) > 0:
                waves_areas = time_series[0].get('areas', [])
                eastern = next(
                    (area for area in waves_areas
                     if area.get('area', {}).get('code') == '140010'),
                    {}
                )
                waves = eastern.get('waves', [])
                if waves:
                    m = re.search(r'(\d+(\.\d+)?)', waves[0])
                    weather_data['wave_height'] = float(m.group(1)) if m else 0.0

            # ── 降水確率 (pops) は time_series[1] にある
            if len(time_series) > 1:
                pops_areas = time_series[1].get('areas', [])
                eastern = next(
                    (area for area in pops_areas
                     if area.get('area', {}).get('code') == '140010'),
                    {}
                )
                pops = eastern.get('pops', [])
                valid = [float(p) for p in pops if p.isdigit()]
                weather_data['precipitation'] = max(valid) if valid else 0.0

            # ── 気温情報 (temps) は time_series[2] にある
            if len(time_series) > 2:
                temps_areas = time_series[2].get('areas', [])
                yokohama = next(
                    (area for area in temps_areas
                     if area.get('area', {}).get('code') == '46106'),
                    {}
                )
                temps = yokohama.get('temps', [])
                if len(temps) >= 2:
                    weather_data['min_temp'] = float(temps[0])
                    weather_data['max_temp'] = float(temps[1])

        # 潮位データの整形
        tide_data = {}
        chart_dict = tide_json.get('tide', {}).get('chart', {})
        chart_data = chart_dict.get(date_for_db)

        if chart_data:
            logging.info(f"Tide APIから日付キー '{date_for_db}' のデータを取得しました。")

            tide_data['tide_name'] = chart_data.get('moon', {}).get('title')
            tide_data['high_tides'] = [
                {"time": ht.get('time'), "height_cm": ht.get('cm')}
                for ht in chart_data.get('flood', [])
            ]
            tide_data['low_tides'] = [
                {"time": lt.get('time'), "height_cm": lt.get('cm')}
                for lt in chart_data.get('edd', [])
            ]
            tide_data['sun'] = {
                "rise": chart_data.get('sun', {}).get('rise'),
                "set": chart_data.get('sun', {}).get('set')
            }
            tide_data['moon'] = {
                "age": chart_data.get('moon', {}).get('age'),
                "rise": re.search(
                    r'\d{1,2}:\d{2}',
                    chart_data.get('moon', {}).get('rise', '')
                ).group()
                if re.search(
                    r'\d{1,2}:\d{2}',
                    chart_data.get('moon', {}).get('rise', '')
                )
                else None,
                "set": re.search(
                    r'\d{1,2}:\d{2}',
                    chart_data.get('moon', {}).get('set', '')
                ).group()
                if re.search(
                    r'\d{1,2}:\d{2}',
                    chart_data.get('moon', {}).get('set', '')
                )
                else None
            }
        else:
            logging.warning(f"Tide APIから本日の潮位データ({date_for_db})を取得できませんでした。")
            if chart_dict:
                logging.debug(f"利用可能な日付キー: {list(chart_dict.keys())}")

        combined_data = {
            "date": date_for_db,
            "weather": weather_data,
            "tide": tide_data
        }
        insert_daily_conditions(combined_data)

    except requests.exceptions.RequestException as e:
        logging.error(f"HTTPリクエストエラー: {e}")
    except Exception as e:
        logging.error("データ解析中に予期せぬエラーが発生しました。", exc_info=True)

# --- 釣果データ取得 ---
def get_fishing_data():
    """釣割から神奈川・千葉・東京の釣果データを取得し、DBに保存する"""
    logging.info("釣果データの取得を開始...")
    
    target_prefs = {
        "神奈川": "14",
        "千葉": "12",
        "東京": "13",
    }
    
    all_results = []

    for pref_name, area_id in target_prefs.items():
        try:
            url = f"https://www.chowari.jp/catcharea/?area={area_id}"
            logging.info(f"[{pref_name}] データを取得中: {url}")
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            catch_cards = soup.select('li.catch_item')
            logging.info(f"[{pref_name}] {len(catch_cards)}件の釣果情報を発見。")
            
            for card in catch_cards:
                shop_name_tag = card.select_one('header h2')
                shop_name = shop_name_tag.text.strip() if shop_name_tag else "N/A"

                date_tag = card.select_one('.catch_item_date')
                date_text = date_tag.text.strip() if date_tag else "N/A"

                match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
                if not match:
                    continue
                
                year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                report_date = f"{year:04d}-{month:02d}-{day:02d}"

                fish_rows = card.select('.catch_item_fish tr')
                for row in fish_rows:
                    fish_name_tag = row.select_one('th')
                    
                    if not fish_name_tag:
                        continue

                    fish_name = re.sub(r'[（\(].*?[）\)]', '', fish_name_tag.text).strip()
                    if not fish_name:
                        continue
                        
                    details_tags = row.select('td')
                    details = ' '.join(td.text.strip() for td in details_tags).strip()
                    
                    all_results.append({
                        "report_date": report_date,
                        "prefecture": pref_name,
                        "shop_name": shop_name,
                        "fish_name": fish_name,
                        "details": details
                    })
        
        except requests.exceptions.RequestException as e:
            logging.error(f"[{pref_name}] の釣果取得に失敗: {e}")
            continue
        except Exception as e:
            logging.error(f"[{pref_name}] の解析中に予期せぬエラーが発生: {e}", exc_info=True)

    if all_results:
        insert_fishing_results(all_results)
    
    logging.info("釣果データの収集処理が完了しました。")