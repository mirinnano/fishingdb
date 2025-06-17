import requests
import logging
from datetime import datetime
import re
from db import insert_weather_data, check_duplicate

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def parse_wave_height(text):
    """波高情報のパース（全角数字＆複数時刻対応）"""
    match = re.search(r'([0-9２-９]+[．.]?\s*[0-9２-９]*)\s*メートル', text)
    if match:
        wave_str = match.group(1).translate(
            str.maketrans('１２３４５６７８９０．', '1234567890.')
        )
        return float(wave_str)
    return 0.0

def get_tide_data():
    """潮位データの取得（新規追加）"""
    today = datetime.now()
    url = f'https://api.tide736.net/get_tide.php?pc=14&hc=16&rg=week&yr={today.year}&mn={today.month}&dy={today.day}'
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        tide_data = response.json()
        
        if tide_data.get('status') != 1:
            raise ValueError(f"APIエラー: {tide_data.get('message', '不明なエラー')}")
            
        if 'chart' not in tide_data:
            raise ValueError("APIレスポンスにchartデータが含まれていません")

        current_date = today.strftime('%Y-%m-%d')
        chart_data = tide_data['chart'].get(current_date)
        
        if not chart_data:
            available_dates = ', '.join(tide_data['chart'].keys())
            raise ValueError(f"{current_date}のデータがありません。利用可能な日付: {available_dates}")
            
        # 日付情報をobservation_dataに追加（全処理の最初に設定）
        observation_data['date'] = current_date
        
        tide_info = {
            'high_tide_1_time': chart_data['flood'][0]['time'],
            'high_tide_1_height': chart_data['flood'][0]['cm'],
            'high_tide_2_time': chart_data['flood'][1]['time'] if len(chart_data['flood']) > 1 else None,
            'high_tide_2_height': chart_data['flood'][1]['cm'] if len(chart_data['flood']) > 1 else None,
            'low_tide_1_time': chart_data['edd'][0]['time'],
            'low_tide_1_height': chart_data['edd'][0]['cm'],
            'low_tide_2_time': chart_data['edd'][1]['time'] if len(chart_data['edd']) > 1 else None,
            'low_tide_2_height': chart_data['edd'][1]['cm'] if len(chart_data['edd']) > 1 else None,
            'sunrise': chart_data['sun']['rise'],
            'sunset': chart_data['sun']['set'],
            'moonrise': re.search(r'\d{1,2}:\d{2}', chart_data['moon']['rise']).group(),
            'moonset': re.search(r'\d{1,2}:\d{2}', chart_data['moon']['set']).group()
        }
        return tide_info
    except Exception as e:
        logging.error(f"潮位データの取得に失敗しました: {str(e)}")
        return None

def get_marine_weather():
    """海上気象データの取得（潮位データ統合版）"""
    weather_url = 'https://www.jma.go.jp/bosai/forecast/data/forecast/140000.json' 
    observation_data = {}
    
    try:
        # 気象データ取得
        response = requests.get(weather_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        forecast_data = data[0]
        time_series = forecast_data['timeSeries']
        current_date = datetime.now().strftime('%Y-%m-%d')

        eastern_area = next((area for item in time_series 
                           for area in item['areas'] 
                           if area['area']['code'] == '140010'), None)

        if eastern_area:
            # 既存の気象データ処理
            temp_series = next((item for item in time_series if 'temps' in item['areas'][0]), None)
            if temp_series:
                yokohama = next(area for area in temp_series['areas'] if area['area']['code'] == '46106')
                observation_data['temperature'] = {
                    'min': float(yokohama['temps'][2]),
                    'max': float(yokohama['temps'][1])
                }

            if 'pops' in eastern_area:
                valid_pops = [float(p) for p in eastern_area['pops'] if p != '--']
                observation_data['precipitation'] = max(valid_pops) if valid_pops else 0.0

            if 'waves' in eastern_area:
                wave_text = eastern_area['waves'][0]
                observation_data['wave_height'] = parse_wave_height(wave_text)

            # 潮位データ取得（新規追加）
            tide_data = get_tide_data()
            if tide_data:
                observation_data['tide'] = tide_data

        if not check_duplicate(current_date, 'weather_data'):
            insert_weather_data(observation_data)
            logging.info(f"{current_date}の気象・潮位データを登録しました")
        else:
            logging.warning(f"{current_date}のデータは既に存在します")
            
        return observation_data
        
    except Exception as e:
        logging.error(f"データ取得に失敗しました: {str(e)}")
        return None

if __name__ == "__main__":
    from db import create_tables
    create_tables()
    get_marine_weather()
