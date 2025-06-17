from ch import get_marine_weather
from db import create_tables
import logging
import time

def main():
    logging.basicConfig(level=logging.INFO)
    create_tables()
    
    while True:
        try:
            weather_data = get_marine_weather()
            if weather_data:
                logging.info(f"データ取得に成功しました: {weather_data}")
            
            # 8時間間隔で実行（28800秒 = 8時間）
            time.sleep(28800)
            
        except KeyboardInterrupt:
            logging.info("プログラムを終了します")
            break
        except Exception as e:
            logging.error(f"予期せぬエラーが発生しました: {str(e)}")
            time.sleep(60)  # エラー後1分待機して再試行

if __name__ == "__main__":
    main()
