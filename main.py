import logging
import time
from db import create_tables
from ch import get_marine_and_tide_data, get_fishing_data

def main():
    """メイン処理"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    logging.info("アプリケーションを開始します。")

    # 最初にデータベースとテーブルを準備
    create_tables()
    
    # 無限ループで定期的にデータを収集
    while True:
        try:
            logging.info("========== データ収集サイクルを開始 ==========")
            
            # 1. 気象・潮位データを取得
            get_marine_and_tide_data()
            
            # 2. 釣果データを取得
            get_fishing_data()
            
            logging.info("========== データ収集サイクルが完了。次の実行まで待機します。 ==========")
            
            # 8時間（28800秒）待機
            time.sleep(28800)
            
        except KeyboardInterrupt:
            logging.info("プログラムを手動で終了します。")
            break
        except Exception as e:
            logging.error(f"メインループで予期せぬエラーが発生しました: {e}")
            logging.info("60秒後に再試行します。")
            time.sleep(60)

if __name__ == "__main__":
    main()