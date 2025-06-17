import logging
import time
import schedule

from db import create_tables
from ch import get_marine_and_tide_data, get_fishing_data

def job():
    logging.info("========== データ収集ジョブ開始 ==========")
    try:
        get_marine_and_tide_data()
        get_fishing_data()
        logging.info("========== データ収集ジョブ完了 ==========")
    except Exception as e:
        logging.error(f"ジョブ実行中にエラー発生: {e}", exc_info=True)

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )
    logging.info("アプリケーションを開始します。")

    # テーブル準備
    create_tables()

    # テスト時に即座に一度ジョブを実行したい場合は次の１行を有効化
    job()

    # スケジュール：月・水・金 00:00
    schedule.every().monday.at("00:00").do(job)
    schedule.every().wednesday.at("00:00").do(job)
    schedule.every().friday.at("00:00").do(job)
    schedule.every().tuesday.at("00:00").do(job)
    schedule.every().thursday.at("00:00").do(job)
    schedule.every().saturday.at("00:00").do(job)
    schedule.every().sunday.at("00:00").do(job)
    schedule.every().monday.at("12:00").do(job)
    schedule.every().wednesday.at("12:00").do(job)
    schedule.every().friday.at("12:00").do(job)
    schedule.every().tuesday.at("12:00").do(job)
    schedule.every().thursday.at("12:00").do(job)
    schedule.every().saturday.at("12:00").do(job)
    schedule.every().sunday.at("12:00").do(job)
    logging.info("スケジュールを設定しました")

    try:
        while True:
            schedule.run_pending()
            time.sleep(600)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt でプロセスを終了します。")
    except Exception as e:
        logging.error(f"メインループで予期せぬエラー: {e}", exc_info=True)

if __name__ == "__main__":
    main()
