import logging
import pandas as pd
import joblib
from datetime import datetime
from analyzer import parse_query, is_fishing_related # analyzer.pyから便利な関数を再利用
import numpy as np

def predict_hottest_fish(area, target_date):
    """訓練済みのAIモデルを読み込んで、釣果を予測する"""
    logging.info(f"AIモデルによる予測を開始します (エリア: {area}, 日付: {target_date.strftime('%Y-%m-%d')})")
    try:
        # 保存したモデルとエンコーダーを読み込む
        model = joblib.load('fish_predictor.joblib')
        encoders = joblib.load('encoders.joblib')
        model_features = joblib.load('model_features.joblib')
    except FileNotFoundError:
        logging.error("モデルファイルが見つかりません。先に model_trainer.py を実行してください。")
        return None, None

    # --- 予測用のデータを作成 ---
    # ここでは、予測日の気象・潮汐データを取得する処理を簡略化しています。
    # 本来は、未来の日付の天気予報や潮汐情報をAPIから取得する機能が必要です。
    # プロトタイプとして、ダミーデータで代用します。
    data = {
        'prefecture': [area],
        'min_temp': [15.0],
        'max_temp': [22.0],
        'precipitation': [10.0],
        'wave_height': [1.5],
        'weekday': [target_date.weekday()],
        'month': [target_date.month],
        'tide_name': ['大潮']
    }
    input_df = pd.DataFrame(data)

    # --- 訓練時と同じ形式に前処理 ---
    for col, encoder in encoders.items():
        if col in input_df.columns and col != 'fish_name':
            # 新しいラベル（例: 未知の都道府県）は未知として扱う
            input_df[col] = input_df[col].apply(lambda x: x if x in encoder.classes_ else 'unknown')
            encoder_classes = encoder.classes_.tolist()
            if 'unknown' not in encoder_classes:
                encoder.classes_ = np.append(encoder.classes_, 'unknown')
            input_df[col] = encoder.transform(input_df[col])

    # 訓練時の特徴量カラム順に合わせる
    input_df = input_df.reindex(columns=model_features).fillna(0)

    # --- 予測の実行 ---
    prediction_id = model.predict(input_df)[0]
    
    # 予測IDを元の魚名に変換
    hottest_fish = encoders['fish_name'].inverse_transform([prediction_id])[0]

    # (簡易的に)その魚がよく釣れる船宿をDBから取得
    # この部分は、より精度の高い推薦ロジックに改善の余地あり
    try:
        with sqlite3.connect('fishing_data.db') as conn:
            query = "SELECT DISTINCT shop_name FROM fishing_results WHERE fish_name = ? AND prefecture = ? LIMIT 5"
            ships_df = pd.read_sql_query(query, conn, params=(hottest_fish, area))
            recommended_ships = ships_df['shop_name'].tolist()
    except Exception:
        recommended_ships = []

    return hottest_fish, recommended_ships


def main():
    """AI予測を対話形式で実行する"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    print("\n" + "="*50)
    print("🎣 釣果予測AI (機械学習版) �")
    print("      (例: 「来週の三崎で釣れる魚は？」と聞いてみてください)")
    print("      (終了するには 'q' と入力してください)")
    print("="*50)

    while True:
        try:
            user_query = input("\nあなた: ")
            if user_query.lower() == 'q':
                print("AIを終了します。ご利用ありがとうございました。")
                break

            if not is_fishing_related(user_query):
                print("AI: 申し訳ありませんが、釣りの質問にのみお答えできます。")
                continue

            intent = parse_query(user_query)
            area = intent.get('area')
            target_date = intent.get('date')

            if not area:
                print("AI: すみません、地名（例：三崎、東京湾）が聞き取れませんでした。もう一度お願いします。")
                continue
            
            hottest_fish, ships = predict_hottest_fish(area, target_date)

            print("-"*50)
            print(f"AI: 「{target_date.strftime('%Y年%m月%d日')}頃の「{area}」エリアの釣果ですね。AIによる予測結果はこちらです。")
            print("-"*50)

            if hottest_fish:
                print(f"✅ 予測: 「{hottest_fish}」が釣れる可能性が高いです。")
                if ships:
                    print("\n💡 関連する船宿リスト:")
                    for ship in ships:
                        print(f"  - {ship}")
            else:
                print("❌ 予測: 申し訳ありません。予測に必要なモデルが見つからないか、エラーが発生しました。")
            print("-"*50)

        except Exception as e:
            logging.error(f"予期せぬエラーが発生しました: {e}", exc_info=True)
            print("AI: 予期せぬエラーが発生しました。もう一度試してください。")

if __name__ == "__main__":
    main()
