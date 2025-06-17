# model_trainer.py
import sqlite3
import pandas as pd
import joblib
import logging
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, accuracy_score
import numpy as np

def get_connection():
    return sqlite3.connect('fishing_data.db')

def time_to_hours(time_str):
    """ 'HH:MM'形式の文字列を時間に変換する (例: '07:30' -> 7.5) """
    if not isinstance(time_str, str):
        return None
    try:
        h, m = map(int, time_str.split(':'))
        return h + m / 60.0
    except (ValueError, AttributeError):
        return None

def prepare_data():
    logging.info("データの前処理と特徴量エンジニアリングを開始します...")
    with get_connection() as conn:
        query = "SELECT r.prefecture, r.fish_name, c.* FROM fishing_results r LEFT JOIN daily_conditions_flat c ON r.report_date = c.date WHERE c.date IS NOT NULL"
        df = pd.read_sql_query(query, conn)

    if df.empty:
        logging.warning("訓練データがありません。")
        return None, None

    fish_counts = df['fish_name'].value_counts()
    rare_fish = fish_counts[fish_counts < 2].index
    if not rare_fish.empty:
        df = df[~df['fish_name'].isin(rare_fish)]

    if df.empty:
        logging.warning("レア魚種除外後、訓練データがなくなりました。")
        return None, None
        
    df['date'] = pd.to_datetime(df['date'])
    df['weekday'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month

    # --- 特徴量エンジニアリング強化 ---
    # 時刻データを数値に変換
    time_cols = ['high_tide_1_time', 'high_tide_2_time', 'low_tide_1_time', 'low_tide_2_time', 'sun_rise', 'sun_set', 'moon_rise', 'moon_set']
    for col in time_cols:
        df[col] = df[col].apply(time_to_hours)
    
    # 新しい特徴量を作成
    df['temp_range'] = df['max_temp'] - df['min_temp']
    df['tide_range_1'] = abs(df['high_tide_1_height'] - df['low_tide_1_height'])
    df['tide_range_2'] = abs(df['high_tide_2_height'] - df['low_tide_2_height'])

    # 元の日付と不要なカラムを削除
    df = df.drop(columns=['date'])
    
    # 欠損値を中央値で補完
    for col in df.select_dtypes(include=np.number).columns:
        df[col] = df[col].fillna(df[col].median())
    
    encoders = {}
    for col in ['prefecture', 'fish_name', 'tide_name']:
        if col in df.columns:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
            encoders[col] = le
    
    logging.info("データの前処理が完了しました。")
    return df, encoders

def train_model():
    df, encoders = prepare_data()
    if df is None: return

    X = df.drop('fish_name', axis=1)
    y = df['fish_name']
    
    if y.empty:
        logging.warning("ターゲットデータが空のため、訓練をスキップします。")
        return

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    logging.info("AIモデルの訓練を開始します...")
    model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', n_jobs=-1)
    model.fit(X_train, y_train)
    logging.info("AIモデルの訓練が完了しました。")
    
    # --- モデル評価の強化 ---
    y_pred = model.predict(X_test)
    logging.info("--- モデル評価レポート ---")
    report = classification_report(y_test, y_pred, target_names=encoders['fish_name'].inverse_transform(np.unique(y_test)), zero_division=0)
    print(report)
    
    # --- 特徴量の重要度を表示 ---
    logging.info("--- 特徴量の重要度 ---")
    feature_importances = pd.DataFrame(model.feature_importances_, index=X_train.columns, columns=['importance']).sort_values('importance', ascending=False)
    print(feature_importances)
    
    joblib.dump(model, 'fish_predictor.joblib')
    joblib.dump(encoders, 'encoders.joblib')
    joblib.dump(X.columns.tolist(), 'model_features.joblib')
    logging.info("訓練済みのAIモデルとエンコーダーをファイルに保存しました。")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    train_model()