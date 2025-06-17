import logging
from datetime import datetime, timedelta
import re

def is_fishing_related(query):
    """ユーザーの質問が釣りに関連するかを判定する"""
    fishing_keywords = [
        '釣り', '釣果', '魚', '船', '船宿', 'マダイ', 'アジ', 'イカ', 'タチウオ',
        'フグ', 'カワハギ', 'ヒラメ', 'シーバス', '三崎', '本牧', '東京湾', '金沢',
        '釣れる', '釣れてる', 'ホットな'
    ]
    return any(keyword in query for keyword in fishing_keywords)

def parse_query(query):
    """ユーザーの自然言語の質問を解析し、意図を抽出する"""
    intent = {'area': None, 'date': None}

    area_keywords = ['三崎', '本牧', '金沢', '東京湾', '横浜', '神奈川', '千葉', '東京']
    for area in area_keywords:
        if area in query:
            # 「東京湾」は「東京」としても認識されるため、より長いキーワードを優先
            if intent['area'] is None or len(area) > len(intent['area']):
                intent['area'] = area

    today = datetime.now()
    if '明日' in query:
        intent['date'] = today + timedelta(days=1)
    elif '明後日' in query:
        intent['date'] = today + timedelta(days=2)
    elif '来週' in query:
        intent['date'] = today + timedelta(weeks=1)
    elif '再来週' in query:
        intent['date'] = today + timedelta(weeks=2)
    else:
        match = re.search(r'(\d+)\s*週間?後', query)
        if match:
            weeks = int(match.group(1))
            intent['date'] = today + timedelta(weeks=weeks)
        else:
            intent['date'] = today
            
    return intent