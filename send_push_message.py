#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Send push notifications to all users in DynamoDB `users` table via Amazon SNS.
"""

import os
import logging
from typing import Dict, List
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ---------- 設定 ----------
TABLE_NAME  = os.getenv("USERS_TABLE_NAME", "users")
REGION      = os.getenv("AWS_REGION", "ap-northeast-1")   # 例: 東京
# Connection pool is full, discarding connection: sns.ap-northeast-1.amazonaws.com. Connection pool size: 10
MAX_WORKERS = int(os.getenv("PUSH_WORKERS", 10))          # スレッド数

# 言語ごとのメッセージ（実運用ではもっと丁寧に）
MESSAGES: Dict[str, str] = {
    "en": "July is here and pre-season is just around the corner. Matchday now lets you view the pre-season schedule. Update to the latest version of the app and get ready!",
    "es": "Julio ya llegó y la pretemporada está a la vuelta de la esquina. Ahora puedes consultar el calendario de la pretemporada en Matchday. ¡Actualiza la aplicación a la última versión y prepárate!",
    "ja": "7月になりプレシーズンの季節が近づいてきました。Matchdayではプレシーズンのスケジュールも確認できるようになりました。アプリを最新版に更新してプレシーズンの準備にとりかかりましょう！",
    "fr": "Juillet est arrivé et la présaison approche à grands pas. Vous pouvez désormais consulter le calendrier de la présaison sur Matchday. Mettez votre application à jour vers la dernière version et préparez-vous !",
    "ru": "Наступил июль, и предсезонье уже не за горами. В Matchday теперь доступно расписание предсезонки. Обновите приложение до последней версии и начинайте подготовку!",
}

# ---------- 初期化 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
sns      = boto3.client("sns", region_name=REGION)
table    = dynamodb.Table(TABLE_NAME)

# ---------- DynamoDB 全件取得 ----------
def scan_all(table) -> List[Dict]:
    items: List[Dict] = []
    params = {}
    while True:
        resp = table.scan(**params)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    logging.info("Fetched %d users from %s", len(items), TABLE_NAME)
    return items

# ---------- 通知送信 ----------
success, failure = 0, 0
lock = Lock()  # カウンタ保護

def send_push(user: Dict):
    global success, failure
    lang = user.get("lang_code", "en")
    message = MESSAGES.get(lang, MESSAGES["en"])
    target_arn = user.get("push_endpoint_arn")
    if not target_arn:
        logging.warning("User %s has no endpoint ARN", user.get("user_id"))
        with lock:
            failure += 1
        return

    try:
        sns.publish(TargetArn=target_arn, Message=message)
        with lock:
            success += 1
    except ClientError as e:
        logging.error("Publish failed for %s: %s", user.get("user_id"), e.response["Error"]["Message"])
        with lock:
            failure += 1

def main():
    users = scan_all(table)

    # ThreadPool で並列送信
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(send_push, u) for u in users]
        for _ in as_completed(futures):
            pass  # 完了待ち

    logging.info("Done. Success: %d  Failure: %d", success, failure)

if __name__ == "__main__":
    main()
