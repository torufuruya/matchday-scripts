#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
team_follows-stg テーブルの teamId マイグレーションスクリプト

要件:
1) 全データ取得（Scan）
2) teamId が int に変換できるもののみ抽出し、総数を表示
3) 抽出されたレコードのみ更新（= 新teamIdへ移し替え）
   - マイグレーションマップ（migration_map）から新teamIdを取得
   - 新teamId で Put、旧レコードを Delete（同一トランザクション）
4) 成功件数 / 失敗件数を表示

注意:
- teamId は Sort Key のため「上書き更新」は不可。Put(新キー)→Delete(旧キー) の
  TransactWriteItems で実施。
- Put は二重作成を防ぐため条件付き（新キーが未存在なら）
- Delete は念のため条件付き（旧キーが存在なら）
"""

import os
import sys
import math
import time
from typing import Dict, Any, List, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


# ======== ここを編集してください（例）========
# 旧 teamId(int) -> 新 teamId(str) の対応を記載
# 例: 1 は "1001" に、2 は "1002" に移行する
migration_map: Dict[int, str] = {
    2: "167",       # TSG 1899 Hoffenheim
    3: "168",       # Bayer 04 Leverkusen
    4: "165",       # Borussia Dortmund
    5: "157",       # FC Bayern München
    7: "175",       # Hamburger SV
    15: "164",      # FSV Mainz 05
    17: "160",      # SC Freiburg
    19: "169",      # Eintracht Frankfurt
    44: "180",      # FC Heidenheim 1846
    57: "42",       # Arsenal FC
    58: "66",       # Aston Villa FC
    61: "49",       # Chelsea FC
    62: "45",       # Everton FC
    63: "36",       # Fulham FC
    64: "40",       # Liverpool FC
    65: "50",       # Manchester City FC
    66: "33",       # Manchester United FC
    67: "34",       # Newcastle United FC
    71: "746",      # Sunderland AFC
    73: "47",       # Tottenham Hotspur FC
    76: "39",       # Wolverhampton Wanderers FC
    77: "531",      # Athletic Club
    78: "530",      # Club Atlético de Madrid
    80: "540",      # RCD Espanyol de Barcelona
    81: "529",      # FC Barcelona
    82: "546",      # Getafe CF
    86: "541",      # Real Madrid CF
    87: "728",      # Rayo Vallecano de Madrid
    88: "539",      # Levante UD
    89: "798",      # RCD Mallorca
    90: "543",      # Real Betis Balompié
    92: "548",      # Real Sociedad de Fútbol
    94: "533",      # Villarreal CF
    98: "489",      # AC Milan
    100: "497",     # AS Roma
    102: "499",     # Atalanta BC
    108: "505",     # FC Internazionale Milano
    109: "496",     # Juventus FC
    112: "523",     # Parma Calcio 1913
    113: "492",     # SSC Napoli
    263: "542",     # Deportivo Alavés
    332: "54",      # Birmingham City FC
    341: "63",      # Leeds United FC
    351: "65",      # Nottingham Forest FC
    354: "52",      # Crystal Palace FC
    384: "58",      # Millwall FC
    397: "51",      # Brighton & Hove Albion FC
    457: "520",     # US Cremonese
    511: "96",      # Toulouse FC
    512: "106",     # Stade Brestois 29
    516: "81",      # Olympique de Marseille
    519: "108",     # AJ Auxerre
    521: "79",      # Lille OSC
    522: "84",      # OGC Nice
    523: "80",      # Olympique Lyonnais (Lyon)
    524: "85",      # Paris Saint-Germain FC
    525: "97",      # FC Lorient
    529: "94",      # Stade Rennais FC 1901 (Rennes)
    532: "77",      # Angers SCO
    533: "111",     # Le Havre AC
    543: "83",      # FC Nantes
    545: "112",     # FC Metz
    546: "116",     # Racing Club de Lens
    548: "91",      # AS Monaco FC
    558: "538",     # RC Celta de Vigo
    559: "536",     # Sevilla FC
    563: "48",      # West Ham United FC
    576: "95",      # RC Strasbourg Alsace
    610: "645",     # Galatasaray SK
    678: "194",     # AFC Ajax
    721: "173",     # RB Leipzig
    1044: "35",     # AFC Bournemouth
    1045: "114",    # Paris FC
    5721: "327",    # FK Bodø/Glimt
    7397: "895",    # Como 1907
}

# ==========================================


TABLE_NAME = os.environ.get("TABLE_NAME", "team_follows")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")

# DynamoDB client（強化リトライ設定）
dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(
        retries={"max_attempts": 10, "mode": "standard"},
        user_agent_extra="team-follows-migrator/1.0",
    ),
)


def is_int_like(s: Any) -> bool:
    """teamId が int に変換できるか判定"""
    if s is None:
        return False
    try:
        int(str(s))
        return True
    except (TypeError, ValueError):
        return False


def scan_all_items(table_name: str) -> List[Dict[str, Any]]:
    """テーブル全件を Scan で取得（ページング対応）"""
    items: List[Dict[str, Any]] = []
    exclusive_start_key = None

    while True:
        params = {"TableName": table_name}
        if exclusive_start_key:
            params["ExclusiveStartKey"] = exclusive_start_key

        resp = dynamodb.scan(**params)
        items.extend(resp.get("Items", []))

        exclusive_start_key = resp.get("LastEvaluatedKey")
        if not exclusive_start_key:
            break

    return items


def as_s(item: Dict[str, Any], key: str) -> str:
    """DynamoDB AttributeValue から文字列（S）を安全に取り出す"""
    v = item.get(key)
    if v is None:
        return ""
    # サポート: {"S": "str"}, {"N": "123"} を文字列化
    if "S" in v:
        return v["S"]
    if "N" in v:
        return v["N"]
    # それ以外（例: NULL, BOOL）は用途外
    return ""


def build_put_item(new_team_id: str, old_item: Dict[str, Any]) -> Dict[str, Any]:
    """旧アイテムをベースに teamId を差し替えた Put 用アイテムを構築"""
    new_item = {}
    for k, v in old_item.items():
        new_item[k] = v

    # Keys: userId(S), teamId(S)
    # teamId を新しい値に差し替え（常に文字列として格納）
    new_item["teamId"] = {"S": f"af:team:{new_team_id}"}
    return new_item


def transact_put_delete(
    table_name: str,
    user_id: str,
    old_team_id: str,
    new_item: Dict[str, Any],
) -> None:
    """
    Put(新キー) + Delete(旧キー) を1トランザクションで実行
    - Put は新キー未存在を条件に
    - Delete は旧キー存在を条件に
    """

    request = {
        "TransactItems": [
            {
                "Put": {
                    "TableName": table_name,
                    "Item": new_item,
                    "ConditionExpression": "attribute_not_exists(userId)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                }
            },
            {
                "Delete": {
                    "TableName": table_name,
                    "Key": {
                        "userId": {"S": user_id},
                        "teamId": {"S": old_team_id},
                    },
                    "ConditionExpression": "attribute_exists(userId)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                }
            },
        ]
    }

    dynamodb.transact_write_items(**request)


def main() -> int:
    print(f"Target table: {TABLE_NAME} (region={AWS_REGION})")

    # 1) 全件取得
    items = scan_all_items(TABLE_NAME)
    total = len(items)
    print(f"scanned items: {total}")

    # 2) teamId が int に変換できるレコードのみ抽出
    candidates: List[Tuple[Dict[str, Any], str, int]] = []  # (item, user_id, old_team_id_int)
    for it in items:
        user_id = as_s(it, "userId")
        team_id_raw = as_s(it, "teamId")
        if is_int_like(team_id_raw):
            candidates.append((it, user_id, int(team_id_raw)))

    print(f"int-convertible teamId count: {len(candidates)}")

    # 3) 抽出レコードの更新（移し替え）
    success = 0
    failed = 0
    skipped_no_map = 0

    for it, user_id, old_team_id_int in candidates:
        old_team_id_str = str(old_team_id_int)

        # マップがない場合はスキップ（要件に忠実: マップから新teamIdを取得して上書き）
        if old_team_id_int not in migration_map:
            skipped_no_map += 1
            continue

        new_team_id = str(migration_map[old_team_id_int])  # 文字列で格納
        new_item = build_put_item(new_team_id, it)

        try:
            transact_put_delete(
                table_name=TABLE_NAME,
                user_id=user_id,
                old_team_id=old_team_id_str,
                new_item=new_item,
            )
            success += 1
        except ClientError as e:
            failed += 1
            # 失敗の詳細はログ出力して継続
            print(
                f"[ERROR] userId={user_id}, old_teamId={old_team_id_str} -> new_teamId={new_team_id}: {e}",
                file=sys.stderr,
            )

    # 4) 成功/失敗件数を表示
    print("migration summary")
    print(f"  - target records (int-like): {len(candidates)}")
    print(f"  - skipped (no mapping):      {skipped_no_map}")
    print(f"  - success (migrated):        {success}")
    print(f"  - failed:                    {failed}")

    # 正常終了コード: 失敗がなければ 0、あれば 1
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
