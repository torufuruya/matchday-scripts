#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
watchlist テーブルの match_id マイグレーションスクリプト（ドライラン対応）

機能:
1) 全件 Scan
2) migration_map(旧match_id→新match_id) にヒットするレコードのみ対象化
3) Put(新キー) + Delete(旧キー) を TransactWriteItems で同一トランザクション実行
   - Put は attribute_not_exists(user_id)
   - Delete は attribute_exists(user_id)
4) --dry-run で実行せず計画のみ表示（安全）
5) --verbose で対象レコードの詳細（user_id, old→new）を表示

注意:
- watchlist: PK=user_id(S), SK=match_id(S) を前提
"""

import os
import sys
import argparse
from typing import Dict, Any, List, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ========= ここを編集してください =========
# 旧 match_id (str) -> 新 match_id (str)
migration_map: Dict[str, str] = {
    "544307": "af:fixture:1390916",
    "544553": "af:fixture:1391161",
    "537860": "af:fixture:1379043",
    "544293": "af:fixture:1390901",
    "544337": "af:fixture:1390940",
    "544295": "af:fixture:1390904",
    "544313": "af:fixture:1390921",
    "544327": "af:fixture:1390931",
    "544347": "af:fixture:1390950",
    "544395": "af:fixture:1391000"
}
# =========================================

TABLE_NAME = os.environ.get("TABLE_NAME", "watchlist")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")

dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(
        retries={"max_attempts": 10, "mode": "standard"},
        user_agent_extra="watchlist-matchid-migrator/1.1",
    ),
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Migrate watchlist.match_id (SK) from old to new by migration_map.")
    p.add_argument("--table", default=TABLE_NAME, help=f"DynamoDB table name (default: {TABLE_NAME})")
    p.add_argument("--region", default=AWS_REGION, help=f"AWS region (default: {AWS_REGION})")
    p.add_argument("--dry-run", action="store_true", help="実行せず計画のみ表示（書き込みなし）")
    p.add_argument("--verbose", action="store_true", help="対象レコードを詳細表示（user_id, old→new）")
    return p.parse_args()


def scan_all_items(table_name: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    eks = None
    while True:
        params = {"TableName": table_name}
        if eks:
            params["ExclusiveStartKey"] = eks
        resp = dynamodb.scan(**params)
        items.extend(resp.get("Items", []))
        eks = resp.get("LastEvaluatedKey")
        if not eks:
            break
    return items


def as_s(item: Dict[str, Any], key: str) -> str:
    v = item.get(key)
    if v is None:
        return ""
    if "S" in v:
        return v["S"]
    if "N" in v:
        return v["N"]
    return ""


def build_put_item_with_new_match_id(old_item: Dict[str, Any], new_match_id: str) -> Dict[str, Any]:
    new_item = {k: v for k, v in old_item.items()}
    new_item["match_id"] = {"S": str(new_match_id)}  # Stringで保存
    return new_item


def transact_put_delete(table_name: str, user_id: str, old_match_id: str, new_item: Dict[str, Any]) -> None:
    request = {
        "TransactItems": [
            {
                "Put": {
                    "TableName": table_name,
                    "Item": new_item,
                    "ConditionExpression": "attribute_not_exists(user_id)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                }
            },
            {
                "Delete": {
                    "TableName": table_name,
                    "Key": {
                        "user_id": {"S": user_id},
                        "match_id": {"S": old_match_id},
                    },
                    "ConditionExpression": "attribute_exists(user_id)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                }
            },
        ]
    }
    dynamodb.transact_write_items(**request)


def main() -> int:
    args = parse_args()

    # env/引数の最終決定（clientはすでに初期化済みだが表示用に記録）
    table = args.table
    region = args.region

    print(f"Target table: {table} (region={region})")
    print(f"migration_map size: {len(migration_map)}")
    print(f"mode: {'DRY-RUN' if args.dry_run else 'EXECUTE'}")
    if not migration_map:
        print("migration_map が空です。旧→新の対応を設定してください。", file=sys.stderr)
        return 2

    # 1) 全件取得
    items = scan_all_items(table)
    total = len(items)
    print(f"scanned items: {total}")

    # 2) 対象抽出
    candidates: List[Tuple[Dict[str, Any], str, str]] = []
    for it in items:
        user_id = as_s(it, "user_id")
        old_match_id = as_s(it, "match_id")
        if not user_id or not old_match_id:
            continue
        if old_match_id in migration_map:
            candidates.append((it, user_id, old_match_id))
    print(f"target records (hit in migration_map): {len(candidates)}")

    # 3) 実行 or ドライラン
    planned = 0
    success = 0
    failed = 0
    skipped_same = 0

    for it, user_id, old_match_id in candidates:
        new_match_id = str(migration_map[old_match_id])
        if old_match_id == new_match_id:
            skipped_same += 1
            if args.verbose:
                print(f"[SKIP same] user_id={user_id} match_id={old_match_id}")
            continue

        if args.dry_run:
            planned += 1
            if args.verbose:
                print(f"[PLAN] user_id={user_id} {old_match_id} -> {new_match_id}")
            continue

        # 実行モード
        new_item = build_put_item_with_new_match_id(it, new_match_id)
        try:
            transact_put_delete(table, user_id, old_match_id, new_item)
            success += 1
            if args.verbose:
                print(f"[OK] user_id={user_id} {old_match_id} -> {new_match_id}")
        except ClientError as e:
            failed += 1
            print(
                f"[ERROR] user_id={user_id}, old_match_id={old_match_id} -> new_match_id={new_match_id}: {e}",
                file=sys.stderr,
            )

    # 4) サマリ
    print("migration summary")
    if args.dry_run:
        print(f"  - planned (would migrate): {planned}")
        print(f"  - skipped (same id):       {skipped_same}")
        print(f"  - execute mode:            DRY-RUN (no writes)")
        return 0
    else:
        print(f"  - success (migrated):      {success}")
        print(f"  - failed:                  {failed}")
        print(f"  - skipped (same id):       {skipped_same}")
        return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
