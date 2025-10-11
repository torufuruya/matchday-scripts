#!/usr/bin/env python3
import argparse
import json
import os
from collections import Counter
from typing import Dict, List, Tuple

import boto3
from botocore.config import Config


def scan_all(table, projection_expr: str = "#u,#t", expr_attr_names=None) -> List[Dict]:
    """Table.scan のページネーションを処理して全件取得"""
    if expr_attr_names is None:
        expr_attr_names = {"#u": "userId", "#t": "teamId"}

    items = []
    scan_kwargs = {
        "ProjectionExpression": projection_expr,
        "ExpressionAttributeNames": expr_attr_names,
    }

    while True:
        resp = table.scan(**scan_kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def count_and_sort(items: List[Dict]) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
    team_counts = Counter()
    user_counts = Counter()
    for it in items:
        # PK/SK は必ずある前提だが、念のため get で読む
        u = it.get("userId")
        t = it.get("teamId")
        if u is not None:
            user_counts[u] += 1
        if t is not None:
            team_counts[t] += 1

    team_sorted = sorted(team_counts.items(), key=lambda x: x[1], reverse=True)
    user_sorted = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)
    return team_sorted, user_sorted

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('team_follows')

def main():
    parser = argparse.ArgumentParser(description="Count records by teamId and userId from a DynamoDB table.")
    # parser.add_argument("--table", required=True, help="DynamoDB table name")
    # parser.add_argument("--region", default=os.getenv("AWS_REGION") or "ap-northeast-1")
    # parser.add_argument("--profile", default=os.getenv("AWS_PROFILE"))
    parser.add_argument("--output", choices=["json", "text"], default="json")
    args = parser.parse_args()

    # session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    # dynamodb = session.resource("dynamodb", region_name=args.region, config=Config(retries={"max_attempts": 10}))
    # table = dynamodb.Table(args.table)

    items = scan_all(table)
    team_sorted, user_sorted = count_and_sort(items)

    if args.output == "json":
        print(json.dumps(
            {
                "team_counts_desc": [{"teamId": k, "count": v} for k, v in team_sorted],
                "user_counts_desc": [{"userId": k, "count": v} for k, v in user_sorted],
                "total_items": len(items),
            },
            ensure_ascii=False,
            indent=2,
        ))
    else:
        print(f"Total items: {len(items)}\n")
        print("== teamId counts (desc) ==")
        for k, v in team_sorted:
            print(f"{k}\t{v}")
        print("\n== userId counts (desc) ==")
        for k, v in user_sorted:
            print(f"{k}\t{v}")


if __name__ == "__main__":
    main()

