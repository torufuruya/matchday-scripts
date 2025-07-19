import csv
import boto3
import hashlib
import sys
import argparse

def generate_match_id(home_team_id, away_team_id, utc_date):
    ids = sorted([str(home_team_id), str(away_team_id)])
    # FIXME: utc_date can be changed so better not to use it.
    base = f"{ids[0]}|{ids[1]}|{utc_date}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]

def main():
    parser = argparse.ArgumentParser(
        description="Import matches from CSV to DynamoDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--csv-path', help='Path to the CSV file')
    parser.add_argument('--dry-run', action='store_true', help='Print only, do not write to DynamoDB')
    args = parser.parse_args()

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('matches')
    with open(args.csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            match_id = generate_match_id(row['home_team_id'], row['away_team_id'], row['utcDate'])
            item = {
                'id': match_id,
                'utcDate': row['utcDate'],
                'status': row['status'] or 'SCHEDULED',
                'matchday': row['matchday'] or None,
                'home_team_id': int(row['home_team_id']),
                'home_team_name': row['home_team_name'],
                'home_team_short_name': row['home_team_short_name'],
                'home_team_tla': row['home_team_tla'],
                'home_team_crest': row['home_team_crest'],
                'away_team_id': int(row['away_team_id']),
                'away_team_name': row['away_team_name'],
                'away_team_short_name': row['away_team_short_name'],
                'away_team_tla': row['away_team_tla'],
                'away_team_crest': row['away_team_crest'],
                'competition_id': int(row['competition_id']),
                'competition_name': row['competition_name'],
                'competition_emblem': row['competition_emblem'],
                'matchup_key': f"{min(row['home_team_id'], row['away_team_id'])}-{max(row['home_team_id'], row['away_team_id'])}"
            }
            # 空文字をNoneに変換
            for k, v in item.items():
                if v == '':
                    item[k] = None
            if args.dry_run:
                print(f"[DRY RUN] Would insert/update: {item}")
            else:
                table.put_item(Item=item)
                print(f"Inserted/Updated match: {item['id']}")

if __name__ == "__main__":
    main()
