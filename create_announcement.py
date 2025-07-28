import boto3
import uuid
import csv
import argparse
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('announcements')

def generate_announcement_id():
    now = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"announcement#{now}_{suffix}"

def insert_announcement(announcement_id, lang, title, body, publish_at, dry_run=False):
    item = {
        'announcement_id': announcement_id,
        'lang': lang,
        'title': title,
        'body': body,
        'publish_at': publish_at,
        'is_active': True
    }
    if dry_run:
        print(f"📝 Dry run: would insert [{lang}] → {item}")
    else:
        table.put_item(Item=item)
        print(f"✅ Inserted [{lang}]")

def load_translations_from_csv(filepath):
    translations = {}
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            lang = row['lang'].strip()
            title = row['title'].strip()
            body = row['body'].strip()
            translations[lang] = {"title": title, "body": body}
    return translations

def main():
    parser = argparse.ArgumentParser(description="Insert announcements into DynamoDB")
    parser.add_argument('--dry-run', action='store_true', help='Run in dry mode without inserting into DynamoDB')
    parser.add_argument('--csv', type=str, default='announcement.csv', help='Path to the translations CSV file')
    args = parser.parse_args()

    translations = load_translations_from_csv(args.csv)

    publish_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    print(f"📅 Using publish_at: {publish_at}")

    announcement_id = generate_announcement_id()
    print(f"🆔 Generated Notice ID: {announcement_id}")

    for lang, content in translations.items():
        insert_announcement(announcement_id, lang, content['title'], content['body'], publish_at, dry_run=args.dry_run)

    print("✅ Done (dry run mode: {})".format(args.dry_run))

if __name__ == "__main__":
    main()
