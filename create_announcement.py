import boto3
import argparse
import uuid
from datetime import datetime, timezone

# DynamoDB初期化
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('announcements')

def generate_announcement_id():
    now = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"announcement#{now}_{suffix}"

def insert_announcement(announcement_id, lang, title, body, publish_at):
    item = {
        'announcement_id': announcement_id,
        'lang': lang,
        'title': title,
        'body': body,
        'publish_at': publish_at,
        'is_active': True
    }
    table.put_item(Item=item)
    print(f"✅ Inserted [{lang}]")

def main():
    parser = argparse.ArgumentParser(description="Post multilingual announcement to DynamoDB")

    parser.add_argument('--title-ja', required=True)
    parser.add_argument('--body-ja', required=True)
    parser.add_argument('--title-en', required=True)
    parser.add_argument('--body-en', required=True)
    parser.add_argument('--title-es', required=True)
    parser.add_argument('--body-es', required=True)
    parser.add_argument('--publish-at', required=False, help="ISO format: e.g., 2025-05-06T00:00:00Z")

    args = parser.parse_args()

    # publish_at を決定
    if args.publish_at:
        try:
            dt = datetime.fromisoformat(args.publish_at.replace("Z", "+00:00"))
            publish_at = dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            print("❌ Invalid publish_at format. Use ISO 8601 (e.g., 2025-05-06T00:00:00Z)")
            return
    else:
        publish_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        print(f"📅 No publish_at provided. Using current time: {publish_at}")

    announcement_id = generate_announcement_id()
    print(f"🆔 Generated Notice ID: {announcement_id}")

    translations = {
        "ja": {"title": args.title_ja, "body": args.body_ja},
        "en": {"title": args.title_en, "body": args.body_en},
        "es": {"title": args.title_es, "body": args.body_es}
    }

    for lang, content in translations.items():
        insert_announcement(announcement_id, lang, content['title'], content['body'], publish_at)

    print("🎉 All translations inserted successfully.")

if __name__ == "__main__":
    main()
