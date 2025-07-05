import boto3
import uuid
from datetime import datetime, timezone

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
    translations = {
        "ja": {"title": "プレシーズンがやってきました！", "body": "7月になりプレシーズンの季節が近づいてきました。v1.11.1からプレシーズンのスケジュールも確認できるようになりました。アプリを最新版に更新してプレシーズンの準備にとりかかりましょう！"},
        "en": {"title": "Pre-season is here!", "body": "July is here and preseason is approaching fast. Starting with v1.11.1, you can now check the preseason schedule. Update to the latest version of the app and get ready for pre-season!"},
        "es": {"title": "¡La pretemporada ya está aquí!", "body": "¡Julio ya llegó y la pretemporada se acerca! Desde la versión v1.11.1 puedes consultar el calendario de la pretemporada. ¡Actualiza la aplicación a la última versión y prepárate!"},
        "fr": {"title": "La pré-saison est là !", "body": "Juillet est là et la pré-saison approche à grands pas. À partir de la version 1.11.1, vous pouvez désormais consulter le calendrier de la pré-saison. Mettez à jour l'application vers la dernière version et préparez-vous pour la pré-saison !"},
        "ru": {"title": "Предсезонье уже здесь!", "body": "Июль уже наступил, а предсезонье не за горами. Начиная с версии 1.11.1, вы можете проверить расписание предсезонья. Обновите приложение до последней версии и готовьтесь к предсезонью!"}
    }

    publish_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    print(f"📅 Using publish_at: {publish_at}")

    announcement_id = generate_announcement_id()
    print(f"🆔 Generated Notice ID: {announcement_id}")

    for lang, content in translations.items():
        insert_announcement(announcement_id, lang, content['title'], content['body'], publish_at)

    print("🎉 All translations inserted successfully.")

if __name__ == "__main__":
    main()
