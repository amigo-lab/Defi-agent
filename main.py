import requests
import os

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text
    }
    r = requests.post(url, json=payload, timeout=20)
    print(r.text)

def main():
    message = """[테스트 메시지]

디파이 에이전트 연결 성공
이제 자동 알림이 가능합니다.
"""
    send_telegram_message(message)

if __name__ == "__main__":
    main()
