import os
from aiogram import Bot, Dispatcher, types
from kafka import KafkaProducer
import json

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tg-updates")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


async def handle_message(message: types.Message):
    data = {
        "chat_id": message.chat.id,
        "text": message.text,
        "from_user": message.from_user.id
    }
    producer.send(KAFKA_TOPIC, data)
    await message.reply("Message received and sent to Kafka!")

if __name__ == "__main__":
    dp.start_polling(bots=[bot])
