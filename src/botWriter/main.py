import os
import json
from aiogram import Bot
from kafka import KafkaConsumer

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tg-updates")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='bot-writer-group'
)

import asyncio

async def consume():
    async with bot:
        for msg in consumer:
            chat_id = msg.value.get("chat_id")
            text = msg.value.get("text")
            if chat_id and text:
                await bot.send_message(chat_id, text)

if __name__ == "__main__":
    asyncio.run(consume())
