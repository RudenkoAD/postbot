import os
from aiogram import Bot
from common.Utils.KafkaUtils import ConsumerGroup, KafkaRouter, Topic
import asyncio
from common.logging_config import setup_logging
import logging

setup_logging()
log = logging.getLogger(__name__)


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "your-telegram-bot-token")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
consumer = KafkaRouter.get_consumer(
    topic=Topic.WRITER,
    consumer_group=ConsumerGroup.WRITER,
)


async def consume():
    async with bot:
        for msg in consumer:
            chat_id = msg.value.get("chat_id")
            text = msg.value.get("text")
            if chat_id and text:
                await bot.send_message(chat_id, text)


if __name__ == "__main__":
    log.info("Starting botWriter main")
    asyncio.run(consume())
