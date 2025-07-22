import os
from aiogram import Bot, Dispatcher, F, Router, html
from kafka import KafkaProducer
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
import json
from addGroupFSM import add_group_router
from mainFSM import main_menu_router
from states import ScreenStates
from text_storage import texts
from keyboards import keyboards
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tg-updates")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
@dp.message(Command("start"))
async def handle_message(message: Message, state: FSMContext):
    await state.set_state(ScreenStates.main)
    await message.answer(
        text = texts[ScreenStates.main],
        reply_markup=keyboards[ScreenStates.main]
    )

if __name__ == "__main__":
    dp.include_routers(add_group_router, main_menu_router)
    dp.run_polling(bot)
