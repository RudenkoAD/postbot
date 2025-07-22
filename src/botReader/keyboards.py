from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from states import AddGroupStates, ScreenStates

keyboards = {
    AddGroupStates.link: ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text = "отмена")]
    ]),
    AddGroupStates.accept: ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text = "да"),KeyboardButton(text = "нет"),],
        [KeyboardButton(text = "отмена")]
    ]),
    ScreenStates.main: ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text = "добавить группу"),KeyboardButton(text = "профиль"),],
    ]),
}