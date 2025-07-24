from aiogram import F, Router
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from keyboards import keyboards
from states import *
from text_storage import texts

main_menu_router = Router()


@main_menu_router.message(ScreenStates.main, F.text.casefold() == "добавить группу")
async def go_to_addgroup(message: Message, state: FSMContext) -> None:
    await state.set_state(AddGroupStates.link)
    await message.answer(
        texts[AddGroupStates.link],
        reply_markup=keyboards[AddGroupStates.link],
    )


@main_menu_router.message(ScreenStates.main, F.text.casefold() == "профиль")
async def go_to_profile(message: Message, state: FSMContext) -> None:
    await state.set_state(AddGroupStates.link)
    await message.answer(
        texts[AddGroupStates.link],
        reply_markup=keyboards[AddGroupStates.link],
    )
