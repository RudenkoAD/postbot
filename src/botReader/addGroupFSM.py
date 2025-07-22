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
from common.Commands.FetcherAdminCommand import SocialMediaType
from keyboards import keyboards
from states import *
from text_storage import texts

from common.Utils.KafkaUtils import KafkaProducerWrapper
import os
from common.Commands.CommandCreator import CommandCreator
add_group_router = Router()

@add_group_router.message(AddGroupStates.accept, F.text.casefold() == "нет")
async def add_group_accept_no(message: Message, state: FSMContext) -> None:
    await state.set_state(AddGroupStates.link)
    await message.answer(
        "В таком случае, полжалуйста проверьте ссылку и отправьте снова",
        reply_markup=keyboards[AddGroupStates.link],
    )

@add_group_router.message(AddGroupStates.accept, F.text.casefold() == "да")
async def add_group_accept_yes(message: Message, state: FSMContext) -> None:
    await state.set_state(ScreenStates.main)
    await message.answer(
        "Добавили группу!",
        reply_markup=keyboards[ScreenStates.main],
    )

@add_group_router.message(AddGroupStates.accept, F.text.casefold() == "отмена")
async def add_group_accept_cancel(message: Message, state: FSMContext) -> None:
    await state.set_state(ScreenStates.main)
    await message.answer(
        "Возвращаемся в главное меню!",
        reply_markup=keyboards[ScreenStates.main],
    )

@add_group_router.message(AddGroupStates.accept)
async def add_group_accept_default(message: Message, state: FSMContext) -> None:
    await message.answer(
        "Пожалуйста подтвердите, или отмените, добавление группы",
        reply_markup=keyboards[AddGroupStates.accept],
    )

@add_group_router.message(AddGroupStates.link, F.text.casefold() == "отмена")
async def add_group_link_cancel(message: Message, state: FSMContext) -> None:
    await state.set_state(ScreenStates.main)
    await message.answer(
        "Возвращаемся в главное меню!",
    )
    await message.answer(
        texts[ScreenStates.main],
        reply_markup=keyboards[ScreenStates.main],
    )


@add_group_router.message(AddGroupStates.link)
async def add_group_link_link(message: Message, state: FSMContext) -> None:
    group_link = message.text.strip()
    # VK group link validation
    valid_prefixes = [
        "https://vk.com/club", "http://vk.com/club",
        "https://vk.com/public", "http://vk.com/public",
        "https://vk.com/", "http://vk.com/",
        "vk.com/club", "vk.com/public", "vk.com/"
    ]
    if not any(group_link.startswith(prefix) for prefix in valid_prefixes):
        await message.answer(
            "Некорректная ссылка. Пожалуйста, введите ссылку на группу ВКонтакте (например, https://vk.com/club123456 или https://vk.com/public123456)",
            reply_markup=keyboards[AddGroupStates.link],
        )
        return

    await state.set_state(AddGroupStates.link)
    await message.answer(
        "Проверяем группу, пожалуйста подождите",
        reply_markup=ReplyKeyboardRemove(),
    )
    # Create and send add group command
    kafka_producer = KafkaProducerWrapper()
    command = CommandCreator.getAddGroupCommand(
        social_media_type=SocialMediaType.VK,
        groups={group_link}
    )
    kafka_producer.sendCommand(
        topic=os.getenv("FETCHER_ADMIN_COMMANDS_TOPIC_NAME", "fetcher_admin_commands"),
        command=command
    )
    await message.answer(
        "Группа отправлена на добавление! Ожидайте подтверждения.",
        reply_markup=keyboards[AddGroupStates.accept],
    )
