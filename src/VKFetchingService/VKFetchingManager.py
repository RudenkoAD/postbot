from calendar import c
import os
from queue import Queue
from kafka.consumer import KafkaConsumer
from common.Utils.KafkaUtils import ConsumerGroup, KafkaRouter, Topic
from VKFetcher import VKFetcher
from common.Commands.Command import Command
from dataclasses import asdict
from common.Commands.FetcherAdminCommand import (
    AddGroupsCommand,
    CommandType,
    FetcherAdminCommand,
    RemoveGroupsCommand,
    ClearGroupsCommand,
    AddApiTokenCommand,
    RemoveApiTokenCommand,
    SocialMediaType,
)
from common.Utils.Singleton import Singleton
import asyncio
import logging

log = logging.getLogger(__name__)


class VKFetchingManager(metaclass=Singleton):
    __fetchers: dict[str, VKFetcher] = dict()
    __groups: set[str] = set()
    pulling_tasks_queue: Queue
    __vk_fetchers_command_consumer: KafkaConsumer

    def __init__(self, pulling_tasks_queue: Queue, groups: set[str] = set()):
        self.pulling_tasks_queue = pulling_tasks_queue
        self.__groups = groups
        self.__initKafkaComponents()

    def __initKafkaComponents(self):
        self.__vk_fetchers_command_consumer = KafkaRouter.get_consumer(
            Topic.VK_FETCHERS_COMMANDS, ConsumerGroup.VK_FETCHERS
        )

    async def start(self, api_tokens: list[str]):
        self.__loop = asyncio.get_running_loop()
        self.__loop.create_task(self.__startPulling())
        for token in api_tokens:
            self.__addAPIToken(
                AddApiTokenCommand(APIToken=token, social_media_type=SocialMediaType.VK)
            )
        self.__loop.create_task(self.__startReadingCommands())

    async def __startPulling(self):
        while True:
            if len(self.__fetchers) > 0:
                for group_id in self.__groups:
                    self.pulling_tasks_queue.put(group_id)
            await asyncio.sleep((len(self.__groups) * 0.5) / len(self.__fetchers))

    async def __startReadingCommands(self):
        for msg in self.__vk_fetchers_command_consumer:
            command = FetcherAdminCommand(**msg.value)
            if command is not None:
                log.debug(f"Got command {command}. Starting decoding")
                await self.__processCommand(command)
            else:
                log.error(f"Command {msg.value} is incorrect. Dropping command")

    async def __processCommand(self, command: FetcherAdminCommand):
        match (command.command_type):
            case CommandType.ADD_GROUPS:
                command = AddGroupsCommand(**asdict(command))
                log.debug(f"Decoded AddGroupsCommand. Starting processing")
                self.__addGroups(command)
            case CommandType.REMOVE_GROUPS:
                command = RemoveGroupsCommand(**asdict(command))
                log.debug(f"Decoded RemoveGroupsCommand. Starting processing")
                self.__removeGroups(command)
            case CommandType.CLEAR_GROUPS:
                command = ClearGroupsCommand(**asdict(command))
                log.debug(f"Decoded ClearGroupsCommand. Starting processing")
                self.__clearGroups(command)
            case CommandType.ADD_API_TOKEN:
                command = AddApiTokenCommand(**asdict(command))
                log.debug(f"Decoded AddApiTokenCommand. Starting processing")
                self.__addAPIToken(command)
            case CommandType.REMOVE_API_TOKEN:
                command = RemoveApiTokenCommand(**asdict(command))
                log.debug(f"Decoded RemoveApiTokenCommand. Starting processing")
                self.__removeAPIToken(command)
            case _:
                log.debug(f"Decoded UNKNOWN command. Dropping command")

    def __addGroups(self, command: AddGroupsCommand):
        for group in command.groups:
            if not group in self.__groups and self.__isValidVkGroup(group):
                self.__groups.add(group)
            else:
                log.warning(f"Skipped invalid or duplicate VK group: {group}")

    def __isValidVkGroup(self, group: str) -> bool:
        # Accepts links like vk.com/club123, vk.com/public123, vk.com/somegroup, or just group id/names
        import re

        vk_group_patterns = [
            r"^(https?://)?vk\.com/(club|public)?[a-zA-Z0-9_]+/?$",
            r"^[a-zA-Z0-9_]+$",
        ]
        for pattern in vk_group_patterns:
            if re.match(pattern, group):
                return True
        return False

    def __removeGroups(self, command: RemoveGroupsCommand):
        for group in command.groups:
            if not group in self.__groups:
                self.__groups.remove(group)

    def __clearGroups(self, command: ClearGroupsCommand):
        self.__groups.clear()

    def __addAPIToken(self, command: AddApiTokenCommand):
        if command.APIToken in self.__fetchers:
            log.warning(
                f"API token {command.APIToken} already exists. Skipping addition."
            )
            return
        else:
            log.debug(f"Adding API token {command.APIToken} to fetchers")
            self.__fetchers[command.APIToken] = VKFetcher(
                pulling_tasks_queue=self.pulling_tasks_queue,
                vk_token=command.APIToken,
            )

    def __removeAPIToken(self, command: RemoveApiTokenCommand):
        if command.APIToken not in self.__fetchers:
            log.warning(
                f"API token {command.APIToken} does not exist. Skipping removal."
            )
            return
        else:
            log.debug(f"Removing API token {command.APIToken} from fetchers")
            self.__fetchers.pop(command.APIToken)
