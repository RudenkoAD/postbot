import asyncio
import logging
import os
import re
from queue import Queue
from dataclasses import asdict

from kafka.consumer import KafkaConsumer

from common.Utils.KafkaUtils import ConsumerGroup, KafkaRouter, Topic
from common.Utils.Singleton import Singleton
from common.Commands.Command import Command
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
from VKFetcher import VKFetcher

log = logging.getLogger(__name__)


class VKFetchingManager(metaclass=Singleton):
    def __init__(self, pulling_tasks_queue: Queue, groups: set[str] | None = None):
        self.pulling_tasks_queue = pulling_tasks_queue
        self._groups = groups or set()
        self._fetchers: dict[str, VKFetcher] = {}
        self._init_kafka_components()

    def _init_kafka_components(self):
        self._vk_fetchers_command_consumer = KafkaRouter.get_consumer(
            Topic.VK_FETCHERS_COMMANDS, ConsumerGroup.VK_FETCHERS
        )

    async def start(self, api_tokens: list[str]):
        self._loop = asyncio.get_running_loop()
        self._loop.create_task(self._start_pulling())
        for token in api_tokens:
            self._add_api_token(
                AddApiTokenCommand(APIToken=token, social_media_type=SocialMediaType.VK)
            )
        self._loop.create_task(self._start_reading_commands())

    async def _start_pulling(self):
        while True:
            if self._fetchers:
                for group_id in self._groups:
                    self.pulling_tasks_queue.put(group_id)
                sleep_time = (len(self._groups) * 0.5) / len(self._fetchers)
                await asyncio.sleep(sleep_time if sleep_time > 0 else 0.1)
            else:
                await asyncio.sleep(1)

    async def _start_reading_commands(self):
        for msg in self._vk_fetchers_command_consumer:
            try:
                command = FetcherAdminCommand(**msg.value)
            except Exception as e:
                log.error(f"Failed to parse command {msg.value}: {e}")
                continue
            if command:
                log.debug(f"Got command {command}. Starting decoding")
                await self._process_command(command)
            else:
                log.error(f"Command {msg.value} is incorrect. Dropping command")

    async def _process_command(self, command: FetcherAdminCommand):
        match command.command_type:
            case CommandType.ADD_GROUPS:
                cmd = AddGroupsCommand(**asdict(command))
                log.debug("Decoded AddGroupsCommand. Starting processing")
                self._add_groups(cmd)
            case CommandType.REMOVE_GROUPS:
                cmd = RemoveGroupsCommand(**asdict(command))
                log.debug("Decoded RemoveGroupsCommand. Starting processing")
                self._remove_groups(cmd)
            case CommandType.CLEAR_GROUPS:
                cmd = ClearGroupsCommand(**asdict(command))
                log.debug("Decoded ClearGroupsCommand. Starting processing")
                self._clear_groups(cmd)
            case CommandType.ADD_API_TOKEN:
                cmd = AddApiTokenCommand(**asdict(command))
                log.debug("Decoded AddApiTokenCommand. Starting processing")
                self._add_api_token(cmd)
            case CommandType.REMOVE_API_TOKEN:
                cmd = RemoveApiTokenCommand(**asdict(command))
                log.debug("Decoded RemoveApiTokenCommand. Starting processing")
                self._remove_api_token(cmd)
            case _:
                log.debug("Decoded UNKNOWN command. Dropping command")

    def _add_groups(self, command: AddGroupsCommand):
        for group in command.groups:
            if group not in self._groups and self._is_valid_vk_group(group):
                self._groups.add(group)
            else:
                log.warning(f"Skipped invalid or duplicate VK group: {group}")

    @staticmethod
    def _is_valid_vk_group(group: str) -> bool:
        vk_group_patterns = [
            r"^(https?://)?vk\.com/(club|public)?[a-zA-Z0-9_]+/?$",
            r"^[a-zA-Z0-9_]+$",
        ]
        return any(re.match(pattern, group) for pattern in vk_group_patterns)

    def _remove_groups(self, command: RemoveGroupsCommand):
        for group in command.groups:
            self._groups.discard(group)

    def _clear_groups(self, command: ClearGroupsCommand):
        self._groups.clear()

    def _add_api_token(self, command: AddApiTokenCommand):
        if command.APIToken in self._fetchers:
            log.warning(
                f"API token {command.APIToken} already exists. Skipping addition."
            )
            return
        log.debug(f"Adding API token {command.APIToken} to fetchers")
        self._fetchers[command.APIToken] = VKFetcher(
            pulling_tasks_queue=self.pulling_tasks_queue,
            vk_token=command.APIToken,
        )

    def _remove_api_token(self, command: RemoveApiTokenCommand):
        if command.APIToken not in self._fetchers:
            log.warning(
                f"API token {command.APIToken} does not exist. Skipping removal."
            )
            return
        log.debug(f"Removing API token {command.APIToken} from fetchers")
        self._fetchers.pop(command.APIToken)
