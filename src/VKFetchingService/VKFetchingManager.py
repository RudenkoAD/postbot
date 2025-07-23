from calendar import c
import os
from queue import Queue
from kafka.consumer import KafkaConsumer
from common.Utils.KafkaUtils import ConsumerGroup, KafkaRouter, Topic
from VKFetcher import VKFetcher
from common.Commands.Command import Command
from common.Commands.FetcherAdminCommand import (
    AddGroupsCommand, RemoveGroupsCommand, ClearGroupsCommand, AddApiTokenCommand, RemoveApiTokenCommand
)
from common.Utils.Singleton import Singleton
import asyncio
from common.Utils.Encoder import Encoder
import logging
log = logging.getLogger(__name__)
class VKFetchingManager(metaclass=Singleton):
    __fetchers: dict[str, VKFetcher] = dict()
    __groups: set[str] = set()
    kafka_router: KafkaRouter
    pulling_tasks_queue: Queue
    __vk_fetchers_command_consumer: KafkaConsumer


    def __init__(self, pulling_tasks_queue: Queue, initGroups: set = set()):
        self.pulling_tasks_queue = pulling_tasks_queue
        self.__groups = initGroups
        self.__initKafkaComponents()

    
    def __initKafkaComponents(self):
        self.kafka_router = KafkaRouter()
        self.__vk_fetchers_command_consumer = self.kafka_router.get_consumer(
            Topic.VK_FETCHERS_COMMANDS, ConsumerGroup.VK_FETCHERS
        )


    async def start(self, api_tokens: list[str]):
        self.__loop = asyncio.get_running_loop()
        self.__loop.create_task(self.__startPulling())
        for token in api_tokens:
            self.__addAPIToken(AddApiTokenCommand(APIToken=token))
        self.__loop.create_task(self.__startReadingCommands())


    async def __startPulling(self):
        while True:
            if len(self.__fetchers) > 0:
                self.__createAndSendPullingTasks()
                await asyncio.sleep((len(self.__groups) * 0.5) / len(self.__fetchers))


    def __createAndSendPullingTasks(self):
        for group_id in self.__groups:
            self.pulling_tasks_queue.put(group_id)

    async def __startReadingCommands(self):
        for msg in self.__vk_fetchers_command_consumer:
            command = Encoder.decodeCommandFromJSON(msg)
            if command is not None:
                log.debug(f"Got command {command}. Starting decoding")
                await self.__processCommand(command)
            else:
                log.error(f"Command {msg.value} is incorrect. Dropping command")


    async def __processCommand(self, command: Command):
        if isinstance(command, AddGroupsCommand):
            log.debug(f"Decoded AddGroupsCommand. Starting processing")
            self.__addGroups(command)
        elif isinstance(command, RemoveGroupsCommand):
            log.debug(f"Decoded RemoveGroupsCommand. Starting processing")
            self.__removeGroups(command)
        elif isinstance(command, ClearGroupsCommand):
            log.debug(f"Decoded ClearGroupsCommand. Starting processing")
            self.__clearGroups(command)
        elif isinstance(command, AddApiTokenCommand):
            log.debug(f"Decoded AddApiTokenCommand. Starting processing")
            self.__addAPIToken(command)
        elif isinstance(command, RemoveApiTokenCommand):
            log.debug(f"Decoded RemoveApiTokenCommand. Starting processing")
            self.__removeAPIToken(command)
        else:
            log.debug(f"Decoded UNKNOWN command. Dropping command")


    def __addGroups(self, command: AddGroupsCommand):
        for group in command.groups:
            if not self.__checkIfGroupObserved(group) and self.__isValidVkGroup(group):
                self.__groups.add(group)
            else:
                log.warning(f"Skipped invalid or duplicate VK group: {group}")
                

    def __isValidVkGroup(self, group: str) -> bool:
        # Accepts links like vk.com/club123, vk.com/public123, vk.com/somegroup, or just group id/names
        import re
        vk_group_patterns = [
            r"^(https?://)?vk\.com/(club|public)?[a-zA-Z0-9_]+/?$",
            r"^[a-zA-Z0-9_]+$"
        ]
        for pattern in vk_group_patterns:
            if re.match(pattern, group):
                return True
        return False


    def __removeGroups(self, command: RemoveGroupsCommand):
        for group in command.groups:
            if not self.__checkIfGroupObserved(group):
                self.__groups.remove(group)


    def __checkIfGroupObserved(self, group: str) -> bool:
        return group in self.__groups
    

    def __clearGroups(self, command: ClearGroupsCommand):
        self.__groups.clear()


    def __addAPIToken(self, command: AddApiTokenCommand):
        self.__fetchers[command.APIToken] = VKFetcher(
            pulling_tasks_queue=self.pulling_tasks_queue,
            vk_token=command.APIToken,
        )


    def __removeAPIToken(self, command: RemoveApiTokenCommand):
        if self.__fetchers.get(command.APIToken) is not None:
            self.__fetchers.pop(command.APIToken)
