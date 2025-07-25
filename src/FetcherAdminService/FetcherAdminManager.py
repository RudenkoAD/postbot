import os
from common.Commands.FetcherAdminCommand import FetcherAdminCommand, SocialMediaType
from common.Utils.KafkaUtils import ConsumerGroup, KafkaRouter, Topic
from common.Commands.Command import Command
from common.Commands.FetcherAdminCommand import (
    AddGroupsCommand,
    RemoveGroupsCommand,
    ClearGroupsCommand,
    AddApiTokenCommand,
    RemoveApiTokenCommand,
)
from common.Utils.Singleton import Singleton
from common.logging_config import setup_logging
import logging

setup_logging()
log = logging.getLogger(__name__)


class FetcherAdminManager(metaclass=Singleton):
    def __init__(self):
        log.info("Initializing FetcherAdminManager")
        self.kafka_router = KafkaRouter()
        self.initKafkaComponents()

    def initKafkaComponents(self):
        self.__command_consumer = self.kafka_router.get_command_consumer(
            consumer_group=ConsumerGroup.FETCHER_ADMIN
        )

    def startReadingCommands(self):
        log.info("FetcherAdminManager started reading commands")
        for command in self.__command_consumer:
            log.debug(f"Received command: {command}")
            self.__sendCommandToCorrespondingManager(command)

    def __sendCommandToCorrespondingManager(self, command: FetcherAdminCommand):
        topic = self.__getCommandSocialMediaManagerKafkaTopic(command)
        log.info(f"Sending command to topic {topic}: {command}")
        self.kafka_router.send_command_to_topic(topic, command)

    def __getCommandSocialMediaManagerKafkaTopic(self, command: FetcherAdminCommand):
        match command.social_media_type:
            case SocialMediaType.VK:
                return Topic.VK_FETCHERS_COMMANDS
            case _:
                log.error(f"Unknown social media type: {command.social_media_type}")
                raise ValueError(
                    f"Unknown social media type: {command.social_media_type}"
                )


if __name__ == "__main__":
    log.info("Starting FetcherAdminManager main")
    admin_manager = FetcherAdminManager()
    admin_manager.startReadingCommands()
