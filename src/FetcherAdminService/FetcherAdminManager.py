import os
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
import time
from common.Utils.Encoder import Encoder


class FetcherAdminManager(metaclass=Singleton):
    def __init__(self):
        self.kafka_router = KafkaRouter()
        self.initKafkaComponents()

    def initKafkaComponents(self):
        self.__command_consumer = self.kafka_router.get_consumer(
            Topic.FETCHER_ADMIN_COMMANDS, ConsumerGroup.FETCHER_ADMIN
        )

    def startReadingCommands(self):
        for msg in self.__command_consumer:
            command = Encoder.decodeCommandFromJSON(msg)
            self.__sendCommandToCorrespondingManager(command)

    def __sendCommandToCorrespondingManager(self, command):
        topic = self.__getCommandSocialMediaManagerKafkaTopic(command)
        self.kafka_router.send_command_to_topic(topic, command)

    def __getCommandSocialMediaManagerKafkaTopic(self, command):
        return command.content.social_media_type.name + "_manager_commands"


if __name__ == "__main__":
    admin_manager = FetcherAdminManager()
    admin_manager.startReadingCommands()
