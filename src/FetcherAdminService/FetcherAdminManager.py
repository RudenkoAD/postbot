import os
from common.Commands.FetcherAdminCommand import FetcherAdminCommand
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


class FetcherAdminManager(metaclass=Singleton):
    def __init__(self):
        self.kafka_router = KafkaRouter()
        self.initKafkaComponents()

    def initKafkaComponents(self):
        self.__command_consumer = self.kafka_router.get_command_consumer(
            consumer_group=ConsumerGroup.FETCHER_ADMIN
        )

    def startReadingCommands(self):
        for command in self.__command_consumer:
            self.__sendCommandToCorrespondingManager(command)

    def __sendCommandToCorrespondingManager(self, command: FetcherAdminCommand):
        topic = self.__getCommandSocialMediaManagerKafkaTopic(command)
        self.kafka_router.send_command_to_topic(topic, command)

    def __getCommandSocialMediaManagerKafkaTopic(self, command: FetcherAdminCommand):
        return command.social_media_type.value + "_manager_commands"


if __name__ == "__main__":
    admin_manager = FetcherAdminManager()
    admin_manager.startReadingCommands()
