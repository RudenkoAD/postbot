import os
from common.Utils.KafkaUtils import KafkaProducerWrapper, initTopicConsumer
from common.Commands.Command import Command
from common.Commands.FetcherAdminCommand import (
    AddGroupsCommand, RemoveGroupsCommand, ClearGroupsCommand, AddApiTokenCommand, RemoveApiTokenCommand
)
from common.Utils.Singleton import Singleton
import time
from common.Utils.Encoder import Encoder


class FetcherAdminManager(metaclass=Singleton):
    __command: object


    def __init__(self):
        self.__kafka_producer = KafkaProducerWrapper()
        self.__initKafkaComponents()
        self.__startReadingCommands()


    def __initKafkaComponents(self):
        self.__initTopics()
        self.__command_consumer = initTopicConsumer(os.getenv("FETCHER_ADMIN_COMMANDS_TOPIC_NAME", "fetcher_admin_commands"))


    def __initTopics(self):
        self.__kafka_producer.initTopic(os.getenv("FETCHER_ADMIN_COMMANDS_TOPIC_NAME", "fetcher_admin_commands"))
        self.__kafka_producer.initTopic(os.getenv("VK_FETCHERS_COMMANDS_TOPIC_NAME", "vk_fetcher_commands"))
        time.sleep(1)


    def __startReadingCommands(self):
        for msg in self.__command_consumer:
            self.__command = Encoder.decodeCommandFromJSON(msg)
            self.__sendCommandToCorrespondingManager()


    def __sendCommandToCorrespondingManager(self):
        topic = self.__getCommandSocialMediaManagerKafkaTopic()
        self.__kafka_producer.sendCommand(topic, self.__command)


    def __getCommandSocialMediaManagerKafkaTopic(self):
        return self.__command.content.social_media_type.name + "_manager_commands"


if __name__ == "__main__":
    admin_manager = FetcherAdminManager()
