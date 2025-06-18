from Utils.KafkaUtils import KafkaProducerWrapper, initTopicConsumer
from Commands.Command import Command
from Commands.FetcherAdminCommand import FetcherAdminCommandContent
from Utils.Singleton import Singleton
from config import log, ENCODER
import config
import time


class FetcherAdminManager(metaclass=Singleton):
    __command: Command[FetcherAdminCommandContent]


    def __init__(self):
        self.__kafka_producer = KafkaProducerWrapper()
        self.__initKafkaComponents()
        self.__startReadingCommands()


    def __initKafkaComponents(self):
        self.__initTopics()
        self.__command_consumer = initTopicConsumer(config.FETCHER_ADMIN_COMMANDS_TOPIC_NAME)


    def __initTopics(self):
        self.__kafka_producer.initTopic(config.FETCHER_ADMIN_COMMANDS_TOPIC_NAME)
        self.__kafka_producer.initTopic(config.VK_FETCHERS_COMMANDS_TOPIC_NAME)
        time.sleep(1)


    def __startReadingCommands(self):
        for msg in self.__command_consumer:
            self.__command = ENCODER.decodeCommandFromJSON(msg)
            self.__sendCommandToCorrespondingManager()


    def __sendCommandToCorrespondingManager(self):
        topic = self.__getCommandSocialMediaManagerKafkaTopic()
        self.__kafka_producer.sendCommand(topic, self.__command)


    def __getCommandSocialMediaManagerKafkaTopic(self):
        return self.__command.content.social_media_type.name + "_manager_commands"


if __name__ == "__main__":
    admin_manager = FetcherAdminManager()
