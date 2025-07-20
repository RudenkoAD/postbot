import os
from kafka.admin import KafkaAdminClient, NewTopic
from common.Utils.KafkaUtils import initTopicConsumer
from common.Commands.Command import Command
from common.Commands.KafkaAdminCommand import KafkaAdminCommandContent, EKafkaAdminCommandType 
from common.Utils.Singleton import Singleton
from common.Utils.Encoder import Encoder
import logging
log = logging.getLogger(__name__)

class KafkaAdminManager(metaclass=Singleton):
    __command: Command[KafkaAdminCommandContent]


    def __init__(self):
        self.kafka_admin_client = KafkaAdminClient(bootstrap_servers="kafka:9092", client_id='admin')
        self.__initCommandTopic()
        self.__startReadingCommands()


    def __initCommandTopic(self):
        if not self.__checkIfTopicExists(os.getenv("KAFKA_ADMIN_COMMANDS_TOPIC_NAME")):
            self.__createTopics({NewTopic(name=os.getenv("KAFKA_ADMIN_COMMANDS_TOPIC_NAME"), num_partitions=1, replication_factor=1)})
        self.__command_consumer = initTopicConsumer(os.getenv("KAFKA_ADMIN_COMMANDS_TOPIC_NAME"))


    def __startReadingCommands(self):
        for msg in self.__command_consumer:
            log.debug(f"Got command {msg.value}.\n Starting decoding")
            self.__command = Encoder().decodeCommandFromJSON(msg)
            if(self.__command != None):
                self.__processCommand()
            else:
                log.error(f"Command {msg.value} is incorrect. Dropping command")


    def __processCommand(self):
        match self.__command.content.command_type:
            case EKafkaAdminCommandType.CREATE_TOPIC.value:
                log.debug(f"Decoded CREATE_TOPICS command. Starting processing")
                topics = self.__prepareTopics()
                self.__createTopics(topics)
            case _:
                log.debug(f"Decoded UNKNOWN command. Dropping command")

    def __prepareTopics(self):
        log.debug(f"Requested creation of {len(self.__command.content.topics_names)} topics")
        topics = []
        for name in self.__command.content.topics_names:
            if self.__checkIfTopicExists(name):
                log.debug(f"Topic {name} already exists. Skipping")
                continue

            parameters = self.__command.content.getTopicParameters(name)
            if(parameters is not None):
                topics.append(NewTopic(name=name, num_partitions=parameters.num_partitions, replication_factor=parameters.replication_factor))
            else:
                log.error(f"Can't create topic {name}: No parameters found")
        return topics
    

    def __createTopics(self, topics):
        if(len(topics) > 0):
            self.kafka_admin_client.create_topics(new_topics=topics, validate_only=False)
            log.debug(f"{len(topics)} topics successfully created")
        else:
            log.debug("List of topics for creation is empty. Dropping command")

    def __checkIfTopicExists(self, name) -> bool:
        if name in self.kafka_admin_client.list_topics():
            return True
        return False


if __name__ == "__main__":
    admin_manager = KafkaAdminManager()
