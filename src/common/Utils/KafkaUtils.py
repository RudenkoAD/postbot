from dataclasses import asdict
from enum import Enum
import logging
import os
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
from common.API.validatorApi import GroupValidationRequest
from common.API.botWriterApi import MessageWriteRequest
from common.Commands.Command import Command
from common.Commands.CommandCreator import CommandCreator
from common.Utils.Encoder import Encoder

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VALIDATOR_TOPIC_NAME = os.getenv("VALIDATOR_TOPIC_NAME", "validator_commands")
WRITER_TOPIC_NAME = os.getenv("WRITER_TOPIC_NAME", "writer_commands")

class Topic(str, Enum):
    VALIDATOR = VALIDATOR_TOPIC_NAME
    WRITER = WRITER_TOPIC_NAME
    FETCHER_ADMIN_COMMANDS = os.getenv("FETCHER_ADMIN_COMMANDS_TOPIC_NAME", "fetcher_admin_commands")
    VK_FETCHERS_COMMANDS = os.getenv("VK_FETCHERS_COMMANDS_TOPIC_NAME", "vk_fetcher_commands")
    KAFKA_ADMIN_COMMANDS = os.getenv("KAFKA_ADMIN_COMMANDS_TOPIC_NAME", "kafka_admin_commands")

class ConsumerGroup(str, Enum):
    VALIDATOR = "validator_service"
    WRITER = "writer_service"
    FETCHER_ADMIN = "fetcher_admin_service"
    VK_FETCHERS = "vk_fetchers_service"
    KAFKA_ADMIN = "kafka_admin_service"

class KafkaRouter:
    __kafka_producer: KafkaProducer

    def __init__(self):
        self.__kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        if self.__kafka_producer.bootstrap_connected():
            log.debug(f"Kafka producer bootstrap connection succeed")
        else:
            log.error(f"Kafka producer bootstrap connection failed")

    def send_to_validator(self, data: GroupValidationRequest):
        self.__kafka_producer.send(
            topic=VALIDATOR_TOPIC_NAME, value=Encoder.encodeData(asdict(data))
        )

    def send_to_writer(self, data: MessageWriteRequest):
        self.__kafka_producer.send(
            topic=WRITER_TOPIC_NAME, value=Encoder.encodeData(asdict(data))
        )

    def send_command_to_topic(self, topic: str, command: Command):
        self.__kafka_producer.send(
            topic=topic, value=Encoder.encodeData(asdict(command))
        )

    def send_dict_to_topic(self, topic: str, data: dict):
        self.__kafka_producer.send(topic=topic, value=Encoder.encodeData(data))

    def get_consumer(self, topic: Topic, consumer_group: ConsumerGroup) -> KafkaConsumer:
        return KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=consumer_group,
            value_deserializer=lambda m: Encoder.decodeData(m),
        )