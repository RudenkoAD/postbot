from dataclasses import asdict
from enum import Enum
import json
import logging
import os
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
from common.API.validatorApi import GroupValidationRequest
from common.API.botWriterApi import MessageWriteRequest
from common.Commands.Command import Command
from common.Commands.FetcherAdminCommand import (
    AddGroupsCommand,
    ClearGroupsCommand,
    RemoveGroupsCommand,
    AddApiTokenCommand,
    RemoveApiTokenCommand,
    FetcherAdminCommand,
    CommandType,
)

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


class Topic(str, Enum):
    VALIDATOR = os.getenv("VALIDATOR_TOPIC_NAME", "validator_commands")
    WRITER = os.getenv("WRITER_TOPIC_NAME", "writer_commands")
    FETCHER_ADMIN_COMMANDS = os.getenv(
        "FETCHER_ADMIN_COMMANDS_TOPIC_NAME", "fetcher_admin_commands"
    )
    VK_FETCHERS_COMMANDS = os.getenv(
        "VK_FETCHERS_COMMANDS_TOPIC_NAME", "vk_fetcher_commands"
    )
    KAFKA_ADMIN_COMMANDS = os.getenv(
        "KAFKA_ADMIN_COMMANDS_TOPIC_NAME", "kafka_admin_commands"
    )


class ConsumerGroup(str, Enum):
    VALIDATOR = "validator_service"
    WRITER = "writer_service"
    FETCHER_ADMIN = "fetcher_admin_service"
    VK_FETCHERS = "vk_fetchers_service"
    KAFKA_ADMIN = "kafka_admin_service"


class KafkaRouter:
    __kafka_producer: KafkaProducer

    def __init__(self):
        self.__kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode(
                "utf-8"
            ),
        )
        if self.__kafka_producer.bootstrap_connected():
            log.debug(f"Kafka producer bootstrap connection succeed")
        else:
            log.error(f"Kafka producer bootstrap connection failed")

    def send_to_validator(self, data: GroupValidationRequest):
        self.__kafka_producer.send(
            topic=Topic.VALIDATOR, value=json.dumps(asdict(data))
        )

    def send_to_writer(self, data: MessageWriteRequest):
        self.__kafka_producer.send(topic=Topic.WRITER, value=asdict(data))

    def send_command_to_topic(self, topic: str, command: Command):
        self.__kafka_producer.send(topic=topic, value=asdict(command))

    def send_dict_to_topic(self, topic: str, data: dict):
        self.__kafka_producer.send(topic=topic, value=data)

    @staticmethod
    def get_consumer(topic: Topic, consumer_group: ConsumerGroup) -> KafkaConsumer:
        return KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=consumer_group,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    @staticmethod
    def get_command_consumer(consumer_group: ConsumerGroup) -> KafkaConsumer:
        def command_from_dict(d: dict) -> Command:
            cmd_type = d.get("command_type")
            if cmd_type == "add_groups":
                return AddGroupsCommand(**d)
            elif cmd_type == "remove_groups":
                return RemoveGroupsCommand(**d)
            elif cmd_type == "clear_groups":
                return ClearGroupsCommand(**d)
            elif cmd_type == "add_api_token":
                return AddApiTokenCommand(**d)
            elif cmd_type == "remove_api_token":
                return RemoveApiTokenCommand(**d)
            else:
                return FetcherAdminCommand(**d)

        return KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=consumer_group,
            value_deserializer=lambda m: command_from_dict(
                json.loads(m.decode("utf-8"))
            ),
        )
