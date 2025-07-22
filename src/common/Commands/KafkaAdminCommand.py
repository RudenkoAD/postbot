from dataclasses import dataclass
from dataclasses_json import dataclass_json
from common.Commands.Command import Command, CommandTarget


@dataclass_json
@dataclass(kw_only=True)
class KafkaAdminCommand(Command):
    """
    Base class for commands sent to the Kafka Admin Service.
    Attributes:
        target_type (CommandTarget): The type of the command target, which is always Kafka Admin.
    """

    target_type: CommandTarget = CommandTarget.KAFKA_ADMIN


@dataclass_json
@dataclass
class CreateTopicCommand(KafkaAdminCommand):
    topics_names: list[str]
    num_partitions: int
    replication_factor: int
