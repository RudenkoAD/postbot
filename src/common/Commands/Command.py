from dataclasses import dataclass
from dataclasses_json import dataclass_json
from enum import Enum
from typing import TypeVar, Generic


class CommandTarget(str, Enum):
    KAFKA_ADMIN = "kafka_admin"
    FETCHER_ADMIN = "fetcher_admin"


@dataclass_json
@dataclass(kw_only=True)
class Command:
    """
    Base class for commands sent to the Kafka Admin Service or Fetcher Admin Service.
    Attributes:
        target_type (CommandTarget): The type of the command target, either Kafka Admin or Fetcher Admin.
    """

    target_type: CommandTarget
    id: int = 0
