from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass(kw_only=True)
class Command:
    """
    Base class for commands sent to the Kafka Admin Service or Fetcher Admin Service.
    Attributes:
        target_type (CommandTarget): The type of the command target, either Kafka Admin or Fetcher Admin.
    """
    id: int = 0 #TODO make sure that id is unique for each command
