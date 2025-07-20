from dataclasses import dataclass
from enum import Enum

class EKafkaAdminCommandType(Enum):
    CREATE_TOPIC = 0

@dataclass
class KafkaAdminCommandContent:
    @dataclass
    class TopicParameters:
        num_partitions: int
        replication_factor: int

        def __init__(self, parameters: list):
            self.num_partitions = parameters[0]
            self.replication_factor = parameters[1]

    command_type: int
    id: int
    topics_names: list
    topics_parameters: dict[str, list]

    def __init__(self):
        pass

    def __init__(self, command_type, id, topics_names, topics_parameters):
        self.command_type = command_type
        self.id = id
        self.topics_names = topics_names
        self.topics_parameters = topics_parameters

    def getTopicParameters(self, name):
        raw_parameters = self.topics_parameters.get(name)
        return self.TopicParameters(raw_parameters)
