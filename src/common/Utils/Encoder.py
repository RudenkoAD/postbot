import logging
from common.Commands.Command import Command, ECommandTargetTypes
from common.Commands.KafkaAdminCommand import KafkaAdminCommandContent
from common.Commands.FetcherAdminCommand import FetcherAdminCommandContent
from dataclasses import asdict
import dacite
import json

log = logging.getLogger(__name__)

class Encoder:
    @staticmethod
    def encodeCommandToJSON(command: Command):
        return Encoder.encodeData(asdict(command))

    @staticmethod
    def decodeCommandFromJSON(raw_command: str):
        structured_command = Encoder.decodeData(raw_command)
        match structured_command["target_type"]:
            case ECommandTargetTypes.KAFKA_ADMIN.value:
                command = dacite.from_dict(data_class=Command[KafkaAdminCommandContent], data=structured_command)
            case ECommandTargetTypes.FETCHER_ADMIN.value:
                command = dacite.from_dict(data_class=Command[FetcherAdminCommandContent], data=structured_command)
            case _:
                command = None
        return command
    
    @staticmethod
    def encodeData(data):
        return json.dumps(data, ensure_ascii=False).encode(encoding="utf-8")
    @staticmethod
    def decodeData(data):
        return json.loads(data.value.decode("utf-8"))
