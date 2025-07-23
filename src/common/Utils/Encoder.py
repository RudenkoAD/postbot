import logging
from dataclasses import asdict
import json
from common.Commands.Command import Command

log = logging.getLogger(__name__)

class Encoder:
    @staticmethod
    def encodeCommandToJSON(command: Command) -> str:
        return Encoder.encodeData(asdict(command))
    @staticmethod
    def decodeCommandFromJSON(raw_command: str) -> Command:
        dict = Encoder.decodeData(raw_command)
        return Command.from_dict(dict)
    @staticmethod
    def encodeData(data: dict) -> str:
        return json.dumps(data, ensure_ascii=False).encode(encoding="utf-8")
    @staticmethod
    def decodeData(data: str) -> dict:
        return json.loads(data.decode(encoding="utf-8"))
