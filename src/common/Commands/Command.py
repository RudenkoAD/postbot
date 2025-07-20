from dataclasses import dataclass
from enum import Enum
from typing import TypeVar, Generic

class ECommandTargetTypes(Enum):
    KAFKA_ADMIN = 0
    FETCHER_ADMIN = 1

T = TypeVar('T')

@dataclass
class Command(Generic[T]):
    target_type: int
    content: T

    def __init__(self, target_type, content):
        self.target_type = target_type
        self.content = content

    def __init__(self, target_type, content):
        self.target_type = target_type
        self.content = content