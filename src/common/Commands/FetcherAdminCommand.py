from dataclasses import dataclass
from enum import Enum

class EFetcherAdminCommandType(Enum):
    ADD_GROUPS = 0
    REMOVE_GROUPS = 1
    CLEAR_GROUPS = 2
    ADD_API_TOKEN = 3
    REMOVE_API_TOKEN = 4


class ESocialMediaType(Enum):
    VK = 0
    Twitter = 1
    LinkedIn = 2


@dataclass
class FetcherAdminCommandContent:
    command_type: int
    social_media_type: int
    groups: set[str]
    APIToken: str


    def __init__(self):
        pass