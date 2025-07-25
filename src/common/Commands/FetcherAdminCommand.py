from dataclasses import dataclass
from enum import Enum
from common.Commands.Command import Command
from typing import ClassVar


class SocialMediaType(str, Enum):
    VK = "vk"
    Twitter = "twitter"
    LinkedIn = "linkedin"


class CommandType(str, Enum):
    ADD_GROUPS = "add_groups"
    REMOVE_GROUPS = "remove_groups"
    CLEAR_GROUPS = "clear_groups"
    ADD_API_TOKEN = "add_api_token"
    REMOVE_API_TOKEN = "remove_api_token"


@dataclass(kw_only=True)
class FetcherAdminCommand(Command):
    """Base class for commands sent to the Fetcher Admin Service.
    Attributes:
        social_media_type (SocialMediaType): The type of social media.
    """

    command_type: ClassVar[CommandType]
    social_media_type: SocialMediaType


@dataclass
class AddGroupsCommand(FetcherAdminCommand):
    """Command to add groups to the Fetcher Admin Service.
    Attributes:
        groups (list[str]): List of group IDs to add.
    """

    groups: list[str]
    command_type: ClassVar[CommandType] = CommandType.ADD_GROUPS


@dataclass
class RemoveGroupsCommand(FetcherAdminCommand):
    """Command to remove groups from the Fetcher Admin Service.
    Attributes:
        groups (list[str]): List of group IDs to remove.
    """

    groups: list[str]
    command_type: ClassVar[CommandType] = CommandType.REMOVE_GROUPS


@dataclass
class ClearGroupsCommand(FetcherAdminCommand):
    command_type: ClassVar[CommandType] = CommandType.CLEAR_GROUPS


@dataclass
class AddApiTokenCommand(FetcherAdminCommand):
    APIToken: str = ""
    command_type: ClassVar[CommandType] = CommandType.ADD_API_TOKEN


@dataclass
class RemoveApiTokenCommand(FetcherAdminCommand):
    APIToken: str = ""
    command_type: ClassVar[CommandType] = CommandType.REMOVE_API_TOKEN
