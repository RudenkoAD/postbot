from dataclasses import dataclass
from enum import Enum
from dataclasses_json import dataclass_json
from common.Commands.Command import Command, CommandTarget


class SocialMediaType(Enum):
    VK = "vk"
    Twitter = "twitter"
    LinkedIn = "linkedin"


@dataclass_json
@dataclass(kw_only=True)
class FetcherAdminCommand(Command):
    """Base class for commands sent to the Fetcher Admin Service."""

    target_type: CommandTarget = CommandTarget.FETCHER_ADMIN
    social_media_type: SocialMediaType = SocialMediaType.VK


@dataclass_json
@dataclass
class AddGroupsCommand(FetcherAdminCommand):
    groups: set[str]


@dataclass_json
@dataclass
class RemoveGroupsCommand(FetcherAdminCommand):
    groups: set[str]


@dataclass_json
@dataclass
class ClearGroupsCommand(FetcherAdminCommand):
    groups: set[str]


@dataclass_json
@dataclass
class AddApiTokenCommand(FetcherAdminCommand):
    APIToken: str


@dataclass_json
@dataclass
class RemoveApiTokenCommand(FetcherAdminCommand):
    APIToken: str
