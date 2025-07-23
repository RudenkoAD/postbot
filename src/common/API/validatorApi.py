from dataclasses import dataclass

from common.Commands.FetcherAdminCommand import SocialMediaType


@dataclass
class GroupValidationRequest:
    """
    Represents a request to validate a group.
    Attributes:
        social_media_type (SocialMediaType): The type of social media platform.
        group_id (str): The ID of the group to be validated.
        user_id (int): The ID of the user making the validation request.
    """

    social_media_type: SocialMediaType
    group_id: str
    user_id: int


@dataclass
class GroupValidationResponse:
    """
    Represents the response to a group validation request.
    Attributes:
        group_id (str): The ID of the group that was validated.
        user_id (int): The ID of the user who requested the validation.
        is_valid (bool): Indicates whether the group is valid or not.
        message (str): Additional information about the validation result.
    """

    group_id: str
    user_id: int
    is_valid: bool
    message: str
