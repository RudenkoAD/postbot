from dataclasses import dataclass
from vkbottle_types.responses.wall import WallWallpostFull, WallGetResponseModel


@dataclass(eq=True, frozen=True)
class Post:
    """
    Represents a post in the system.
    Attributes:
        group_id (str): The ID of the group to which the post belongs.
        post_id (int): The unique identifier for the post.
        text (str): The content of the post.
    """

    group_link: str | None = None
    group_name: str | None = None
    group_id: int | None = None
    post_id: int | None = None
    text: str | None = None
    attachments: list[str] | None = None

    @staticmethod
    def from_wall_post(wall_post: WallWallpostFull):
        post = Post(
            group_link=wall_post.owner_id,
            group_name=wall_post.owner_id,
            group_id=wall_post.owner_id,
            post_id=wall_post.id,
            text=wall_post.text,
            attachments=(
                [attachment.type for attachment in wall_post.attachments]
                if wall_post.attachments
                else None
            ),
        )
        return post


class EnrichedPost(Post):
    """
    Represents an enriched post with additional user information.
    Inherits from Post and adds a list of user IDs associated with the post.
    """

    user_ids: list[int]
