from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Post:
    """
    Represents a post in the system.
    Attributes:
        group_id (str): The ID of the group to which the post belongs.
        post_id (int): The unique identifier for the post.
        text (str): The content of the post.
    """

    group_id: str
    post_id: int
    text: str
    attachments: list[str] = None


class EnrichedPost(Post):
    """
    Represents an enriched post with additional user information.
    Inherits from Post and adds a list of user IDs associated with the post.
    """

    user_ids: list[int] = None
