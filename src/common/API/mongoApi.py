from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class User:
    """
    Represents a user in the system.
    Attributes:
        user_id (str): Unique identifier for the user.
        username (str): Username of the user.
        email (str): Email address of the user.
    """

    user_id: int
    groups: list[str]


@dataclass(eq=True, frozen=True)
class Group:
    """
    Represents a group in the system.
    Attributes:
        group_id (str): Unique identifier for the group.
        name (str): Name of the group.
        users (list[int]): List of user IDs that are members of the group.
    """

    group_id: str
    name: str
    users: list[int]  # List of user IDs that are members of the group
