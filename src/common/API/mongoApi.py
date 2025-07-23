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
    groups: set[str]  # Set of group IDs the user subscribed to


@dataclass(eq=True, frozen=True)
class Group:
    """
    Represents a group in the system.
    Attributes:
        group_id (str): Unique identifier for the group.
        name (str): Name of the group.
        users (set[int]): Set of user IDs that are members of the group.
    """

    group_id: str
    name: str
    users: set[int]  # Set of user IDs that are members of the group
