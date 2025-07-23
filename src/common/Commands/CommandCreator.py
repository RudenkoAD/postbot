from common.Utils.Singleton import Singleton
from common.Commands.KafkaAdminCommand import CreateTopicCommand
from common.Commands.FetcherAdminCommand import (
    AddGroupsCommand,
    RemoveGroupsCommand,
    ClearGroupsCommand,
    AddApiTokenCommand,
    RemoveApiTokenCommand,
    SocialMediaType,
)
from common.Commands.Command import Command, CommandTarget


class CommandCreator(metaclass=Singleton):
    __last_id: int = 0

    @staticmethod
    def get_id():
        CommandCreator.__last_id += 1
        return CommandCreator.__last_id

    @staticmethod
    def set_id(value):
        CommandCreator.__last_id = value

    @staticmethod
    def getCreateTopicCommand(
        topics_names: list[str], num_partitions: int = 1, replication_factor: int = 1
    ) -> CreateTopicCommand:
        """Creates a command to create Kafka topics.
        Args:
            topics_names (list[str]): List of topic names to create.
            num_partitions (int): Number of partitions for each topic. Default is 1.
            replication_factor (int): Replication factor for each topic. Default is 1.
        Returns:
            CreateTopicCommand: The command to create Kafka topics.
        """
        return CreateTopicCommand(
            id=CommandCreator.get_id(),
            topics_names=topics_names,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )

    @staticmethod
    def getAddGroupCommand(
        social_media_type: SocialMediaType, groups: set[str]
    ) -> AddGroupsCommand:
        """Creates a command to add groups to the Fetcher Admin Service.
        Args:
            social_media_type (SocialMediaType): The type of social media.
            groups (set[str]): Set of group IDs to add.
        """
        return AddGroupsCommand(
            id=CommandCreator.get_id(),
            social_media_type=social_media_type,
            groups=groups,
        )
