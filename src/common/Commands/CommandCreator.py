from common.Utils.Singleton import Singleton
from common.Commands.KafkaAdminCommand import KafkaAdminCommandContent, EKafkaAdminCommandType
from common.Commands.FetcherAdminCommand import FetcherAdminCommandContent, EFetcherAdminCommandType, ESocialMediaType
from common.Commands.Command import Command, ECommandTargetTypes

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
    def createKafkaCreateTopicCommand(topics_names: list, topics_parameters: dict[str, KafkaAdminCommandContent.TopicParameters]):
        return Command[KafkaAdminCommandContent](
            ECommandTargetTypes.KAFKA_ADMIN.value,
            KafkaAdminCommandContent(
                EKafkaAdminCommandType.CREATE_TOPIC.value,
                CommandCreator.get_id(),
                topics_names,
                topics_parameters
                )
            )
        
