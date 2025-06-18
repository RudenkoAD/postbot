from kafka.consumer import KafkaConsumer
from Utils.KafkaUtils import KafkaProducerWrapper, initTopicConsumer
from FetchingServices.VKFetchingService.VKFetcher import VKFetcher
from Commands.Command import Command
from Commands.FetcherAdminCommand import FetcherAdminCommandContent, EFetcherAdminCommandType
from Utils.Singleton import Singleton
from config import log, ENCODER, pulling_tasks_queue
import config
import asyncio

class VKFetchingManager(metaclass=Singleton):
    __fetchers: dict[str, VKFetcher] = dict()
    __command: Command[FetcherAdminCommandContent]
    __kafka_producer: KafkaProducerWrapper
    __vk_fetchers_command_consumer: KafkaConsumer


    def __init__(self, initGroups: set = set()):
        self.__groups: set = initGroups
        self.__initKafkaComponents()

    
    def __initKafkaComponents(self):
        self.__kafka_producer = KafkaProducerWrapper()
        self.__initTopics()
        self.__vk_fetchers_command_consumer = initTopicConsumer(config.VK_FETCHERS_COMMANDS_TOPIC_NAME)


    def __initTopics(self):
        pass


    async def start(self):
        self.__loop = asyncio.get_running_loop()
        self.__loop.create_task(self.__startPulling())
        self.__loop.create_task(self.__addAPIToken())
        self.__loop.create_task(self.__startReadingCommands())


    async def __startPulling(self):
        while True:
            if len(self.__fetchers) > 0:
                self.__createAndSendPullingTasks()
                await asyncio.sleep((len(self.__groups) * 0.5) / len(self.__fetchers))


    def __createAndSendPullingTasks(self):
        log.debug(f"Create new tasks for pulling. Send them in topic {config.VK_PULLING_TASKS_TOPIC_NAME}")
        for group_id in self.__groups:
            pulling_tasks_queue.put(group_id)

    def __startReadingCommands(self):
        for msg in self.__vk_fetchers_command_consumer:
            self.__command = ENCODER.decodeCommandFromJSON(msg)
            self.__processCommand()


    async def __processCommand(self):
        match self.__command.content.command_type:
            case EFetcherAdminCommandType.ADD_GROUPS.value:
                log.debug(f"Decoded ADD_GROUPS command. Starting processing")
                self.__addGroups()
            case EFetcherAdminCommandType.REMOVE_GROUPS.value:
                log.debug(f"Decoded REMOVE_GROUPS command. Starting processing")
                self.__removeGroups()
            case EFetcherAdminCommandType.CLEAR_GROUPS.value:
                log.debug(f"Decoded CLEAR_GROUPS command. Starting processing")
                self.__clearGroups()
            case EFetcherAdminCommandType.ADD_API_TOKEN.value:
                log.debug(f"Decoded ADD_API_TOKEN command. Starting processing")
                self.__addAPIToken()
            case EFetcherAdminCommandType.REMOVE_API_TOKEN.value:
                log.debug(f"Decoded REMOVE_API_TOKEN command. Starting processing")
                self.__removeAPIToken()
            case _:
                log.debug(f"Decoded UNKNOWN command. Dropping command")


    def __addGroups(self):
        for group in self.__command.content.groups:
            if not self.__checkIfGroupObserved(group):
                self.__groups.add(group)


    def __removeGroups(self):
        for group in self.__command.content.groups:
            if not self.__checkIfGroupObserved(group):
                self.__groups.remove(group)

    
    def __checkIfGroupObserved(self, group):
        return group in self.__groups
    

    def __clearGroups(self):
        self.__groups.clear()


    def __addAPIToken(self):
        log.debug(f"Added new API Token. Create corresponding fetcher")


    def __removeAPIToken(self):
        if self.__fetchers.get(self.__command.api_token) is not None:
            self.__fetchers.pop(self.__command.api_token)


def main():
    vk_fetchers_manager = VKFetchingManager(config.target_groups_ids)
    loop = asyncio.new_event_loop()
    vk_fetchers_start_task = loop.create_task(vk_fetchers_manager.start())
    loop.run_forever()
    print("end")

if __name__ == "__main__":
    main()