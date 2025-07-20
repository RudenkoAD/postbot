from dataclasses import dataclass
from common.Utils.KafkaUtils import KafkaProducerWrapper
import asyncio
import logging
log = logging.getLogger(__name__)
class VKFetchingWorker:
    @dataclass
    class PullingTask:
        group_id: str
        sending_topic: str

        def __init__(self, group_id, sending_topic = None):
            self.group_id = group_id
            self.sending_topic = sending_topic


    __sleep_time: int
    __default_sending_topic: str
    __kafka_producer: KafkaProducerWrapper


    def __init__(self, pulling_tasks_queue, sleep_time, vk_fetcher, default_sending_topic):
        log.debug(f"Started initialization of new worker with parameters: sleep_time: {sleep_time}, default_sending_topic: {default_sending_topic}")
        self.pulling_tasks_queue = pulling_tasks_queue
        self.__sleep_time = sleep_time
        self.__vk_fetcher = vk_fetcher
        self.__default_sending_topic = default_sending_topic
        self.__initKafkaComponents()
        self.__loop = asyncio.get_running_loop()
        self.__loop.create_task(self.__startPullingLoop())


    def __initKafkaComponents(self):
        self.__kafka_producer = KafkaProducerWrapper()


    async def __startPullingLoop(self):
        while True:
            task = self.__getNextTask()
            if self.__vk_fetcher.isBeenPulledFirstTime(task.group_id):
                self.__loop.create_task(self.__pullGroupAndSend(task))
            else:
                self.__loop.create_task(self.__updateOnGroupAndSend(task))
            await asyncio.sleep(self.__sleep_time)  

  
    def __getNextTask(self):
        return self.PullingTask(self.pulling_tasks_queue.get())


    async def __pullGroupAndSend(self, task):
        posts = await self.__vk_fetcher.pullGroupPosts(task)
        if posts is not None:
            for post in posts:
                self.__kafka_producer.sendData(self.getSendingTopic(task), post)
        return posts
    
    
    async def __updateOnGroupAndSend(self, task):
        posts = await self.__vk_fetcher.updateOnGroupPosts(task)
        if posts is not None:
            for post in posts:
                self.__kafka_producer.sendData(self.getSendingTopic(task), post)
        return posts  
    

    def getSendingTopic(self, task):
        return self.__default_sending_topic
