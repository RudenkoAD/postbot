import asyncio
import logging
from VKFetchingManager import VKFetchingManager
logging.basicConfig(level=logging.DEBUG)
from queue import Queue

pulling_tasks_queue = Queue()
vk_fetchers_manager = VKFetchingManager(pulling_tasks_queue, [])
loop = asyncio.new_event_loop()
vk_fetchers_start_task = loop.create_task(vk_fetchers_manager.start())
loop.run_forever()
print("end")
