import os
import logging
from xml import dom
from vkbottle import VKAPIError
from vkbottle.api import API
from dataclasses import dataclass
from common.Utils.KafkaUtils import KafkaRouter
import asyncio

log = logging.getLogger(__name__)

VK_PULLED_POSTS_TOPIC_NAME = os.getenv("VK_PULLED_POSTS_TOPIC_NAME", "vk_posts")


@dataclass
class PullingTask:
    group_id: str
    sending_topic: str | None = None


class VKFetcher:
    _last_post_ids: dict[str, int] = {}
    ITERATION_LIMIT = 10
    POSTS_PACK_SIZE = 10
    TIMEOUT_LENGTH = 0.5

    def __init__(
        self,
        pulling_tasks_queue,
        vk_token: str,
        sleep_time: int = 1,
        default_sending_topic: str = VK_PULLED_POSTS_TOPIC_NAME,
    ):
        log.debug("Started initialization of VKFetcher.")
        self.pulling_tasks_queue = pulling_tasks_queue
        self._api = API(vk_token)
        log.debug("API token initialized")
        self._sleep_time = sleep_time
        self._default_sending_topic = default_sending_topic
        self._init_kafka_components()
        self._loop = asyncio.get_event_loop()
        self._loop.create_task(self._start_pulling_loop())

    def _init_kafka_components(self):
        self._kafka_router = KafkaRouter()

    async def _start_pulling_loop(self):
        while True:
            task = self._get_next_task()
            if not self.has_been_pulled(task.group_id):
                self._loop.create_task(self._pull_group_and_send(task))
            else:
                self._loop.create_task(self._update_on_group_and_send(task))
            await asyncio.sleep(self._sleep_time)

    def _get_next_task(self) -> PullingTask:
        group_id = self.pulling_tasks_queue.get()
        return PullingTask(group_id=group_id)

    async def _pull_group_and_send(self, task: PullingTask):
        posts = await self.pull_group_posts(task)
        if posts is not None:
            for post in posts:
                self._kafka_router.send_dict_to_topic(
                    self.get_sending_topic(task), post
                )
        return posts

    async def _update_on_group_and_send(self, task: PullingTask):
        posts = await self.update_on_group_posts(task)
        if posts is not None:
            for post in posts:
                self._kafka_router.send_dict_to_topic(
                    self.get_sending_topic(task), post
                )
        return posts

    def get_sending_topic(self, task: PullingTask) -> str:
        return self._default_sending_topic

    async def update_on_group_posts(self, task: PullingTask):
        log.debug(f"Updating on {task.group_id} posts")
        posts = []
        for i in range(self.ITERATION_LIMIT):
            posts_pack = await self._try_pull_posts(
                domain=task.group_id,
                count=self.POSTS_PACK_SIZE,
                offset=self.POSTS_PACK_SIZE * i,
            )
            if posts_pack is None:
                break
            new_posts = self._find_posts_newer_than_last(
                group_id=task.group_id, posts_pack=posts_pack
            )
            if new_posts:
                posts.extend(new_posts)
            else:
                break
        return posts

    async def pull_group_posts(self, task: PullingTask) -> list:
        log.debug(f"Pulling for the first time from {task.group_id}")
        return await self._try_pull_posts(
            domain=task.group_id, count=self.POSTS_PACK_SIZE, offset=0
        )

    def _get_last_post_id(self, group_id) -> int:
        return self._last_post_ids.get(group_id, 0)

    def _find_posts_newer_than_last(self, group_id, posts_pack):
        last_post_id = self._get_last_post_id(group_id)
        return [
            post
            for post in posts_pack
            if post.get("id") is not None and post["id"] > last_post_id
        ]

    def _update_last_post_id(self, group_id, posts):
        if not posts:
            return
        for post in posts:
            if self._get_last_post_id(group_id) < post["id"]:
                self._last_post_ids[group_id] = post["id"]

    async def _try_pull_posts(self, domain, count, offset) -> list:
        try:
            posts = await self._pull_posts(domain, count, offset)
            self._update_last_post_id(domain, posts)
            return posts
        except VKAPIError as error:
            log.debug(f"Couldn't get new posts for {domain}. Error: {error}")
            return []

    async def _pull_posts(self, domain, count, offset) -> list:
        posts = await self._api.request(
            "wall.get", {"domain": domain, "count": count, "offset": offset}
        )
        return posts["response"]["items"]

    def has_been_pulled(self, group_id) -> bool:
        return self._last_post_ids.get(group_id) is not None
