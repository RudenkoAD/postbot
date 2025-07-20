import os
from vkbottle import VKAPIError
from vkbottle.api import API
from VKFetchingWorker import VKFetchingWorker
import logging
log = logging.getLogger(__name__)
class VKFetcher:
    class RequestParameters:
        parameters: dict

        def __init__(self, domain = None, count = None, offset = None):
            if(domain is not None):
                self.parameters = {"domain": domain}
            else:
                self.parameters = {"domain": ""}
            self.addParameter("count", count)
            self.addParameter("offset", offset)

        def addParameter(self, parameter_name: str, parameter = None):
            if parameter is not None:
                self.parameters[parameter_name] = parameter

        
        def getParameter(self, key):
            return self.parameters.get(key)


    __worker: VKFetchingWorker
    __last_post_ids: dict[str, int] = dict()

    ITERATION_LIMIT = 10
    POSTS_PACK_SIZE = 10
    TIMEOUT_LENGTH = 0.5


    def __init__(self, vk_token: str):
        log.debug(f"Started initialization of new fetcher. Initialize corresponding worker.")
        self.__api_token = API(vk_token)
        log.debug(f"API token initialized")
        self.__init_worker()
        super().__init__()


    def __init_worker(self):
        self.__worker = VKFetchingWorker(
                                pulling_tasks_queue=self.pulling_tasks_queue,
                                sleep_time=1, 
                                vk_fetcher=self, 
                                default_sending_topic=os.getenv("VK_PULLED_POSTS_TOPIC_NAME"))


    async def updateOnGroupPosts(self, task: VKFetchingWorker.PullingTask):
        log.debug(f"Updating on {task.group_id} posts")
        posts = []
        for i in range(self.ITERATION_LIMIT):
            parameters = self.RequestParameters(
                                domain=task.group_id,
                                count=self.POSTS_PACK_SIZE,
                                offset=self.POSTS_PACK_SIZE * i)
            posts_pack = await self.__tryPullPosts(parameters)
            if posts_pack is None:
                break
            new_posts = self.__findPostsNewerThenLastPost(group_id=task.group_id, posts_pack=posts_pack)
            if(len(new_posts) > 0):
                posts.extend(new_posts)
            else:
                break
        return posts
    

    async def pullGroupPosts(self, task: VKFetchingWorker.PullingTask):
        log.debug(f"Pulling for the first time from {task.group_id}")
        parameters = self.RequestParameters(domain=task.group_id)
        posts = await self.__tryPullPosts(parameters)
        return posts
    

    def __getLastPostId(self, group_id) -> int:
        last_post_id = self.__last_post_ids.get(group_id)
        if last_post_id is not None:
            return last_post_id
        else:
            return 0
        

    def __findPostsNewerThenLastPost(self, group_id, posts_pack):
        return [post for post in posts_pack if post["id"] is not None and post["id"] > self.__getLastPostId(group_id=group_id)]
    

    def __updateLastPostId(self, group_id, posts):
        if posts is None:
            return
        
        for post in posts:
            if self.__getLastPostId(group_id=group_id) < post["id"]:
                self.__last_post_ids[group_id] = post["id"]
    

    async def __tryPullPosts(self, parameters: RequestParameters):
        try:
            posts = await self.__pullPosts(parameters)
            self.__updateLastPostId(parameters.getParameter("domain"), posts)
            return posts
        except VKAPIError as error:
            log.debug(f"Couldn't get new posts for {parameters.getParameter("domain")}. Error: {error}")
            return []


    async def __pullPosts(self, parameters: RequestParameters):
        posts = await self.__api_token.request("wall.get", parameters.parameters)
        return posts["response"]["items"] 
    

    def isBeenPulledFirstTime(self, group_id) -> bool:
        if self.__last_post_ids.get(group_id) is None:
            return True
        else:
            return False
