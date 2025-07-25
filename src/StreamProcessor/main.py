import logging
import six
import sys

from StreamProcessor.attachmentmanager import get_attachments_links
from StreamProcessor.postparser import get_message_texts
from common.API.postApi import Post

sys.modules["kafka.vendor.six.moves"] = six.moves
import faust
from pymongo import MongoClient
from common.Utils.MongoUtils import MongoInterface
import json

from common.logging_config import setup_logging

setup_logging()
log = logging.getLogger(__name__)

app = faust.App(
    "enrichposts",
    broker="kafka://kafka:9092",
    value_serializer="raw",
)


mongo_interface = MongoInterface()
vk_posts_topic = app.topic("vk-posts")
enriched_topic = app.topic("enriched-vk-posts")


class ProcessedPost:
    users: list[int] = []
    text: str = ""


@app.agent(vk_posts_topic)
async def process(posts):
    async for post_bytes in posts:
        post = Post(**json.loads(post_bytes.decode("utf-8")))
        media = get_attachments_links(post.attachments)
        # limit media to 10 links only
        media = media[:10]
        if len(media) > 10:
            log.warning("we've just cut the media")
        photos = [i.link for i in media if i.attachment_type == "photo"]
        post_texts = get_message_texts(group_name, post, media)
        for i, post_text in enumerate(post_texts):
            await put_message_into_queue(
                user_ids,
                post_text,
                photos if i == 0 else None,
                notifications=notifications_list,
            )
        logger.debug(f"put message into queue for users {user_ids}")

        group_id = post.group_id
        user_ids = mongo_interface.get_group_users(group_id)
        enriched = {"post": post, "users": user_ids}
        await enriched_topic.send(value=json.dumps(enriched).encode())


if __name__ == "__main__":
    app.main()
