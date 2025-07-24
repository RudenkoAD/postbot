import six
import sys

if sys.version_info >= (3, 12, 0):
    sys.modules["kafka.vendor.six.moves"] = six.moves
import faust
from pymongo import MongoClient
from common.Utils.MongoUtils import MongoInterface
import json

app = faust.App(
    "enrichposts",
    broker="kafka://kafka:9092",
    value_serializer="raw",
)


mongo_interface = MongoInterface()
vk_posts_topic = app.topic("vk-posts")
enriched_topic = app.topic("enriched-vk-posts")


@app.agent(vk_posts_topic)
async def process(posts):
    async for post_bytes in posts:
        post = json.loads(post_bytes.decode("utf-8"))
        group_id = post.get("group_id")
        user_ids = mongo_interface.get_group_users(group_id)
        enriched = {"post": post, "users": user_ids}
        await enriched_topic.send(value=json.dumps(enriched).encode())


if __name__ == "__main__":
    app.main()
