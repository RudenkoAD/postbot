import faust
from pymongo import MongoClient

app = faust.App(
    "enrichposts",
    broker="kafka://kafka:9092",
    value_serializer="raw",
)

mongo_client = MongoClient("mongodb://mongo:27017/")
db = mongo_client["your_db"]
collection = db["subscriptions"]

vk_posts_topic = app.topic("vk-posts")
enriched_topic = app.topic("enriched-vk-posts")


@app.agent(vk_posts_topic)
async def process(posts):
    async for post_bytes in posts:
        import json

        post = json.loads(post_bytes)
        group_id = post.get("group_id")
        users = list(collection.find({"group_id": group_id}))
        user_ids = [u["user_id"] for u in users]
        enriched = {"post": post, "users": user_ids}
        await enriched_topic.send(value=json.dumps(enriched).encode())


if __name__ == "__main__":
    app.main()
