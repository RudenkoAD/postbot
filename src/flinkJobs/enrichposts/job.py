from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import json
from pymongo import MongoClient

KAFKA_BOOTSTRAP = "kafka:9092"
MONGO_URI = "mongodb://mongo:27017/"
MONGO_DB = "your_db"
MONGO_COLLECTION = "subscriptions"
VK_POSTS_TOPIC = "vk-posts"
ENRICHED_TOPIC = "enriched-vk-posts"


def enrich_with_users(post_json):
    post = json.loads(post_json)
    group_id = post.get("group_id")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    users = list(db[MONGO_COLLECTION].find({"group_id": group_id}))
    user_ids = [u["user_id"] for u in users]
    enriched = {"post": post, "users": user_ids}
    return json.dumps(enriched)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = FlinkKafkaConsumer(
        topics=VK_POSTS_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "flink-enrich-group",
        },
    )
    stream = env.add_source(kafka_source)

    enriched_stream = stream.map(enrich_with_users)

    kafka_sink = FlinkKafkaProducer(
        topic=ENRICHED_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
    )
    enriched_stream.add_sink(kafka_sink)

    env.execute("VK Post Enrichment Job")


if __name__ == "__main__":
    main()
