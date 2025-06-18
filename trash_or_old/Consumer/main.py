from kafka import KafkaConsumer
import time
from logging import getLogger, INFO, basicConfig

log = getLogger("consumer")
basicConfig(level=INFO)

def main():
    log.info(f"bench :{time.perf_counter()}")
    consumer = KafkaConsumer('vk_posts', bootstrap_servers="kafka:9092")
    for msg in consumer:
        text = msg.value.decode("utf-8")
        log.info(f"bench :{time.perf_counter()}")
    

main()



