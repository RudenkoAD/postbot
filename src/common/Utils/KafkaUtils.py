import logging
import os
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
from common.Commands.CommandCreator import CommandCreator
from common.Utils.Encoder import Encoder

log = logging.getLogger(__name__)

class KafkaProducerWrapper:
    __kafka_producer: KafkaProducer


    def __init__(self):
        self.__kafka_producer = KafkaProducer(bootstrap_servers="kafka:9092")
        if(self.__kafka_producer.bootstrap_connected()):
            log.debug(f"Kafka producer bootstrap connection succeed")
        else: 
            log.error(f"Kafka producer bootstrap connection failed")


    def sendCommand(self, topic, command):
        self.__kafka_producer.send(topic=topic, value=Encoder.encodeCommandToJSON(command))


    def sendData(self, topic, data):
        self.__kafka_producer.send(topic=topic, value=Encoder.encodeData(data))

    
    def initTopic(self, topic):
        command = CommandCreator.createKafkaCreateTopicCommand(
            topics_names=list([topic]), 
            topics_parameters=dict({topic: [1, 1]})
            )
        self.sendCommand(os.getenv("FETCHER_ADMIN_COMMANDS_TOPIC_NAME"), command)


def initTopicConsumer(topic, group_id = None):
    kafka_consumer = KafkaConsumer(topic, bootstrap_servers="kafka:9092", group_id=group_id)
    if(kafka_consumer.bootstrap_connected()):
        log.debug(f"Kafka consumer bootstrap connection succeed. Topic: {topic}")
        return kafka_consumer
    else: 
        log.error(f"Kafka consumer bootstrap connection failed. Topic: {topic}")
        return None
