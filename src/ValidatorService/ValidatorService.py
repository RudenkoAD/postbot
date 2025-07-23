import os
import json
from kafka import KafkaConsumer, KafkaProducer
from MongoUtils import MongoInterface
from common.API.validatorApi import GroupValidationRequest
from common.API.botWriterApi import MessageWriteRequest
from KafkaUtils import KafkaRouter
from common.Commands.FetcherAdminCommand import AddGroupsCommand
import vkbottle
import logging

logging.basicConfig(level=logging.INFO)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ADD_GROUP_TOPIC = os.getenv("ADD_GROUP_TOPIC", "add-group-requests")
VALIDATOR_GROUP_ID = os.getenv("VALIDATOR_GROUP_ID", "validator-service")
VALIDATOR_API_TOKEN = os.getenv("VALIDATOR_API_TOKEN", "validator-api-token")


class ValidatorService:
    def __init__(self):
        self.consumer = KafkaConsumer(
            ADD_GROUP_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=VALIDATOR_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.kafka_router = KafkaRouter()
        self.mongo_interface = MongoInterface()
        self.vk_api = vkbottle.API(VALIDATOR_API_TOKEN)

    async def validate_group(self, group_id):
        try:
            response = await self.vk_api.wall.get(group_id=group_id)
            if not response or not response.items:
                raise ValueError(f"Group {group_id} does not exist or has no posts.")
            logging.info(f"Group {group_id} is valid.")
            return True
        except Exception as e:
            logging.error(f"Validation failed for group {group_id}: {e}")
            return False

    def run(self):
        logging.info(
            "ValidatorService started, waiting for AddGroupCommand messages..."
        )
        for msg in self.consumer:
            data = GroupValidationRequest(**msg.value)
            logging.info(f"Received AddGroupCommand: {data}")
            valid = self.validate_group(data.group_id)
            if valid:
                logging.info(f"Group {data.group_id} is valid.")
                self.kafka_router.send_command_to_topic(
                    topic=ADD_GROUP_TOPIC,
                    command=AddGroupsCommand(groups={data.group_id}),
                )
                self.mongo_interface.add_group_user_link(data.group_id, data.user_id)
                self.kafka_router.send_to_writer(
                    MessageWriteRequest(chat_id=data.user_id, text=f"Group {data.group_id} is valid.")
                )
            else:
                logging.error(f"Group {data.group_id} is invalid.")


if __name__ == "__main__":
    service = ValidatorService()
    service.run()
