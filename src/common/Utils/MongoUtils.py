from dataclasses import asdict
from gc import collect
from pymongo import MongoClient
from pymongo.collection import Collection

from common.API.mongoApi import Group, User

MONGO_DB_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "postbot_db"


class MongoInterface:
    def __init__(self):
        self.client = MongoClient(MONGO_DB_URI)
        self.db = self.client[MONGO_DB_NAME]

    def get_collection(self, collection_name: str) -> Collection:
        return self.db[collection_name]

    def get_mongo_collection(self, db_name: str, collection_name: str) -> Collection:
        db = self.client[db_name]
        return db[collection_name]

    def insert_user(self, user: User):
        """
        Insert a user document into the collection.
        :param collection: MongoDB collection
        :param user: User document to insert
        """
        collection_name = "users"  # Replace with your collection name
        collection = self.get_mongo_collection(self.db.name, collection_name)
        collection.insert_one(asdict(user))
    
    def insert_group(self, group: dict):
        """
        Insert a group document into the collection.
        :param collection: MongoDB collection
        :param group: Group document to insert
        """
        collection_name = "groups"
        collection = self.get_mongo_collection(self.db.name, collection_name)
        collection.insert_one(group)
    
    def add_group_user_link(self, group_id: str, user_id: int):
        """
        Add a link between a group and a user.
        :param collection: MongoDB collection
        :param group_id: Group ID
        :param user_id: User ID
        """
        group_collection_name = "groups"
        collection = self.get_mongo_collection(self.db.name, group_collection_name)
        collection.update_one(
            {"group_id": group_id},
            {"$addToSet": {"users": user_id}}
        )
        user_collection_name = "users"
        user_collection = self.get_mongo_collection(self.db.name, user_collection_name)
        user_collection.update_one(
            {"user_id": user_id},
            {"$addToSet": {"groups": group_id}}
        )

    def remove_group_user_link(self, group_id: str, user_id: int):
        """
        Remove a link between a group and a user.
        :param collection: MongoDB collection
        :param group_id: Group ID
        :param user_id: User ID
        """
        group_collection_name = "groups"
        collection = self.get_mongo_collection(self.db.name, group_collection_name)
        collection.update_one(
            {"group_id": group_id},
            {"$pull": {"users": user_id}}
        )
        user_collection_name = "users"
        user_collection = self.get_mongo_collection(self.db.name, user_collection_name)
        user_collection.update_one(
            {"user_id": user_id},
            {"$pull": {"groups": group_id}}
        )
        
    def get_user_groups(self, user_id: int) -> set[str]:
        """
        Get a list of groups for a user.
        :param user_id: User ID
        :return: List of group IDs
        """
        user_collection_name = "users"
        collection = self.get_mongo_collection(self.db.name, user_collection_name)
        user = collection.find_one({"user_id": user_id})
        return set(user.get("groups", [])) if user else set()
    
    def get_group_users(self, group_id: str) -> set[int]:
        """
        Get a list of users for a group.
        :param group_id: Group ID
        :return: List of user IDs
        """
        group_collection_name = "groups"
        collection = self.get_mongo_collection(self.db.name, group_collection_name)
        group = collection.find_one({"group_id": group_id})
        return set(group.get("users", [])) if group else set()