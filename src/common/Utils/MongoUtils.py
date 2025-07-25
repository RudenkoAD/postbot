from dataclasses import asdict
import os
from pymongo import MongoClient
from pymongo.collection import Collection

from common.API.mongoApi import Group, User

MONGO_DB_URI = os.getenv("MONGO_DB_URI", "mongodb://mongodb:27017/")
MONGO_DB_NAME = "postbot_db"


class MongoInterface:
    """
    MongoDB interface for managing users and groups.
    This class provides methods to insert users and groups, manage their relationships,
    and retrieve user and group information.
    It uses the pymongo library to interact with MongoDB.
    Attributes:
        client (MongoClient): MongoDB client instance.
        db (Database): MongoDB database instance.

    Methods:
        get_collection(collection_name: str) -> Collection:
            Returns a MongoDB collection by name.
        insert_user(user: User):
            Inserts a user document into the "users" collection.
        insert_group(group: dict):
            Inserts a group document into the "groups" collection.
        add_group_user_link(group_id: str, user_id: int):
            Adds a link between a group and a user by updating both collections.
        remove_group_user_link(group_id: str, user_id: int):
            Removes a link between a group and a user by updating both collections.
        get_user_groups(user_id: int) -> list[str]:
            Retrieves a list of group IDs associated with a user.
        get_group_users(group_id: str) -> list[int]:
            Retrieves a list of user IDs associated with a group.
    """

    def __init__(self):
        self.client = MongoClient(MONGO_DB_URI)
        self.db = self.client[MONGO_DB_NAME]

    def get_collection(self, collection_name: str) -> Collection:
        return self.db[collection_name]

    def insert_user(self, user: User):
        """
        Insert a user document into the collection.
        :param collection: MongoDB collection
        :param user: User document to insert
        """
        collection_name = "users"
        collection = self.get_collection(collection_name)
        collection.insert_one(asdict(user))

    def insert_group(self, group: dict):
        """
        Insert a group document into the collection.
        :param collection: MongoDB collection
        :param group: Group document to insert
        """
        collection_name = "groups"
        collection = self.get_collection(collection_name)
        collection.insert_one(group)

    def add_group_user_link(self, group_id: str, user_id: int):
        """
        Add a link between a group and a user.
        :param collection: MongoDB collection
        :param group_id: Group ID
        :param user_id: User ID
        """
        group_collection_name = "groups"
        collection = self.get_collection(group_collection_name)
        collection.update_one({"group_id": group_id}, {"$addToSet": {"users": user_id}})
        user_collection_name = "users"
        user_collection = self.get_collection(user_collection_name)
        user_collection.update_one(
            {"user_id": user_id}, {"$addToSet": {"groups": group_id}}
        )

    def remove_group_user_link(self, group_id: str, user_id: int):
        """
        Remove a link between a group and a user.
        :param collection: MongoDB collection
        :param group_id: Group ID
        :param user_id: User ID
        """
        group_collection_name = "groups"
        collection = self.get_collection(group_collection_name)
        collection.update_one({"group_id": group_id}, {"$pull": {"users": user_id}})
        user_collection_name = "users"
        user_collection = self.get_collection(user_collection_name)
        user_collection.update_one(
            {"user_id": user_id}, {"$pull": {"groups": group_id}}
        )

    def get_user_groups(self, user_id: int) -> list[str]:
        """
        Get a list of groups for a user.
        :param user_id: User ID
        :return: List of group IDs
        """
        user_collection_name = "users"
        collection = self.get_collection(user_collection_name)
        user = collection.find_one({"user_id": user_id})
        return user.get("groups", []) if user else []

    def get_group_users(self, group_id: str) -> list[int]:
        """
        Get a list of users for a group.
        :param group_id: Group ID
        :return: List of user IDs
        """
        group_collection_name = "groups"
        collection = self.get_collection(group_collection_name)
        group = collection.find_one({"group_id": group_id})
        return group.get("users", []) if group else []
