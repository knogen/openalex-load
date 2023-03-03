from enum import unique
from xmlrpc.client import boolean
import pymongo

class WorkSchedule:

    def __init__(self, mongodb_uri: str) -> None:
        self.client = pymongo.MongoClient(mongodb_uri)
        self.db = self.client.get_database("openalex_scheduler")
        self.db.work_key.create_index('key',background=True, unique=True)

    def get_worker_key(self, key: str) -> bool:
        doc = self.db.work_key.find_one({"key":key})
        if doc:
            return True
        else:
            return False

    def set_worker_key(self, key: str):
        try:
            self.db.work_key.insert_one({"key":key})
        except pymongo.errors.DuplicateKeyError:
            pass

    def close(self):
        self.client.close()
