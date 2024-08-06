from src.mongo_integration import mongo_connection
from pymongo.collection import Collection
from typing import Any, Dict
from datetime import datetime
import os


class LogManager:
    DB_NAME = "Logs"
    COLLECTIONS_NAME = {
        "RawOrder": "CsvToRawOrder",
        "IncomingOrder": "IncomingRawOrderLogs",
        "IncomingItems": "IncomingRawOrderLogs",
        "PipelineItems": "PipelineItems",
        "PipelineOrders": "PipelineOrders",
        "create_incoming_raw_items": "create_incoming_raw_items",
    }

    def __init__(self, stage, id_execution) -> None:
        self.stage = stage
        self.db_logs = self._connect_to_mongodb(
            os.getenv("UPLAN_URI_MONGO"), self.DB_NAME)
        self.collection_name = self.COLLECTIONS_NAME[stage]
        self.collection: Collection = self.db_logs[self.collection_name]
        self.id_execution = id_execution

    def _connect_to_mongodb(self, uri: str, db_name: str):
        """Establish a connection to the MongoDB database."""
        db = mongo_connection.connect_to_mongodb(uri)
        return db[self.DB_NAME]

    def save_log(self, log_data: Dict[str, Any]) -> None:
        """Save a log entry to the database."""
        log_entry = self._format_log_entry(log_data)
        self.collection.insert_one(log_entry)

    def _format_log_entry(self, log_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format the log entry to match the desired schema."""
        return {
            "id_execution": self.id_execution,
            "timestamp": datetime.utcnow(),
            "stage": self.stage,
            "context": log_data,
        }
