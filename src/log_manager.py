from src.mongo_integration import mongo_connection
from pymongo.collection import Collection
from typing import Any, Dict
from datetime import datetime
import os


class LogManager:
    DB_NAME = "Logs"
    COLLECTIONS_NAME = {
        "RawOrder": "CsvToRawOrder",
        "create_incoming_orders": "create_incoming_orders",
        "IncomingItems": "IncomingRawOrderLogs",
        "PipelineItems": "PipelineItems",
        "transpose_incoming_order": "transpose_incoming_order",
        "create_incoming_raw_items": "create_incoming_raw_items",
    }

    def __init__(self, stage, process_id, default_log) -> None:
        self.stage = stage
        self.db_logs = self._connect_to_mongodb(
            os.getenv("UPLAN_URI_MONGO"))
        self.collection_name = self.COLLECTIONS_NAME[stage]
        self.collection: Collection = self.db_logs[self.collection_name]
        self.process_id = process_id
        self.default_log = default_log
        self.set_main_log()

    def _connect_to_mongodb(self, uri: str):
        """Establish a connection to the MongoDB database."""
        db = mongo_connection.connect_to_mongodb(uri)
        return db[self.DB_NAME]

    def set_main_log(self):
        data = self._format_log_entry(self.default_log)
        self.context = data["context"]
        log_obj = self.collection.insert_one(data)
        self.log_id = log_obj.inserted_id

    def _format_log_entry(self, log_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format the log entry to match the desired schema."""
        formatted_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "stage": self.stage,
            "processId": self.process_id,
            "context": {},
            "createdAt": formatted_date,
        }
        log_entry.update(log_data)
        return log_entry

    def update_main_log(self) -> None:
        """Update the main log entry with a new context."""
        formatted_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        updated_log_entry = {
            "context": self.context,
            "updatedAt": formatted_date
        }
        self.collection.update_one(
            {"_id": self.log_id},
            {"$set": updated_log_entry}
        )

    def update_context(self, context):
        self.context = context

    def get_context(self):
        return self.context


#         # Uso do LogManager
# log_manager = LogManager(stage="IncomingOrder",
#                          id_execution="exec123", log_base={"initial": "data"})
# log_manager.update_main_log(new_context={"updated": "context"})
