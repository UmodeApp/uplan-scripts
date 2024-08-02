import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from src.mongo_integration import mongo_connection
from src.controllers.csv_orders_to_raw_orders import LoadCSVtoRawOrders
from src.controllers.incoming_to_live_items import copyDocumentsToNewCollection
from src.controllers.raw_orders_to_incoming_orders import refresh_incoming_orders
from src.log_manager import LogManager


load_dotenv()


class PipelineDadaOrders:
    def __init__(self, integration_id, path_orders_to_item_slim=None, chunk_size=10000, start_chunk=0) -> None:
        self.integration_id = integration_id
        self.path_orders_to_item_slim = path_orders_to_item_slim
        self.chunk_size = chunk_size
        self.start_chunk = start_chunk
        self.setup()

    def setup(self):
        uri = os.getenv("UPLAN_URI_MONGO")
        uri_anubis = os.getenv("UPLAN_URI_MONGO_ANUBIS")
        self.db_athemis = mongo_connection.connect_to_mongodb(uri)
        self.db_anubis = mongo_connection.connect_to_mongodb(uri_anubis)
        self.set_logs_default()

    def set_logs_default(self):
        self.logs = LogManager("PipelineOrders")
        self.logs_data = {
            "integration_id": self.integration_id,
            "tracer": {}
        }

    def step1(self):
        load = LoadCSVtoRawOrders(
            integration_id, self.db_athemis["IncomingRawData"]["IncomingRawOrders"], self.path_orders_to_item_slim, self.chunk_size)
        load.run(self.start_chunk)

    def step2(self):
        query = {"partnerId": self.integration_id}
        refresh_incoming_orders(query, self.integration_id)

    def step3(self):
        collection_incoming_orders = self.db_athemis["Incoming"]["Orders"]
        collection_live_orders = self.db_anubis["Items"]["Orders"]

        # query to filter items
        query = {"partnerId": self.integration_id}

        copyDocumentsToNewCollection(
            query, collection_incoming_orders, collection_live_orders)

    def run(self):
        try:
            self.step1()
            self.step2()
            self.step3()
        except Exception as e:
            self.logs_data["tracer"][
                "error"] = f"Pipeline execution failed with error: {str(e)}"
        finally:
            self.logs.save_log(self.logs_data)


integration_id = "666788e0eb8f5b0ac6f826cc"
path_orders_to_item_slim = 'files/[uPlan][Lojas Colmeia] Itens Vendidos.csv'
pipe = PipelineDadaOrders(integration_id, path_orders_to_item_slim)
chunk_size = 5000
start_chunk = 0
pipe.step1()
