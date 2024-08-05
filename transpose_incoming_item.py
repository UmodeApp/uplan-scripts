import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from src.mongo_integration import mongo_connection
from controllers.create_incoming_item import RefreshIncomingItems
from controllers.create_live_items import copyDocumentsToNewCollection
from src.controllers import update_sku
from src.log_manager import LogManager


load_dotenv()


class PipelineDataItems:
    def __init__(self, integration_id, items_uplan=None, path_orders_to_item_slim=None) -> None:
        self.integration_id = integration_id
        self.items_uplan = items_uplan
        self.path_orders_to_item_slim = path_orders_to_item_slim
        self.setup()

    def setup(self):
        uri = os.getenv("UPLAN_URI_MONGO")
        uri_anubis = os.getenv("UPLAN_URI_MONGO_ANUBIS")
        self.db_athemis = mongo_connection.connect_to_mongodb(uri)
        self.db_anubis = mongo_connection.connect_to_mongodb(uri_anubis)
        self.set_logs_default()

    def set_logs_default(self):
        self.logs = LogManager("PipelineItems")
        self.logs_data = {
            "integration_id": self.integration_id,
            "tracer": {}
        }

    def step1(self):
        create_at_date = datetime.fromisoformat(
            '2024-06-17T04:25:56.026+00:00')

        query = {"itemBrandID": self.integration_id}

        result = RefreshIncomingItems(
            self.db_athemis["IncomingRawData"], self.db_athemis["Incoming"], query)

        self.logs_data["tracer"]["step1"] = {
            "status": result["status"],
            "processed_count": result["processed_count"],
            "errors": result["errors"]
        }

    def step2(self):
        if not self.items_uplan or not self.path_orders_to_item_slim:
            self.logs_data["tracer"]["step2"] = {
                "status": "Jumpped",
                "message": "Without files to execute."
            }
            return

        try:
            data_ref = pd.read_csv(self.items_uplan, sep=",")
            df_orders = pd.read_csv(
                self.path_orders_to_item_slim, sep=",")

            list_missing_refer = update_sku.check_exist_product_ref( data_ref, self.db_athemis["Incoming"])
            update_sku.refresh_products_ref(
                self.integration_id, self.db_athemis["Incoming"], df_orders, list_missing_refer)
        except Exception as e:
            self.logs_data["tracer"]["step2"] = {
                "status": "Error",
                "message": f"{e}"
            }
            return

    def step3(self):
        collection_incoming_items = self.db_athemis["Incoming"]["Items"]
        collection_live_items = self.db_anubis["Items"]["Items"]

        # query to filter items
        query = {"partnerId": self.integration_id}

        copyDocumentsToNewCollection(
            query, collection_incoming_items, collection_live_items)

    def run(self):
        try:
            # self.step1()
            self.step2()
            self.step3()
        except Exception as e:
            self.logs_data["tracer"][
                "error"] = f"Pipeline execution failed with error: {str(e)}"
        finally:
            self.logs.save_log(self.logs_data)


integration_id = "615f066546669c7609cd0168"
items_uplan = 'files/REF_COR-sku.csv'
path_orders_to_item_slim = 'files/[uPlan][Lojas Colmeia] Itens Vendidos.csv'
pipe = PipelineDataItems(integration_id, items_uplan, path_orders_to_item_slim)

pipe.run()
