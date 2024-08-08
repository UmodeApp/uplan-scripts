import os
import time
from dotenv import load_dotenv
from src.mongo_integration import mongo_connection
from src.controllers.create_incoming_raw_orders import LoadCSVtoRawOrders
from src.controllers.create_live_items import copyDocumentsToNewCollection
from src.controllers.create_incoming_orders import CreateIncomingOrders
from src.log_manager import LogManager

load_dotenv()


class PipelineDataStock:
    def __init__(self, integration_id,
                 path_stock_file=None,
                 process_id=str(time.time()).replace(".", "")
                 ) -> None:

        self.integration_id = integration_id
        self.path_stock_file = path_stock_file
        self.process_id = process_id
        self.setup()

    def setup(self):
        uri = os.getenv("UPLAN_URI_MONGO")
        uri_anubis = os.getenv("UPLAN_URI_MONGO_ANUBIS")
        self.db_athemis = mongo_connection.connect_to_mongodb(uri)
        self.db_anubis = mongo_connection.connect_to_mongodb(uri_anubis)
        self.set_logs_default()

    def set_logs_default(self):
        default_log = {
            "integration_id": self.integration_id,
            "configuration": {
                "path_stock_file": self.path_stock_file,
            }
        }
        self.logs = LogManager("transpose_incoming_stock",
                               self.process_id, default_log)

    def step1(self):
        load = LoadCSVtoRawOrders(
            integration_id,
            self.process_id,
            self.db_athemis["IncomingRawData"]["IncomingRawOrders"],
            self.path_orders_to_item_slim,
            self.chunk_size
        )
        load.run(self.start_chunk)

    def step2(self):
        orderIncoming = CreateIncomingOrders(self.integration_id,
                                             self.process_id,
                                             self.db_athemis["IncomingRawData"]["IncomingRawOrders"],
                                             self.db_athemis["Incoming"]["Orders"])
        orderIncoming.run()

    def step3(self):
        collection_incoming_stock = self.db_athemis["Incoming"]["Stock"]
        collection_live_stock = self.db_anubis["Items"]["Stock"]

        copyDocumentsToNewCollection(integration_id,
                                     self.process_id,
                                     collection_incoming_stock,
                                     collection_live_stock
                                     )

    def run(self):
        # try:
        print("================= step1 ======================")
        # self.step1()
        print("================= step2 ======================")
        self.step2()
        print("================= step3 ======================")
        self.step3()
        print("================= Process Finished ======================")
        # except Exception as e:
        #     self.logs_data["tracer"][
        #         "error"] = f"Pipeline execution failed with error: {str(e)}"
        # finally:
        #     self.logs.save_log(self.logs_data)

    def run_to_fix(self, process_id_re):
        CreateIncomingOrders.re_run_fails_chunck(self.process_id, process_id_re, self.db_athemis)


# GERAL
integration_id = "666788e0eb8f5b0ac6f826cc"
process_id = "17229622640886564"

# STEP 1
path_orders_to_item_slim = 'files/vendas_240710.csv'
chunk_size = 5000
start_chunk = 0

# STEP 2

pipe = PipelineDataStock(
    integration_id, path_orders_to_item_slim, chunk_size, start_chunk)
pipe.run_to_fix(process_id)
