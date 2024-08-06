import pandas as pd
from src.log_manager import LogManager


def colmeia_get_order_id(data):
    return str(data.get("NR_TRANSACAO", data.get("order_id")))


class LoadCSVtoRawOrders:
    INTEGRATIONS = {
        "colmeia": colmeia_get_order_id
    }

    def __init__(self, integration_id, process_id, collection_incoming_raw_items, csv_file, chunk_size=10000) -> None:
        self.integration_id = integration_id
        self.collection_incoming_raw_items = collection_incoming_raw_items
        self.chunk_size = chunk_size
        self.csv_file = csv_file
        self.process_id = process_id
        self.set_logs_default()
        self.handler_process_brand = self.get_process_brand()

    def get_process_brand(self):
        """ 
            This function must search on database for integration_id brand
        and return the correct format class to the brand.
        """
        return self.INTEGRATIONS["colmeia"]

    def set_logs_default(self):
        self.logs = LogManager("RawOrder", self.process_id)
        self.logs_data = {
            "chunk_size": self.chunk_size,
            "integration_id": self.integration_id,
            "csv_file": self.csv_file,
            "tracer": []
        }

    def count_rows_in_chunks(self):
        row_count = 0
        for chunk in pd.read_csv(self.csv_file, sep=",", chunksize=self.chunk_size):
            row_count += len(chunk)
        return row_count

    def finish_logs(self):
        self.logs_data["tracer"].append("Finished execution!")
        self.logs.save_log(self.logs_data)

    def format_order(self, data):
        order = {
            "partnerId": self.integration_id,
            "orderId": self.handler_process_brand(data),
            "processId": self.process_id,
            "orderPartnerData": data,
        }
        return order

    def add_orders(self, data_chunk):
        count = 0
        formatted_orders = []

        for _, row in data_chunk.iterrows():
            formatted_order = self.format_order(row.to_dict())
            formatted_orders.append(formatted_order)
            count += 1

        if formatted_orders:
            self.collection_incoming_raw_items.insert_many(formatted_orders)

        return count

    def run(self, start_chunk=0):
        count_chunk, logs_chunck = 0, 0
        for data_chunk in pd.read_csv(self.csv_file, sep=";", chunksize=self.chunk_size):
            count_chunk += 1
            if count_chunk <= start_chunk:
                continue

            try:
                count = self.add_orders(data_chunk)
                print(
                    f"Chunk {count_chunk} - Inserted {count} orders successfully.")
            except Exception as e:
                self.logs_data["tracer"].append(
                    f"Error in chunk {count_chunk}: {e}")

            if count_chunk % 20 == 0:
                self.logs_data["tracer"].append(
                    f"Executions count_chunk {logs_chunck} - {count_chunk}")
                self.logs.save_log(self.logs_data)
                self.logs_data["tracer"] = []
                logs_chunck = count_chunk + 1

        print("All data has been processed and inserted.")
        self.finish_logs()


if __name__ == "__main__":
    import os
    from src.mongo_integration import mongo_connection
    from dotenv import load_dotenv

    load_dotenv()

    integration_id = "666788e0eb8f5b0ac6f826cc"
    csv_file = 'files/[uPlan][Lojas Colmeia] Itens Vendidos.csv'
    chunk_size = 5000

    # Supondo que `mongo_connection` retorna a conexão MongoDB e a coleção apropriada
    client = mongo_connection()
    db = client['your_database_name']
    collection_incoming_raw_items = db['your_collection_name']

    load = LoadCSVtoRawOrders(
        integration_id, collection_incoming_raw_items, csv_file, chunk_size)
    load.run()
