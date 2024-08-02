import pandas as pd
from src.log_manager import LogManager


class LoadCSVtoRawOrders:
    def __init__(self, integration_id, collection_incoming_raw_items, csv_file, chunk_size=10000) -> None:
        self.integration_id = integration_id
        self.collection_incoming_raw_items = collection_incoming_raw_items
        self.chunk_size = chunk_size
        self.csv_file = csv_file
        self.set_logs_default()

    def set_logs_default(self):
        self.logs = LogManager("RawOrder")
        self.logs_data = {
            "chunk_size": self.chunk_size,
            "integration_id": self.integration_id,
            "csv_file": self.csv_file,
            "tracer": []
        }

    def finish_logs(self):
        if len(self.logs_data["tracer"]) == 0:
            self.logs_data["tracer"].append("Data 100% loaded without erros!")
        else:
            self.logs_data["tracer"].append(
                f"Data loaded without {len(self.logs_data['tracer'])} erros!")

        self.logs.save_log(self.logs_data)

    def get_order_id_by_brand(self, data):
        return data.get("order_id")

    def formatOrder(self, data):
        return {
            "partnerId": self.integration_id,
            "orderId": self.get_order_id_by_brand(data),
            "orderPartnerData": data,
        }

    def add_orders(self, data_chunk):
        count = 0
        formatted_orders = []

        for _, row in data_chunk.iterrows():
            formatted_order = self.formatOrder(row.to_dict())
            formatted_orders.append(formatted_order)
            count += 1

        if formatted_orders:
            self.collection_incoming_raw_items.insert_many(formatted_orders)

        print(f"Inserted {count} orders successfully.")

    def run(self, start_chunk=0):
        count_chunk = 0
        for data_chunk in pd.read_csv(self.csv_file, sep=",", chunksize=self.chunk_size):
            count_chunk += 1
            if count_chunk <= start_chunk:
                continue
            try:
                self.add_orders(data_chunk)
            except Exception as e:
                self.logs_data["tracer"].append(
                    f"Erro in count_chunk {count_chunk}: {e}")

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

    load = LoadCSVtoRawOrders(integration_id, csv_file, chunk_size)
    load.run()
