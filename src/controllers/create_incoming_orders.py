import math
from datetime import datetime
from pymongo import InsertOne, UpdateOne
from src.mongo_integration.mongo_connection import remove_none_keys
from src.log_manager import LogManager


class FormatOrdersColmeia:
    ID_ECOMERCE = 15

    def __init__(self) -> None:
        pass

    def select_sales_channel(self, loja_id):
        return 1 if loja_id == self.ID_ECOMERCE else 2

    def format_price(self, price):
        try:
            if not price:
                return 0
            price = price.replace(",", ".")
            price = float(price)
            return price
        except:
            return None

    def select_order_status(self):
        # Missing undertanding
        return 1

    def parse_date(self, date_str):
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S,%f")
        except ValueError:
            return None

    def format_money(self, value) -> int:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        value = self.format_price(value)
        value = value * 100
        return int(value)

    def format_number_or_none(self, value):
        try:
            return int(value)
        except:
            return None

    def format_number(self, value):
        try:
            return int(value)
        except:
            return 0

    def format(self, order_group, process_id):
        order_items = []

        for document in order_group["documents"]:
            itemListPrice = self.format_money(document.get(
                "orderPartnerData", {}).get("PREC_ORIGINAL"))
            itemSalePrice = self.format_money(document.get(
                "orderPartnerData", {}).get("VL_COMPRA"))
            itemDiscount = self.format_money(document.get(
                "orderPartnerData", {}).get("PR_DESCONTO"))
            item = {
                "ean": document.get("orderPartnerData", {}).get("CD_PRODUTO", ""),
                "quantity": self.format_number_or_none(document.get("orderPartnerData", {}).get("QT_VENDIDA", 1)),
                "refId": document.get("orderPartnerData", {}).get("DS_GRUPO").replace('"', ''),
                "itemListPrice": self.format_number_or_none(itemListPrice),
                "itemSalePrice": self.format_number_or_none(itemSalePrice),
                "itemDiscount": self.format_number_or_none(itemDiscount),
                "itemPartnerInstoreSKU": document.get("orderPartnerData", {}).get("CD_PRODUTO", ""),
                "color":  document.get("orderPartnerData", {}).get("DS_COR").replace('"', ''),
            }
            order_items.append(item)

        orderTotalListValue = sum(self.format_number(item["itemListPrice"]) * item["quantity"]
                                  for item in order_items)
        orderTotalDiscount = sum(
            self.format_number(item["itemDiscount"]) * item["quantity"] for item in order_items)
        orderTotalNetValue = sum(
            self.format_number(item["itemSalePrice"]) * item["quantity"] for item in order_items)

        order = {
            "partnerId": order_group["partnerId"],
            "processId": process_id,
            "orderId": order_group["orderId"],
            "orderStatus": self.select_order_status(),
            "orderCreationDate": self.parse_date(order_group["documents"][0].get("orderPartnerData", {}).get("DT_TRANSF")),
            "orderInvoiceDate": self.parse_date(order_group["documents"][0].get("orderPartnerData", {}).get("DT_TRANSACAO")),
            "orderTotals": {
                "orderTotalListValue": self.format_number(orderTotalListValue),
                "orderTotalDiscount": self.format_number(orderTotalDiscount),
                "orderTotalNetValue": self.format_number(orderTotalNetValue),
                "orderTotalShippingValue": 0
            },
            "orderItems": order_items,
            "orderSalesChannel": self.select_sales_channel(document.get("orderPartnerData", {}).get("CD_LOJA", 0)),
        }
        return remove_none_keys(order)


class CreateIncomingOrders:
    INTEGRATIONS = {
        "colmeia": FormatOrdersColmeia
    }

    def __init__(self, integration_id, process_id, collection_incoming_raw_items, collection_incoming_orders, **kwargs):
        self.integration_id = integration_id
        self.process_id = process_id
        self.collection_incoming_raw_items = collection_incoming_raw_items
        self.collection_incoming_orders = collection_incoming_orders
        self.handler_process_brand = self.get_process_brand()

        # Parâmetros com valores padrão
        self.batch_size = kwargs.get('batch_size', 1000)
        self.set_logs_default()

    def set_logs_default(self):
        default_log = {
            "integration_id": self.integration_id,
            "configuration": {
                "batch_size": self.batch_size,
            }
        }
        self.logs = LogManager("create_incoming_orders",
                               self.process_id, default_log)
        self.logs.context = {
            "tracer": [],
            "error": []
        }

    def add_logs(self, key, value):
        " Add this function on Log class with automatic dict creation keys "
        self.logs.context[key].append(value)

    def get_process_brand(self):
        """ 
            This function must search on database for integration_id brand
        and return the correct format class to the brand.
        """
        return self.INTEGRATIONS["colmeia"]()

    def get_order_ids(self, skip=0, limit=12000):
        query = {"partnerId": self.integration_id,
                 "processId": self.process_id}

        cursor = self.collection_incoming_raw_items.find(query).skip(
            skip).sort("_id", 1).limit(limit).allow_disk_use(True)
        return list(set([order["orderId"] for order in cursor]))

    def process_order_ids(self, order_ids):
        query = {
            "partnerId": self.integration_id,
            "processId": self.process_id,
            "orderId": {"$in": order_ids}
        }
        pipeline = [
            {"$match": query},
            {"$sort": {"createdAt": 1}},
            {"$group": {
                "_id": "$orderId",
                "orderId": {"$first": "$orderId"},
                "partnerId": {"$first": "$partnerId"},
                "documents": {"$push": "$$ROOT"}
            }}
        ]
        order_groups = list(self.collection_incoming_raw_items.aggregate(
            pipeline, allowDiskUse=True))

        bulk_operations = []

        for order_group in order_groups:
            new_format = self.handler_process_brand.format(
                order_group, self.process_id)
            existing_document = self.collection_incoming_orders.find_one(
                {"orderId": order_group["orderId"]})

            if existing_document:
                pass
                # bulk_operations.append(
                #     UpdateOne(
                #         {"_id": existing_document["_id"]},
                #         {"$set": new_format},
                #         upsert=True
                #     )
                # )
            else:
                bulk_operations.append(InsertOne(new_format))

        return bulk_operations

    def bulk_write_operations(self, bulk_operations):
        current_time = datetime.now().strftime("%H:%M:%S")
        if bulk_operations:
            try:
                self.collection_incoming_orders.bulk_write(bulk_operations)
                msg = f"Iteration {self.iteration} - Saved {len(bulk_operations)} Orders"
                print(msg)
                self.add_logs("tracer", msg)
                self.logs.update_main_log()
                return
            except Exception as e:
                self.log_erro_save(e)
                return

        msg = f"{current_time} - Skip Iteration {self.iteration} - Saved {len(bulk_operations)} Orders"
        print(msg)
        self.add_logs("tracer", msg)
        self.logs.update_main_log()

    def run(self, skip=0, iterations_run=[]):
        self.add_logs("tracer", "Start Process!")

        skip = skip * self.batch_size
        self.iteration = 0
        list_orders_id = []
        while True:
            self.iteration += 1
            if iterations_run and self.iteration not in iterations_run:
                continue
            try:
                order_ids = self.get_order_ids(
                    skip=skip, limit=self.batch_size)

                if not order_ids:
                    break

                current_order_ids = []

                for order_id in order_ids:
                    if order_id not in list_orders_id:
                        current_order_ids.append(order_id)

                list_orders_id += current_order_ids
                bulk_operations = self.process_order_ids(current_order_ids)

                self.bulk_write_operations(bulk_operations)

            except Exception as e:
                self.log_erro_save(e)

            skip += self.batch_size

        print("Transformation completed successfully.")
        print(f"Processed {len(list_orders_id)} documents.")

    def log_erro_save(self, e):
        current_time = datetime.now().strftime("%H:%M:%S")
        msg = f"{current_time} - Iteration {self.iteration} - Erro: {e}"
        print(msg)
        self.add_logs("tracer", msg)
        self.add_logs("error", self.iteration)
        self.logs.update_main_log()
