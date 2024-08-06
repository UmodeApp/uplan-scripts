from datetime import datetime
from src.mongo_integration import mongo_connection
from src.mongo_integration.mongo_connection import remove_none_keys
import math


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

    def __init__(self, integration_id, process_id, collection_incoming_raw_items, collection_incoming_orders):
        self.integration_id = integration_id
        self.process_id = process_id
        self.collection_incoming_raw_items = collection_incoming_raw_items
        self.collection_incoming_orders = collection_incoming_orders
        self.handler_process_brand = self.get_process_brand()

    def get_process_brand(self):
        """ 
            This function must search on database for integration_id brand
        and return the correct format class to the brand.
        """
        return self.INTEGRATIONS["colmeia"]()

    def run(self):
        query = {"partnerId": self.integration_id,
                 "processId": self.process_id}

        pipeline = [
            {"$match": query},
            {"$sort": {"createdAt": 1}},
            {"$limit": 1000},
            {"$group": {
                "_id": "$orderId",
                "orderId": {"$first": "$orderId"},
                "partnerId": {"$first": "$partnerId"},
                "documents": {"$push": "$$ROOT"}
            }}
        ]

        order_groups = list(self.collection_incoming_raw_items.aggregate(
            pipeline, allowDiskUse=True))

        count = 0
        for order_group in order_groups:
            new_format = self.handler_process_brand.format(
                order_group, self.process_id)
            existing_document = self.collection_incoming_orders.find_one(
                {"orderId": order_group["orderId"]})

            if existing_document:
                count += 1
                self.collection_incoming_orders.update_one(
                    {"_id": existing_document["_id"]},
                    {"$set": new_format},
                    upsert=True
                )
            else:
                count += 1
                self.collection_incoming_orders.insert_one(new_format)

        print("Transformation completed successfully.")


if __name__ == "__main__":
    from src.mongo_integration import mongo_connection
    from dotenv import load_dotenv
    import os

    load_dotenv()

    uri = os.getenv("UPLAN_URI_MONGO")
    db_raw = mongo_connection.connect_to_mongodb(uri, "IncomingRawData")
    db_incoming = mongo_connection.connect_to_mongodb(uri, "Incoming")

    # Collections to search
    collection_incoming_raw_items = db_raw["IncomingRawOrders"]
    collection_incoming_items = db_incoming["Orders"]

    # Query to filter items
    integration_id = "666788e0eb8f5b0ac6f826cc"

    parserColmeia = FormatOrdersColmeia()
    refresh_incoming_orders(query, parserColmeia.run)
