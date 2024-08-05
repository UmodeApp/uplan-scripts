from src.mongo_integration import mongo_connection
from typing import Callable
from datetime import datetime
from src.mongo_integration.mongo_connection import remove_none_keys


class FormatOrdersColmeia:
    ID_ECOMERCE = 15

    def __init__(self) -> None:
        pass

    def select_sales_channel(self, loja_id):
        return 1 if loja_id == self.ID_ECOMERCE else 2

    def format_price(self, price):
        if not price:
            return 0
        price = price.replace(",", ".")
        price = float(price)
        return price

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
        if not value:
            return 0
        value = self.format_price(value)
        value = value * 100
        return int(value)

    def run(self, order_group):
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
                "quantity": int(document.get("orderPartnerData", {}).get("QT_VENDIDA", 1)),
                "refId": document.get("orderPartnerData", {}).get("DS_GRUPO").replace('"', ''),
                "itemListPrice": int(itemListPrice),
                "itemSalePrice": int(itemSalePrice),
                "itemDiscount": int(itemDiscount),
                "itemPartnerInstoreSKU": document.get("orderPartnerData", {}).get("CD_PRODUTO", ""),
                "color":  document.get("orderPartnerData", {}).get("DS_COR").replace('"', ''),
            }
            order_items.append(item)

        orderTotalListValue = sum(item["itemListPrice"] * item["quantity"]
                                  for item in order_items)
        orderTotalDiscount = sum(
            item["itemDiscount"] * item["quantity"] for item in order_items)
        orderTotalNetValue = sum(
            item["itemSalePrice"] * item["quantity"] for item in order_items)

        order = {
            "partnerId": order_group["partnerId"],
            "orderId": order_group["orderId"],
            "orderStatus": self.select_order_status(),
            "orderCreationDate": self.parse_date(order_group["documents"][0].get("orderPartnerData", {}).get("DT_TRANSF")),
            "orderInvoiceDate": self.parse_date(order_group["documents"][0].get("orderPartnerData", {}).get("DT_TRANSACAO")),
            "orderTotals": {
                "orderTotalListValue": int(orderTotalListValue),
                "orderTotalDiscount": int(orderTotalDiscount),
                "orderTotalNetValue": int(orderTotalNetValue),
                "orderTotalShippingValue": 0
            },
            "orderItems": order_items,
            "orderSalesChannel": self.select_sales_channel(document.get("orderPartnerData", {}).get("CD_LOJA", 0)),
        }
        return remove_none_keys(order)


def selecionar_class_brand(integration_id) -> Callable[[list], dict]:
    return FormatOrdersColmeia


def refresh_incoming_orders(query, integration_id,):
    count = 0

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

    order_groups = list(collection_incoming_raw_items.aggregate(pipeline))

    for order_group in order_groups:
        new_format = selecionar_class_brand(integration_id)(order_group)
        existing_document = collection_incoming_items.find_one(
            {"orderId": order_group["orderId"]})

        if existing_document:
            count += 1
            collection_incoming_items.update_one(
                {"_id": existing_document["_id"]},
                {"$set": new_format},
                upsert=True
            )
        else:
            count += 1
            collection_incoming_items.insert_one(new_format)

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
    query = {"partnerId": integration_id}

    parserColmeia = FormatOrdersColmeia()
    refresh_incoming_orders(query, parserColmeia.run)
