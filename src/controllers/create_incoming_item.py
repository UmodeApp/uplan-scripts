from src.mongo_integration.mongo_connection import remove_none_keys


class FormatItemColmeia:
    def __init__(self) -> None:
        pass

    def build_item_partner_data(self, document: dict):
        skuSpecifications = document.get(
            "partnerData", {}).get("skuSpecifications", [])
        skuSpecifications_filtred = [{"FieldName": spec.get("FieldName"), "FieldValues": spec.get(
            "FieldValues")} for spec in skuSpecifications]
        partner_data = {
            "refSkus": document.get("partnerData", {}).get("refSkus"),
            "skuSpecifications": skuSpecifications_filtred
        }
        return partner_data

    def run(self, document: dict):
        item = {
            "partnerId": document.get("itemBrandID"),
            "itemOfficialImage": {
                "s10": document.get("itemOfficialImage", {}).get("s10"),
                "s200": document.get("itemOfficialImage", {}).get("s200"),
                "s400": document.get("itemOfficialImage", {}).get("s400"),
                "s600": document.get("itemOfficialImage", {}).get("s600"),
                "s1000": document.get("itemOfficialImage", {}).get("s1000"),
                "original": document.get("itemOfficialImage", {}).get("original")
            },
            "itemGenieImage": document.get("itemGenieImage"),
            "itemComposition": document.get("itemComposition", []),
            "itemHeadline": document.get("itemHeadline", ""),
            "itemDescription": document.get("ProductDescription", ""),
            "itemPartnerEcomSKUArray": document.get("itemBrandSKUArray", []),
            "itemYear": document.get("itemYear", ""),
            "itemPartnerInstoreSKUArray": document.get("itemBrandInstoreSkuArray", []),
            "fashionHint": document.get("fashionHint", ""),
            "genderId": document.get("genderId"),
            "stillImage": document.get("stillImage", False),
            "colorId": document.get("colorId"),
            "hueId": document.get("hueId", 0),
            "legacyId": {
                "$oid": str(document.get("_id", ""))
            },

            "reviewed": False,
            "itemPartnerData": self.build_item_partner_data(document),
        }
        return remove_none_keys(item)


def RefreshIncomingItems(db_raw, db_incoming, query):
    # Collections to search
    collection_incoming_raw_items = db_raw["IncomingRawItems"]
    collection_incoming_items = db_incoming["Items"]

    formatItemColmeia = FormatItemColmeia()

    count = 0
    errors = []
    for document in collection_incoming_raw_items.find(query).sort("createdAt", 1):
        try:
            new_format = formatItemColmeia.run(document)
            existing_document = collection_incoming_items.find_one(
                {"legacyId.$oid": str(document["_id"])})

            if existing_document:
                collection_incoming_items.update_one(
                    {"_id": existing_document["_id"]},
                    {"$set": new_format},
                    upsert=True
                )
            else:
                collection_incoming_items.insert_one(new_format)

            count += 1

        except Exception as e:
            errors.append({"item_id": document["_id"], "error": str(e)})

    return {
        "status": "completed" if not errors else "completed_with_errors",
        "processed_count": count,
        "errors": errors
    }


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from src.mongo_integration import mongo_connection

    load_dotenv()

    # Database connection
    uri = os.getenv("UPLAN_URI_MONGO")
    db_raw = mongo_connection.connect_to_mongodb(uri, "IncomingRawData")
    db_incoming = mongo_connection.connect_to_mongodb(uri, "Incoming")

    # Query to filter items
    integration_id = "666788e0eb8f5b0ac6f826cc"
    query = {"itemBrandID": integration_id}
    RefreshIncomingItems(db_raw, db_incoming, query)
