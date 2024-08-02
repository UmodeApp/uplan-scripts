import pandas
from src.mongo_integration.mongo_connection import remove_none_keys


def check_exist_product_ref2(partnerId, data_ref: pandas.DataFrame, db_instance):
    # Collection to search
    collection = db_instance["Items"]

    # Get unique SKUs and reference codes from the reference data
    sku_data = data_ref.set_index('sku')['CD_REF'].to_dict()

    # Fetch all documents from the collection
    all_items = collection.find(
        {"partnerId": partnerId}, {"itemPartnerEcomSKUArray": 1, "itemPartnerInstoreSKUArray": 1, "itemPartnerData": 1})

    # Convert to a set for quick lookup
    refers_on_database = set()
    skus_on_database = set()

    count_not_found = 1
    for doc in all_items:
        partner_data = doc.get("itemPartnerData", {})
        ref_skus = partner_data.get("refSkus", [])

        # Adding reference SKU to the set
        if ref_skus:
            refers_on_database.add(ref_skus[0])

        # Adding SKUs to the set
        ecom_skus = doc.get("itemPartnerEcomSKUArray", [])
        instore_skus = doc.get("itemPartnerInstoreSKUArray", [])

        if ref_skus not in refers_on_database or instore_skus not in skus_on_database:
            count_not_found += 1
            list_missing_refer.append((ref_skus, instore_skus))

    print(f"Number of SKU batches not found: {count_not_found}")
    return list_missing_refer


def check_exist_product_ref(data_ref: pandas.DataFrame, db_instance):
    # Collection to search
    collection = db_instance["Items"]

    # Get unique SKUs and reference codes from the reference data
    sku_data = data_ref.set_index('CD_REF')['sku'].to_dict()

    # Fetch all documents from the collection
    all_items = collection.find(
        {}, {"itemPartnerEcomSKUArray": 1, "itemPartnerInstoreSKUArray": 1, "partnerData": 1})

    # Convert to a set for quick lookup
    refers_on_database = set()
    skus_on_database = set()

    for doc in all_items:
        partner_data = doc.get("partnerData", {})
        ref_skus = partner_data.get("refSkus", [])

        # Adding reference SKU to the set
        if ref_skus:
            refers_on_database.add(ref_skus[0])

        # Adding SKUs to the set
        ecom_skus = doc.get("itemPartnerEcomSKUArray", [])
        instore_skus = doc.get("itemPartnerInstoreSKUArray", [])

        skus_on_database.update(ecom_skus)
        skus_on_database.update(instore_skus)

    # Count how many SKU batches are missing
    count_not_found = 0
    list_missing_refer = []

    for refer, sku in sku_data.items():
        if refer not in refers_on_database or sku not in skus_on_database:
            count_not_found += 1
            list_missing_refer.append((refer, sku))

    print(f"Number of SKU batches not found: {count_not_found}")
    return list_missing_refer


def create_item_slim(row_sell, sku_list, integration_id):
    partner_data = {
        "refSkus": [row_sell.get("CD_REF")],
        "skuSpecifications": [
            {"FieldName": "Cor", "FieldValues": [row_sell.get("color")]}
        ]
    }
    item = {
        "partnerId": integration_id,
        "itemHeadline": row_sell.get("name"),
        "itemPartnerData": partner_data,
        "itemPartnerInstoreSKUArray": sku_list,
    }
    return remove_none_keys(item)


def refresh_products_ref(integration_id, db_incoming, df_orders: pandas.DataFrame, skus_refs: list):
    collectionItems = db_incoming["Items"]

    df_orders: pandas.DataFrame

    refes = df_orders.groupby('CD_REF')['sku'].apply(
        lambda x: list(set(x))).to_dict()

    count = 0
    for sku_ref in skus_refs:
        ref, sku = sku_ref

        # Find the first row that matches the SKU and reference
        row = df_orders[(df_orders["sku"] == sku) &
                        (df_orders["CD_REF"] == ref)]
        if len(row) == 0:
            continue

        row = row.iloc[0]

        # Create the item format
        new_format = create_item_slim(row, refes.get(ref, []), integration_id)

        # Insert the new item into the database
        collectionItems.insert_one(new_format)

        count += 1

    print(f"{count} items inserted into the database.")


if __name__ == "__main__":
    import os
    from src.mongo_integration import mongo_connection
    from dotenv import load_dotenv

    load_dotenv()

    # Database conection
    uri = os.getenv("UPLAN_URI_MONGO")
    db = mongo_connection.connect_to_mongodb(uri, "Incoming")

    partnerId = "666788e0eb8f5b0ac6f826cc"
    list_missing_refer = check_exist_product_ref()
    refresh_products_ref(list_missing_refer)
