import os
import pyodbc
import logging
import pprint


def products():
    try:
        # Connect to EPOS SQL database
        eposConnection = pyodbc.connect(os.environ["NRU-EPOSCONNECTION"])
        eposCursor = eposConnection.cursor()
        logging.debug("PRODUCTS: Connected to EPOS DB")

        # Connect to website SQL database
        webConnection = pyodbc.connect(os.environ["NRU-WEBCONNECTION"])
        webCursor = webConnection.cursor()
        logging.debug("PRODUCTS: Connected to Web DB")

        # Retreive products
        epos_products = read_epos_products(eposCursor)
        print(os.environ["NRU-EPOSCONNECTION"])
        logging.info(f"Found {len(epos_products)} EPOS products")
        web_products = read_web_products(webCursor)
        logging.info(f"Found {len(web_products)} web products")

        # Process web products
        process_web_products(
            webConnection,
            webCursor,
            epos_products,
            web_products,
        )

    except pyodbc.Error as ex:
        logging.ERROR("An error has occured processing products: ", ex)

    # Close DB connections
    webCursor.close()
    webConnection.close()
    eposCursor.close()
    eposConnection.close()
    return


def read_epos_products(eposCursor):
    # Read all products where updated datestamp > last processed datestamp
    eposCursor.execute(
        "select cast(sku as int) as sku, product, brand, status, ro_sell, cost, tax_rate, barcode, rrp, soh, weight, store, collect, delivery, date_created, last_updated, category, subcategory from epos_sync where isnumeric(sku) = 1 and (store = 1 or collect = 1)"
    )
    records = eposCursor.fetchall()

    epos_products = []
    columnNames = [column[0] for column in eposCursor.description]

    for record in records:
        epos_products.append(dict(zip(columnNames, record)))

    return epos_products


def read_web_products(webCursor):
    web_products = []

    sql = f"""
        select p.Id, cast(p.Sku as int) as Sku, m.Name as 'Brand', p.Name, p.Deleted, p.Price, p.ProductCost, p.StockQuantity, p.Gtin, p.Weight, p.Published, m.Id as 'BrandId'
        from Product p
        left join Product_Manufacturer_Mapping pmm on pmm.ProductId = p.Id
        left join Manufacturer m on m.Id = pmm.ManufacturerId
        where isnumeric(p.Sku) = 1 and p.Deleted = 0
    """

    webCursor.execute(sql)
    records = webCursor.fetchall()

    columnNames = [column[0] for column in webCursor.description]
    for record in records:
        web_products.append(dict(zip(columnNames, record)))

    # Sort webProduct by sku
    ordered_web_products = sorted(web_products, key=lambda d: d["Sku"])
    return ordered_web_products


def process_web_products(webConnection, webCursor, epos_products, web_products):
    # Loop through web products
    found = 0
    updated = 0
    for web_product in web_products:
        for epos_product in epos_products:
            if web_product["Sku"] == epos_product["sku"]:
                found += 1

                if (web_product["Price"] != epos_product["ro_sell"]) or (
                    web_product["StockQuantity"] != epos_product["soh"]
                ):
                    updated += 1
                    logging.debug(
                        f'Product: {web_product["Name"]} ({web_product["Sku"]}) - WSell:{web_product["Price"]} / ESell:{epos_product["ro_sell"]} - WSOH:{web_product["StockQuantity"]} / ESOH{epos_product["soh"]}'
                    )
                    update_web_product(webConnection, webCursor, epos_product)

                break

    logging.info(f"Found {found} web products")
    logging.info(f"Updated {updated} web products")

    return


def update_web_product(webConnection, webCursor, epos_product):
    sku = epos_product["sku"]
    if epos_product["status"] == 1:
        deleted = 0
    else:
        deleted = 1

    if epos_product["store"] == 0 and epos_product["collect"] == 0:
        deleted = 1

    if epos_product["soh"] > 0:
        soh = epos_product["soh"]
        disableBuyButton = 0
        orderMaximumQuantity = 10000
    else:
        soh = 0
        disableBuyButton = 1
        orderMaximumQuantity = 0

    weight = epos_product["weight"]
    price = epos_product["ro_sell"]
    barcode = epos_product["barcode"]

    sql = f"update Product set Deleted = {deleted}, StockQuantity = {soh}, Price = {price}, Gtin = '{barcode}', Weight = {weight}, OrderMaximumQuantity = {orderMaximumQuantity}, DisableBuyButton = {disableBuyButton}, IsShipEnabled = 1 where Sku = '{sku}'"

    logging.debug(f"SQL:{sql}")
    webCursor.execute(sql)
    webConnection.commit()

    return
