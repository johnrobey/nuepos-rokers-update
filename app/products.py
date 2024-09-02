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
        logging.info(f"3/9: Found {len(epos_products)} EPOS products")
        web_products = read_web_products(webCursor)
        logging.info(f"4/9: Found {len(web_products)} web products")

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
        select p.Id, cast(p.Sku as int) as Sku, m.Name as 'Brand', p.Name, p.Deleted, p.Price, p.ProductCost, p.StockQuantity, p.Gtin, p.Weight, p.Published, m.Id as 'BrandId', p.DisableBuyButton
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
    deletecount = 0
    for web_product in web_products:

        # Does the web_product exist in epos_products
        if not any(
            epos_product["sku"] == web_product["Sku"] for epos_product in epos_products
        ):
            # does not exist
            deletecount += 1
            # print(f'WEB NOT IN EPOS: {web_product["Name"]} ({web_product["Sku"]})')
            delete_web_product(webConnection, webCursor, web_product["Sku"])

        else:
            # Find web product in epos product
            for epos_product in epos_products:

                if web_product["Sku"] == epos_product["sku"]:
                    found += 1

                    # print("¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬")
                    # pprint.pp(web_product)
                    # print("¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬")
                    if check_product_needs_update(web_product, epos_product):
                        updated += 1
                        web_product_id = web_product["Id"]

                        update_web_product(
                            webConnection, webCursor, epos_product, web_product_id
                        )

                    break

    # Check for new epos products not in web products
    new_product_count = 0
    for epos_product in epos_products:
        if not any(
            web_product["Sku"] == epos_product["sku"] for web_product in web_products
        ):
            # Found new epos product
            create_web_product(webConnection, webCursor, epos_product)
            new_product_count += 1

    logging.info(f"5/9: Updated {updated} web products")
    logging.info(f"6/9: Deleted {deletecount} web products")
    logging.info(f"7/9: New {new_product_count} web products")

    return


def check_product_needs_update(web_product, epos_product):
    # Run through value comparisons to check if a product needs to be updated
    if web_product["Price"] != epos_product["ro_sell"]:
        return True

    if web_product["StockQuantity"] != epos_product["soh"]:
        return True

    if web_product["Brand"] == None and epos_product["brand"] != "Unbranded":
        return True

    if web_product["Brand"] != epos_product["brand"] and web_product["Brand"] != None:
        logging.debug(
            f'BRAND DIFFERENT: {web_product["Name"]} ({web_product["Sku"]}) - WBRAND: {web_product["Brand"]} - EPOSBRAND: {epos_product["brand"]}'
        )
        return True

    if web_product["DisableBuyButton"] == 0 and epos_product["collect"] == 0:
        logging.debug(
            f'STORE ONLY - WEB BUY ENABLED: {web_product["Name"]} ({web_product["Sku"]})'
        )
        return True

    return False


def update_web_product(webConnection, webCursor, epos_product, web_product_id):
    # Check brand before updating
    # if epos_product["brand"] != "Unbranded" or epos_product["brand"] == None:
    check_product_brand(webConnection, webCursor, epos_product, web_product_id)

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

    if epos_product["collect"] == 0:
        disableBuyButton = 1
        orderMaximumQuantity = 0

    weight = epos_product["weight"]
    price = epos_product["ro_sell"]
    barcode = epos_product["barcode"]

    sql = f"update Product set Deleted = {deleted}, StockQuantity = {soh}, Price = {price}, Gtin = '{barcode}', Weight = {weight}, OrderMaximumQuantity = {orderMaximumQuantity}, DisableBuyButton = {disableBuyButton}, IsShipEnabled = 0 where Sku = '{sku}'"

    logging.debug(f"SQL:{sql}")
    webCursor.execute(sql)
    webConnection.commit()

    return


def check_product_brand(webConnection, webCursor, epos_product, product_id):
    # Check brand exists
    if epos_product["brand"] == None:
        return

    brand = read_brand_record(webConnection, webCursor, epos_product["brand"])
    if brand:
        # Brand exists
        brand_id = brand.Id
        # Check if product brand link record exists
        if check_product_brand_link_record(
            webConnection, webCursor, brand_id, product_id
        ):
            return
        # Link record does not exist
        create_product_brand_link_record(webConnection, webCursor, brand_id, product_id)
    else:
        # Brand does not exist
        brand_id = create_new_brand(webConnection, webCursor, epos_product["brand"])
        get_new_brand = read_brand_record(
            webConnection, webCursor, epos_product["brand"]
        )
        if get_new_brand:
            brand_id = get_new_brand.Id
            create_product_brand_link_record(
                webConnection, webCursor, brand_id, product_id
            )

    return


def create_product_brand_link_record(webConnection, webCursor, brand_id, product_id):
    sql = f"insert into Product_Manufacturer_Mapping (ProductId, ManufacturerId, IsFeaturedProduct, DisplayOrder) values ({product_id}, {brand_id}, 0, 1)"
    webCursor.execute(sql)
    webConnection.commit()
    return


def check_product_brand_link_record(webConnection, webCursor, brand_id, product_id):
    sql = f"select top 1 * from Product_Manufacturer_Mapping where ProductId = {product_id}"
    webCursor.execute(sql)
    link_record = webCursor.fetchone()
    if link_record:
        # Record exists so check it has the correct brand
        if link_record.ManufacturerId != brand_id:
            update_product_brand_link_record(
                webConnection, webCursor, brand_id, product_id
            )
        return True
    return False


def update_product_brand_link_record(webConnection, webCursor, brand_id, product_id):
    sql = f"update Product_Manufacturer_Mapping set ManufacturerId = {brand_id} where ProductId = {product_id}"

    logging.debug(f"UPDATE PRODUCT BRAND LINK SQL:{sql}")
    webCursor.execute(sql)
    webConnection.commit()
    return


def create_new_brand(webConnection, webCursor, brand_name):
    print(f"Creating new brand: {brand_name}")
    brand_name = brand_name.replace("'", "''")
    sql = f"insert into Manufacturer (Name, ManufacturerTemplateId, PictureId, PageSize, AllowCustomersToSelectPageSize, PageSizeOptions, SubjectToAcl, LimitedToStores, Published, Deleted, DisplayOrder, CreatedOnUtc, UpdatedOnUtc, PriceRangeFiltering, PriceFrom, PriceTo, ManuallyPriceRange) values ('{brand_name}', 1, 0, 12, 1, '12, 24, 48', 0, 0, 1, 0, 0, getdate(), getdate(), 1, 0, 10000, 0)"
    webCursor.execute(sql)
    webConnection.commit()
    return


def read_brand_record(webConnection, webCursor, find_brand):
    search_brand = find_brand.replace("'", "''")
    sql = f"select top 1 * from Manufacturer where Name = '{search_brand}'"
    webCursor.execute(sql)
    brand = webCursor.fetchone()
    if brand:
        return brand
    return False


def delete_web_product(webConnection, webCursor, sku):
    # Delete product by setting deleted field to 1
    sql = f"update Product set Deleted = 1 where Sku = '{sku}'"
    webCursor.execute(sql)
    webConnection.commit()
    return


def create_web_product(webConnection, webCursor, product):

    web_product_id = check_web_product_deleted(webCursor, product["sku"])
    if web_product_id != 0:
        logging.debug(
            f'Old product {product["sku"]} reinstated  - web product id {web_product_id}'
        )
        update_web_product(webConnection, webCursor, product, web_product_id)
        return

    # Insert new product record
    productName = product["product"]
    productName = productName.replace("'", "''")
    sku = product["sku"]
    soh = product["soh"]
    sell = product["ro_sell"]
    cost = product["cost"]
    weight = product["weight"]
    barcode = product["barcode"]
    if product["tax_rate"] == 20:
        taxRate = 1
    elif product["tax_rate"] == 5:
        taxRate = 2
    else:
        taxRate = 3

    sql = f"""insert into Product (ProductTypeId, ParentGroupedProductId, VisibleIndividually, [Name], ProductTemplateId, VendorId, ShowOnHomepage, AllowCustomerReviews, ApprovedRatingSum, NotApprovedRatingSum, ApprovedTotalReviews, NotApprovedTotalReviews, SubjectToAcl, LimitedToStores, Sku, ManufacturerPartNumber, Gtin, IsGiftCard, GiftCardTypeId, IsDownload, DownloadId, UnlimitedDownloads, MaxNumberOfDownloads, DownloadActivationTypeId, HasSampleDownload, SampleDownloadId, HasUserAgreement, IsRecurring, RecurringCycleLength, RecurringCyclePeriodId, RecurringTotalCycles, IsRental, RentalPriceLength, RentalPricePeriodId, IsShipEnabled, IsFreeShipping, ShipSeparately, AdditionalShippingCharge, DeliveryDateId, IsTaxExempt, TaxCategoryId, IsTelecommunicationsOrBroadcastingOrElectronicServices, ManageInventoryMethodId, ProductAvailabilityRangeId, UseMultipleWarehouses, WarehouseId, StockQuantity, DisplayStockAvailability, DisplayStockQuantity, MinStockQuantity, LowStockActivityId, NotifyAdminForQuantityBelow, BackorderModeId, AllowBackInStockSubscriptions, OrderMinimumQuantity, OrderMaximumQuantity, AllowAddingOnlyExistingAttributeCombinations, NotReturnable, DisableBuyButton, DisableWishlistButton, AvailableForPreOrder, CallForPrice, Price, OldPrice, ProductCost, CustomerEntersPrice, MinimumCustomerEnteredPrice, MaximumCustomerEnteredPrice, BasepriceEnabled, BasepriceAmount, BasepriceUnitId, BasepriceBaseAmount, BasepriceBaseUnitId, MarkAsNew, HasTierPrices, HasDiscountsApplied, [Weight], [Length], Width, Height, DisplayOrder, Published, Deleted, CreatedOnUtc, UpdatedOnUtc, RequireOtherProducts, AutomaticallyAddRequiredProducts)
        VALUES (5, 0, 1, '{productName}', 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, '{sku}', '-', '{barcode}', 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, {taxRate}, 0, 1, 0, 0, 0, {soh}, 1, 0, 0, 1, 1, 0, 0, 1, 10000, 0, 0, 0, 0, 0, 0, {sell}, 0, {cost}, 0, 0, 10000, 0, 0, 1, 0, 1, 0, 0, 0, {weight}, 0, 0, 0, 0, 0, 0, getdate(), getdate(), 0, 0)
        """
    webCursor.execute(sql)
    webConnection.commit()

    return


def check_web_product_deleted(webCursor, sku):
    sql = f"select id, sku, name from Product where sku='{sku}'"
    webCursor.execute(sql)
    product = webCursor.fetchone()
    if product:
        return product.id

    return 0
