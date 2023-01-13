#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import re, traceback
import requests, json
from pymysql import connect, cursors
from tenacity import retry, wait_exponential, stop_after_attempt

class Adaptor(object):
    """Adaptor contain MySQL cursor object."""

    def __init__(self, **setting):
        self.mysql_conn = connect(
            host=setting.get("host"),
            user=setting.get("user"),
            password=setting.get("password"),
            db=setting.get("schema"),
            port=int(setting.get("port", 3306)),
            charset="utf8mb4",
            cursorclass=cursors.DictCursor,
        )
        self.mysql_cursor = self.mysql_conn.cursor()

    def __del__(self):
        if self.mysql_conn.open:
            self.disconnect()

    @property
    def mysql_conn(self):
        return self._mysql_conn

    @mysql_conn.setter
    def mysql_conn(self, mysql_conn):
        self._mysql_conn = mysql_conn

    @property
    def mysql_cursor(self):
        """MySQL Server cursor object."""
        return self._mysql_cursor

    @mysql_cursor.setter
    def mysql_cursor(self, mysql_cursor):
        self._mysql_cursor = mysql_cursor

    def disconnect(self):
        self.mysql_conn.close()

    def rollback(self):
        self.mysql_conn.rollback()

    def commit(self):
        self.mysql_conn.commit()


class Mage2Connector(object):

    GETPRODUCTIDBYSKUSQL = (
        """SELECT distinct entity_id FROM catalog_product_entity WHERE sku = %s;"""
    )

    GETROWIDBYENTITYIDSQL = (
        """SELECT distinct row_id FROM catalog_product_entity WHERE entity_id = %s;"""
    )

    ENTITYMETADATASQL = """
        SELECT eet.entity_type_id, eas.attribute_set_id
        FROM eav_entity_type eet, eav_attribute_set eas
        WHERE eet.entity_type_id = eas.entity_type_id
        AND eet.entity_type_code = %s
        AND eas.attribute_set_name = %s;"""

    ATTRIBUTEMETADATASQL = """
        SELECT DISTINCT t1.attribute_id, t2.entity_type_id, t1.backend_type, t1.frontend_input
        FROM eav_attribute t1, eav_entity_type t2
        WHERE t1.entity_type_id = t2.entity_type_id
        AND t1.attribute_code = %s
        AND t2.entity_type_code = %s;"""

    ISENTITYEXITSQL = """SELECT count(*) as count FROM {entity_type_code}_entity WHERE entity_id = %s;"""

    ISATTRIBUTEVALUEEXITSQL = """
        SELECT count(*) as count
        FROM {entity_type_code}_entity_{data_type}
        WHERE attribute_id = %s
        AND store_id = %s
        AND {key} = %s;"""

    REPLACEATTRIBUTEVALUESQL = """REPLACE INTO {entity_type_code}_entity_{data_type} ({cols}) values ({vls});"""

    UPDATEENTITYUPDATEDATSQL = """UPDATE {entity_type_code}_entity SET updated_at = UTC_TIMESTAMP() WHERE entity_id = %s;"""

    GETOPTIONIDSQL = """
        SELECT t2.option_id
        FROM eav_attribute_option t1, eav_attribute_option_value t2
        WHERE t1.option_id = t2.option_id
        AND t1.attribute_id = %s
        AND t2.value = %s
        AND t2.store_id = %s;"""

    GETOPTIONVALUESQL = """
        SELECT value
        FROM eav_attribute_option_value
        WHERE option_id = %s
        AND store_id = %s;"""

    INSERTCATALOGPRODUCTENTITYEESQL = """
        INSERT INTO catalog_product_entity
        (entity_id, created_in, updated_in, attribute_set_id, type_id, sku, has_options, required_options, created_at, updated_at)
        VALUES(0, 1, 2147483647, %s, %s, %s, 0, 0, UTC_TIMESTAMP(), UTC_TIMESTAMP());"""

    INSERTCATALOGPRODUCTENTITYSQL = """
        INSERT INTO catalog_product_entity
        (attribute_set_id, type_id, sku, has_options, required_options, created_at, updated_at)
        VALUES(%s, %s, %s, 0, 0, UTC_TIMESTAMP(), UTC_TIMESTAMP());"""

    UPDATECATALOGPRODUCTSQL = """
        UPDATE catalog_product_entity
        SET attribute_set_id = %s,
        type_id = %s,
        updated_at = UTC_TIMESTAMP()
        WHERE {key} = %s;"""

    INSERTEAVATTRIBUTEOPTIONSQL = (
        """INSERT INTO eav_attribute_option (attribute_id) VALUES (%s);"""
    )

    OPTIONVALUEEXISTSQL = """
        SELECT COUNT(*) as cnt FROM eav_attribute_option_value
        WHERE option_id = %s
        AND store_id = %s;"""

    INSERTOPTIONVALUESQL = """INSERT INTO eav_attribute_option_value (option_id, store_id, value) VALUES (%s, %s, %s);"""

    UPDATEOPTIONVALUESQL = """UPDATE eav_attribute_option_value SET value = %s WHERE option_id = %s AND store_id = %s;"""

    GETCUSTOMOPTIONIDBYTITLESQL = """
        SELECT a.option_id
        FROM catalog_product_option a
        INNER JOIN catalog_product_option_title b ON a.option_id = b.option_id
        WHERE a.product_id = %s AND b.title = %s;"""

    REPLACECUSTOMOPTIONSQL = (
        """REPLACE INTO catalog_product_option ({opt_cols}) VALUES ({opt_vals});"""
    )

    INSERTCUSTOMOPTIONTITLESQL = """
        INSERT INTO catalog_product_option_title
        (option_id, store_id, title)
        VALUES (%s,%s,%s) on DUPLICATE KEY UPDATE title = %s;"""

    INSERTCUSTOMOPTIONPRICESQL = """
        INSERT INTO catalog_product_option_price
        (option_id, store_id, price, price_type)
        VALUES (%s,%s,%s,%s) on DUPLICATE KEY UPDATE price = %s, price_type = %s;"""

    GETCUSTOMOPTIONTYPEIDBYTITLESQL = """
        SELECT a.option_type_id
        FROM catalog_product_option_type_value a
        INNER JOIN catalog_product_option_type_title b on a.option_type_id = b.option_type_id
        WHERE a.option_id = %s
        AND b.title = %s;"""

    REPLACECUSTOMOPTIONTYPEVALUESQL = """REPLACE INTO catalog_product_option_type_value ({opt_val_cols}) VALUES ({opt_val_vals});"""

    INSERTCUSTOMOPTIONTYPETITLESQL = """
        INSERT INTO catalog_product_option_type_title
        (option_type_id, store_id, title)
        VALUES (%s,%s,%s) on DUPLICATE KEY UPDATE title = %s;"""

    INSERTCUSTOMOPTIONTYPEPRICESQL = """
        INSERT INTO catalog_product_option_type_price
        (option_type_id, store_id, price, price_type)
        VALUES (%s,%s,%s,%s) on DUPLICATE KEY UPDATE price = %s, price_type = %s;"""

    DELETECUSTOMOPTIONSQL = """
        DELETE FROM catalog_product_option WHERE product_id = %s"""

    UPDATEPRODUCTHASOPTIONSSQL = """
        UPDATE catalog_product_entity SET has_options = 1 WHERE entity_id = %s;"""

    INSERTPRODUCTIMAGEGALLERYSQL = """
        INSERT INTO catalog_product_entity_media_gallery
        (attribute_id,value,media_type) VALUES (%s,%s,%s);"""

    INSERTPRODUCTIMAGEGALLERYEXTSQL = """
        INSERT INTO catalog_product_entity_media_gallery_ext
        (value_id,media_source,file) VALUES (%s,%s,%s);"""

    INSERTPRODUCTIMAGEGALLERYVALUESQL = """
        INSERT INTO catalog_product_entity_media_gallery_value ({cols}) VALUES ({vals});"""

    INSERTMEDIAVALUETOENTITYSQL = """
        INSERT IGNORE INTO catalog_product_entity_media_gallery_value_to_entity ({cols}) VALUES ({vals});"""

    INSERTPRODUCTIMAGESQL = """
        INSERT INTO catalog_product_entity_varchar ({cols}) VALUES ({vals}) ON DUPLICATE KEY UPDATE value = %s;"""

    SELECTLINKTYPEIDSQL = """
        SELECT link_type_id FROM catalog_product_link_type WHERE code = %s;"""

    SELECTLINKATTSQL = """
        SELECT
        t0.link_type_id,
        t1.product_link_attribute_id
        FROM
        catalog_product_link_type t0,
        catalog_product_link_attribute t1
        WHERE t0.link_type_id = t1.link_type_id
        AND t1.product_link_attribute_code = "position"
        AND t0.code = %s;"""

    INSERTCATALOGPRODUCTLINKSQL = """
        INSERT IGNORE INTO catalog_product_link (product_id,linked_product_id,link_type_id) VALUES (%s,%s,%s);"""

    INSERTCATALOGPRODUCTLINKATTRIBUTEINT = """
        INSERT IGNORE INTO catalog_product_link_attribute_int (product_link_attribute_id,link_id,value) VALUES (%s,%s,%s);"""

    DELETEPRODUCTLINKSQL = """
        DELETE FROM catalog_product_link WHERE product_id = %s and link_type_id = %s"""

    DELETEPRODUCTIMAGEGALLERYSQL = """
        DELETE a
        FROM catalog_product_entity_media_gallery a
        INNER JOIN catalog_product_entity_media_gallery_value b ON a.value_id = b.value_id
        WHERE b.entity_id = %s"""

    DELTEEPRODUCTIMAGEEXTSQL = """
        DELETE FROM catalog_product_entity_media_gallery_ext WHERE file IN (%s)"""

    DELETEPRODUCTIMAGEGALLERYEXTSQL = """
        DELETE a
        FROM catalog_product_entity_media_gallery_ext a
        INNER JOIN catalog_product_entity_media_gallery_value b ON a.value_id = b.value_id
        WHERE b.entity_id = %s"""

    GETPRODUCTOUTOFSTOCKQTYSQL = """
        SELECT min_qty
        FROM cataloginventory_stock_item
        WHERE product_id = %s AND stock_id = %s AND use_config_min_qty = 0;"""

    SETSTOCKSTATUSQL = """
        INSERT INTO cataloginventory_stock_status
        (product_id,website_id,stock_id,qty,stock_status)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        qty = %s,
        stock_status = %s;"""

    SETSTOCKITEMSQL = """
        INSERT INTO  cataloginventory_stock_item
        (product_id,stock_id,qty,is_in_stock,website_id)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        qty = %s,
        is_in_stock = %s;"""

    SETPRODUCTCATEGORYSQL = """
        INSERT INTO catalog_category_product
        (category_id, product_id, position)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        category_id = %s,
        product_id = %s,
        position = %s;"""

    GETPRODUCTCATEGORYIDSSQL = """
        SELECT category_id 
        FROM catalog_category_product
        WHERE product_id = %s;
    """

    DELETEPRODUCTCATEGORYSQL = """
        DELETE FROM catalog_category_product
        WHERE category_id = %s AND product_id = %s;
    """

    UPDATECATEGORYCHILDRENCOUNTSQL = """
        UPDATE catalog_category_entity
        SET children_count = children_count + 1
        where entity_id = %s;"""

    UPDATECATEGORYCHILDRENCOUNTEESQL = """
        UPDATE catalog_category_entity
        SET children_count = children_count + 1
        where row_id = %s;"""

    GETMAXCATEGORYIDSQL = """
        SELECT max(entity_id) as max_category_id FROM catalog_category_entity;"""

    # for Magento 2 CE
    GETCATEGORYIDBYATTRIBUTEVALUEANDPATHSQL = """
        SELECT a.entity_id
        FROM catalog_category_entity a
        INNER JOIN catalog_category_entity_varchar b ON a.entity_id = b.entity_id
        INNER JOIN eav_attribute c ON b.attribute_id = c.attribute_id AND c.attribute_code = 'name' and c.entity_type_id = 3
        WHERE a.level = %s and a.parent_id = %s and b.value = %s;"""

    # for Magento 2 EE
    GETCATEGORYIDBYATTRIBUTEVALUEANDPATHEESQL = """
        SELECT a.row_id
        FROM catalog_category_entity a
        INNER JOIN catalog_category_entity_varchar b ON a.row_id = b.row_id
        INNER JOIN eav_attribute c ON b.attribute_id = c.attribute_id AND c.attribute_code = 'name' and c.entity_type_id = 3
        WHERE a.level = %s and a.parent_id = %s and b.value = %s;"""

    # for Magento 2 CE
    INSERTCATALOGCATEGORYENTITYSQL = """
        INSERT INTO catalog_category_entity
        (attribute_set_id, parent_id, created_at, updated_at, path, level, children_count, position)
        VALUES (%s, %s, now(), now(), %s, %s, %s, %s);"""

    # for Magento 2 EE
    INSERTCATALOGCATEGORYENTITYEESQL = """
        INSERT INTO catalog_category_entity
        (entity_id, created_in, updated_in, attribute_set_id, parent_id, created_at, updated_at, path, level, children_count,position)
        VALUES (%s, 1, 2147483647, %s, %s, now(), now(), %s, %s, %s,%s);"""

    EXPORTPRODUCTSCOUNTSQL = """
        SELECT count(*) AS total
        FROM catalog_product_entity a
        INNER JOIN eav_attribute_set b ON a.attribute_set_id = b.attribute_set_id
        WHERE updated_at >= '{updated_at}' AND b.attribute_set_name LIKE '{attribute_set_name}'
    """

    # for Magento 2 CE
    EXPORTMEDIAIMAGESSQL = """
        SELECT
        t0.sku,
        CONCAT('{base_url}', t1.value) as 'value',
        '{image_type}' as 'type'
        FROM
        catalog_product_entity t0,
        catalog_product_entity_varchar t1,
        eav_attribute t2
        WHERE t0.entity_id = t1.entity_id
        AND t1.attribute_id = t2.attribute_id
        AND t2.attribute_code = '{attribute_code}'
        AND t0.updated_at >= '{updated_at}'
    """

    # for Magento 2 EE
    EXPORTMEDIAIMAGESEESQL = """
        SELECT
        t0.sku,
        CONCAT('{base_url}', t1.value) as 'value',
        '{image_type}' as 'type'
        FROM
        catalog_product_entity t0,
        catalog_product_entity_varchar t1,
        eav_attribute t2
        WHERE t0.row_id = t1.row_id
        AND t1.attribute_id = t2.attribute_id
        AND t2.attribute_code = '{attribute_code}'
        AND t0.updated_at >= '{updated_at}'
    """

    # for Magento 2 CE
    EXPORTMEDIAGALLERYSQL = """
        SELECT
        t0.sku,
        CONCAT('{base_url}', t1.value) as 'value',
        t2.store_id,
        t2.position,
        t2.label,
        'mage2' as 'media_source',
        'media_gallery' as 'type'
        FROM
        catalog_product_entity t0,
        catalog_product_entity_media_gallery t1,
        catalog_product_entity_media_gallery_value t2,
        catalog_product_entity_media_gallery_value_to_entity t3
        WHERE t0.entity_id = t3.entity_id
        AND t1.value_id = t2.value_id
        AND t1.value_id = t3.value_id
        AND t0.updated_at >= '{updated_at}'
    """

    # for Magento 2 EE
    EXPORTMEDIAGALLERYEESQL = """
        SELECT
        t0.sku,
        CONCAT('{base_url}', t1.value) as 'value',
        t2.store_id,
        t2.position,
        t2.label,
        'mage2' as 'media_source',
        'media_gallery' as 'type'
        FROM
        catalog_product_entity t0,
        catalog_product_entity_media_gallery t1,
        catalog_product_entity_media_gallery_value t2,
        catalog_product_entity_media_gallery_value_to_entity t3
        WHERE t0.row_id = t3.row_id
        AND t1.value_id = t2.value_id
        AND t1.value_id = t3.value_id
        AND t0.updated_at >= '{updated_at}'
    """

    # for Magento 2 CE/EE
    GETCONFIGSIMPLEPRODUCTSSQL = """
        SELECT 
        t4.parent_id, 
        t5.attribute_id, 
        t4.product_id as child_id, 
        t3.value, 
        t0.sku
        FROM 
        catalog_product_entity t0,
        catalog_product_entity_int t1,
        eav_attribute_option t2,
        eav_attribute_option_value t3,
        catalog_product_super_link t4,
        catalog_product_super_attribute t5
        WHERE t0.{id} = t1.{id}
        AND t1.attribute_id = t2.attribute_id AND t1.value = t2.option_id
        AND t2.option_id = t3.option_id AND t3.store_id = {store_id}
        AND t4.parent_id = t5.product_id
        AND t0.entity_id = t4.product_id
        AND t5.attribute_id = t1.attribute_id AND t1.store_id = {store_id}
        AND t5.product_id = {product_id} AND t5.attribute_id IN ({attribute_ids})
    """

    REPLACECATALOGPRODUCTRELATIONSQL = """
        REPLACE INTO catalog_product_relation
        (parent_id, child_id)
        VALUES ({parent_id}, {child_id})
    """

    REPLACECATALOGPRODUCTSUPERLINKSQL = """
        REPLACE INTO catalog_product_super_link
        (product_id, parent_id)
        VALUES ({product_id}, {parent_id})
    """

    REPLACECATALOGPRODUCTSUPERATTRIBUTESQL = """
        INSERT IGNORE INTO catalog_product_super_attribute
        (product_id, attribute_id)
        VALUES ({product_id}, {attribute_id})
    """

    UPDATEPRODUCTVISIBILITYSQL = """
        UPDATE catalog_product_entity_int SET value = {value}
        WHERE {id} = {product_id} AND
        attribute_id in (SELECT attribute_id FROM eav_attribute WHERE entity_type_id = 4 AND attribute_code = 'visibility')
    """

    GETORDERSSQL = """
        SELECT
        sales_order.entity_id AS id,
        sales_order.increment_id AS m_order_inc_id,
        sales_order.created_at AS m_order_date,
        sales_order.updated_at AS m_order_update_date,
        sales_order.status AS m_order_status,
        customer_group.customer_group_code AS m_customer_group,
        sales_order.store_id AS m_store_id,
        sales_order.customer_id AS m_customer_id,
        '' AS shipment_carrier,
        IFNULL(sales_order.shipping_method,"") AS shipment_method,
        IFNULL(bill_to.firstname,'') AS billto_firstname,
        IFNULL(bill_to.lastname,'') AS billto_lastname,
        IFNULL(bill_to.email,'') AS billto_email,
        IFNULL(bill_to.company,'') AS billto_companyname,
        IFNULL(bill_to.street,'') AS billto_address,
        IFNULL(bill_to.city,'') AS billto_city,
        IFNULL(bill_to_region.code,'') AS billto_region,
        IFNULL(bill_to.country_id,'') AS billto_country,
        IFNULL(bill_to.postcode,'') AS billto_postcode,
        IFNULL(bill_to.telephone,'') AS billto_telephone,
        IFNULL(ship_to.firstname,'') AS shipto_firstname,
        IFNULL(ship_to.lastname,'') AS shipto_lastname,
        IFNULL(ship_to.company,'') AS shipto_companyname,
        IFNULL(ship_to.street,'') AS shipto_address,
        IFNULL(ship_to.city,'') AS shipto_city,
        IFNULL(ship_to_region.code,'') AS shipto_region,
        IFNULL(ship_to.country_id,'') AS shipto_country,
        IFNULL(ship_to.postcode,'') AS shipto_postcode,
        IFNULL(ship_to.telephone,'') AS shipto_telephone,
        IFNULL(sales_order.total_qty_ordered,0) AS total_qty,
        IFNULL(sales_order.subtotal,0) AS sub_total,
        IFNULL(sales_order.discount_amount,0) AS discount_amt,
        IFNULL(sales_order.shipping_amount,0) AS shipping_amt,
        IFNULL(sales_order.tax_amount,0) AS tax_amt,
        '0' AS giftcard_amt,
        '0' AS storecredit_amt,
        sales_order.grand_total AS grand_total,
        sales_order.coupon_code AS coupon_code,
        IFNULL(sales_order.shipping_tax_amount,0) AS shipping_tax_amt,
        'checkmo' AS payment_method
        FROM
        sales_order
        LEFT JOIN sales_order_address bill_to on (sales_order.entity_id = bill_to.parent_id and bill_to.address_type = 'billing')
        LEFT JOIN sales_order_address ship_to on (sales_order.entity_id = ship_to.parent_id and ship_to.address_type = 'shipping')
        LEFT JOIN directory_country_region bill_to_region on (bill_to.region_id = bill_to_region.region_id and bill_to.country_id = bill_to_region.country_id)
        LEFT JOIN directory_country_region ship_to_region on (ship_to.region_id = ship_to_region.region_id and ship_to.country_id = ship_to_region.country_id)
        LEFT JOIN customer_entity customer on sales_order.customer_id = customer.entity_id
        LEFT JOIN customer_group customer_group on customer.group_id = customer_group.customer_group_id
        WHERE sales_order.updated_at > '{updated_at}'
        ORDER BY sales_order.entity_id
    """

    GETORDERITEMSSQL = """
        SELECT
        sales_order_item.item_id AS id,
        sales_order_item.order_id AS m_order_id,
        sales_order_item.sku AS sku,
        sales_order_item.name AS name,
        '' AS uom,
        sales_order_item.original_price AS original_price,
        sales_order_item.price AS price,
        sales_order_item.discount_amount AS discount_amt,
        sales_order_item.tax_amount AS tax_amt,
        sales_order_item.qty_ordered AS qty,
        sales_order_item.row_total AS sub_total
        FROM
        sales_order_item
        WHERE parent_item_id is null 
        AND order_id = '{order_id}'
    """

    INSERTCATALOGINVENTORYSTOCKITEM = """
        INSERT INTO cataloginventory_stock_item
        (product_id, website_id, stock_id, is_qty_decimal, min_sale_qty, use_config_min_sale_qty, manage_stock, use_config_manage_stock, qty_increments, use_config_qty_increments)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    UPDATECATALOGINVENTORYSTOCKITEM = """
        UPDATE cataloginventory_stock_item
        SET is_qty_decimal  = %s,
        min_sale_qty = %s,
        use_config_min_sale_qty = %s,
        manage_stock = %s,
        use_config_manage_stock = %s,
        qty_increments = %s,
        use_config_qty_increments = %s
        WHERE product_id = %s
        AND website_id = %s
        AND stock_id = %s
    """

    GETCURRENTCATALOGINVENTORYSTOCKITEMSQL = """
        SELECT *
        FROM cataloginventory_stock_item
        WHERE product_id = %s and website_id = %s and stock_id = %s
    """

    GETPRODUCTTYPEBYSKUSQL = """
        SELECT type_id 
        FROM catalog_product_entity
        WHERE sku = %s
    """

    INSERTCATALOGPRODUCTENTITYTIERPRICESQL = """
        INSERT INTO catalog_product_entity_tier_price
        ({key}, all_groups, customer_group_id, qty, value, website_id, percentage_value)
        VALUES(%s, %s, %s, %s, %s, %s, %s)
    """

    DELETECATALOGPRODUCTENTITYTIERPRICESQL = """
        DELETE FROM catalog_product_entity_tier_price
        WHERE {key} = %s and website_id = %s
    """

    GETWEBSITEBYWEBSITEIDSQL = """
        SELECT * 
        FROM store_website
        WHERE website_id = %s
    """

    GETSTOCKIDBYSALESCHANNELSQL = """
        SELECT stock_id 
        FROM inventory_stock_sales_channel
        WHERE type = %s and code = %s
    """

    GETDEFAULTWEBSITEIDSQL = """
        SELECT * 
        FROM store_website
        WHERE is_default = 1
    """

    INSERTINVENTORYSTOCK = """
        INSERT INTO {table_name}
        (sku, quantity, is_salable)
        VALUES(%s, %s, %s)
    """

    UPDATEINVENTORYSTOCK = """
        UPDATE {table_name}
        SET quantity  = %s,
        is_salable = %s
        WHERE sku = %s
    """

    GETINVENTORYSTOCK = """
        SELECT *
        FROM {table_name}
        WHERE sku = %s
    """

    GETINVENTORYSOURCE = """
        SELECT *
        FROM inventory_source
        WHERE source_code = %s
    """

    INSERTINVENTORYSOURCEITEM = """
        INSERT INTO inventory_source_item
        (source_code, sku, quantity, status)
        VALUES(%s, %s, %s, %s)
    """

    UPDATEINVENTORYSOURCEITEM = """
        UPDATE inventory_source_item
        SET quantity  = %s,
        status = %s
        WHERE source_code = %s AND sku = %s
    """

    GETINVENTORYSOURCEITEM = """
        SELECT *
        FROM inventory_source_item
        WHERE source_code = %s AND sku = %s
    """

    GETENTITYATTRIBUTEVALUESQL = """
        SELECT * 
        FROM {entity_type_code}_entity_{data_type} 
        WHERE {id_field} = %s AND attribute_id = %s AND store_id = %s;
    """

    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.adaptor = Adaptor(**setting)
        self.mage2_domain = setting.get("mage2_domain")
        self.mage2_token = setting.get("mage2_token")
        self.mage2_scope = setting.get("mage2_scope", "default")

    def __del__(self):
        if self.adaptor:
            self.adaptor.__del__()
            self.logger.info("Close Mage2 DB connection")

    @property
    def adaptor(self):
        return self._adaptor

    @adaptor.setter
    def adaptor(self, adaptor):
        self._adaptor = adaptor

    def request_magento_rest_api(self, api_path, method="POST", payload={}):
        api_url = "{domain}/rest/{scope}/V1/{api_path}".format(
            domain=self.mage2_domain,
            scope=self.mage2_scope,
            api_path=api_path
        )
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token=self.mage2_token)
        }
        method = method.upper()
        response = requests.request(
            method,
            api_url,
            headers=headers,
            data=json.dumps(payload),
        )
        if response.status_code == 200:
            return response.content
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def get_entity_metadata(
        self, entity_type_code="catalog_product", attribute_set="Default"
    ):
        self.adaptor.mysql_cursor.execute(
            self.ENTITYMETADATASQL, [entity_type_code, attribute_set]
        )
        entity_metadata = self.adaptor.mysql_cursor.fetchone()
        if entity_metadata:
            return entity_metadata
        raise Exception(
            f"attribute_set/entity_type_code: {attribute_set}/{entity_type_code} not existed."
        )

    def get_attribute_metadata(self, attribute_code, entity_type_code):
        self.adaptor.mysql_cursor.execute(
            self.ATTRIBUTEMETADATASQL, [attribute_code, entity_type_code]
        )
        attribute_metadata = self.adaptor.mysql_cursor.fetchone()
        if attribute_metadata is None:
            raise Exception(
                f"Entity Type/Attribute Code: {entity_type_code}/{attribute_code} does not exist"
            )
        data_type = attribute_metadata["backend_type"]
        return (data_type, attribute_metadata)

    def is_entity_exit(self, entity_type_code, entity_id):
        sql = self.ISENTITYEXITSQL.format(entity_type_code=entity_type_code)
        self.adaptor.mysql_cursor.execute(sql, [entity_id])
        exist = self.adaptor.mysql_cursor.fetchone()
        return exist["count"]

    def is_attribute_value_exit(
        self, entity_type_code, data_type, attribute_id, store_id, entity_id
    ):
        key = "row_id" if self.setting["VERSION"] == "EE" else "entity_id"
        sql = self.ISATTRIBUTEVALUEEXITSQL.format(
            entity_type_code=entity_type_code, data_type=data_type, key=key
        )
        self.adaptor.mysql_cursor.execute(sql, [attribute_id, store_id, entity_id])
        exist = self.adaptor.mysql_cursor.fetchone()
        return exist["count"]

    def replace_attribute_value(
        self, entity_type_code, data_type, entity_id, attribute_id, value, store_id=0
    ):
        if (
            entity_type_code == "catalog_product"
            or entity_type_code == "catalog_category"
        ):
            cols = "entity_id, attribute_id, store_id, value"
            if self.setting["VERSION"] == "EE":
                cols = "row_id, attribute_id, store_id, value"
            vls = "%s, %s, {0}, %s".format(store_id)
            param = [entity_id, attribute_id, value]
        else:
            cols = "entity_id, attribute_id, value"
            vls = "%s, %s, %s"
            param = [entity_id, attribute_id, value]
        sql = self.REPLACEATTRIBUTEVALUESQL.format(
            entity_type_code=entity_type_code, data_type=data_type, cols=cols, vls=vls
        )
        self.adaptor.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        self.adaptor.mysql_cursor.execute(sql, param)
        self.adaptor.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    def set_attribute_option_values(
        self, attribute_id, options, admin_store_id=0, update_existing_option=False
    ):
        option_id = self.get_option_id(
            attribute_id, options[admin_store_id], admin_store_id
        )
        if option_id is None:
            self.adaptor.mysql_cursor.execute(
                self.INSERTEAVATTRIBUTEOPTIONSQL, [attribute_id]
            )
            option_id = self.adaptor.mysql_cursor.lastrowid
        for (store_id, option_value) in options.items():
            self.adaptor.mysql_cursor.execute(
                self.OPTIONVALUEEXISTSQL, [option_id, store_id]
            )
            exist = self.adaptor.mysql_cursor.fetchone()
            if not exist or exist["cnt"] == 0:
                self.adaptor.mysql_cursor.execute(
                    self.INSERTOPTIONVALUESQL, [option_id, store_id, option_value]
                )
            elif exist["cnt"] > 0 and update_existing_option == True:
                self.adaptor.mysql_cursor.execute(
                    self.UPDATEOPTIONVALUESQL, [option_value, option_id, store_id]
                )
        return option_id

    def set_multi_select_option_ids(
        self,
        attribute_id,
        values,
        entity_type_code="catalog_product",
        admin_store_id=0,
        delimiter="|",
    ):
        values = values.strip('"').strip("'").strip("\n").strip()
        list_values = [v.strip() for v in values.split(delimiter)]
        list_option_ids = []
        for value in list_values:
            options = {0: value}
            option_id = self.set_attribute_option_values(
                attribute_id,
                options,
                entity_type_code=entity_type_code,
                admin_store_id=admin_store_id,
            )
            list_option_ids.append(str(option_id))
        option_ids = ",".join(list_option_ids) if len(list_option_ids) > 0 else None
        return option_ids

    def get_option_id(self, attribute_id, value, admin_store_id=0):
        self.adaptor.mysql_cursor.execute(
            self.GETOPTIONIDSQL, [attribute_id, value, admin_store_id]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        option_id = None
        if res is not None:
            option_id = res["option_id"]
        return option_id

    def get_multi_select_option_ids(
        self, attribute_id, values, admin_store_id=0, delimiter="|"
    ):
        if values is None:
            return [None]
        values = values.strip('"').strip("'").strip("\n").strip()
        list_values = [v.strip() for v in values.split(delimiter)]
        list_option_ids = []
        for value in list_values:
            option_id = self.get_option_id(
                attribute_id, value, admin_store_id=admin_store_id
            )
            list_option_ids.append(str(option_id))
        option_ids = ",".join(list_option_ids) if len(list_option_ids) > 0 else None
        return option_ids

    def get_product_id_by_sku(self, sku):
        self.adaptor.mysql_cursor.execute(self.GETPRODUCTIDBYSKUSQL, [sku])
        entity = self.adaptor.mysql_cursor.fetchone()
        if entity:
            return int(entity["entity_id"])
        return 0

    def get_row_id_by_entity_id(self, entity_id):
        self.adaptor.mysql_cursor.execute(self.GETROWIDBYENTITYIDSQL, [entity_id])
        entity = self.adaptor.mysql_cursor.fetchone()
        if entity is not None:
            return int(entity["row_id"])
        return 0

    def insert_catalog_product_entity(
        self, sku, attribute_set="Default", type_id="simple"
    ):
        entity_metadata = self.get_entity_metadata("catalog_product", attribute_set)
        if entity_metadata == None:
            return 0

        if self.setting["VERSION"] == "EE":
            self.adaptor.mysql_cursor.execute("""SET FOREIGN_KEY_CHECKS = 0;""")
            self.adaptor.mysql_cursor.execute(
                self.INSERTCATALOGPRODUCTENTITYEESQL,
                (entity_metadata["attribute_set_id"], type_id, sku),
            )
            product_id = self.adaptor.mysql_cursor.lastrowid
            self.adaptor.mysql_cursor.execute(
                """UPDATE catalog_product_entity SET entity_id = row_id WHERE row_id = %s;""",
                (product_id,),
            )
            self.adaptor.mysql_cursor.execute(
                """INSERT INTO sequence_product (sequence_value) VALUES (%s);""",
                (product_id,),
            )
            self.adaptor.mysql_cursor.execute("""SET FOREIGN_KEY_CHECKS = 1;""")
        else:
            self.adaptor.mysql_cursor.execute(
                self.INSERTCATALOGPRODUCTENTITYSQL,
                (entity_metadata["attribute_set_id"], type_id, sku),
            )
            product_id = self.adaptor.mysql_cursor.lastrowid

        return product_id

    def update_catalog_product_entity(
        self, product_id, attribute_set="Default", type_id="simple"
    ):
        entity_metadata = self.get_entity_metadata("catalog_product", attribute_set)
        if entity_metadata == None:
            return 0
        key = "row_id" if self.setting["VERSION"] == "EE" else "entity_id"
        sql = self.UPDATECATALOGPRODUCTSQL.format(key=key)
        self.adaptor.mysql_cursor.execute(
            sql, [entity_metadata["attribute_set_id"], type_id, product_id]
        )

    def insert_update_entity_data(
        self,
        entity_id,
        data,
        entity_type_code="catalog_product",
        store_id=0,
        admin_store_id=0,
    ):
        do_not_update_option_attributes = ["status", "visibility", "tax_class_id"]
        for attribute_code, value in data.items():
            try:
                (data_type, attribute_metadata) = self.get_attribute_metadata(
                    attribute_code, entity_type_code
                )
            except Exception:
                log = traceback.format_exc()
                self.logger.exception(log)
                continue

            if (
                attribute_metadata["frontend_input"] == "select"
                and attribute_code not in do_not_update_option_attributes
            ):
                option_id = self.get_option_id(
                    attribute_metadata["attribute_id"],
                    value,
                    admin_store_id=admin_store_id,
                )
                if option_id is None:
                    options = {0: value}
                    option_id = self.set_attribute_option_values(
                        attribute_metadata["attribute_id"],
                        options,
                        admin_store_id=admin_store_id,
                    )
                value = option_id

            if attribute_metadata["frontend_input"] == "multiselect":
                option_ids = self.get_multi_select_option_ids(
                    attribute_metadata["attribute_id"],
                    value,
                    admin_store_id=admin_store_id,
                )
                if option_ids is None:
                    option_ids = self.set_multi_select_option_ids(
                        attribute_metadata["attribute_id"],
                        value,
                        admin_store_id=admin_store_id,
                    )
                value = option_ids

            # ignore the static datatype.
            if data_type != "static":
                exist = self.is_attribute_value_exit(
                    entity_type_code,
                    data_type,
                    attribute_metadata["attribute_id"],
                    admin_store_id,
                    entity_id,
                )
                store_id = admin_store_id if exist == 0 else store_id
                self.replace_attribute_value(
                    entity_type_code,
                    data_type,
                    entity_id,
                    attribute_metadata["attribute_id"],
                    value,
                    store_id=store_id,
                )

    def get_website_id_by_store_id(self, store_id):
        self.adaptor.mysql_cursor.execute(
            "SELECT website_id FROM store WHERE store_id = %s", [store_id]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        website_id = 0
        if res is not None:
            website_id = res["website_id"]
        return website_id

    def assign_website(self, product_id, store_id):
        website_id = self.get_website_id_by_store_id(store_id)
        if website_id == 0:
            website_id = 1
        self.adaptor.mysql_cursor.execute(
            "INSERT IGNORE INTO catalog_product_website (product_id, website_id) VALUES (%s, %s)",
            [product_id, website_id],
        )

    ## Insert Update Product.
    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(5),
    )
    def insert_update_product(self, sku, attribute_set, data, type_id, store_id):
        try:
            product_id = self.get_product_id_by_sku(sku)
            if product_id == 0:
                product_id = self.insert_catalog_product_entity(
                    sku, attribute_set, type_id
                )
            else:
                self.update_catalog_product_entity(product_id, attribute_set, type_id)

            # update catalog_inventory_stock_item.
            # self.update_catalog_inventory_stock_item(sku, data, store_id)

            # insert update attributes.
            self.insert_update_entity_data(
                product_id, data, entity_type_code="catalog_product", store_id=store_id
            )

            self.assign_website(product_id, store_id)
            self.adaptor.commit()
            return product_id
        except Exception:
            self.adaptor.rollback()
            raise
    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(5),
    )     
    def insert_update_cataloginventory_stock_item(self, sku, stock_data, store_id):
        website_id = self.get_website_id_by_store_id(store_id)
        product_id = self.get_product_id_by_sku(sku)
        type_id = self.get_product_type_id_by_sku(sku)

        if not product_id:
            raise Exception(f"Product ({sku}) does not existed in Magento.")
        
        self.insert_update_inventory_stock(
            website_id=website_id, type_id=type_id, sku=sku, quantity=stock_data.get("quantity", 0), is_salable=1
        )

        sources = stock_data.get("inventory_sources", [])
        for source_data in sources:
            self.insert_update_inventory_source_item(
                source_code=source_data.get("source_code", None), sku=sku, quantity=source_data.get("quantity", 0), status=source_data.get("status", 1)
            )
        
        stock_id = stock_data.get("stock_id", 1)
        current_stock_item = self.get_current_cataloginventory_stock_item(
            product_id=product_id, stock_id=stock_id, website_id=website_id
        )

        if current_stock_item:
            self.update_cataloginventory_stock_item(
                product_id=product_id,
                website_id=website_id,
                stock_id=stock_id,
                stock_data=stock_data,
            )
        else:
            self.insert_cataloginventory_stock_item(
                product_id=product_id,
                website_id=website_id,
                stock_id=stock_id,
                stock_data=stock_data,
            )
    
    def insert_cataloginventory_stock_item(
        self, product_id, website_id, stock_id, stock_data
    ):
        self.adaptor.mysql_cursor.execute(
            self.INSERTCATALOGINVENTORYSTOCKITEM,
            [
                product_id,
                website_id,
                stock_id,
                stock_data.get(
                    "is_qty_decimal",
                    0
                    if float(stock_data.get("min_sale_qty", 1))
                    == int(stock_data.get("min_sale_qty", 1))
                    else 1,
                ),
                stock_data.get("min_sale_qty", 1),
                stock_data.get(
                    "use_config_min_sale_qty",
                    1 if stock_data.get("min_sale_qty", None) is None else 0,
                ),
                stock_data.get("manage_stock", 0),
                stock_data.get("use_config_manage_stock", 0),
                stock_data.get("qty_increments", 1),
                stock_data.get(
                    "use_config_qty_increments",
                    1 if stock_data.get("qty_increments", None) is None else 0,
                ),
            ],
        )

    
    def get_current_cataloginventory_stock_item(self, product_id, stock_id, website_id):
        self.adaptor.mysql_cursor.execute(
            self.GETCURRENTCATALOGINVENTORYSTOCKITEMSQL,
            [product_id, website_id, stock_id],
        )
        exist = self.adaptor.mysql_cursor.fetchone()
        return exist

    def update_cataloginventory_stock_item(
        self, product_id, stock_id, website_id, stock_data
    ):
        self.adaptor.mysql_cursor.execute(
            self.UPDATECATALOGINVENTORYSTOCKITEM,
            [
                stock_data.get(
                    "is_qty_decimal",
                    0
                    if float(stock_data.get("min_sale_qty", 1))
                    == int(stock_data.get("min_sale_qty", 1))
                    else 1,
                ),
                stock_data.get("min_sale_qty", 1),
                stock_data.get(
                    "use_config_min_sale_qty",
                    1 if stock_data.get("min_sale_qty", None) is None else 0,
                ),
                stock_data.get("manage_stock", 0),
                stock_data.get("use_config_manage_stock", 0),
                stock_data.get("qty_increments", 1),
                stock_data.get(
                    "use_config_qty_increments",
                    1 if stock_data.get("qty_increments", None) is None else 0,
                ),
                product_id,
                website_id,
                stock_id,
            ],
        )

    def insert_update_inventory_stock(self, website_id, type_id, sku, quantity=0, is_salable=1):
        stock_id = self.get_stock_id_by_sales_channel(website_id)
        if stock_id is None:
            return

        table_name = self.get_inventory_stock_table_name(stock_id)
        inventory_stock_data = self.get_inventory_stock(table_name, sku)
        if inventory_stock_data is None:
            self.insert_inventory_stock(table_name, sku, quantity, is_salable)
        else:
            self.update_inventory_stock(table_name, sku, quantity, is_salable)

    def insert_inventory_stock(self, table_name, sku, quantity=0, is_salable=1):
        sql = self.INSERTINVENTORYSTOCK.format(table_name=table_name)
        self.adaptor.mysql_cursor.execute(
            sql,
            [sku, quantity, is_salable]
        )
    def update_inventory_stock(self, table_name, sku, quantity=0, is_salable=1):
        sql = self.UPDATEINVENTORYSTOCK.format(table_name=table_name)
        self.adaptor.mysql_cursor.execute(
            sql,
            [quantity, is_salable, sku]
        )

    def get_inventory_stock(self, table_name, sku):
        sql = self.GETINVENTORYSTOCK.format(table_name=table_name)
        self.adaptor.mysql_cursor.execute(
            sql,
            [sku]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        return res

    def get_stock_id_by_sales_channel(self, website_id):
        website = None
        stock_id = None
        if website_id == 0:
            website = self.get_default_website()
        else:
            website = self.get_website_by_website_id(website_id)
        if website is not None:
            self.adaptor.mysql_cursor.execute(
                self.GETSTOCKIDBYSALESCHANNELSQL,
                ["website", website["code"]]
            )
            res = self.adaptor.mysql_cursor.fetchone()
            if res is not None:
                stock_id = res["stock_id"]
        return stock_id

    def get_inventory_stock_table_name(self, stock_id):
        return "inventory_stock_{stock_id}".format(stock_id=stock_id)

    def insert_update_inventory_source_item(self, source_code, sku, quantity, status):
        inventory_source_item = self.get_inventory_source_item(source_code, sku)
        if inventory_source_item is None:
            self.insert_inventory_source_item(source_code, sku, quantity, status)
        else:
            self.update_inventory_source_item(source_code, sku, quantity, status)

    def insert_inventory_source_item(self, source_code, sku, quantity=0, status=1):
        self.adaptor.mysql_cursor.execute(
            self.INSERTINVENTORYSOURCEITEM,
            [source_code, sku, quantity, status]
        )

    def update_inventory_source_item(self, source_code, sku, quantity=0, status=1):
        self.adaptor.mysql_cursor.execute(
            self.UPDATEINVENTORYSOURCEITEM,
            [quantity, status, source_code, sku]
        )

    def get_inventory_source_item(self, source_code, sku):
        self.adaptor.mysql_cursor.execute(
            self.GETINVENTORYSOURCEITEM,
            [source_code, sku]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        return res

    def exists_inventory_source_code(self, source_code):
        self.adaptor.mysql_cursor.execute(
            self.GETINVENTORYSOURCE,
            [source_code]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        if res:
            return True
        else:
            return False

    def get_website_by_website_id(self, website_id):
        self.adaptor.mysql_cursor.execute(
            self.GETWEBSITEBYWEBSITEIDSQL,
            [website_id]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        return res

    def get_default_website(self):
        self.adaptor.mysql_cursor.execute(
            self.GETDEFAULTWEBSITEIDSQL
        )
        res = self.adaptor.mysql_cursor.fetchone()
        return res

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(5),
    )
    def insert_update_product_tier_price(self, sku, tier_price, store_id):
        website_id = self.get_website_id_by_store_id(store_id)
        product_id = self.get_product_id_by_sku(sku)
        type_id = self.get_product_type_id_by_sku(sku)
        if not product_id:
            raise Exception(f"Product ({sku}) does not existed in Magento.")
        if type_id not in ["simple", "virtual"]:
            return

        self.delete_product_tier_price(product_id=product_id, website_id=website_id)
        if len(tier_price) > 0:
            self.insert_product_tier_price(
                product_id=product_id, website_id=website_id, tier_price=tier_price
            )

    def insert_product_tier_price(self, product_id, website_id, tier_price):
        key = "row_id" if self.setting["VERSION"] == "EE" else "entity_id"
        sql = self.INSERTCATALOGPRODUCTENTITYTIERPRICESQL.format(key=key)
        for tier_price_data in tier_price:
            if tier_price_data.get("qty", None) and tier_price_data.get("value", None):
                self.adaptor.mysql_cursor.execute(
                    sql,
                    [
                        product_id,
                        tier_price_data.get("all_groups", 1),
                        tier_price_data.get("customer_group_id", 0),
                        tier_price_data.get("qty", 1),
                        tier_price_data.get("value", 1),
                        website_id,
                        tier_price_data.get("percentage_value", None),
                    ],
                )

    def delete_product_tier_price(self, product_id, website_id):
        key = "row_id" if self.setting["VERSION"] == "EE" else "entity_id"
        sql = self.DELETECATALOGPRODUCTENTITYTIERPRICESQL.format(key=key)
        self.adaptor.mysql_cursor.execute(sql, [product_id, website_id])

    def get_product_type_id_by_sku(self, sku):
        self.adaptor.mysql_cursor.execute(self.GETPRODUCTTYPEBYSKUSQL, [sku])
        entity = self.adaptor.mysql_cursor.fetchone()
        if entity:
            return entity["type_id"]
        return None

    def insert_update_custom_option(self, product_id, option):
        title = option.pop("title")
        store_id = option.pop("store_id", 0)
        option_id = self.getCustomOption(product_id, title)

        opt_cols = [
            "option_id",
            "product_id",
            "type",
            "is_require",
            "sku",
            "max_characters",
            "file_extension",
            "image_size_x",
            "image_size_y",
            "sort_order",
        ]
        opt_vals = ["%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s"]
        values = [
            option_id,
            product_id,
            option.pop("type"),
            option.pop("is_require"),
            option.pop("option_sku", None),
            option.pop("max_characters", 0),
            option.pop("file_extension", None),
            option.pop("image_size_x", 0),
            option.pop("image_size_y", 0),
            option.pop("sort_order", 1),
        ]

        if option_id == 0:
            if store_id == 0:
                # opt_cols.pop("option_id")
                opt_cols.pop(0)
                opt_vals.pop(0)
                values.pop(0)

                # Replace custom option.
                self.adaptor.mysql_cursor.execute(
                    self.REPLACECUSTOMOPTIONSQL.format(
                        opt_cols=",".join(opt_cols), opt_vals=",".join(opt_vals)
                    ),
                    values,
                )
                option_id = self.adaptor.mysql_cursor.lastrowid
            else:
                # There is no option for store# 0.
                raise Exception(
                    f"You have to input the option title ({title}) with store ({store_id}) first."
                )
        else:
            # Replace custom option.
            self.adaptor.mysql_cursor.execute(
                self.REPLACECUSTOMOPTIONSQL.format(
                    opt_cols=",".join(opt_cols), opt_vals=",".join(opt_vals)
                ),
                values,
            )

        # Insert custom option title.
        if store_id == 0:
            option_title = title
        else:
            option_title = option.pop("title_alt")
        self.adaptor.mysql_cursor.execute(
            self.INSERTCUSTOMOPTIONTITLESQL,
            [option_id, store_id, option_title, option_title],
        )
        option_title_id = self.adaptor.mysql_cursor.lastrowid

        # insert custom option price.
        if "option_price" in option.keys():
            option_price = (option.pop("option_price"),)
            option_price_type = option.pop("option_price_type")
            self.adaptor.mysql_cursor.execute(
                self.INSERTCUSTOMOPTIONPRICESQL,
                [
                    option_id,
                    store_id,
                    option_price,
                    option_price_type,
                    option_price,
                    option_price_type,
                ],
            )
            option_price_id = self.adaptor.mysql_cursor.lastrowid

        return option_id

    def get_custom_option_value(self, option_id, value_title):
        self.adaptor.mysql_cursor.execute(
            self.GETCUSTOMOPTIONTYPEIDBYTITLESQL, [option_id, value_title]
        )
        entity = self.adaptor.mysql_cursor.fetchone()
        if entity:
            return int(entity["option_type_id"])
        return 0

    def insert_update_custom_option_value(self, option_id, option_value):
        value_title = option_value.pop("option_value_title")
        store_id = option_value.pop("store_id", 0)
        option_type_id = self.get_custom_option_value(option_id, value_title)

        opt_val_cols = [
            "option_type_id",
            "option_id",
            "sku",
            "sort_order",
        ]
        opt_val_vals = ["%s", "%s", "%s", "%s"]
        values = [
            option_type_id,
            option_id,
            option_value.pop("option_value_sku", None),
            option_value.pop("option_value_sort_order", 1),
        ]

        if option_type_id == 0:
            if store_id == 0:
                # opt_val_cols.pop("option_type_id")
                opt_val_cols.pop(0)
                opt_val_vals.pop(0)
                values.pop(0)

                # Replace custom option.
                self.adaptor.mysql_cursor.execute(
                    self.REPLACECUSTOMOPTIONTYPEVALUESQL.format(
                        opt_val_cols=",".join(opt_val_cols),
                        opt_val_vals=",".join(opt_val_vals),
                    ),
                    values,
                )
                option_type_id = self.adaptor.mysql_cursor.lastrowid
            else:
                # There is no option value for store# 0.
                raise Exception(
                    f"You have to input the option type title ({value_title}) with store ({store_id}) first."
                )
        else:
            # Replace custom option.
            self.adaptor.mysql_cursor.execute(
                self.REPLACECUSTOMOPTIONTYPEVALUESQL.format(
                    opt_val_cols=",".join(opt_val_cols),
                    opt_val_vals=",".join(opt_val_vals),
                ),
                values,
            )

        # Insert custom option typle title.
        if store_id == 0:
            option_type_title = value_title
        else:
            option_type_title = option_value.pop("option_value_title_alt")
        self.adaptor.mysql_cursor.execute(
            self.INSERTCUSTOMOPTIONTYPETITLESQL,
            [option_type_id, store_id, option_type_title, option_type_title],
        )
        option_type_title_id = self.adaptor.mysql_cursor.lastrowid

        # insert custom option type price.
        if "option_value_price" in option_value.keys():
            option_type_price = (option_value.pop("option_value_price"),)
            option_value_price_type = option_value.pop("option_value_price_type")
            self.adaptor.mysql_cursor.execute(
                self.INSERTCUSTOMOPTIONTYPEPRICESQL,
                [
                    option_type_id,
                    store_id,
                    option_type_price,
                    option_value_price_type,
                    option_type_price,
                    option_value_price_type,
                ],
            )
            option_type_price_id = self.adaptor.mysql_cursor.lastrowid
        return option_type_id

    ## Insert Update Custom Options.
    def insert_update_custom_options(self, sku, data):
        product_id = self.get_product_id_by_sku(sku)
        if self.setting["VERSION"] == "EE":
            product_id = self.get_row_id_by_entity_id(product_id)

        self.adaptor.mysql_cursor.execute(self.DELETECUSTOMOPTIONSQL, [product_id])
        for option in data:
            option_id = self.insert_update_custom_option(product_id, option)
            if len(option["option_values"]) > 0:
                for option_value in option["option_values"]:
                    option_type_id = self.insert_update_custom_option_value(
                        option_id, option_value
                    )

        self.adaptor.mysql_cursor.execute(self.UPDATEPRODUCTHASOPTIONSSQL, [product_id])

        return product_id

    def get_variants(self, product_id, attribute_ids, admin_store_id=0):
        sql = self.GETCONFIGSIMPLEPRODUCTSSQL.format(
            id="row_id" if self.setting["VERSION"] == "EE" else "entity_id",
            store_id=admin_store_id,
            product_id=product_id,
            attribute_ids=",".join(
                [str(attribute_id) for attribute_id in attribute_ids]
            ),
        )
        self.adaptor.mysql_cursor.execute(sql)
        rows = self.adaptor.mysql_cursor.fetchall()
        variants = []
        if len(rows) > 0:
            metadata = dict((k, list(set(map(lambda d: d[k], rows)))) for k in ["sku"])
            for sku in metadata["sku"]:
                attributes = list(filter(lambda row: row["sku"] == sku, rows))
                variants.append(
                    {
                        "sku": sku,
                        "attributes": dict(
                            (attribute["attribute_id"], attribute["value"])
                            for attribute in attributes
                        ),
                    }
                )
        return variants

    ## Insert Update Variant.
    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(5),
    )
    def insert_update_variant(self, sku, data, store_id):
        # Validate the child product.
        # data = {
        #     "variant_visibility": True,
        #     "parent_product_sku": "xxxxx",
        #     "variant_attributes": {"att_a": "abc", "att_x": "xyz"},
        # }
        parent_product_id = self.get_product_id_by_sku(data["parent_product_sku"])

        attributes = []
        for attribute_code, value in data["variant_attributes"].items():
            (data_type, attribute_metadata) = self.get_attribute_metadata(
                attribute_code, "catalog_product"
            )
            # Check if the attribute is a "SELECT".
            assert (
                attribute_metadata["frontend_input"] == "select"
            ), "The attribute({attribute_code}) is not a 'SELECT' type.".format(
                attribute_code=attribute_code
            )
            # Check if the product has the attribute with the option value.
            option_id = self.get_option_id(
                attribute_metadata["attribute_id"],
                value,
                admin_store_id=store_id,
            )
            if option_id is None:
                options = {0: value}
                self.set_attribute_option_values(
                    attribute_metadata["attribute_id"],
                    options,
                    admin_store_id=store_id,
                )
            attributes.append(
                {
                    "attribute_code": attribute_code,
                    "value": value,
                    "attribute_id": attribute_metadata["attribute_id"],
                }
            )

            attribute_ids = [attribute["attribute_id"] for attribute in attributes]

            # Check if there is a product with the same attributes.
            variants = list(
                filter(
                    lambda product: product["attributes"]
                    == dict(
                        (attribute["attribute_id"], attribute["value"])
                        for attribute in attributes
                    ),
                    self.get_variants(
                        parent_product_id,
                        attribute_ids,
                        admin_store_id=store_id,
                    ),
                )
            )
            # If there is a product matched with the attribute and value set and the sku is not matched,
            # raise an exception.
            if len(variants) != 0:
                assert (
                    variants[0]["sku"] == sku
                ), "There is already a product ({sku}) matched with the attributes ({attributes}) of the sku ({variant_sku}).".format(
                    sku=variants[0]["sku"],
                    attributes=",".join(
                        [
                            f"{attribute['attribute_code']}:{attribute['value']}"
                            for attribute in attributes
                        ]
                    ),
                    variant_sku=sku,
                )

        # Insert or update the child product.
        self.logger.info(f"Insert/Update the item ({sku}).")
        for attribute_id in attribute_ids:
            self.adaptor.mysql_cursor.execute(
                self.REPLACECATALOGPRODUCTSUPERATTRIBUTESQL.format(
                    product_id=parent_product_id, attribute_id=attribute_id
                )
            )

        product_id = self.get_product_id_by_sku(sku)
        if self.setting["VERSION"] == "EE" and product_id != 0:
            product_id = self.get_row_id_by_entity_id(product_id)
        self.adaptor.mysql_cursor.execute(
            self.REPLACECATALOGPRODUCTRELATIONSQL.format(
                parent_id=parent_product_id, child_id=product_id
            )
        )
        self.adaptor.mysql_cursor.execute(
            self.REPLACECATALOGPRODUCTSUPERLINKSQL.format(
                product_id=product_id, parent_id=parent_product_id
            )
        )
        self.adaptor.mysql_cursor.execute(
            self.UPDATEPRODUCTVISIBILITYSQL.format(
                id="row_id" if self.setting["VERSION"] == "EE" else "entity_id",
                value=4 if data.get("variant_visibility", False) else 1,
                product_id=product_id,
            )
        )

        # Visibility for the parent product.
        self.adaptor.mysql_cursor.execute(
            self.UPDATEPRODUCTVISIBILITYSQL.format(
                id="row_id" if self.setting["VERSION"] == "EE" else "entity_id",
                value=4,
                product_id=parent_product_id,
            )
        )
        self.adaptor.commit()
        return parent_product_id

    ## Insert Update Variants.
    def insert_update_variants(self, sku, data, store_id):
        # Validate the child product.
        # data = {
        #     "variant_visibility": True,
        #     "variants": [
        #         {
        #         "variant_sku": "abc",
        #         "attributes": {"att_a": "abc", "att_x": "xyz"}
        #         }
        #     ]
        # }
        parent_product_id = self.get_product_id_by_sku(sku)

        attrbute_ids = None
        for product in data.get("variants"):
            attributes = []
            for attribute_code, value in product["attributes"].items():
                (data_type, attribute_metadata) = self.get_attribute_metadata(
                    attribute_code, "catalog_product"
                )
                # Check if the attribute is a "SELECT".
                assert (
                    attribute_metadata["frontend_input"] == "select"
                ), "The attribute({attribute_code}) is not a 'SELECT' type.".format(
                    attribute_code=attribute_code
                )
                # Check if the product has the attribute with the option value.
                option_id = self.get_option_id(
                    attribute_metadata["attribute_id"],
                    value,
                    admin_store_id=store_id,
                )
                if option_id is None:
                    options = {0: value}
                    self.set_attribute_option_values(
                        attribute_metadata["attribute_id"],
                        options,
                        admin_store_id=store_id,
                    )
                attributes.append(
                    {
                        "attribute_code": attribute_code,
                        "value": value,
                        "attribute_id": attribute_metadata["attribute_id"],
                    }
                )

            if attrbute_ids is None:
                attribute_ids = [attribute["attribute_id"] for attribute in attributes]
            else:
                _attribute_ids = [attribute["attribute_id"] for attribute in attributes]
                assert (
                    len(list(set(attribute_ids) - set(_attribute_ids))) == 0
                ), "Previous attributes ({previous_attribute_ids}) are not matched with attributes ({attribute_ids}).".format(
                    previous_attribute_ids=",".join(
                        [str(attribute_id) for attribute_id in attribute_ids]
                    ),
                    attribute_ids=",".join(
                        [str(attribute_id) for attribute_id in _attribute_ids]
                    ),
                )

            # Check if there is a product with the same attributes.
            variants = list(
                filter(
                    lambda product: product["attributes"]
                    == dict(
                        (attribute["attribute_id"], attribute["value"])
                        for attribute in attributes
                    ),
                    self.get_variants(
                        parent_product_id,
                        attribute_ids,
                        admin_store_id=store_id,
                    ),
                )
            )
            # If there is a product matched with the attribute and value set and the sku is not matched,
            # raise an exception.
            if len(variants) != 0:
                assert (
                    variants[0]["sku"] == product["variant_sku"]
                ), "There is already a product ({sku}) matched with the attributes ({attributes}) of the sku ({variant_sku}).".format(
                    sku=variants[0]["sku"],
                    attributes=",".join(
                        [
                            f"{attribute['attribute_code']}:{attribute['value']}"
                            for attribute in attributes
                        ]
                    ),
                    variant_sku=product["variant_sku"],
                )

        # Insert or update the child product.
        for product in data.get("variants"):
            self.logger.info(f"Insert/Update the item ({product['variant_sku']}).")
            for attribute_id in attribute_ids:
                self.adaptor.mysql_cursor.execute(
                    self.REPLACECATALOGPRODUCTSUPERATTRIBUTESQL.format(
                        product_id=parent_product_id, attribute_id=attribute_id
                    )
                )

            product_id = self.get_product_id_by_sku(product["variant_sku"])
            if self.setting["VERSION"] == "EE" and product_id != 0:
                product_id = self.get_row_id_by_entity_id(product_id)
            self.adaptor.mysql_cursor.execute(
                self.REPLACECATALOGPRODUCTRELATIONSQL.format(
                    parent_id=parent_product_id, child_id=product_id
                )
            )
            self.adaptor.mysql_cursor.execute(
                self.REPLACECATALOGPRODUCTSUPERLINKSQL.format(
                    product_id=product_id, parent_id=parent_product_id
                )
            )
            self.adaptor.mysql_cursor.execute(
                self.UPDATEPRODUCTVISIBILITYSQL.format(
                    id="row_id" if self.setting["VERSION"] == "EE" else "entity_id",
                    value=4 if data.get("variant_visibility", False) else 1,
                    product_id=product_id,
                )
            )
            self.adaptor.commit()

        # Visibility for the parent product.
        self.adaptor.mysql_cursor.execute(
            self.UPDATEPRODUCTVISIBILITYSQL.format(
                id="row_id" if self.setting["VERSION"] == "EE" else "entity_id",
                value=4,
                product_id=parent_product_id,
            )
        )
        self.adaptor.commit()
        return parent_product_id

    def set_product_category(self, product_id, category_id, position=0):
        if product_id is None or category_id is None:
            pass
        else:
            self.adaptor.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            self.adaptor.mysql_cursor.execute(
                self.SETPRODUCTCATEGORYSQL,
                [category_id, product_id, position, category_id, product_id, position],
            )
            self.adaptor.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            if self.setting["VERSION"] == "EE":
                self.adaptor.mysql_cursor.execute(
                    self.UPDATECATEGORYCHILDRENCOUNTEESQL, [category_id]
                )
            else:
                self.adaptor.mysql_cursor.execute(
                    self.UPDATECATEGORYCHILDRENCOUNTSQL, [category_id]
                )

    def get_max_category_id(self):
        self.adaptor.mysql_cursor.execute(self.GETMAXCATEGORYIDSQL)
        res = self.adaptor.mysql_cursor.fetchone()
        max_category_id = int(res["max_category_id"])
        return max_category_id

    def insert_catalog_category_entity(self, current_path_ids, attribute_set="Default"):
        entity_metadata = self.get_entity_metadata("catalog_category", attribute_set)
        if entity_metadata == None:
            return 0
        entity_id = self.get_max_category_id() + 1
        parent_id = current_path_ids[-1]
        level = len(current_path_ids)
        path_ids = current_path_ids[:]
        path_ids.append(entity_id)
        path = "/".join([str(path_id) for path_id in path_ids])
        children_count = 0
        position = 0
        if self.setting["VERSION"] == "EE":
            self.adaptor.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            self.adaptor.mysql_cursor.execute(
                self.INSERTCATALOGCATEGORYENTITYEESQL,
                [
                    entity_id,
                    entity_metadata["attribute_set_id"],
                    parent_id,
                    path,
                    level,
                    children_count,
                    position,
                ],
            )
            category_id = self.adaptor.mysql_cursor.lastrowid
            self.adaptor.mysql_cursor.execute(
                """UPDATE catalog_category_entity SET entity_id = row_id WHERE row_id = %s;""",
                (category_id,),
            )
            self.adaptor.mysql_cursor.execute(
                """INSERT INTO sequence_catalog_category (sequence_value) VALUES (%s);""",
                (category_id,),
            )
            self.adaptor.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        else:
            self.adaptor.mysql_cursor.execute(
                self.INSERTCATALOGCATEGORYENTITYSQL,
                [
                    entity_metadata["attribute_set_id"],
                    parent_id,
                    path,
                    level,
                    children_count,
                    position,
                ],
            )
            category_id = self.adaptor.mysql_cursor.lastrowid
        return category_id

    def create_category(self, current_path_ids, category, store_id):
        category_id = self.insert_catalog_category_entity(current_path_ids)
        url_key = re.sub("[^0-9a-zA-Z ]+", "", category).replace(" ", "-").lower()
        data = {
            "name": category,
            "url_key": url_key,
            "url_path": url_key,
            "is_active": "1",
            "is_anchor": "1",
            "include_in_menu": "0",
            "custom_use_parent_settings": "0",
            "custom_apply_to_products": "0",
            "display_mode": "PRODUCTS",
        }
        self.insert_update_entity_data(
            category_id, data, entity_type_code="catalog_category", store_id=store_id
        )
        return category_id

    def get_category_id(self, level, parent_id, category):
        category_id = None
        sql = self.GETCATEGORYIDBYATTRIBUTEVALUEANDPATHSQL
        if self.setting["VERSION"] == "EE":
            sql = self.GETCATEGORYIDBYATTRIBUTEVALUEANDPATHEESQL
        self.adaptor.mysql_cursor.execute(sql, [level, parent_id, category])
        res = self.adaptor.mysql_cursor.fetchone()
        if res is not None and len(res) > 0:
            category_id = (
                res["row_id"] if self.setting["VERSION"] == "EE" else res["entity_id"]
            )
        return category_id

    ## Insert Update Categories.
    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(5),
    )
    def insert_update_categories(self, sku, data, ignore_category_ids=[]):
        product_id = self.get_product_id_by_sku(sku)
        if product_id == 0:
            raise Exception(f"Product ({sku}) does not existed in Magento")
        current_product_category_ids = self.get_product_category_ids(product_id)
        synced_category_ids = []
        for row in data:
            store_id = row.pop("store_id", 0)
            delimeter = row.pop("delimeter", "/")
            apply_all_levels = row.pop("apply_all_levels")
            path = row.pop("path")
            position = row.pop("position", 0)
            if path is None or path.strip() == "":
                raise Exception(f"Path is empty for {sku}")
            elif product_id == 0:
                raise Exception(f"Product ({sku}) does not existed in Magento")
            else:
                categories = path.split(delimeter)
                try:
                    parent_id = 1
                    current_path_ids = [1]
                    for idx in range(0, len(categories)):
                        current_path = delimeter.join(categories[0 : idx + 1])
                        self.logger.info(f"current_path: {current_path}")
                        category = categories[idx]
                        level = idx + 1
                        category_id = self.get_category_id(level, parent_id, category)
                        if category_id is None:
                            category_id = self.create_category(
                                current_path_ids, category, store_id
                            )

                        if apply_all_levels == True:
                            if level == 1:
                                parent_id = category_id
                                current_path_ids.append(category_id)
                                continue
                            else:
                                self.set_product_category(
                                    product_id, category_id, position=position
                                )
                                if category_id is not None:
                                    synced_category_ids.append(str(category_id))
                        elif level == len(categories):
                            self.set_product_category(
                                product_id, category_id, position=position
                            )
                            if category_id is not None:
                                synced_category_ids.append(str(category_id))

                        current_path_ids.append(category_id)
                        parent_id = category_id

                    self.adaptor.commit()
                except Exception:
                    log = traceback.format_exc()
                    self.logger.exception(log)
                    self.adaptor.rollback()
                    raise
        try:
            delete_product_category_ids = list(set(current_product_category_ids).difference(set(synced_category_ids)))
            delete_product_category_ids = list(set(delete_product_category_ids).difference(set(ignore_category_ids)))
            if len(delete_product_category_ids) > 0:
                self.logger.info("remove_category_ids: {remove_ids}".format(remove_ids=",".join(delete_product_category_ids)))
                for delete_product_category_id in delete_product_category_ids:
                    self.delete_product_category(product_id, delete_product_category_id)
                self.adaptor.commit()
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            self.adaptor.rollback()
            raise
        return product_id

    def get_product_category_ids(self, product_id):
        self.adaptor.mysql_cursor.execute(
            self.GETPRODUCTCATEGORYIDSSQL,
            [product_id]
        )
        rows = self.adaptor.mysql_cursor.fetchall()
        return [
            str(row["category_id"])
            for row in rows
        ]
    
    def delete_product_category(self, product_id, category_id):
        self.adaptor.mysql_cursor.execute(
            self.DELETEPRODUCTCATEGORYSQL, [category_id, product_id]
        )

    ## Insert Update Imagegallery.
    def insert_update_imagegallery(self, sku, data):
        product_id = self.get_product_id_by_sku(sku)
        if self.setting["VERSION"] == "EE":
            product_id = self.get_row_id_by_entity_id(product_id)

        attributes = dict((k, v) for (k, v) in data.items() if type(v) is not list)
        # self.logger.info(attributes)
        gallery_attribute_code = list(
            filter(lambda k: (type(data[k]) is list), data.keys())
        )[0]
        (data_type, gallery_attribute_metadata) = self.get_attribute_metadata(
            gallery_attribute_code, "catalog_product"
        )
        # self.logger.info(gallery_attribute_code)
        # self.logger.info(data_type)
        # self.logger.info(gallery_attribute_metadata)
        image_values = [d["value"] for d in data["media_gallery"] if "value" in d]
        # self.logger.info(image_values)
        self.adaptor.mysql_cursor.execute(
            self.DELETEPRODUCTIMAGEGALLERYEXTSQL, [product_id]
        )
        # self.logger.info(self.adaptor.mysql_cursor._last_executed)
        self.adaptor.mysql_cursor.execute(
            self.DELETEPRODUCTIMAGEGALLERYSQL, [product_id]
        )
        # self.logger.info(self.adaptor.mysql_cursor._last_executed)
        for image_value in image_values:
            self.adaptor.mysql_cursor.execute(
                self.DELTEEPRODUCTIMAGEEXTSQL, [image_value]
            )
            # self.logger.info(self.adaptor.mysql_cursor._last_executed)
        for image in data[gallery_attribute_code]:
            value = image.pop("value")
            store_id = image.pop("store_id", 0)
            label = image.pop("label", "")
            position = image.pop("position", 1)
            media_type = image.pop("media_type", "image")
            media_source = image.pop("media_source", "S3")

            self.adaptor.mysql_cursor.execute(
                self.INSERTPRODUCTIMAGEGALLERYSQL,
                [gallery_attribute_metadata["attribute_id"], value, media_type],
            )
            # self.logger.info(self.adaptor.mysql_cursor._last_executed)
            value_id = self.adaptor.mysql_cursor.lastrowid

            self.adaptor.mysql_cursor.execute(
                self.INSERTPRODUCTIMAGEGALLERYEXTSQL, [value_id, media_source, value]
            )
            # self.logger.info(self.adaptor.mysql_cursor._last_executed)

            cols = (
                ["value_id", "store_id", "row_id", "label", "position"]
                if self.setting["VERSION"] == "EE"
                else ["value_id", "store_id", "entity_id", "label", "position"]
            )
            vals = ["%s", "%s", "%s", "%s", "%s"]
            params = [value_id, store_id, product_id, label, position]
            sql = self.INSERTPRODUCTIMAGEGALLERYVALUESQL.format(
                cols=",".join(cols), vals=",".join(vals)
            )
            self.adaptor.mysql_cursor.execute(sql, params)

            cols = (
                ["value_id", "row_id"]
                if self.setting["VERSION"] == "EE"
                else ["value_id", "entity_id"]
            )
            vals = ["%s", "%s"]
            params = [value_id, product_id]
            sql = self.INSERTMEDIAVALUETOENTITYSQL.format(
                cols=",".join(cols), vals=",".join(vals)
            )
            self.adaptor.mysql_cursor.execute(sql, params)

            attribute_codes = list(
                filter(lambda k: (attributes[k] == value), attributes.keys())
            )
            if len(attribute_codes) > 0:
                for attribute_code in attribute_codes:
                    # assign the attribute.
                    (data_type, attribute_metadata) = self.get_attribute_metadata(
                        attribute_code, "catalog_product"
                    )
                    cols = (
                        ["attribute_id", "store_id", "row_id", "value"]
                        if self.setting["VERSION"] == "EE"
                        else ["attribute_id", "store_id", "entity_id", "value"]
                    )
                    vals = ["%s", "%s", "%s", "%s"]
                    params = [
                        attribute_metadata["attribute_id"],
                        store_id,
                        product_id,
                        value,
                        value,
                    ]
                    sql = self.INSERTPRODUCTIMAGESQL.format(
                        cols=",".join(cols), vals=",".join(vals)
                    )
                    self.adaptor.mysql_cursor.execute(sql, params)

        return product_id

    def get_link_attributes(self, code):
        self.adaptor.mysql_cursor.execute(self.SELECTLINKATTSQL, [code])
        row = self.adaptor.mysql_cursor.fetchone()
        if row is not None:
            link_type_id = int(row["link_type_id"])
            product_link_attribute_id = int(row["product_link_attribute_id"])
        else:
            raise Exception(f"cannot find link_type_id for {code}.")
        return (link_type_id, product_link_attribute_id)

    ## Insert Update Links.
    def insert_update_links(self, sku, data):
        product_id = self.get_product_id_by_sku(sku)
        if self.setting["VERSION"] == "EE":
            product_id = self.get_row_id_by_entity_id(product_id)
        if product_id == 0:
            return product_id
        for code, links in data.items():
            (link_type_id, product_link_attribute_id) = self.get_link_attributes(code)
            if links:
                self.adaptor.mysql_cursor.execute(
                    self.DELETEPRODUCTLINKSQL, [product_id, link_type_id]
                )
            for link in links:
                linked_product_id = self.get_product_id_by_sku(link["linked_sku"])
                if self.setting["VERSION"] == "EE":
                    linked_product_id = self.get_row_id_by_entity_id(linked_product_id)
                if linked_product_id == 0:
                    self.logger.warning(
                        f"sku/link: {sku}/{link} failed. Linked product is not found."
                    )
                    continue
                self.adaptor.mysql_cursor.execute(
                    self.INSERTCATALOGPRODUCTLINKSQL,
                    [product_id, linked_product_id, link_type_id],
                )
                linkId = self.adaptor.mysql_cursor.lastrowid
                self.adaptor.mysql_cursor.execute(
                    self.INSERTCATALOGPRODUCTLINKATTRIBUTEINT,
                    [product_link_attribute_id, linkId, link["position"]],
                )
        return product_id

    def get_product_qut_of_stock_qty(self, product_id, min_qty=0):
        out_of_stock_qty = min_qty
        self.adaptor.mysql_cursor.execute(
            self.GETPRODUCTOUTOFSTOCKQTYSQL, [product_id, 1]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        if res is not None and len(res) > 0:
            out_of_stock_qty = res["min_qty"]
        return out_of_stock_qty

    ## Insert Update Inventory.
    def insert_update_inventory(self, sku, data):
        product_id = self.get_product_id_by_sku(sku)
        metadata = dict((k, list(set(map(lambda d: d[k], data)))) for k in ["store_id"])
        for store_id in metadata["store_id"]:
            website_id = self.get_website_id_by_store_id(store_id)
            qty = sum(
                int(item["qty"])
                for item in list(filter(lambda t: (t["store_id"] == store_id), data))
            )
            out_of_stock_qty = self.get_product_qut_of_stock_qty(
                product_id, website_id=website_id
            )
            if qty > int(out_of_stock_qty):
                is_in_stock = 1
                stock_status = 1
            else:
                is_in_stock = 0
                stock_status = 0
            self.adaptor.mysql_cursor.execute(
                self.SETSTOCKSTATUSQL,
                [product_id, website_id, 1, qty, stock_status, qty, stock_status],
            )
            self.adaptor.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            self.adaptor.mysql_cursor.execute(
                self.SETSTOCKITEMSQL,
                [product_id, 1, qty, is_in_stock, website_id, qty, is_in_stock],
            )
            self.adaptor.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        return product_id

    def get_entity_attribute_value(self, entity_type_code, entity_id, attribute_code, store_id=0):
        try:
            (data_type, attribute_metadata) = self.get_attribute_metadata(attribute_code, entity_type_code)
        except Exception as e:
            return None
        attribute_id = attribute_metadata["attribute_id"]
        if data_type != "static":
            if (
                entity_type_code == "catalog_product"
                or entity_type_code == "catalog_category"
            ):
                id_field = "entity_id"
                if self.setting["VERSION"] == "EE":
                    id_field = "row_id"
            else:
                id_field = "entity_id"
            sql = self.GETENTITYATTRIBUTEVALUESQL.format(
                entity_type_code=entity_type_code, data_type=data_type, id_field=id_field
            )
            self.adaptor.mysql_cursor.execute(sql, [entity_id, attribute_id, store_id])
            res =self.adaptor.mysql_cursor.fetchone()
            if not res:
                return None
            value = res["value"]
            if attribute_metadata["frontend_input"] == "select":
                option_id = value
                value = self.get_option_value(option_id, store_id)
            elif attribute_metadata["frontend_input"] == "multiselect":
                option_ids = []
                if value:
                    option_ids = value.split(",")
                    multi_values = []
                    for option_id in option_ids:
                        line_value = self.get_option_value(option_id, store_id)
                        if line_value:
                            multi_values.append(line_value)
                    value = multi_values
            return value
        else:
            return None
        
    def get_option_value(self, option_id, store_id):
        self.adaptor.mysql_cursor.execute(
            self.GETOPTIONVALUESQL,
            [option_id, store_id]
        )
        res =self.adaptor.mysql_cursor.fetchone()
        value = None
        if res is not None:
            value = res["value"]
        return value


class Mage2OrderConnector(object):

    STATE_NEW = "new"
    STATE_PENDING_PAYMENT = "pending_payment"
    STATE_PROCESSING = "processing"
    STATE_COMPLETE = "complete"
    STATE_CLOSED = "closed"
    STATE_CANCELED = "canceled"
    STATE_HOLDED = "holded"
    STATE_PAYMENT_REVIEW = "payment_review"

    GETORDERBYINCREMENTIDSQL = """
        SELECT *
        FROM sales_order
        WHERE increment_id = %s;
    """

    UPDATEORDERSTATUSANDSTATESQL = """
        UPDATE sales_order
        SET state = %s,
        status = %s
        WHERE entity_id = %s;
    """

    GETORDERCOMMENTLISTSQL = """
        SELECT *
        FROM sales_order_status_history
        WHERE parent_id = %s
    """

    INSERTORDERCOMMENTSQL = """
        INSERT INTO sales_order_status_history
        (entity_id, parent_id, is_customer_notified, is_visible_on_front, comment, status, created_at, entity_name)
        VALUES(NULL, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), %s);
    """

    GETORDERITEMSSQL = """
        SELECT *
        FROM sales_order_item
        WHERE order_id = %s
    """
    

    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.adaptor = Adaptor(**setting)
        self.mage2_domain = setting.get("mage2_domain")
        self.mage2_token = setting.get("mage2_token")
        self.mage2_scope = setting.get("mage2_scope", "default")

    def __del__(self):
        if self.adaptor:
            self.adaptor.__del__()
            self.logger.info("Close Mage2 DB connection")

    @property
    def adaptor(self):
        return self._adaptor

    @adaptor.setter
    def adaptor(self, adaptor):
        self._adaptor = adaptor
    
    def request_magento_rest_api(self, api_path, method="POST", payload={}):
        api_url = "{domain}/rest/{scope}/V1/{api_path}".format(
            domain=self.mage2_domain,
            scope=self.mage2_scope,
            api_path=api_path
        )
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token=self.mage2_token)
        }
        method = method.upper()
        response = requests.request(
            method,
            api_url,
            headers=headers,
            data=json.dumps(payload),
        )
        if response.status_code == 200:
            return response.content
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def get_order_by_increment_id(self, increment_id):
        self.adaptor.mysql_cursor.execute(
            self.GETORDERBYINCREMENTIDSQL,
            [increment_id]
        )
        res = self.adaptor.mysql_cursor.fetchone()
        return res

    def ship_order(self, order_id, items=[], notify=False, append_comment=False, comment=None, is_visible_on_front=False, tracks=[], packages=[], arguments=None):
        
        payload = {
            "items": items,
            "notify": notify,
            "appendComment": append_comment,
            "comment": {"comment": comment, "isVisibleOnFront": 1 if is_visible_on_front else 0} if comment is not None else None,
            "tracks": tracks
        }
        api_path = "order/{order_id}/ship".format(order_id=order_id)
        return self.request_magento_rest_api(api_path=api_path, method="POST", payload=payload)

    def invoice_order(self, order_id, capture=False, items=[], notify=False, append_comment=False, comment=None, is_visible_on_front=False, arguments=None):
        payload = {
            "items": items,
            "notify": notify,
            "capture": capture,
            "appendComment": append_comment,
            "comment": {"comment": comment, "isVisibleOnFront":1 if is_visible_on_front else 0} if comment is not None else None
        }
        api_path = "order/{order_id}/invoice".format(order_id=order_id)
        return self.request_magento_rest_api(api_path=api_path, method="POST", payload=payload)
    
    def insert_order_comment(self, order_id, comment, status, allow_duplicate_comment=False, is_customer_notified=0, is_visible_on_front=0, entity_name="order"):
        can_add_comment = True
        if not allow_duplicate_comment:
            order_comments = self.get_order_comment_list(order_id)
            for order_comment in order_comments:
                if order_comment["comment"] == comment:
                    can_add_comment = False
        if can_add_comment:
            self.add_order_comment(
                order_id=order_id,
                comment=comment,
                status=status,
                is_customer_notified=is_customer_notified,
                is_visible_on_front=is_visible_on_front,
                entity_name=entity_name
            )

    def get_order_comment_list(self, order_id):
        self.adaptor.mysql_cursor.execute(
            self.GETORDERCOMMENTLISTSQL,
            [order_id]
        )
        rows = self.adaptor.mysql_cursor.fetchall()
        return rows

    def add_order_comment(self, order_id, comment, status, is_customer_notified=0, is_visible_on_front=0, entity_name="order"):
        self.adaptor.mysql_cursor.execute(
            self.INSERTORDERCOMMENTSQL,
            [
                order_id,
                is_customer_notified,
                is_visible_on_front,
                comment,
                status,
                entity_name
            ],
        )
        self.adaptor.commit()

    def get_order_items(self, order_id):
        self.adaptor.mysql_cursor.execute(
            self.GETORDERITEMSSQL,
            [order_id]
        )
        rows = self.adaptor.mysql_cursor.fetchall()
        return rows

    def is_dummy_order_item(self, format_order_item, is_shipment=False):
        if is_shipment:
            if format_order_item.get("has_children", False) and format_order_item.get("is_ship_separately", False):
                return True
            
            if format_order_item.get("has_children", False) and not format_order_item.get("is_ship_separately", False):
                return False

            if format_order_item.get("has_parent", False) and format_order_item.get("is_ship_separately", False):
                return False

            if format_order_item.get("has_parent", False) and not format_order_item.get("is_ship_separately", False):
                return True
        else:
            if format_order_item.get("has_children", False) and format_order_item.get("is_children_calculated", False):
                return True
            
            if format_order_item.get("has_children", False) and not format_order_item.get("is_children_calculated", False):
                return False

            if format_order_item.get("has_parent", False) and format_order_item.get("is_children_calculated", False):
                return False

            if format_order_item.get("has_parent", False) and not format_order_item.get("is_children_calculated", False):
                return True
        return False

    def format_order_items(self, order_items):
        format_items = []
        for item in order_items:
            has_children = False
            has_parent = False
            is_ship_separately = False
            is_children_calculated = False
            parent_item = None
            for order_item in order_items:
                if item["item_id"] == order_item["item_id"]:
                    continue
                if item["parent_item_id"] == order_item["item_id"]:
                    has_parent = True
                    parent_item = order_item
                if item["item_id"] == order_item["parent_item_id"]:
                    has_children = True
                    
            if has_parent and parent_item:
                product_options = json.loads(item.get("product_options")) if item.get("product_options", None) is not None else None
                if product_options is not None and product_options.get("shipment_type") == 1:
                    is_ship_separately = True
                if product_options is not None and product_options.get("product_calculations") == 0:
                    is_children_calculated = True

            item["has_children"] = has_children
            item["has_parent"] = has_parent
            item["is_ship_separately"] = is_ship_separately
            item["is_children_calculated"] = is_children_calculated
            format_items.append(item)

        return format_items


    def can_ship_order(self, order):
        order_state = order.get("state")
        is_virtual = order.get("is_virtual")
        if self.can_unhold_order(order_state) or self.is_order_payment_review(order_state):
            return False
        if self.is_order_canceled(order_state) or is_virtual:
            return False
        order_items = self.get_order_items(order.get("entity_id"))
        format_items = self.format_order_items(order_items)
        for format_order_item in format_items:
            if self.is_dummy_order_item(format_order_item, True):
                continue
            qty_to_ship = format_order_item.get("qty_ordered", 0) - max([format_order_item.get("qty_shipped", 0), format_order_item.get("qty_refunded", 0), format_order_item.get("qty_canceled", 0)])
            if (
                qty_to_ship > 0
                and not format_order_item.get("is_virtual") 
                and not format_order_item.get("locked_do_ship") 
                and not (format_order_item.get("qty_refunded") == format_order_item.get("qty_ordered"))
            ):
                return True
        return False

    def can_ship_order_items(self, order, order_item_ids=[]):
        order_state = order.get("state")
        is_virtual = order.get("is_virtual")
        if self.can_unhold_order(order_state) or self.is_order_payment_review(order_state):
            return False
        if self.is_order_canceled(order_state) or is_virtual:
            return False
        order_items = self.get_order_items(order.get("entity_id"))
        format_items = self.format_order_items(order_items)
        can_ship_items = True
        for format_order_item in format_items:
            if len(order_item_ids) > 0 and (format_order_item.get("item_id") not in order_item_ids and format_order_item.get("parent_item_id") not in order_item_ids):
                continue
            if self.is_dummy_order_item(format_order_item, True):
                continue
            qty_to_ship = format_order_item.get("qty_ordered", 0) - max([format_order_item.get("qty_shipped", 0), format_order_item.get("qty_refunded", 0), format_order_item.get("qty_canceled", 0)])
            if (
                qty_to_ship > 0
                and not format_order_item.get("is_virtual") 
                and not format_order_item.get("locked_do_ship") 
                and not (format_order_item.get("qty_refunded") == format_order_item.get("qty_ordered"))
            ):
                continue
            else:
                can_ship_items = False
        return can_ship_items

    def can_invoice_order(self, order):
        order_state = order.get("state")
        if self.can_unhold_order(order_state) or self.is_order_payment_review(order_state):
            return False
        if self.is_order_canceled(order_state) or order_state == self.STATE_COMPLETE or order_state == self.STATE_CLOSED:
            return False

        order_items = self.get_order_items(order.get("entity_id"))
        format_items = self.format_order_items(order_items)
        for format_order_item in format_items:
            if self.is_dummy_order_item(format_order_item):
                continue
            qty_to_invoice = format_order_item.get("qty_ordered", 0) - format_order_item.get("qty_invoiced", 0) -format_order_item.get("qty_canceled", 0)
            if (
                qty_to_invoice > 0
                and not format_order_item.get("locked_do_invoice")
            ):
                return True
        return False

    def can_invoice_order_items(self, order, order_item_ids=[]):
        order_state = order.get("state")
        if self.can_unhold_order(order_state) or self.is_order_payment_review(order_state):
            return False
        if self.is_order_canceled(order_state) or order_state == self.STATE_COMPLETE or order_state == self.STATE_CLOSED:
            return False

        order_items = self.get_order_items(order.get("entity_id"))
        format_items = self.format_order_items(order_items)
        can_invoice_items = True
        for format_order_item in format_items:
            if len(order_item_ids) > 0 and (format_order_item.get("item_id") not in order_item_ids and format_order_item.get("parent_item_id") not in order_item_ids):
                continue
            if self.is_dummy_order_item(format_order_item):
                continue
            qty_to_invoice = format_order_item.get("qty_ordered", 0) - format_order_item.get("qty_invoiced", 0) -format_order_item.get("qty_canceled", 0)
            if (
                qty_to_invoice > 0
                and not format_order_item.get("locked_do_invoice")
            ):
                continue
            else:
                can_invoice_items = False
        return can_invoice_items

    def update_order_state_status(self, order_id, state, status):
        self.adaptor.mysql_cursor.execute(
            self.UPDATEORDERSTATUSANDSTATESQL,
            [
               state,
               status,
               order_id
            ],
        )
        self.adaptor.commit()
    
    def can_unhold_order(self, order_state):
        if self.is_order_payment_review(order_state):
            return False
        return order_state == self.STATE_HOLDED

    def is_order_canceled(self, order_state):
        return order_state == self.STATE_CANCELED
    
    def is_order_payment_review(self, order_state):
        return order_state == self.STATE_PAYMENT_REVIEW
