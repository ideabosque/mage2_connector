#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import logging, sys, unittest, os, traceback, time
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

setting = {
    "VERSION": os.getenv("VERSION"),
    "SSHSERVER": os.getenv("SSHSERVER"),
    "SSHSERVERPORT": int(os.getenv("SSHSERVERPORT")),
    "SSHUSERNAME": os.getenv("SSHUSERNAME"),
    "REMOTEBINDSERVER": os.getenv("REMOTEBINDSERVER"),
    "REMOTEBINDSERVERPORT": int(os.getenv("REMOTEBINDSERVERPORT")),
    "LOCALBINDSERVER": os.getenv("LOCALBINDSERVER"),
    "LOCALBINDSERVERPORT": int(os.getenv("LOCALBINDSERVERPORT")),
    "host": os.getenv("host"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "schema": os.getenv("schema"),
    "port": int(os.getenv("port")),
    "SSHPKEY": os.getenv("SSHPKEY"),
}


sys.path.insert(0, "/var/www/projects/mage2_connector")
# sys.path.insert(0, "C:/Users/Bibo W/GitHub/silvaengine/mage2_connector")

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

from mage2_connector import Mage2Connector


class Mage2ConnectorTest(unittest.TestCase):
    def setUp(self):
        self.mage2_connector = Mage2Connector(logger, **setting)
        logger.info("Initiate Mage2ConnectorTest ...")

    def tearDown(self):
        del self.mage2_connector
        logger.info("Destory Mage2ConnectorTest ...")

    # @unittest.skip("demonstrating skipping")
    def test_insert_update_product(self):
        try:
            logger.info(f"Stated at {time.strftime('%X')}")
            sku = "106247-000-10250-11236"
            attribute_set = "Default"
            data = {
                "am_email": "bwang@abacusipllc.com",
                "manufacturer": "NUTRAMAX",
                # "applications": ["Food and Beverage", "Applications"],
                # "base_price": "",
                # "cas_number": "64849-39-4",
                # "categories": ["Food Additives", "Category", "IO Connect"],
                # "certifications": [],
                # "chempax_sku": "",
                # "container_height": 0,
                # "container_length": 0,
                # "container_weight": 0,
                # "container_width": 0,
                # "country_of_origin": "CN",
                # "default_base_price": "",
                # "description": "<p>Sweet Blackberry Leaves Extract 70%-95% HPLC is 100% water-soluble, has good PH value, and stability. It is thought to have antioxidant properties. It has the clear fragrance of sweet tea, pure sweetness, and a comfortable taste. It is 300 times sweeter than sucrose. It has low calories and zero fat. It is often used in flavors and fragrances.&nbsp;</p>",
                # "factory_name": "Hunan NutraMax Inc.",
                # "freight_class": "70",
                # "highlights": "<ul><li>Flavor Enhancement</li><li>Flavor Modifier</li></ul>",
                # "hvb_enable": 0,
                # "io_connect": 1,
                # "is_branded": 0,
                # "is_high_value": 0,
                # "item_name": "Sweet Blackberry Leaves Extract 70%-95% HPLC by NUTRAMAX",
                # "master_product_code": "",
                # "meta_title": "Sweet Blackberry Leaves Extract 70%-95% HPLC - Hunan Nutramax Inc. | Ingredients Online",
                # "min_allowed_qty": 0,
                # "min_bidding_qty": 0,
                # "molecular_formula": "C32H50O13",
                # "must_ship_freight": 1,
                # "name": "Sweet Blackberry Leaves Extract 70%-95% HPLC by Hunan NutraMax",
                # "news_from_date": "",
                # "news_to_date": "",
                # "nmfc_class": "072170",
                # "packaging_code": "",
                # "product_division": "Local",
                # "product_name": "Sweet Blackberry Leaves Extract 70%-95% HPLC",
                # "promoted_count": 0,
                # "qty_increments": 0,
                # "seller_display_name": "Hunan NutraMax",
                # "seller_id": 11236,
                # "seller_sku": "",
                # "shelf_life": 24,
                # "sku": "106247-000-10250-11236",
                # "solubility": "Easily soluble in water and ethanol",
                # "ss2_doc": {
                #     "company_info": {
                #         "annual_export_amt": None,
                #         "annual_export_vol": None,
                #         "certificaitons": "",
                #         "company_url": "www.nutra-max.com",
                #         "found": None,
                #         "logo": "https://ss2-us-public-live.s3-us-west-1.amazonaws.com/factory-logo/2082_logo.jpg",
                #         "major_customers": None,
                #         "major_ingredients": None,
                #         "name": "Hunan NutraMax Inc.",
                #         "number_of_employees": None,
                #         "url": "https://www.ingredientsonline.com/factories/nutramax.html",
                #     },
                #     "docs": [
                #         {
                #             "doc_prefix": "SPEC",
                #             "name": "SPEC-11236-NUTRAMAX",
                #             "sort_order": 165,
                #             "type": "product_documentation",
                #             "url": "https://ss2-us-public-live.s3-us-west-1.amazonaws.com/seller-document/2207/55wXq9j7XCdrfG2rkExK2hFahiWUtikvTnx0Snoj.pdf",
                #         },
                #         {
                #             "name": "All-Product-Documents.zip",
                #             "type": "product_documentation",
                #             "url": "https://ss2-us-public-live.s3-us-west-1.amazonaws.com/zip-documents/product-docs/6247/All-Product-Documents.zip",
                #         },
                #     ],
                # },
                # "status": 1,
                # "tare_weight": 0,
                # "type": "virtual",
                # "uom": "",
                # "url_key": "",
                # "warehouse_lead_time": [],
                # "warnings": "",
                # "weight": 1,
            }
            type_id = "virtual"
            store_id = 0
            self.mage2_connector.insert_update_product(
                sku, attribute_set, data, type_id, store_id
            )
            logger.info(f"Finished at {time.strftime('%X')}")
        except Exception:
            log = traceback.format_exc()
            logger.exception(log)


if __name__ == "__main__":
    import paramiko, io
    from sshtunnel import SSHTunnelForwarder

    try:
        with SSHTunnelForwarder(
            (setting["SSHSERVER"], setting["SSHSERVERPORT"]),
            ssh_username=setting["SSHUSERNAME"],
            ssh_pkey=paramiko.RSAKey.from_private_key(
                io.StringIO(setting.get("SSHPKEY", None))
            ),
            ssh_password=setting.get("SSHPASSWORD", None),
            remote_bind_address=(
                setting["REMOTEBINDSERVER"],
                setting["REMOTEBINDSERVERPORT"],
            ),
            local_bind_address=(
                setting["LOCALBINDSERVER"],
                setting["LOCALBINDSERVERPORT"],
            ),
        ) as server:
            unittest.main()
            server.stop()
            server.close()
    except Exception as e:
        log = "Failed to connect ssh server with error: %s" % str(e)
        logger.exception(log)
        raise
