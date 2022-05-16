#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import logging, sys, unittest, os, time, json, traceback
from dotenv import load_dotenv

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

    @unittest.skip("demonstrating skipping")
    def test_insert_update_product(self):
        try:
            logger.info(f"Stated at {time.strftime('%X')}")
            product = json.load(
                open(
                    "/var/www/projects/mage2_connector/mage2_connector/tests/product.json",
                    "r",
                )
            )
            sku = product["sku"]
            attribute_set = product["data"].pop("attribute_set")
            type_id = product["data"].pop("type_id")
            store_id = product["data"].pop("store_id")
            self.mage2_connector.insert_update_product(
                sku, attribute_set, product["data"], type_id, store_id
            )
            logger.info(f"Finished at {time.strftime('%X')}")
        except Exception:
            log = traceback.format_exc()
            logger.exception(log)

    # @unittest.skip("demonstrating skipping")
    def test_insert_update_categories(self):
        try:
            logger.info(f"Stated at {time.strftime('%X')}")
            sku = "107141-100-10641-12219"
            data = [
                {"path": "Default Category/catalog", "apply_all_levels": False},
                {
                    "path": "Default Category/Catalog/IO Connect",
                    "apply_all_levels": False,
                },
                # {"path": "Default Category/Catalog/High Volume Bid", "apply_all_levels": False},
                # {"path": "Default Category/Catalog/Short Shelf Life", "apply_all_levels": False},
            ]

            self.mage2_connector.insert_update_categories(sku, data)
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
