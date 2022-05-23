import json, pendulum

FIELDS = {
    "applications": {
        "magento_attribute": "path",
        "custom_handle": {
            "func":"format_categories",
            "additional_fields": {
            }
        }
    },
    "base_price": {
        "magento_attribute": "price"
    },
    "cas_number": {
        "magento_attribute": "cas"
    },
    "categories": {
        "magento_attribute": "path",
        "custom_handle": {
            "func":"format_categories",
            "additional_fields": {
            }
        }
    },
    "certifications": {
        "magento_attribute": "certifications",
        "custom_handle": {
            "func": "format_list",
            "additional_fields": {
                "delimiter": "|"
            }
        }
    },
    "chempax_sku": {
        "magento_attribute": None
    },
    "container_height": {
        "magento_attribute": "con_height"
    },
    "container_length": {
        "magento_attribute": "con_length"
    },
    "container_weight": {
        "magento_attribute": "con_weight"
    },
    "container_width": {
        "magento_attribute": "con_width"
    },
    "country_of_origin": {
        "magento_attribute": "country_of_origin"
    },
    "default_base_price": {
        "magento_attribute": None
    },
    "description": {
        "magento_attribute": "description"
    },
    "factory_code": {
        "magento_attribute": "manufacturer"
    },
    "freight_class": {
        "magento_attribute": "freight_class"
    },
    "highlights": {
        "magento_attribute": "highlights"
    },
    "hvb_enable": {
        "magento_attribute": "allow_bidding"
    },
    "io_connect": {
        "magento_attribute": "io_connect"
    },
    "is_branded": {
        "magento_attribute": "advanced_product"
    },
    "is_high_value": {
        "magento_attribute": "high_value"
    },
    "is_ssl": {
        "magento_attribute": "short_shelf_life"
    },
    "is_paid_promotion": {
        "magento_attribute": None
    },
    "item_name": {
        "magento_attribute": None
    },
    "master_product_code": {
        "magento_attribute": "master_product_code"
    },
    "meta_description": {
        "magento_attribute": "meta_description"
    },
    "meta_keywords": {
        "magento_attribute": "meta_keyword"
    },
    "meta_title": {
        "magento_attribute": "meta_title"
    },
    "min_allowed_qty": {
        "magento_attribute": "min_sale_qty",
        "custom_handle": {
            "func":"format_stock_data"
        }
    },
    "min_bidding_qty": {
        "magento_attribute": None
    },
    "molecular_formula": {
        "magento_attribute": "molecular_formula"
    },
    "must_ship_freight": {
        "magento_attribute": "must_ship_freight"
    },
    "name": {
        "magento_attribute": "name"
    },
    "news_from_date": {
        "magento_attribute": "news_from_date",
    },
    "news_to_date": {
        "magento_attribute": "news_to_date"
    },
    "nmfc_class": {
        "magento_attribute": "nmfc_class"
    },
    "other_names": {
        "magento_attribute": "alias"
    },
    "packaging_code": {
        "magento_attribute": "pack_type"
    },
    "product_division": {
        "magento_attribute": "product_division"
    },
    "product_name": {
        "magento_attribute": None
    },
    "promoted_count": {
        "magento_attribute": "promoted_count"
    },
    "qty_increments": {
        "magento_attribute": "qty_increments",
        "custom_handle": {
            "func":"format_stock_data"
        }
    },
    "scientific_findings": {
        "magento_attribute": None
    },
    "seller_display_name": {
        "magento_attribute": None
    },
    "seller_id": {
        "magento_attribute": "seller_id"
    },
    "seller_sku": {
        "magento_attribute": None
    },
    "shelf_life": {
        "magento_attribute": "shelf_life_days"
    },
    "sku": {
        "magento_attribute": "sku"
    },
    "solubility": {
        "magento_attribute": "solubility"
    },
    "ss2_doc": {
        "magento_attribute": "ss2_doc",
        "custom_handle": {
            "func":"format_jsonstring"
        }
    },
    "status": {
        "magento_attribute": "status"
    },
    "tare_weight": {
        "magento_attribute": "tare_weight"
    },
    "type": {
        "magento_attribute": "type_id"
    },
    "uom": {
        "magento_attribute": "uom"
    },
    "url_key": {
        "magento_attribute": None
    },
    "warehouse_lead_time": {
        "magento_attribute": None
    },
    "warnings": {
        "magento_attribute": "specs"
    },
    "weight": {
        "magento_attribute": "weight"
    },
    "pricelevels": {
        "magento_attribute": "tier_price",
        "custom_handle": {
            "func":"format_tier_price"
        }
    },
    "special_price": {
        "magento_attribute": "special_price",
        "custom_handle": {
            "func":"format_special_price"
        }
    },
    "parent_sku": {
        "magento_attribute": "parent_product_sku",
        "custom_handle": {
            "func": "format_variant"
        }
    }
}

class FormatProduct(object):

    def __init__(self, data):
        super().__init__()
        self.data = data
        self.base_data = {}
        self.categories = []
        self.stock_data = {}
        self.tier_prices = []

    def get_magento_data(self):
        for ss2_field, value in self.data.items():
            if FIELDS.get(ss2_field):
                if FIELDS.get(ss2_field, {}).get("custom_handle") and FIELDS.get(ss2_field, {}).get("magento_attribute"):
                    self.solve_custom_handle(FIELDS.get(ss2_field, {}).get("custom_handle"), FIELDS.get(ss2_field, {}).get("magento_attribute"), value)
                elif FIELDS.get(ss2_field, {}).get("magento_attribute"):
                    self.base_data[FIELDS.get(ss2_field, {}).get("magento_attribute")] = value

        self.solve_product_price_data()
        self.base_data["attribute_set"] = "Default"
        self.base_data["store_id"] = 0
        if self.base_data["type_id"] == "simple":
            self.base_data['visibility'] = 1
        else:
            self.base_data['visibility'] = 4

        variant_data = None
        if self.base_data.get("type_id") == "simple" and self.data.get("parent_sku", None) and self.base_data.get("pack_type", None):
            variant_data = {
                "variant_visibility": True,
                "parent_product_sku": self.data.get("parent_sku"),
                "variant_attributes": {"pack_type": self.base_data.get("pack_type")},
            }

        magento_data = {
            "product_data" : self.base_data,
            "category_data" : self.categories,
            "stock_data": self.stock_data,
            "tier_price_data": self.tier_prices,
            "variant_data": variant_data
        }
        return magento_data
        
    def solve_custom_handle(self, custom_handle_object, magento_field, value):
        function_name = custom_handle_object.get("func")
        func = getattr(self, function_name, None)
        if func is not None:
            if custom_handle_object.get("additional_fields"):
                func(magento_field, value, custom_handle_object.get("additional_fields"))
            else:
                func(magento_field, value)

    def format_stock_data(self, field, value):
        self.stock_data = dict(
            self.stock_data, **{field: value}
        )

    def format_categories(self, field, value, additional_fields={}):
        if isinstance(value, str):
            value = [value]
        for category_name_path in value:
            path_arr = ["Default Category"]
            path_arr.append(category_name_path)
            category_data = {"path": "/".join(path_arr), "apply_all_levels": False}
            self.categories.append(category_data)
    
    def format_jsonstring(self, field, value):
        self.base_data[field] = json.dumps(value)

    def format_list(self, field, value, additional_fields={}):
        if isinstance(value, list):
            delimiter = additional_fields.get("delimiter", "|")
            value = delimiter.join(value)
        self.base_data[field] = value

    def format_tier_price(self, field, value):
        tier_prices = []
        self.base_data["vip_price"] = None
        for group_price in value:
            if group_price.get("name") == "ALL GROUPS":
                for tier_price in group_price.get("pricelist", []):
                    tier_prices.append(
                        {
                            "value": tier_price.get("price"),
                            "qty": tier_price.get("qty"),
                            "all_groups": 1,
                            "customer_group_id": 0
                        }
                    )
            if group_price.get("name") == "Tax Exempted - VIP" and len(group_price.get("pricelist", [])) > 0:
                self.base_data["vip_price"] = group_price.get("pricelist")[0].get("price")
        self.tier_prices = tier_prices

    def format_special_price(self, field, value):
        self.base_data["special_price"] = value.get("unit_price", None)
        self.base_data["special_from_date"] = value.get("start_date", None)
        self.base_data["special_to_date"] = value.get("end_date", None)

    def solve_product_price_data(self):
        has_promotion_price = False
        if self.base_data.get("special_price"):
            today = pendulum.now(tz="utc").to_date_string()
            from_date = None
            if self.base_data.get("special_from_date", None):
                from_date = pendulum.parse(self.base_data.get("special_from_date"), tz="utc").to_date_string()
            to_date = None
            if self.base_data.get("special_to_date", None):
                to_date = pendulum.parse(self.base_data.get("special_to_date"), tz="utc").to_date_string()

            if from_date is None and to_date is None:
                has_promotion_price = True
            elif from_date is None and to_date is not None and to_date >= today:
                has_promotion_price = True
            elif from_date is not None and to_date is None and from_date <= today:
                has_promotion_price = True
            elif from_date is not None and to_date is not None and from_date <= today and to_date >= today:
                has_promotion_price = True

        if has_promotion_price:
            self.tier_prices = []
            self.base_data["vip_price"] = None
        else:
            self.base_data["special_price"] = None
            self.base_data["special_from_date"] = None
            self.base_data["special_to_date"] = None

    

            

        