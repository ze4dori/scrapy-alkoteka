import scrapy
import json
import time


class SiteSpider(scrapy.Spider):
    name = "spider_name"
    allowed_domains = ["alkoteka.com"]

    start_urls  = [
        "slaboalkogolnye-napitki-2",
        "bezalkogolnye-napitki-1",
        "aksessuary-2"
    ]

    # UUID города Краснодара
    city_uuid = "65e2983b-d801-11eb-80d3-00155d03900a"

    per_page = 20

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_products = set()

    def start_requests(self):
        for category in self.start_urls:
            url = self.build_api_url(category, page=1)
            yield scrapy.Request(
                url,
                callback=self.parse_api,
                meta={"category": category, "page": 1}
            )

    def build_api_url(self, category_slug, page):
        return (
            f"https://alkoteka.com/web-api/v1/product"
            f"?city_uuid={self.city_uuid}"
            f"&page={page}&per_page={self.per_page}"
            f"&root_category_slug={category_slug}"
        )

    def parse_api(self, response):
        data = json.loads(response.text)
        category = response.meta["category"]
        page = response.meta["page"]

        results = data.get("results", [])

        for product in results:
            timestamp = int(time.time())
            product_url = product.get("product_url")
            product_slug = product_url.rstrip("/").split("/")[-1]

            if product_slug in self.seen_products:
                return

            self.seen_products.add(product_slug)

            detail_url = (
                f"https://alkoteka.com/web-api/v1/product/{product_slug}"
                f"?city_uuid={self.city_uuid}"
            )

            yield scrapy.Request(
                detail_url,
                callback=self.parse_product_detail,
                meta={
                    "timestamp": timestamp,
                    "short_product": product,
                }
            )

        if results:
            next_page = page + 1
            next_url = self.build_api_url(category, next_page)
            yield scrapy.Request(
                next_url,
                callback=self.parse_api,
                meta={"category": category, "page": next_page}
            )

    # ============ КАРТОЧКА ТОВАРА ============
    def parse_product_detail(self, response):
        data = json.loads(response.text)
        full = data.get("results", {})
        short = response.meta["short_product"]

        timestamp = response.meta["timestamp"]

        title_name = short.get("name")

        attributes = [
            f.get("title") for f in short.get("filter_labels", [])
            if f.get("filter") in ["obem"]
        ]
        if attributes and not any(attr in title_name for attr in attributes):
            title_name = f"{title_name}, {', '.join(attributes)}"

        sale_tag = None
        marketing_tags = []
        for label in short.get("action_labels", []):
            marketing_tags.append(label.get("title"))
            if sale_tag is None:
                sale_tag = label.get("title")

        section = []
        for fl in short.get("filter_labels", []):
            if fl.get("filter") in ["categories", "cvet"]:
                if fl.get("title"):
                    section.append(fl.get("title"))

        # ============ МЕТАДАННЫЕ ============
        metadata = {}

        for block in full.get("text_blocks", []):
            if block.get("title") == "Описание":
                metadata["__description"] = block.get("content", "")
                break

        for block in full.get("description_blocks", []):
            title = block.get("title")
            if not title:
                continue

            if block["type"] == "select":
                values = [str(v.get("name")) for v in block.get("values", []) if v.get("name") is not None]
                metadata[title] = ", ".join(values)

            elif block["type"] == "range":
                mn = block.get("min")
                mx = block.get("max")
                unit = block.get("unit", "")
                if mn == mx:
                    metadata[title] = f"{mn}{unit}"
                else:
                    metadata[title] = f"{mn}–{mx}{unit}"

        metadata["Артикул"] = str(full.get("vendor_code", ""))
        metadata["Страна производитель"] = full.get("country_name", "")

        variants = len({
            fl.get("filter")
            for fl in short.get("filter_labels", [])
            if fl.get("filter") in ["cvet", "obem"]
        })

        if variants <= 2:
            variants = 1

        yield {
            "timestamp": timestamp,
            "RPC": short.get("vendor_code"),
            "url": short.get("product_url"),
            "title": title_name,
            "marketing_tags": marketing_tags,
            "brand": None,
            "section": section,

            "price_data": {
                "current": short.get("price"),
                "original": short.get("prev_price"),
                "sale_tag": sale_tag,
            },

            "stock": {
                "in_stock": short.get("available", True),
                "count": short.get("quantity_total", 0),
            },

            "assets": {
                "main_image": short.get("image_url"),
                "set_images": [],
                "view360": [],
                "video": []
            },

            "metadata": metadata,
            "variants": variants
        }

    def extract_category_tree(self, product):
        tree = []
        category = product.get("category")
        if category:
            parent = category.get("parent")
            if parent:
                tree.append(parent.get("name"))
            tree.append(category.get("name"))
