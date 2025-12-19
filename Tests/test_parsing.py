from pathlib import Path

from shopware_bmecat.bmecat import iter_bmecat_products
from shopware_bmecat.config import MappingConfig, PriceSelector


def test_iter_bmecat_products(tmp_path: Path) -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<BMECAT>
  <T_NEW_CATALOG>
    <ARTICLE>
      <SUPPLIER_PID>ABC123</SUPPLIER_PID>
      <ARTICLE_DETAILS>
        <DESCRIPTION_SHORT>Test Product</DESCRIPTION_SHORT>
        <DESCRIPTION_LONG>Long Description</DESCRIPTION_LONG>
        <INTERNATIONAL_PID>4012345678901</INTERNATIONAL_PID>
        <MANUFACTURER_NAME>Acme</MANUFACTURER_NAME>
      </ARTICLE_DETAILS>
      <ARTICLE_ORDER_DETAILS>
        <ARTICLE_PRICE_DETAILS>
          <ARTICLE_PRICE price_type="net_list">
            <PRICE_AMOUNT>10.00</PRICE_AMOUNT>
            <PRICE_CURRENCY>EUR</PRICE_CURRENCY>
          </ARTICLE_PRICE>
        </ARTICLE_PRICE_DETAILS>
      </ARTICLE_ORDER_DETAILS>
      <TAX>0.19</TAX>
      <PRODUCT_FEATURES>
        <FEATURE>
          <FNAME>Color</FNAME>
          <FVALUE>Red</FVALUE>
        </FEATURE>
      </PRODUCT_FEATURES>
    </ARTICLE>
  </T_NEW_CATALOG>
</BMECAT>
"""
    xml_path = tmp_path / "sample.xml"
    xml_path.write_text(xml, encoding="utf-8")

    mapping = MappingConfig(
        product_number="SUPPLIER_PID",
        name="DESCRIPTION_SHORT",
        description="DESCRIPTION_LONG",
        ean="INTERNATIONAL_PID",
        manufacturer="MANUFACTURER_NAME",
        price_selector=PriceSelector(price_type="net_list", currency="EUR"),
    )

    products = list(iter_bmecat_products(xml_path, mapping))
    assert len(products) == 1

    product = products[0]
    assert product["productNumber"] == "ABC123"
    assert product["name"] == "Test Product"
    assert product["description"] == "Long Description"
    assert product["ean"] == "4012345678901"
    assert product["manufacturer"] == "Acme"
    assert product["price"]["net"] == 10.0
    assert product["tax_rate"] == 0.19
    assert product["customFields"]["etim"][0]["name"] == "Color"
