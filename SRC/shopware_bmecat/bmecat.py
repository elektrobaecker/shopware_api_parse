from __future__ import annotations

import logging
import shutil
import zipfile
from pathlib import Path
from typing import Iterable, Iterator

from lxml import etree

from .config import MappingConfig
from .ndjson import write_ndjson

LOGGER = logging.getLogger(__name__)


def extract_input(input_path: Path, workdir: Path) -> Path:
    workdir.mkdir(parents=True, exist_ok=True)
    target = workdir / "input.xml"
    if input_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(input_path, "r") as archive:
            xml_members = [name for name in archive.namelist() if name.lower().endswith(".xml")]
            if not xml_members:
                raise ValueError("No XML file found inside ZIP archive.")
            with archive.open(xml_members[0]) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
    else:
        shutil.copy2(input_path, target)
    return target


def iter_bmecat_products(xml_path: Path, mapping: MappingConfig) -> Iterator[dict]:
    context = etree.iterparse(str(xml_path), events=("end",), recover=True, huge_tree=True)
    for _event, elem in context:
        if _strip_ns(elem.tag) == "ARTICLE":
            product = _parse_article(elem, mapping)
            if product:
                yield product
            _cleanup_element(elem)


def write_products_ndjson(products: Iterable[dict], output_path: Path) -> None:
    write_ndjson(products, output_path)


def _parse_article(elem: etree._Element, mapping: MappingConfig) -> dict | None:
    product_number = _find_text(elem, mapping.product_number)
    name = _find_text(elem, mapping.name)
    if not product_number or not name:
        LOGGER.warning("Skipping article without product_number or name.")
        return None

    description = _find_text(elem, mapping.description)
    ean = _find_text(elem, mapping.ean)
    manufacturer = _find_text(elem, mapping.manufacturer)

    price_info = _find_price(elem, mapping.price_selector.price_type)
    price_net = price_info.get("net")
    currency = price_info.get("currency", mapping.price_selector.currency)

    tax_rate = _find_tax(elem)
    features = _find_features(elem)
    media = _find_media(elem)

    product: dict = {
        "productNumber": product_number,
        "name": name,
        "description": description,
        "ean": ean,
        "manufacturer": manufacturer,
        "price": {"net": price_net, "currency": currency},
        "tax_rate": tax_rate,
        "customFields": {"etim": features},
    }
    if media:
        product["media"] = media
    return product


def _find_text(elem: etree._Element, tag: str) -> str | None:
    found = elem.find(f".//{{*}}{tag}")
    if found is None or found.text is None:
        return None
    text = found.text.strip()
    return text or None


def _find_price(elem: etree._Element, price_type: str) -> dict:
    for price in elem.findall(".//{*}ARTICLE_PRICE"):
        if price.get("price_type") != price_type:
            continue
        amount = _find_text(price, "PRICE_AMOUNT")
        currency = _find_text(price, "PRICE_CURRENCY")
        try:
            net = float(amount) if amount is not None else None
        except ValueError:
            net = None
        return {"net": net, "currency": currency}
    return {"net": None, "currency": None}


def _find_tax(elem: etree._Element) -> float | None:
    for tag in ("TAX", "TAX_RATE", "TAX_RATE_PERCENT"):
        value = _find_text(elem, tag)
        if value is None:
            continue
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _find_features(elem: etree._Element) -> list[dict]:
    features: list[dict] = []
    for parent_tag in ("ARTICLE_FEATURES", "PRODUCT_FEATURES"):
        for feature in elem.findall(f".//{{*}}{parent_tag}/{{*}}FEATURE"):
            name = _find_text(feature, "FNAME") or _find_text(feature, "NAME")
            value = _find_text(feature, "FVALUE") or _find_text(feature, "VALUE")
            unit = _find_text(feature, "FUNIT") or _find_text(feature, "UNIT")
            if not name and not value:
                continue
            entry = {"name": name, "value": value}
            if unit:
                entry["unit"] = unit
            features.append(entry)
    return features


def _find_media(elem: etree._Element) -> list[str]:
    media: list[str] = []
    for mime in elem.findall(".//{*}MIME_INFO/{*}MIME"):
        source = _find_text(mime, "MIME_SOURCE")
        if source:
            media.append(source)
    return media


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _cleanup_element(elem: etree._Element) -> None:
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]
