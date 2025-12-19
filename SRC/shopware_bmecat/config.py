from __future__ import annotations

from dataclasses import dataclass
import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def repl(match: re.Match[str]) -> str:
            return os.getenv(match.group(1), "")

        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {key: _expand_env(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_expand_env(val) for val in value]
    return value


@dataclass
class PriceSelector:
    price_type: str = "net_list"
    currency: str = "EUR"


@dataclass
class MappingConfig:
    product_number: str
    name: str
    description: str
    ean: str
    manufacturer: str
    price_selector: PriceSelector


@dataclass
class ShopwareConfig:
    base_url: str
    client_id: str | None
    client_secret: str | None
    token: str | None
    sales_channel_id: str | None
    default_visibility: int | None


@dataclass
class TaxConfig:
    default_rate: float
    mapping: dict[str, float]


@dataclass
class Settings:
    source_name: str
    mapping: MappingConfig
    shopware: ShopwareConfig
    tax: TaxConfig

    @classmethod
    def load(cls, path: Path) -> "Settings":
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        data = _expand_env(data)

        mapping_raw = data.get("mapping", {})
        price_selector_raw = mapping_raw.get("price_selector", {})
        mapping = MappingConfig(
            product_number=mapping_raw["product_number"],
            name=mapping_raw["name"],
            description=mapping_raw["description"],
            ean=mapping_raw["ean"],
            manufacturer=mapping_raw["manufacturer"],
            price_selector=PriceSelector(
                price_type=price_selector_raw.get("price_type", "net_list"),
                currency=price_selector_raw.get("currency", "EUR"),
            ),
        )

        shopware_raw = data.get("shopware", {})
        shopware = ShopwareConfig(
            base_url=shopware_raw["base_url"],
            client_id=shopware_raw.get("client_id") or None,
            client_secret=shopware_raw.get("client_secret") or None,
            token=shopware_raw.get("token") or None,
            sales_channel_id=shopware_raw.get("sales_channel_id"),
            default_visibility=shopware_raw.get("default_visibility"),
        )

        tax_raw = data.get("tax", {})
        mapping_raw = tax_raw.get("mapping", {})
        tax_mapping = {str(key): float(val) for key, val in mapping_raw.items()}
        tax = TaxConfig(
            default_rate=float(tax_raw.get("default_rate", 19)),
            mapping=tax_mapping,
        )

        return cls(
            source_name=data.get("source_name", "bmecat"),
            mapping=mapping,
            shopware=shopware,
            tax=tax,
        )
