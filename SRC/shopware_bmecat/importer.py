from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import uuid

import httpx

from .config import Settings
from .ndjson import append_ndjson, iter_ndjson
from .shopware import ShopwareClient, SyncError

LOGGER = logging.getLogger(__name__)


@dataclass
class ImportStats:
    products_processed: int = 0
    products_uploaded: int = 0
    errors: int = 0


def calculate_gross(net: float, tax_rate: float) -> float:
    rate = tax_rate / 100 if tax_rate > 1 else tax_rate
    return round(net * (1 + rate), 2)


def import_products(ndjson_path: Path, settings: Settings, batch_size: int = 250) -> ImportStats:
    stats = ImportStats()
    error_path = ndjson_path.parent / "errors.ndjson"
    media_queue_path = ndjson_path.parent / "media_queue.ndjson"

    client = ShopwareClient(settings.shopware)
    try:
        currency_id = client.get_currency_id(settings.mapping.price_selector.currency)
        tax_cache = client.load_taxes()

        manufacturer_names = _collect_manufacturer_names(ndjson_path)
        _upsert_manufacturers(client, manufacturer_names)

        batch: list[dict] = []
        for product in iter_ndjson(ndjson_path):
            stats.products_processed += 1
            payload = _build_product_payload(
                product, settings, currency_id, tax_cache
            )
            if payload is None:
                stats.errors += 1
                append_ndjson({"item": product, "reason": "missing_required_fields"}, error_path)
                continue
            if product.get("media"):
                append_ndjson(
                    {"productNumber": product.get("productNumber"), "media": product.get("media")},
                    media_queue_path,
                )
            batch.append(payload)
            if len(batch) >= batch_size:
                _send_batch(batch, client, stats, error_path)
                batch = []
            if stats.products_processed % 100 == 0:
                LOGGER.info("Processed %s products", stats.products_processed)
        if batch:
            _send_batch(batch, client, stats, error_path)
    finally:
        client.close()
    return stats


def _collect_manufacturer_names(ndjson_path: Path) -> set[str]:
    names: set[str] = set()
    for product in iter_ndjson(ndjson_path):
        name = product.get("manufacturer")
        if name:
            names.add(name)
    return names


def _upsert_manufacturers(client: ShopwareClient, names: set[str]) -> None:
    payload: list[dict] = []
    for name in sorted(names):
        payload.append({"id": _stable_uuid("manufacturer", name), "name": name})
    _send_sync_batches(client, "product_manufacturer", payload, batch_size=200)


def _build_product_payload(
    product: dict,
    settings: Settings,
    currency_id: str,
    tax_cache: dict[float, str],
) -> dict | None:
    product_number = product.get("productNumber")
    name = product.get("name")
    if not product_number or not name:
        return None

    price_net = _coerce_float(product.get("price", {}).get("net"))
    if price_net is None:
        return None

    tax_rate = _resolve_tax_rate(product.get("tax_rate"), settings)
    tax_id = tax_cache.get(tax_rate)
    if not tax_id:
        raise ValueError(f"Tax rate {tax_rate} not available in Shopware.")

    manufacturer = product.get("manufacturer")
    manufacturer_id = _stable_uuid("manufacturer", manufacturer) if manufacturer else None

    payload: dict = {
        "id": _stable_uuid("product", product_number),
        "productNumber": product_number,
        "name": name,
        "description": product.get("description"),
        "ean": product.get("ean"),
        "manufacturerId": manufacturer_id,
        "taxId": tax_id,
        "price": [
            {
                "currencyId": currency_id,
                "net": price_net,
                "gross": calculate_gross(price_net, tax_rate),
                "linked": True,
            }
        ],
        "stock": 0,
        "active": True,
        "customFields": product.get("customFields") or {},
    }

    if settings.shopware.sales_channel_id:
        payload["visibilities"] = [
            {
                "salesChannelId": settings.shopware.sales_channel_id,
                "visibility": settings.shopware.default_visibility or 30,
            }
        ]
    return payload


def _send_batch(
    batch: list[dict],
    client: ShopwareClient,
    stats: ImportStats,
    error_path: Path,
) -> None:
    try:
        client.sync_upsert("product", batch)
        stats.products_uploaded += len(batch)
    except (SyncError, httpx.HTTPError) as exc:
        if len(batch) == 1:
            stats.errors += 1
            append_ndjson({"item": batch[0], "reason": str(exc)}, error_path)
            return
        mid = len(batch) // 2
        _send_batch(batch[:mid], client, stats, error_path)
        _send_batch(batch[mid:], client, stats, error_path)


def _send_sync_batches(
    client: ShopwareClient,
    entity: str,
    payload: list[dict],
    batch_size: int,
) -> None:
    batch: list[dict] = []
    for item in payload:
        batch.append(item)
        if len(batch) >= batch_size:
            client.sync_upsert(entity, batch)
            batch = []
    if batch:
        client.sync_upsert(entity, batch)


def _resolve_tax_rate(raw_rate: float | str | None, settings: Settings) -> float:
    if raw_rate is None:
        return settings.tax.default_rate
    try:
        rate = float(raw_rate)
    except (TypeError, ValueError):
        return settings.tax.default_rate
    rate_percent = rate * 100 if rate <= 1 else rate
    mapped = (
        settings.tax.mapping.get(str(raw_rate))
        or settings.tax.mapping.get(str(rate))
        or settings.tax.mapping.get(str(rate_percent))
    )
    return float(mapped) if mapped is not None else rate_percent


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stable_uuid(prefix: str, value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{prefix}:{value}"))
