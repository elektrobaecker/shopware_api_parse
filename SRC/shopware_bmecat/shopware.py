from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any
import uuid

import httpx

from .config import ShopwareConfig

LOGGER = logging.getLogger(__name__)


class SyncError(RuntimeError):
    def __init__(self, message: str, errors: list[dict] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


@dataclass
class ShopwareResponse:
    status_code: int
    json: dict


class ShopwareClient:
    def __init__(self, config: ShopwareConfig) -> None:
        self.base_url = config.base_url.rstrip("/")
        self.config = config
        self._token: str | None = config.token
        self._client = httpx.Client(base_url=self.base_url, timeout=60.0)
        self._currency_cache: dict[str, str] = {}
        self._tax_cache: dict[float, str] = {}

    def close(self) -> None:
        self._client.close()

    def _get_token(self) -> str:
        if self._token:
            return self._token
        if not self.config.client_id or not self.config.client_secret:
            raise ValueError("Shopware auth requires token or client credentials.")
        response = self._client.post(
            "/api/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            },
        )
        response.raise_for_status()
        data = response.json()
        self._token = data["access_token"]
        return self._token

    def _request(self, method: str, path: str, **kwargs: Any) -> ShopwareResponse:
        token = self._get_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        response = self._client.request(method, path, headers=headers, **kwargs)
        json_data = response.json() if response.content else {}
        return ShopwareResponse(status_code=response.status_code, json=json_data)

    def sync_upsert(self, entity: str, payload: list[dict]) -> ShopwareResponse:
        if not payload:
            return ShopwareResponse(status_code=200, json={})
        key = f"write-{entity}-{uuid.uuid4().hex[:8]}"
        response = self._request(
            "POST",
            "/api/_action/sync",
            json={key: {"entity": entity, "action": "upsert", "payload": payload}},
        )
        if response.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"Sync request failed with status {response.status_code}",
                request=None,
                response=httpx.Response(response.status_code),
            )
        if response.json.get("errors"):
            raise SyncError("Sync returned errors", response.json.get("errors"))
        return response

    def get_currency_id(self, iso_code: str) -> str:
        if iso_code in self._currency_cache:
            return self._currency_cache[iso_code]
        response = self._request(
            "POST",
            "/api/search/currency",
            json={
                "filter": [{"type": "equals", "field": "isoCode", "value": iso_code}],
                "limit": 1,
            },
        )
        data = response.json.get("data", [])
        if not data:
            raise ValueError(f"Currency {iso_code} not found.")
        currency_id = data[0]["id"]
        self._currency_cache[iso_code] = currency_id
        return currency_id

    def load_taxes(self) -> dict[float, str]:
        if self._tax_cache:
            return self._tax_cache
        response = self._request("POST", "/api/search/tax", json={"limit": 500})
        data = response.json.get("data", [])
        for item in data:
            rate = float(item["taxRate"])
            self._tax_cache[rate] = item["id"]
        return self._tax_cache

    def get_tax_id(self, tax_rate: float) -> str:
        taxes = self.load_taxes()
        if tax_rate not in taxes:
            raise ValueError(f"Tax rate {tax_rate} not found in Shopware.")
        return taxes[tax_rate]

    def get_manufacturer_id(self, name: str) -> str | None:
        response = self._request(
            "POST",
            "/api/search/product-manufacturer",
            json={
                "filter": [{"type": "equals", "field": "name", "value": name}],
                "limit": 1,
            },
        )
        data = response.json.get("data", [])
        if not data:
            return None
        return data[0]["id"]
