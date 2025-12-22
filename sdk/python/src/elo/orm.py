from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, TypeVar

from .session import get_client
from .types import GeoPoint, encode_geohash, geohash_precision_for_km


def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    return str(value)


class NodeMeta:
    node_type: Optional[str] = None
    geo_key: str = "location"
    geo_hash_key: str = "geo_hash"
    schema_fields: Optional[List[str]] = None


TNode = TypeVar("TNode", bound="Node")


class Node:
    Meta: Type[NodeMeta] = NodeMeta

    def __init__(self, id: str, **data: Any) -> None:
        self.id = id
        payload = self._prepare_payload(data)
        self.data: Dict[str, str] = payload

    @classmethod
    def _meta_value(cls, name: str, default: str) -> str:
        value = getattr(cls.Meta, name, None)
        return value if isinstance(value, str) and value else default

    @classmethod
    def _node_type(cls) -> Optional[str]:
        value = getattr(cls.Meta, "node_type", None)
        return value if isinstance(value, str) and value else None

    @classmethod
    def _geo_key(cls) -> str:
        return cls._meta_value("geo_key", "location")

    @classmethod
    def _geo_hash_key(cls) -> str:
        return cls._meta_value("geo_hash_key", "geo_hash")

    @classmethod
    def _ensure_geo_hash(cls, payload: Dict[str, str]) -> None:
        geo_key = cls._geo_key()
        geo_hash_key = cls._geo_hash_key()
        if geo_hash_key in payload:
            return
        value = payload.get(geo_key)
        if value is None:
            return
        try:
            point = GeoPoint.from_string(value)
        except ValueError:
            return
        payload[geo_hash_key] = encode_geohash(point.lat, point.lon, precision=9)

    @classmethod
    def _prepare_payload(cls, data: Dict[str, Any]) -> Dict[str, str]:
        payload: Dict[str, str] = {}
        for key, value in data.items():
            if isinstance(value, GeoPoint):
                payload[key] = str(value)
            else:
                payload[key] = _stringify(value)
        cls._ensure_geo_hash(payload)
        return payload

    def save(self) -> "Node":
        payload = dict(self.data)
        node_type = self._node_type()
        if node_type:
            payload.setdefault("type", _stringify(node_type))
        self._ensure_geo_hash(payload)
        client = get_client()
        client.create_node(self.id, data=payload or None)
        self.data = payload
        return self

    def update(self, **data: Any) -> "Node":
        payload = self._prepare_payload(data)
        if not payload:
            return self
        client = get_client()
        client.update_node(self.id, data=payload)
        self.data.update(payload)
        return self

    @staticmethod
    def _client():
        return get_client()._client

    @classmethod
    def find_near(
        cls: Type[TNode],
        lat: float,
        lon: float,
        radius_km: float = 10.0,
        limit: Optional[int] = None,
    ) -> List[TNode]:
        node_type = cls._node_type()
        if not node_type:
            raise ValueError("node_type is required for find_near")
        precision = geohash_precision_for_km(radius_km)
        prefix = encode_geohash(lat, lon, precision=precision)
        client = get_client()
        results = client.nearby(
            node_type=node_type,
            geo_hash_prefix=prefix,
            geo_hash_key=cls._geo_hash_key(),
            limit=limit,
        )
        return [cls._from_view(item) for item in results]

    @classmethod
    def _from_view(cls: Type[TNode], view) -> TNode:
        data = dict(view.data)
        geo_key = cls._geo_key()
        value = data.get(geo_key)
        if value:
            try:
                data[geo_key] = GeoPoint.from_string(value)
            except ValueError:
                pass
        return cls(id=view.id, **data)

    @classmethod
    def register_schema(cls) -> None:
        fields = getattr(cls.Meta, "schema_fields", None)
        if not fields:
            return
        client = get_client()
        client.upsert_schema("node", fields)
