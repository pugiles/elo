from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import httpx

from .exceptions import (
    AuthenticationError,
    ConnectionError,
    EloError,
    NotFoundError,
    ServerError,
    ValidationError,
)
from .models import CreateEdge, CreateNode, EdgeListResult, NodeView, Recommendation


class EloClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:3000",
        api_key: Optional[str] = None,
        timeout: float = 10.0,
        client: Optional[httpx.Client] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._timeout = timeout
        self._client = client or httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "EloClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def create_node(
        self, node: CreateNode | str, data: Optional[Dict[str, str]] = None
    ) -> None:
        if isinstance(node, CreateNode):
            payload = node.model_dump()
        else:
            payload = {"id": node}
            if data:
                payload["data"] = data
        self._request("POST", "/nodes", json=payload)

    def create_edge(
        self,
        from_id: str | CreateEdge,
        to_id: Optional[str] = None,
        data: Optional[Dict[str, str]] = None,
    ) -> None:
        if isinstance(from_id, CreateEdge):
            payload = from_id.model_dump(by_alias=True)
        else:
            if to_id is None:
                raise ValueError("to_id is required when from_id is a string")
            payload = {"from": from_id, "to": to_id}
            if data:
                payload["data"] = data
        self._request("POST", "/edges", json=payload)

    def set_node_data(self, node_id: str, key: str, value: str) -> None:
        self._request(
            "PUT",
            f"/nodes/{node_id}/data",
            json={"key": key, "value": value},
        )

    def set_edge_data(self, from_id: str, to_id: str, key: str, value: str) -> None:
        self._request(
            "PUT",
            "/edges",
            json={"from": from_id, "to": to_id, "key": key, "value": value},
        )

    def update_node(
        self,
        node_id: str,
        data: Optional[Dict[str, str]] = None,
        **kwargs: str,
    ) -> None:
        payload = dict(data or {})
        payload.update(kwargs)
        if not payload:
            raise ValueError("update_node requires at least one field")
        self._request("PATCH", f"/nodes/{node_id}", json={"data": payload})

    def update_edge(
        self,
        from_id: str,
        to_id: str,
        data: Optional[Dict[str, str]] = None,
        **kwargs: str,
    ) -> None:
        payload = dict(data or {})
        payload.update(kwargs)
        if not payload:
            raise ValueError("update_edge requires at least one field")
        self._request(
            "PATCH",
            "/edges",
            json={"from": from_id, "to": to_id, "data": payload},
        )

    def upsert_schema(self, entity: str, fields: Iterable[str]) -> None:
        payload = {"entity": entity, "fields": list(fields)}
        self._request("POST", "/schema", json=payload)

    def get_schema(self, entity: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"entity": entity} if entity else None
        response = self._request("GET", "/schema", params=params)
        return response.json()

    def delete_node(self, node_id: str) -> None:
        self._request("DELETE", f"/nodes/{node_id}")

    def delete_edge(self, from_id: str, to_id: str) -> None:
        self._request("DELETE", "/edges", params={"from": from_id, "to": to_id})

    def get_node(self, node_id: str, hydrate: Optional[bool] = None) -> NodeView:
        params = {"hydrate": self._bool_param(hydrate)} if hydrate is not None else None
        response = self._request("GET", f"/nodes/{node_id}", params=params)
        return NodeView.model_validate(response.json())

    def list_nodes(
        self, node_type: Optional[str] = None, hydrate: Optional[bool] = None
    ) -> List[NodeView]:
        params: Dict[str, str] = {}
        if node_type:
            params["type"] = node_type
        if hydrate is not None:
            params["hydrate"] = self._bool_param(hydrate)
        response = self._request("GET", "/nodes", params=params or None)
        data = response.json()
        return [NodeView.model_validate(item) for item in data]

    def list_edges(
        self,
        edge_type: Optional[str] = None,
        from_id: Optional[str] = None,
        to_id: Optional[str] = None,
        hydrate: Optional[bool] = None,
    ) -> List[EdgeListResult]:
        params: Dict[str, str] = {}
        if edge_type:
            params["type"] = edge_type
        if from_id:
            params["from"] = from_id
        if to_id:
            params["to"] = to_id
        if hydrate is not None:
            params["hydrate"] = self._bool_param(hydrate)
        response = self._request("GET", "/edges", params=params or None)
        data = response.json()
        return [EdgeListResult.model_validate(item) for item in data]

    def nearby(
        self,
        node_type: str,
        geo_hash_prefix: str,
        geo_hash_key: Optional[str] = None,
        limit: Optional[int] = None,
        hydrate: Optional[bool] = None,
    ) -> List[NodeView]:
        params: Dict[str, str] = {"type": node_type, "geo_hash_prefix": geo_hash_prefix}
        if geo_hash_key:
            params["geo_hash_key"] = geo_hash_key
        if limit is not None:
            params["limit"] = str(limit)
        if hydrate is not None:
            params["hydrate"] = self._bool_param(hydrate)
        response = self._request("GET", "/nearby", params=params)
        data = response.json()
        return [NodeView.model_validate(item) for item in data]

    def path_exists(self, from_id: str, to_id: str) -> bool:
        response = self._request("GET", "/path", params={"from": from_id, "to": to_id})
        return bool(response.json().get("exists"))

    def recommendations(
        self,
        start: str,
        node_type: str,
        num_key: Optional[str] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        limit: Optional[int] = None,
        geo_key: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        radius_km: Optional[float] = None,
        hydrate: Optional[bool] = None,
    ) -> List[Recommendation]:
        params: Dict[str, str] = {"start": start, "type": node_type}
        if num_key:
            params["num_key"] = num_key
        if min_value is not None:
            params["min"] = str(min_value)
        if max_value is not None:
            params["max"] = str(max_value)
        if limit is not None:
            params["limit"] = str(limit)
        if geo_key:
            params["geo_key"] = geo_key
        if lat is not None:
            params["lat"] = str(lat)
        if lon is not None:
            params["lon"] = str(lon)
        if radius_km is not None:
            params["radius_km"] = str(radius_km)
        if hydrate is not None:
            params["hydrate"] = self._bool_param(hydrate)
        response = self._request("GET", "/recommendations", params=params)
        data = response.json()
        return [Recommendation.model_validate(item) for item in data]

    def _headers(self) -> Dict[str, str]:
        headers = {}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        return headers

    def _request(
        self,
        method: str,
        path: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        url = f"{self.base_url}{path}"
        try:
            response = self._client.request(
                method,
                url,
                headers=self._headers(),
                json=json,
                params=params,
            )
        except httpx.RequestError as exc:
            raise ConnectionError(str(exc)) from exc

        if 200 <= response.status_code < 300:
            return response

        message = response.text or response.reason_phrase
        self._raise_for_status(response.status_code, message)
        return response

    @staticmethod
    def _bool_param(value: bool) -> str:
        return "true" if value else "false"

    @staticmethod
    def _raise_for_status(status_code: int, message: str) -> None:
        if status_code == 400:
            raise ValidationError(message)
        if status_code == 401:
            raise AuthenticationError(message)
        if status_code == 404:
            raise NotFoundError(message)
        if status_code >= 500:
            raise ServerError(message)
        raise EloError(message)
