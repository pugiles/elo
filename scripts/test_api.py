#!/usr/bin/env python3
import json
import os
import sys
import urllib.error
import urllib.request

BASE_URL = os.environ.get("ELO_BASE_URL", "http://127.0.0.1:3000")
API_KEY = os.environ.get("ELO_API_KEY", "seu_token")


def request(method, path, payload=None):
    if not API_KEY:
        raise RuntimeError("ELO_API_KEY is required")

    url = f"{BASE_URL}{path}"
    data = None
    headers = {"x-api-key": API_KEY}

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as response:
            body = response.read().decode("utf-8")
            return response.status, body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        return exc.code, body


def report(label, status, body, ok_statuses):
    if status in ok_statuses:
        print(f"ok  {label} -> {status} {body}")
    else:
        print(f"warn {label} -> {status} {body}")


def main():
    nodes = ["Eu", "Rust", "Elo", "Python"]
    for node in nodes:
        status, body = request("POST", "/nodes", {"id": node})
        report(f"POST /nodes {node}", status, body, {200, 201})

    edges = [("Eu", "Rust"), ("Eu", "Elo"), ("Elo", "Python")]
    for from_id, to_id in edges:
        status, body = request("POST", "/edges", {"from": from_id, "to": to_id})
        report(f"POST /edges {from_id}->{to_id}", status, body, {200, 201})

    status, body = request("PUT", "/nodes/Eu/data", {"key": "name", "value": "Bruno"})
    report("PUT /nodes/Eu/data", status, body, {200, 204})

    status, body = request(
        "PUT",
        "/edges",
        {"from": "Eu", "to": "Rust", "key": "weight", "value": "1.0"},
    )
    report("PUT /edges Eu->Rust weight", status, body, {200, 204})

    status, body = request("GET", "/nodes/Eu")
    report("GET /nodes/Eu", status, body, {200})

    status, body = request("GET", "/path?from=Eu&to=Python")
    report("GET /path?from=Eu&to=Python", status, body, {200})

    status, body = request("GET", "/path?from=Rust&to=Python")
    report("GET /path?from=Rust&to=Python", status, body, {200})

    status, body = request("POST", "/nodes", {"id": "user:123"})
    report("POST /nodes user:123", status, body, {200, 201})
    status, body = request(
        "PUT",
        "/nodes/user:123/data",
        {"key": "type", "value": "user"},
    )
    report("PUT /nodes/user:123/data type", status, body, {200, 204})
    status, body = request(
        "PATCH",
        "/nodes/user:123",
        {"data": {"rating": "850", "status": "active"}},
    )
    report("PATCH /nodes/user:123 bulk", status, body, {200, 204})

    status, body = request("POST", "/nodes", {"id": "team:42"})
    report("POST /nodes team:42", status, body, {200, 201})
    status, body = request(
        "PUT",
        "/nodes/team:42/data",
        {"key": "type", "value": "team"},
    )
    report("PUT /nodes/team:42/data type", status, body, {200, 204})
    status, body = request(
        "PUT",
        "/nodes/team:42/data",
        {"key": "rating", "value": "400"},
    )
    report("PUT /nodes/team:42/data rating", status, body, {200, 204})

    status, body = request("POST", "/edges", {"from": "user:123", "to": "team:42"})
    report("POST /edges user:123->team:42", status, body, {200, 201})
    status, body = request(
        "PUT",
        "/edges",
        {"from": "user:123", "to": "team:42", "key": "type", "value": "owner"},
    )
    report("PUT /edges user:123->team:42 type", status, body, {200, 204})
    status, body = request(
        "PATCH",
        "/edges",
        {
            "from": "user:123",
            "to": "team:42",
            "data": {"role": "owner", "since": "2025"},
        },
    )
    report("PATCH /edges user:123->team:42 bulk", status, body, {200, 204})

    status, body = request("POST", "/nodes", {"id": "team:99"})
    report("POST /nodes team:99", status, body, {200, 201})
    status, body = request(
        "PUT",
        "/nodes/team:99/data",
        {"key": "type", "value": "team"},
    )
    report("PUT /nodes/team:99/data type", status, body, {200, 204})
    status, body = request(
        "PUT",
        "/nodes/team:99/data",
        {"key": "rating", "value": "520"},
    )
    report("PUT /nodes/team:99/data rating", status, body, {200, 204})

    status, body = request("POST", "/edges", {"from": "team:42", "to": "team:99"})
    report("POST /edges team:42->team:99", status, body, {200, 201})
    status, body = request(
        "PUT",
        "/edges",
        {"from": "team:42", "to": "team:99", "key": "weight", "value": "1.2"},
    )
    report("PUT /edges team:42->team:99 weight", status, body, {200, 204})

    status, body = request("GET", "/nodes?type=user")
    report("GET /nodes?type=user", status, body, {200})

    status, body = request("GET", "/edges?type=owner")
    report("GET /edges?type=owner", status, body, {200})

    status, body = request(
        "GET",
        "/recommendations?start=user:123&type=team&min=300&limit=5",
    )
    report("GET /recommendations start=user:123 type=team", status, body, {200})


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        print(f"error: {exc}")
        sys.exit(1)
