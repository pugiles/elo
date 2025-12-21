from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_GEOHASH_CELL_KM: List[Tuple[int, float]] = [
    (1, 5000.0),
    (2, 1250.0),
    (3, 156.0),
    (4, 39.1),
    (5, 4.89),
    (6, 1.22),
    (7, 0.153),
    (8, 0.0382),
    (9, 0.00477),
]


@dataclass(frozen=True)
class GeoPoint:
    lat: float
    lon: float

    def __str__(self) -> str:
        return f"{self.lat},{self.lon}"

    @property
    def geohash(self) -> str:
        return encode_geohash(self.lat, self.lon, precision=9)

    @classmethod
    def from_string(cls, value: str) -> "GeoPoint":
        parts = value.split(",", 1)
        if len(parts) != 2:
            raise ValueError("GeoPoint string must be 'lat,lon'")
        lat = float(parts[0].strip())
        lon = float(parts[1].strip())
        return cls(lat=lat, lon=lon)


def geohash_precision_for_km(radius_km: float) -> int:
    if radius_km <= 0:
        return 9
    for precision, size_km in _GEOHASH_CELL_KM:
        if size_km >= radius_km:
            return precision
    return 1


def encode_geohash(lat: float, lon: float, precision: int = 9) -> str:
    precision = max(1, precision)
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    even = True
    geohash = []

    while len(geohash) < precision:
        if even:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                ch |= bits[bit]
                lon_range[0] = mid
            else:
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                ch |= bits[bit]
                lat_range[0] = mid
            else:
                lat_range[1] = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(_BASE32[ch])
            bit = 0
            ch = 0

    return "".join(geohash)
