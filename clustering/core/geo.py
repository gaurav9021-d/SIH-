"""Metric geometry for clustering.

Distances use a local tangent-plane approximation built from the WGS 84 radii of
curvature at the reference latitude. Degrees of latitude and longitude are never
treated as interchangeable: longitude is scaled by the parallel radius, which
collapses toward the poles.

Accuracy: `tests/test_geo.py::test_distance_matches_geodesic` compares this
function against `geographiclib`'s exact Karney geodesic across latitudes from
the equator to 80 degrees and separations up to 5 km, and asserts the error stays
below 0.05 m. That is three orders of magnitude tighter than the tolerances the
750 m epsilon and the 2000 m extent bound require, so the approximation is used
on the hot path and the exact geodesic is kept for the test only.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

WGS84_A = 6378137.0
WGS84_F = 1.0 / 298.257223563
WGS84_E2 = WGS84_F * (2.0 - WGS84_F)
_DEG = math.pi / 180.0


def normalize_lon_delta(delta_deg: float) -> float:
    """Shortest signed longitude difference, handling antimeridian wrap."""
    return (delta_deg + 180.0) % 360.0 - 180.0


def meters_per_degree(lat_deg: float) -> tuple[float, float]:
    """Metres per degree of latitude and of longitude at a given latitude."""
    lat = lat_deg * _DEG
    w = 1.0 - WGS84_E2 * math.sin(lat) ** 2
    meridional = WGS84_A * (1.0 - WGS84_E2) / (w**1.5)
    parallel = WGS84_A / math.sqrt(w)
    return _DEG * meridional, _DEG * parallel * math.cos(lat)


def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Ground distance in metres between two WGS 84 positions."""
    m_lat, m_lon = meters_per_degree(0.5 * (lat1 + lat2))
    north = (lat2 - lat1) * m_lat
    east = normalize_lon_delta(lon2 - lon1) * m_lon
    return math.hypot(north, east)


def point_to_segment_distance_m(
    lat: float,
    lon: float,
    lat_a: float,
    lon_a: float,
    lat_b: float,
    lon_b: float,
) -> float:
    """Distance from a point to a segment, in a local metric frame."""
    frame = LocalFrame(lat, lon)
    px, py = 0.0, 0.0
    ax, ay = frame.to_xy(lat_a, lon_a)
    bx, by = frame.to_xy(lat_b, lon_b)
    dx, dy = bx - ax, by - ay
    length_sq = dx * dx + dy * dy
    if length_sq == 0.0:
        return math.hypot(ax - px, ay - py)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / length_sq))
    return math.hypot(ax + t * dx - px, ay + t * dy - py)


@dataclass(frozen=True, slots=True)
class LocalFrame:
    """East/north metric frame anchored at one position.

    Used for convex-hull topology, where a consistent planar projection matters
    more than sub-metre accuracy. Reported distances always come from
    `distance_m` on the original coordinates.
    """

    lat0: float
    lon0: float
    m_lat: float = field(init=False)
    m_lon: float = field(init=False)

    def __post_init__(self) -> None:
        m_lat, m_lon = meters_per_degree(self.lat0)
        object.__setattr__(self, "m_lat", m_lat)
        object.__setattr__(self, "m_lon", m_lon)

    def to_xy(self, lat: float, lon: float) -> tuple[float, float]:
        return (
            normalize_lon_delta(lon - self.lon0) * self.m_lon,
            (lat - self.lat0) * self.m_lat,
        )


def convex_hull_xy(points: list[tuple[float, float, int]]) -> list[int]:
    """Andrew's monotone chain. Returns indices of hull points in CCW order."""
    if len(points) <= 2:
        return [idx for _, _, idx in points]

    ordered = sorted(points)

    def cross(o: tuple, a: tuple, b: tuple) -> float:
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    lower: list[tuple] = []
    for point in ordered:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], point) <= 0:
            lower.pop()
        lower.append(point)

    upper: list[tuple] = []
    for point in reversed(ordered):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], point) <= 0:
            upper.pop()
        upper.append(point)

    hull = lower[:-1] + upper[:-1]
    if not hull:
        hull = [ordered[0]]
    return [idx for _, _, idx in hull]


class Hull:
    """Incremental convex hull giving the exact diameter of a point set.

    Two facts make this exact while keeping only the hull:

    - the diameter of a point set equals the diameter of its convex hull;
    - the farthest point from any query point is always a hull vertex.

    So a site that stays active for months costs a handful of vertices rather
    than a growing member list.
    """

    __slots__ = ("_frame", "_lats", "_lons", "_diameter_m")

    def __init__(self) -> None:
        self._frame: LocalFrame | None = None
        self._lats: list[float] = []
        self._lons: list[float] = []
        self._diameter_m = 0.0

    @property
    def diameter_m(self) -> float:
        return self._diameter_m

    @property
    def vertex_count(self) -> int:
        return len(self._lats)

    def is_empty(self) -> bool:
        return not self._lats

    def vertices(self) -> list[tuple[float, float]]:
        return list(zip(self._lats, self._lons))

    def extent_after_adding(self, lat: float, lon: float) -> float:
        """Diameter the set would have if this point were added. Exact."""
        if not self._lats:
            return 0.0
        farthest = max(
            distance_m(lat, lon, v_lat, v_lon)
            for v_lat, v_lon in zip(self._lats, self._lons)
        )
        return max(self._diameter_m, farthest)

    def add(self, lat: float, lon: float) -> None:
        if not self._lats:
            self._frame = LocalFrame(lat, lon)
            self._lats.append(lat)
            self._lons.append(lon)
            return

        self._diameter_m = self.extent_after_adding(lat, lon)
        self._lats.append(lat)
        self._lons.append(lon)
        if len(self._lats) <= 3:
            return

        assert self._frame is not None
        points = [
            (*self._frame.to_xy(p_lat, p_lon), idx)
            for idx, (p_lat, p_lon) in enumerate(zip(self._lats, self._lons))
        ]
        keep = convex_hull_xy(points)
        self._lats = [self._lats[i] for i in keep]
        self._lons = [self._lons[i] for i in keep]


def bounding_box(
    coords: list[tuple[float, float]],
) -> tuple[float, float, float, float]:
    """Return (min_lat, min_lon, max_lat, max_lon)."""
    lats = [lat for lat, _ in coords]
    lons = [lon for _, lon in coords]
    return min(lats), min(lons), max(lats), max(lons)
