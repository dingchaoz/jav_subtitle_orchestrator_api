from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping


class Provider(str, Enum):
    JABLE = "jable"
    BESTJAVPORN = "bestjavporn"


@dataclass(frozen=True)
class ResolvedStream:
    provider: Provider
    page_url: str
    manifest_url: str
    headers: Mapping[str, str]
    expected_duration: float
    refreshable: bool = True
    resource_urls: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))
        object.__setattr__(self, "resource_urls", tuple(self.resource_urls))
