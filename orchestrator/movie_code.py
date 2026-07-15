from __future__ import annotations

import re


MOVIE_CODE_RE = re.compile(r"^([a-zA-Z]+)-?(\d+)$")


def canonical_movie_code(movie_code: str) -> str:
    match = MOVIE_CODE_RE.fullmatch(movie_code.strip())
    if match is None:
        raise ValueError(f"invalid movie code: {movie_code}")
    series, number = match.groups()
    return f"{series.lower()}-{int(number):03d}"
