"""Zaptec utilities."""

from __future__ import annotations

import re

# Precompile the patterns for performance
RE_TO_UNDER1 = re.compile(r"([A-Z]+)([A-Z][a-z])")
RE_TO_UNDER2 = re.compile(r"([a-z\d])([A-Z])")


def to_under(word: str) -> str:
    """Convert TurnOnThisButton to turn_on_this_button."""
    # Ripped from inflection
    word = RE_TO_UNDER1.sub(r"\1_\2", word)
    word = RE_TO_UNDER2.sub(r"\1_\2", word)
    word = word.replace("-", "_")
    return word.lower()


def get_ocmf_max_reader_value(data: dict) -> float:
    """Return the maximum reader value from OCMF data."""

    if not isinstance(data, dict):
        return 0.0
    rds = data.get("RD", [])
    if not rds:
        return 0.0
    return max(float(reading.get("RV", 0.0)) for reading in rds)
