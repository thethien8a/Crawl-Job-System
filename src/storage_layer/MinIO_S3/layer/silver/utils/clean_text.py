from __future__ import annotations

import polars as pl

# '-' is deliberately excluded — needed inside compound terms like
# "end-to-end", "real-time", "13th-month", "work-from-home".
_HTML_TAG_RE = r"<[^>]+>"
_HTML_ENTITY_RE = r"&[a-z]+;|&#\d+;"
_BULLET_RE = r"[•·▪►●◦‣▶★\*]"


def clean_text(col: str, extra_noise_patterns: list[str] | None = None) -> pl.Expr:
    """Vectorised: strip HTML, optional site-noise, bullets, collapse whitespace."""
    expr = pl.col(col).cast(pl.String)
    expr = expr.str.replace_all(_HTML_TAG_RE, " ")
    expr = expr.str.replace_all(_HTML_ENTITY_RE, " ")
    for pattern in extra_noise_patterns or []:
        expr = expr.str.replace_all(pattern, " ")
    expr = expr.str.replace_all(_BULLET_RE, " ")
    expr = expr.str.replace_all(r"\s+", " ")
    return expr.str.strip_chars()
