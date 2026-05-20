"""Hybrid FlashText + regex extractor for taxonomy-driven label extraction.

The seed CSVs under `silver/seeds/` store each row's `keywords` column as
pipe-separated regex alternatives (e.g. `python|\\bpy3?\\b|python3`).

For every alternative we classify it as either:

* a literal that FlashText can match in O(n) per scan, or
* a genuine regex (uses `?`, `*`, `\\d`, `[abc]`, bare `.` wildcard, …)
  that we keep as a compiled `re.Pattern` fallback.

This way we lose zero coverage compared to the original
`pl.col.str.contains` approach while still scanning the bulk of keywords
in linear time via FlashText.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import polars as pl
from flashtext import KeywordProcessor

# Backslash-escaped chars that the taxonomy uses purely to make a literal
# punctuation char regex-safe. After unescaping they go to FlashText as-is.
_LITERAL_ESCAPES = set(".+#-/&")

# Regex meta-characters that, when seen UNESCAPED, prove the alternative
# is not a literal string (so it must go to the regex fallback list).
_REGEX_META_CHARS = set(".^$*+?()[]{}|")


@dataclass
class HybridKeywordExtractor:
    """A FlashText processor paired with a regex fallback list.

    FlashText covers the literal majority; `regex_rules` catches the
    handful of genuinely regex alternatives so we keep parity with the
    pre-FlashText `str.contains` implementation.
    """

    kp: KeywordProcessor
    regex_rules: list[tuple[re.Pattern[str], str]] = field(default_factory=list)

    def extract(self, text: str | None) -> list[str]:
        """Return canonical labels found in `text`, de-duplicated by first occurrence."""
        if not text:
            return []
        seen: dict[str, None] = {}
        for label in self.kp.extract_keywords(text):
            seen.setdefault(label, None)
        for pattern, label in self.regex_rules:
            if pattern.search(text):
                seen.setdefault(label, None)
        return list(seen)


def _classify_alternative(alternative: str) -> tuple[str | None, str | None]:
    """Categorise a single `|`-split keyword alternative.

    Returns one of:
      * `(literal, None)`     — pure literal usable by FlashText
      * `(None, regex_src)`   — needs the regex fallback
      * `(None, None)`        — empty / unusable
    """
    stripped = alternative.strip()
    if not stripped:
        return None, None

    # Always strip `\b` anchors before classifying. FlashText enforces word
    # boundaries on literals; the fallback regex keeps the original anchored
    # form so semantics stay identical.
    inner = stripped
    if inner.startswith(r"\b"):
        inner = inner[2:]
    if inner.endswith(r"\b"):
        inner = inner[:-2]

    literal_buf: list[str] = []
    i = 0
    while i < len(inner):
        ch = inner[i]
        if ch == "\\" and i + 1 < len(inner):
            nxt = inner[i + 1]
            if nxt in _LITERAL_ESCAPES:
                literal_buf.append(nxt)
                i += 2
                continue
            # Any other backslash sequence (\d, \w, \s, \B, \S, \., …) is regex.
            return None, stripped
        if ch in _REGEX_META_CHARS:
            return None, stripped
        literal_buf.append(ch)
        i += 1

    literal = "".join(literal_buf).strip()
    if not literal:
        return None, None
    return literal, None


def build_extractor(
    taxonomy_df: pl.DataFrame,
    label_col: str = "canonical_en",
    keywords_col: str = "keywords",
) -> HybridKeywordExtractor:
    """Compile a hybrid (FlashText + regex) extractor from a taxonomy DataFrame.

    Case-insensitive throughout so taxonomy entries like `\\bIBM\\b` still
    find `ibm` in the normalised text and vice versa.
    """
    kp = KeywordProcessor(case_sensitive=False)
    regex_rules: list[tuple[re.Pattern[str], str]] = []
    for row in taxonomy_df.iter_rows(named=True):
        raw_keywords = row.get(keywords_col) or ""
        label = row.get(label_col)
        if not raw_keywords or not label:
            continue
        for alternative in raw_keywords.split("|"):
            literal, regex_src = _classify_alternative(alternative)
            if literal is not None:
                kp.add_keyword(literal, label)
            elif regex_src is not None:
                regex_rules.append((re.compile(regex_src, re.IGNORECASE), label))
    return HybridKeywordExtractor(kp=kp, regex_rules=regex_rules)


def extract_unique_labels(
    text: str | None, extractor: HybridKeywordExtractor
) -> list[str]:
    """Thin wrapper kept for symmetry with the `apply_*_cleaning` callers."""
    return extractor.extract(text)
