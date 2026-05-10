"""Small helpers for the VietnamWorks crawler."""

from __future__ import annotations
import sys
from typing import Iterable


def silence_asyncio_windows_proactor_error() -> None:
    """
    Workaround for "ValueError: I/O operation on closed pipe" 
    in Python's asyncio on Windows when closing subprocesses like nodriver.
    """
    if sys.platform != "win32":
        return
        
    try:
        from asyncio.proactor_events import _ProactorBasePipeTransport  # type: ignore
        from functools import wraps
        
        def silence_del(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                try:
                    return func(self, *args, **kwargs)
                except (RuntimeError, ValueError):
                    pass
            return wrapper
            
        _ProactorBasePipeTransport.__del__ = silence_del(_ProactorBasePipeTransport.__del__)
    except Exception:
        pass


def encode_keyword(keyword: str) -> str:
    """Turn a free-text keyword into the URL slug VietnamWorks expects."""
    return keyword.replace(" ", "-")


def join_clean(parts: Iterable[str]) -> str | None:
    """Join element text fragments, trim whitespace, return None if empty."""
    cleaned = " ".join(p.strip() for p in parts if p and p.strip())
    return cleaned or None
