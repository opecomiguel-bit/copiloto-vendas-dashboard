from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from sqlalchemy import text

from shared_db_render_ready import get_engine


def _dbname_from_env() -> str | None:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        try:
            parsed = urlparse(db_url)
            return parsed.path.lstrip("/") or None
        except Exception:
            return None
    return os.getenv("DB_NAME")


def run_query_df(query: str, params: dict | tuple | list | None = None) -> tuple[pd.DataFrame, str | None]:
    engine = get_engine()
    dbname = _dbname_from_env()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params or {})
        return df, dbname
    except Exception:
        return pd.DataFrame(), dbname


def run_query(
    query: str,
    params: dict | tuple | list | None = None,
    fetch: str = "all",
) -> dict[str, Any]:
    engine = get_engine()
    dbname = _dbname_from_env()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            if fetch == "one":
                row = result.fetchone()
                cols = list(result.keys())
                return {"ok": True, "data": (row, cols), "dbname": dbname}
            if fetch == "all":
                rows = result.fetchall()
                cols = list(result.keys())
                return {"ok": True, "data": (rows, cols), "dbname": dbname}
            return {"ok": True, "data": None, "dbname": dbname}
    except Exception as e:
        return {"ok": False, "error": str(e), "data": None, "dbname": dbname}


def write_query(query: str, params: dict | tuple | list | None = None) -> tuple[bool, str | None]:
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text(query), params or {})
        return True, None
    except Exception as e:
        return False, str(e)
