from __future__ import annotations

import os
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
from sqlalchemy import create_engine, text


def _with_sslmode_require(db_url: str) -> str:
    try:
        parsed = urlparse(db_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if "render.com" in (parsed.hostname or "") and "sslmode" not in query:
            query["sslmode"] = "require"
            parsed = parsed._replace(query=urlencode(query))
            return urlunparse(parsed)
        return db_url
    except Exception:
        return db_url


def get_engine():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
        elif db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        db_url = _with_sslmode_require(db_url)
        return create_engine(db_url, pool_pre_ping=True)

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([host, db, user, password]):
        raise ValueError("Configuração do banco incompleta")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    url = _with_sslmode_require(url)
    return create_engine(url, pool_pre_ping=True)


def run_query_df(query: str, params=None) -> tuple[pd.DataFrame, str | None]:
    dbname = os.getenv("DB_NAME")
    db_url = os.getenv("DATABASE_URL", "")
    if db_url and not dbname:
        try:
            dbname = urlparse(db_url).path.lstrip("/") or None
        except Exception:
            dbname = None
    try:
        engine = get_engine()
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn, params=params or {}), dbname
    except Exception:
        return pd.DataFrame(), dbname


def run_query(query: str, params=None, fetch: str = "all") -> dict:
    dbname = os.getenv("DB_NAME")
    db_url = os.getenv("DATABASE_URL", "")
    if db_url and not dbname:
        try:
            dbname = urlparse(db_url).path.lstrip("/") or None
        except Exception:
            dbname = None
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            if fetch == "one":
                return {"ok": True, "data": (result.fetchone(), list(result.keys())), "dbname": dbname}
            if fetch == "all":
                return {"ok": True, "data": (result.fetchall(), list(result.keys())), "dbname": dbname}
            return {"ok": True, "data": None, "dbname": dbname}
    except Exception as e:
        return {"ok": False, "error": str(e), "data": None, "dbname": dbname}


def write_query(query: str, params=None) -> tuple[bool, str | None]:
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(query), params or {})
        return True, None
    except Exception as e:
        return False, str(e)
