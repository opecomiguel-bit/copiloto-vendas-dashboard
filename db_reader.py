import os
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
from sqlalchemy import create_engine, text

TABLE_MAP = {
    "base_vendas_master.parquet": "dash_base_vendas_render",
    "saida_daily.csv": "dash_saida_daily",
    "saida_monthly.csv": "dash_saida_monthly",
    "saida_por_canal.csv": "dash_saida_por_canal",
    "saida_abc.csv": "dash_saida_abc",
    "alertas.csv": "dash_alertas",
    "full_reposicao_35d.csv": "dash_full_reposicao_35d",
    "full_candidatos_envio_35d.csv": "dash_full_candidatos_35d",
    "full_auditoria_35d.csv": "dash_full_auditoria_35d",
    "reposicao_geral_estoque.csv": "dash_reposicao_geral",
    "reposicao_geral_acelerando.csv": "dash_reposicao_geral_accel",
}


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
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        raise ValueError("DATABASE_URL não configurada")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    db_url = _with_sslmode_require(db_url)
    return create_engine(db_url, pool_pre_ping=True)


def use_database_tables() -> bool:
    flag = str(os.getenv("USE_DB_TABLES", "")).strip().lower()
    if flag in {"1", "true", "yes", "y", "on"}:
        return True
    return bool((os.getenv("DATABASE_URL") or "").strip())


def resolve_table_name(name: str) -> str:
    return TABLE_MAP.get(name, name)


def _safe_table_name(name: str) -> str:
    name = resolve_table_name(str(name).strip())
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not name or any(ch not in allowed for ch in name):
        raise ValueError(f"Nome de tabela inválido: {name}")
    return name


def load_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()
    safe_name = _safe_table_name(table_name)
    query = text(f'SELECT * FROM {safe_name}')
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def load_artifact_or_empty(name: str, fallback_loader=None) -> pd.DataFrame:
    if use_database_tables():
        try:
            table = resolve_table_name(name)
            print(f"[DB_READER] Carregando tabela: {table}")
            df = load_table(table)
            print(f"[DB_READER] OK tabela={table} shape={df.shape}")
            return df
        except Exception as e:
            print(f"[DB_READER] Erro ao carregar do banco ({name}): {e}")

    if fallback_loader is not None:
        try:
            return fallback_loader()
        except Exception as e:
            print(f"[DB_READER] Erro no fallback local ({name}): {e}")

    print(f"[DB_READER] Retornando dataframe vazio para: {name}")
    return pd.DataFrame()
