import os
import pandas as pd
import streamlit as st
from shared_db_render_ready import get_engine

def use_database_tables() -> bool:
    flag = str(os.getenv("USE_DB_TABLES", "")).strip().lower()
    if flag in {"1", "true", "yes", "y", "on"}:
        return True
    return bool(os.getenv("DATABASE_URL"))

@st.cache_data(ttl=300)
def load_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)

def load_artifact_or_empty(table_name: str, fallback_loader=None) -> pd.DataFrame:
    if use_database_tables():
        try:
            return load_table(table_name)
        except Exception as e:
            print(f"[WARN] Falha ao ler tabela {table_name} do banco: {e}")

    if fallback_loader is not None:
        try:
            return fallback_loader()
        except Exception as e:
            print(f"[WARN] Falha no fallback local de {table_name}: {e}")

    return pd.DataFrame()