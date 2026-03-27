import os
import pandas as pd
from sqlalchemy import create_engine

TABLE_MAP = {
    "base_vendas_master.parquet": "dash_base_vendas",
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

def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL não configurada")
    return create_engine(db_url)

def load_table(table_name):
    engine = get_engine()
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)

def resolve_table_name(name):
    if name in TABLE_MAP:
        return TABLE_MAP[name]
    return name

def load_artifact_or_empty(name, *args, **kwargs):
    use_db = os.getenv("USE_DB_TABLES", "false").lower() == "true"

    if use_db:
        try:
            table = resolve_table_name(name)
            print(f"[DB_READER] Carregando tabela: {table}")
            df = load_table(table)
            return df
        except Exception as e:
            print(f"[DB_READER] Erro ao carregar do banco: {e}")

    print(f"[DB_READER] Retornando dataframe vazio para: {name}")
    return pd.DataFrame()

def use_database_tables():
    return os.getenv("USE_DB_TABLES", "false").lower() == "true"
