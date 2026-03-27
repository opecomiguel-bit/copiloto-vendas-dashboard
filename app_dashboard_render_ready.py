import json
import os
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
import subprocess
import sys
import urllib.parse
from sqlalchemy import text

from db_reader import load_artifact_or_empty, use_database_tables
from shared_db_render_ready import get_engine as _shared_get_engine


# =========================
# CONFIG
# =========================
ARQ_MASTER = "base_vendas_master.parquet"
ARQ_COBERTURA_JSON = "ml_cobertura.json"

ARQ_DAILY = "saida_daily.csv"
ARQ_CANAL = "saida_por_canal.csv"
ARQ_ABC = "saida_abc.csv"
ARQ_ALERTAS = "alertas.csv"

ARQ_RESUMO_JSON = "resumo.json"
ARQ_RELATORIO_MD = "relatorio.md"
ARQ_RESUMO_IA_MD = "resumo_executivo_ia.md"
ARQ_DECISAO_IA_JSON = "decisao_ia.json"
ARQ_DECISAO_IA_MD = "decisao_ia.md"
ARQ_DECISAO_IA_STATUS = "decisao_ia_status.json"
ARQ_RESUMO_IA_STATUS = "resumo_ia_status.json"
ARQ_FULL_REPOSICAO = "full_reposicao_35d.csv"
ARQ_FULL_CANDIDATOS = "full_candidatos_envio_35d.csv"
ARQ_FULL_AUDITORIA = "full_auditoria_35d.csv"

ARQ_REPOSICAO_GERAL = "reposicao_geral_estoque.csv"
ARQ_REPOSICAO_GERAL_ACCEL = "reposicao_geral_acelerando.csv"
ARQ_REPOSICAO_DECISAO = "reposicao_decisao_sku.csv"
ARQ_ALERTAS_TRACKING = "alertas_tracking.csv"
ARQ_ALERTAS_OPERACIONAIS = "alertas_operacionais.csv"

COL_DATA = "Data"
COL_PEDIDO = "Pedido"
COL_EAN = "EAN"

Z_THRESHOLD_DEFAULT = 2.7


# =========================
# UI
# =========================
st.set_page_config(page_title="Copiloto de Vendas — Dashboard", layout="wide")


# =========================
# THEME / LAYOUT MODERNO
# =========================
st.markdown("""
<style>
    .block-container {
        padding-top: 1.1rem;
        padding-bottom: 2rem;
        max-width: 98rem;
    }

    .main-title-wrap {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        padding: 22px 26px;
        border-radius: 22px;
        margin-bottom: 18px;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.16);
    }

    .main-title {
        color: #ffffff;
        font-size: 38px;
        font-weight: 800;
        line-height: 1.1;
        margin: 0;
        letter-spacing: -0.02em;
    }

    .main-subtitle {
        color: #cbd5e1;
        font-size: 13px;
        margin-top: 8px;
    }

    .section-title {
        font-size: 28px;
        font-weight: 800;
        color: #0f172a;
        margin: 8px 0 4px 0;
        letter-spacing: -0.02em;
    }

    .section-subtitle {
        font-size: 13px;
        color: #64748b;
        margin-bottom: 12px;
    }

    .modern-card {
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e5e7eb;
        border-radius: 22px;
        padding: 18px 18px 16px 18px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
        margin-bottom: 16px;
    }

    .metric-card {
        background: #ffffff;
        border: 1px solid #edf2f7;
        border-left: 7px solid #2563eb;
        border-radius: 20px;
        padding: 18px 18px 14px 18px;
        box-shadow: 0 8px 20px rgba(15, 23, 42, 0.06);
        min-height: 118px;
    }

    .metric-title {
        font-size: 13px;
        color: #64748b;
        margin-bottom: 8px;
    }

    .metric-value {
        font-size: 26px;
        line-height: 1.05;
        font-weight: 800;
        color: #0f172a;
        letter-spacing: -0.02em;
    }

    .metric-sub {
        font-size: 12px;
        color: #94a3b8;
        margin-top: 8px;
    }

    .soft-chip {
        display: inline-block;
        background: #eff6ff;
        color: #1d4ed8;
        font-size: 12px;
        font-weight: 700;
        padding: 6px 10px;
        border-radius: 999px;
        margin-bottom: 8px;
    }

    .chart-card {
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e5e7eb;
        border-radius: 22px;
        padding: 16px 16px 8px 16px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
        margin-bottom: 16px;
    }

    .chart-title {
        font-size: 18px;
        font-weight: 800;
        color: #0f172a;
        margin-bottom: 4px;
    }

    .chart-subtitle {
        font-size: 12px;
        color: #64748b;
        margin-bottom: 10px;
    }

    .divider-modern {
        height: 1px;
        border: none;
        background: linear-gradient(90deg, rgba(148,163,184,0.15) 0%, rgba(148,163,184,0.4) 50%, rgba(148,163,184,0.15) 100%);
        margin: 14px 0 18px 0;
    }

    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e5e7eb;
        border-radius: 18px;
        padding: 14px 16px;
        box-shadow: 0 5px 16px rgba(15, 23, 42, 0.04);
    }

    div[data-testid="stDataFrame"] {
        border-radius: 18px;
        overflow: hidden;
    }

    div[data-baseweb="select"] > div,
    div[data-baseweb="input"] > div,
    .stDateInput > div > div,
    .stNumberInput > div > div {
        border-radius: 14px !important;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background: #f8fafc;
        padding: 6px;
        border-radius: 16px;
        border: 1px solid #e5e7eb;
    }

    .stTabs [data-baseweb="tab"] {
        height: 44px;
        border-radius: 12px;
        padding-left: 16px;
        padding-right: 16px;
        font-weight: 700;
    }

    .stTabs [aria-selected="true"] {
        background: #ffffff !important;
        box-shadow: 0 4px 10px rgba(15, 23, 42, 0.08);
    }
</style>
""", unsafe_allow_html=True)


# =========================
# Helpers visuais
# =========================
CRITICIDADE_CORES = {
    "CRÍTICO": "#ff4d4f",
    "URGENTE": "#fa8c16",
    "ATENÇÃO": "#fadb14",
    "SAUDÁVEL": "#52c41a",
    "ALTO": "#13c2c2",
    "EXCESSO": "#2f54eb",
    "SEM_GIRO": "#8c8c8c",
}


def render_main_header():
    st.markdown(
        """
        <div class="main-title-wrap">
            <div class="main-title">📊 Copiloto de Vendas — Dashboard</div>
            <div class="main-subtitle">
                Monitor executivo de vendas, FULL, reposição e performance operacional
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_section_header(title: str, subtitle: str = ""):
    st.markdown(
        f"""
        <div style="margin-bottom:10px;">
            <div class="section-title">{title}</div>
            <div class="section-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_metric_card(title: str, value: str, subtitle: str = "", color: str = "#2563eb"):
    st.markdown(
        f"""
        <div class="metric-card" style="border-left-color:{color};">
            <div class="metric-title">{title}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def open_modern_card(title: str | None = None, subtitle: str = ""):
    head = ""
    if title:
        head = f"""
            <div class="chart-title">{title}</div>
            <div class="chart-subtitle">{subtitle}</div>
        """
    st.markdown(f'<div class="modern-card">{head}', unsafe_allow_html=True)


def open_chart_card(title: str, subtitle: str = ""):
    st.markdown(
        f"""
        <div class="chart-card">
            <div class="chart-title">{title}</div>
            <div class="chart-subtitle">{subtitle}</div>
        """,
        unsafe_allow_html=True
    )


def close_card():
    st.markdown("</div>", unsafe_allow_html=True)


def color_status_bg(val):
    cor = CRITICIDADE_CORES.get(str(val).upper().strip(), "")
    if not cor:
        return ""
    return f"background-color: {cor}; color: white; font-weight: 700;"


def color_status_text(val):
    cor = CRITICIDADE_CORES.get(str(val).upper().strip(), "")
    if not cor:
        return ""
    return f"color: {cor}; font-weight: 700;"


def highlight_cobertura(val):
    try:
        v = float(val)
    except Exception:
        return ""
    if pd.isna(v):
        return ""
    if v <= 15:
        return "background-color: #ffccc7; color: #a8071a; font-weight: 700;"
    if v <= 30:
        return "background-color: #ffe7ba; color: #ad4e00; font-weight: 700;"
    if v <= 60:
        return "background-color: #fff1b8; color: #874d00;"
    return ""


def highlight_reposicao(val):
    try:
        v = float(val)
    except Exception:
        return ""
    if pd.isna(v):
        return ""
    if v >= 300:
        return "background-color: #ffd6e7; color: #780650; font-weight: 700;"
    if v >= 100:
        return "background-color: #fff0f6; color: #c41d7f; font-weight: 700;"
    if v >= 30:
        return "background-color: #fff7e6; color: #ad6800;"
    return ""


def highlight_growth(val):
    try:
        v = float(val)
    except Exception:
        return ""
    if pd.isna(v):
        return ""
    if v >= 40:
        return "background-color: #d9f7be; color: #135200; font-weight: 700;"
    if v >= 20:
        return "background-color: #f6ffed; color: #237804; font-weight: 700;"
    if v <= -30:
        return "background-color: #fff1f0; color: #a8071a; font-weight: 700;"
    return ""


def highlight_urgency_score(val):
    try:
        v = float(val)
    except Exception:
        return ""
    if pd.isna(v):
        return ""
    if v >= 0.75:
        return "background-color: #ffccc7; color: #a8071a; font-weight: 700;"
    if v >= 0.55:
        return "background-color: #ffe7ba; color: #ad4e00; font-weight: 700;"
    return ""


def highlight_manual_suggestion(val):
    try:
        v = float(val)
    except Exception:
        return ""
    if pd.isna(v):
        return ""
    if v > 0:
        return "background-color: #e6f4ff; color: #0958d9; font-weight: 700;"
    return ""


def render_exec_card(title: str, value: str, subtitle: str = "", color: str = "#1677ff"):
    st.markdown(
        f"""
        <div style="
            border: 1px solid #f0f0f0;
            border-left: 8px solid {color};
            background: linear-gradient(180deg, #ffffff 0%, #fafafa 100%);
            padding: 18px 18px 14px 18px;
            border-radius: 16px;
            box-shadow: 0 3px 14px rgba(0,0,0,0.06);
            min-height: 128px;
        ">
            <div style="font-size: 13px; color: #595959; margin-bottom: 8px; letter-spacing: 0.2px;">{title}</div>
            <div style="font-size: 32px; font-weight: 800; color: #111827; line-height: 1.1;">{value}</div>
            <div style="font-size: 12px; color: #8c8c8c; margin-top: 10px;">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_section_banner(title: str, subtitle: str = ""):
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, #111827 0%, #1f2937 100%);
            color: white;
            padding: 16px 20px;
            border-radius: 16px;
            margin: 6px 0 14px 0;
        ">
            <div style="font-size: 22px; font-weight: 800; margin-bottom: 4px;">{title}</div>
            <div style="font-size: 13px; color: #d1d5db;">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_kpi_strip(items: list[tuple[str, str, str]]):
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        title, value, color = item
        with col:
            st.markdown(
                f"""
                <div style="
                    background: {color};
                    color: white;
                    padding: 12px 14px;
                    border-radius: 14px;
                    text-align: center;
                    font-weight: 700;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.08);
                ">
                    <div style="font-size: 12px; opacity: 0.95;">{title}</div>
                    <div style="font-size: 24px; margin-top: 4px;">{value}</div>
                </div>
                """,
                unsafe_allow_html=True
            )


def render_criticidade_legend():
    chips = []
    for nome, cor in CRITICIDADE_CORES.items():
        text_color = "#111111" if nome == "ATENÇÃO" else "white"
        chips.append(
            f"""
            <span style="
                display:inline-block;
                background:{cor};
                color:{text_color};
                padding:6px 10px;
                border-radius:999px;
                margin-right:8px;
                margin-bottom:6px;
                font-size:12px;
                font-weight:700;
            ">{nome}</span>
            """
        )
    st.markdown("".join(chips), unsafe_allow_html=True)


render_main_header()


# =========================
# Helpers (arquivos)
# =========================
def file_exists(p: str) -> bool:
    return Path(p).exists()


def read_text(p: str) -> str:
    if not file_exists(p):
        return ""
    return Path(p).read_text(encoding="utf-8", errors="ignore")


def try_read_json(p: str):
    if not file_exists(p):
        return None
    try:
        return json.loads(read_text(p))
    except Exception:
        return None


# =========================
# DB Helpers (robusto)
# =========================
def _safe_error_to_str(e: Exception) -> str:
    try:
        if getattr(e, "args", None) and len(e.args) > 0 and isinstance(e.args[0], (bytes, bytearray)):
            b = e.args[0]
            try:
                return b.decode("cp1252", errors="replace")
            except Exception:
                return b.decode("latin-1", errors="replace")
    except Exception:
        pass

    try:
        return str(e)
    except Exception:
        return repr(e)



def _get_db_cfg_candidates():
    """
    Mantido apenas para compatibilidade visual/debug.
    Quando DATABASE_URL estiver definida, a conexão real é feita por SQLAlchemy
    em shared_db_render_ready.py.
    """
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if db_url:
        try:
            parsed = urllib.parse.urlparse(db_url)
            host = parsed.hostname or "(db-url)"
            port = parsed.port or 5432
            user = parsed.username or ""
            dbname = (parsed.path or "/").lstrip("/")
            candidates = [dbname] if dbname else []
            return host, port, user, "***", candidates
        except Exception:
            return "(db-url)", 5432, "", "***", []

    host = (os.getenv("DB_HOST") or "127.0.0.1").strip()
    port = int((os.getenv("DB_PORT") or "5433").strip())
    user = (os.getenv("DB_USER") or "n8n").strip()
    password = (
        os.getenv("DB_POSTGRESDB_PASSWORD")
        or os.getenv("POSTGRES_PASSWORD")
        or os.getenv("DB_PASSWORD")
        or ""
    ).strip()
    env_db = (os.getenv("DB_NAME") or "").strip()
    candidates = [env_db] if env_db else []
    for db in ["postgres", "n8n"]:
        if db not in candidates:
            candidates.append(db)
    return host, port, user, password, candidates


def _run_query_with_fallback(sql: str, params=None, fetch="one"):
    """
    Wrapper único para leitura/escrita usando a conexão nova.
    Retorna no mesmo formato legado esperado pelo app:
    {ok, error, data, dbname}
    """
    fetch = str(fetch or "one").strip().lower()
    try:
        engine = _shared_get_engine()
        with engine.begin() as conn:
            stmt = text(sql)
            exec_params = params if params is not None else {}
            result = conn.execute(stmt, exec_params)
            try:
                dbname = conn.engine.url.database
            except Exception:
                dbname = None

            if fetch == "one":
                row = result.fetchone() if getattr(result, "returns_rows", False) else None
                cols = list(result.keys()) if getattr(result, "returns_rows", False) else []
                return {"ok": True, "error": None, "data": (row, cols), "dbname": dbname}

            if fetch == "all":
                rows = result.fetchall() if getattr(result, "returns_rows", False) else []
                cols = list(result.keys()) if getattr(result, "returns_rows", False) else []
                return {"ok": True, "error": None, "data": (rows, cols), "dbname": dbname}

            return {"ok": True, "error": None, "data": None, "dbname": dbname}
    except Exception as e:
        return {"ok": False, "error": _safe_error_to_str(e), "data": None, "dbname": None}


def load_ml_full_coverage_kpi():
    """
    Lê a cobertura ML no banco novo e falha com elegância.
    Se a tabela ainda não existir, a Home continua carregando.
    """
    sql = """
        SELECT catalog, queue, coverage, last_catalog_update, generated_at
        FROM dashboard_kpi_coverage
        ORDER BY id DESC
        LIMIT 1;
    """

    try:
        res = _run_query_with_fallback(sql, fetch="one")

        if not res or not isinstance(res, dict):
            return {"ok": False, "error": "Falha ao consultar dashboard_kpi_coverage", "data": None}

        if not res.get("ok"):
            err = str(res.get("error") or "")
            if "undefinedtable" in err.lower() or "does not exist" in err.lower():
                return {"ok": True, "error": None, "data": None}
            return {"ok": False, "error": err, "data": None}

        row, _ = res["data"]
        if not row:
            return {"ok": True, "error": None, "data": None}

        return {
            "ok": True,
            "error": None,
            "data": {
                "catalog": int(row[0] or 0),
                "queue": int(row[1] or 0),
                "coverage": float(row[2] or 0),
                "last_catalog_update": row[3].isoformat() if row[3] else None,
                "generated_at": row[4].isoformat() if row[4] else None,
                "dbname_used": res.get("dbname"),
            }
        }
    except Exception as e:
        err = _safe_error_to_str(e)
        if "undefinedtable" in err.lower() or "does not exist" in err.lower():
            return {"ok": True, "error": None, "data": None}
        return {"ok": False, "error": err, "data": None}

def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _qualified_name(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


@st.cache_data(show_spinner=False, ttl=120)
def discover_ml_full_source():
    preferred_names = [
        ("public", "v_dash_ml_full_current"),
        ("public", "dash_ml_full_current"),
        ("public", "vw_dash_ml_full_current"),
        ("public", "ml_full_current"),
        ("public", "v_ml_full_current"),
    ]

    for schema, table in preferred_names:
        test_sql = f"""
            SELECT 1
            FROM {_qualified_name(schema, table)}
            LIMIT 1
        """
        res = _run_query_with_fallback(test_sql, fetch="one")
        if res["ok"]:
            return {
                "ok": True,
                "schema": schema,
                "table": table,
                "qualified": f"{schema}.{table}",
                "dbname_used": res["dbname"],
                "error": None,
            }

    discover_sql = """
        WITH objs AS (
            SELECT
                c.table_schema,
                c.table_name,
                COUNT(DISTINCT CASE
                    WHEN c.column_name IN (
                        'seller_id',
                        'item_id',
                        'status',
                        'is_fulfillment',
                        'price',
                        'available_quantity',
                        'sold_quantity',
                        'logistic_type',
                        'health',
                        'last_updated_at',
                        'created_at'
                    ) THEN c.column_name
                END) AS matched_cols
            FROM information_schema.columns c
            WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
            GROUP BY c.table_schema, c.table_name
        )
        SELECT table_schema, table_name, matched_cols
        FROM objs
        WHERE matched_cols >= 6
        ORDER BY
            CASE
                WHEN table_name = 'v_dash_ml_full_current' THEN 0
                WHEN table_name ILIKE '%dash%ml%full%current%' THEN 1
                WHEN table_name ILIKE '%ml%full%current%' THEN 2
                WHEN table_name ILIKE '%full%current%' THEN 3
                WHEN table_name ILIKE '%ml%full%' THEN 4
                ELSE 9
            END,
            matched_cols DESC,
            table_schema,
            table_name
        LIMIT 20
    """
    res = _run_query_with_fallback(discover_sql, fetch="all")
    if not res["ok"]:
        return {
            "ok": False,
            "schema": None,
            "table": None,
            "qualified": None,
            "dbname_used": None,
            "error": res["error"],
        }

    rows, cols = res["data"]
    if not rows:
        return {
            "ok": False,
            "schema": None,
            "table": None,
            "qualified": None,
            "dbname_used": res["dbname"],
            "error": "Nenhuma view/tabela compatível com o layout ML FULL foi encontrada no banco.",
        }

    best = rows[0]
    schema = best[0]
    table = best[1]

    return {
        "ok": True,
        "schema": schema,
        "table": table,
        "qualified": f"{schema}.{table}",
        "dbname_used": res["dbname"],
        "error": None,
    }

    if not res["ok"]:
        return {"ok": False, "error": res["error"], "data": None}

    row, _ = res["data"]
    if not row:
        return {"ok": True, "error": None, "data": None}

    return {
        "ok": True,
        "error": None,
        "data": {
            "catalog": int(row[0] or 0),
            "queue": int(row[1] or 0),
            "coverage": float(row[2] or 0),
            "last_catalog_update": row[3].isoformat() if row[3] else None,
            "generated_at": row[4].isoformat() if row[4] else None,
            "dbname_used": res["dbname"],
        }
    }


def load_ml_full_current(seller_id: int, limit: int = 5000):
    source = discover_ml_full_source()
    if not source["ok"]:
        return {
            "ok": False,
            "error": source["error"],
            "data": None,
            "dbname_used": source.get("dbname_used"),
            "source_used": None,
        }

    qname = _qualified_name(source["schema"], source["table"])

    sql = f"""
        SELECT seller_id, item_id, status, is_fulfillment, price,
               available_quantity, sold_quantity, logistic_type, health,
               last_updated_at, created_at
        FROM {qname}
        WHERE seller_id = %s
        ORDER BY item_id
        LIMIT %s;
    """

    res = _run_query_with_fallback(sql, params=(int(seller_id), int(limit)), fetch="all")

    if not res["ok"]:
        return {
            "ok": False,
            "error": res["error"],
            "data": None,
            "dbname_used": res.get("dbname"),
            "source_used": source["qualified"],
        }

    rows, cols = res["data"]
    df = pd.DataFrame(rows, columns=cols)
    return {
        "ok": True,
        "error": None,
        "data": df,
        "dbname_used": res["dbname"],
        "source_used": source["qualified"],
    }

    rows, cols = res["data"]
    df = pd.DataFrame(rows, columns=cols)
    return {"ok": True, "error": None, "data": df, "dbname_used": res["dbname"]}


@st.cache_data(show_spinner=False, ttl=120)
def load_ml_catalog_snapshot(seller_id: int, limit: int = 20000):
    source = discover_ml_full_source()
    if not source["ok"]:
        return {
            "ok": False,
            "error": source["error"],
            "data": None,
            "dbname_used": source.get("dbname_used"),
            "source_used": None,
        }

    qname = _qualified_name(source["schema"], source["table"])
    sql = f"""
        SELECT *
        FROM {qname}
        WHERE seller_id = %s
        ORDER BY item_id
        LIMIT %s;
    """
    res = _run_query_with_fallback(sql, params=(int(seller_id), int(limit)), fetch="all")
    if not res["ok"]:
        return {
            "ok": False,
            "error": res["error"],
            "data": None,
            "dbname_used": res.get("dbname"),
            "source_used": source["qualified"],
        }
    rows, cols = res["data"]
    df = pd.DataFrame(rows, columns=cols)
    return {
        "ok": True,
        "error": None,
        "data": df,
        "dbname_used": res["dbname"],
        "source_used": source["qualified"],
    }


def _norm_text_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().replace({"nan": "", "None": "", "<NA>": ""})


def build_ml_catalog_governance(df_catalog: pd.DataFrame) -> dict:
    empty = {
        "ativos": 0,
        "ativos_sem_sku": 0,
        "eans_duplicados": 0,
        "skus_full_flex": 0,
        "estoque_divergente": 0,
        "sem_sku_df": pd.DataFrame(),
        "duplicados_df": pd.DataFrame(),
        "full_flex_df": pd.DataFrame(),
        "divergencia_df": pd.DataFrame(),
        "ok": False,
    }
    if df_catalog is None or df_catalog.empty:
        return empty

    df = df_catalog.copy()
    for c in ["item_id", "status", "logistic_type", "seller_custom_field", "ean_extracted", "sku_extracted", "ean_sku_ref"]:
        if c not in df.columns:
            df[c] = None
    for c in ["is_fulfillment"]:
        if c not in df.columns:
            df[c] = False
    for c in ["available_quantity", "sold_quantity", "price"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["item_id"] = _norm_text_series(df["item_id"])
    df["status"] = _norm_text_series(df["status"])
    df["logistic_type"] = _norm_text_series(df["logistic_type"])
    df["seller_custom_field"] = _norm_text_series(df["seller_custom_field"])
    df["ean_extracted"] = _norm_text_series(df["ean_extracted"])
    df["sku_extracted"] = _norm_text_series(df["sku_extracted"])
    df["ean_sku_ref"] = _norm_text_series(df["ean_sku_ref"])
    df["is_fulfillment"] = df["is_fulfillment"].fillna(False).astype(bool)
    df["status_norm"] = df["status"].str.lower()
    ativos = df[df["status_norm"].eq("active")].copy()
    if ativos.empty:
        return empty | {"ok": True}

    sem_sku = ativos[ativos["ean_sku_ref"].eq("")].copy()
    sem_sku_df = sem_sku[[c for c in ["item_id", "status", "logistic_type", "is_fulfillment", "available_quantity", "sold_quantity", "seller_custom_field", "ean_extracted", "sku_extracted"] if c in sem_sku.columns]].copy()

    base_ok = ativos[~ativos["ean_sku_ref"].eq("")].copy()
    if base_ok.empty:
        return {
            "ativos": int(len(ativos)),
            "ativos_sem_sku": int(len(sem_sku)),
            "eans_duplicados": 0,
            "skus_full_flex": 0,
            "estoque_divergente": 0,
            "sem_sku_df": sem_sku_df,
            "duplicados_df": pd.DataFrame(),
            "full_flex_df": pd.DataFrame(),
            "divergencia_df": pd.DataFrame(),
            "ok": True,
        }

    grouped_rows = []
    for ean_ref, g in base_ok.groupby("ean_sku_ref", dropna=False):
        item_ids = sorted({x for x in g["item_id"].astype(str).tolist() if x})
        full_g = g[g["is_fulfillment"]]
        non_full_g = g[~g["is_fulfillment"]]
        avail = pd.to_numeric(g["available_quantity"], errors="coerce").fillna(0)
        full_av = pd.to_numeric(full_g["available_quantity"], errors="coerce").fillna(0)
        non_full_av = pd.to_numeric(non_full_g["available_quantity"], errors="coerce").fillna(0)
        sold = pd.to_numeric(g["sold_quantity"], errors="coerce").fillna(0)
        spread = float(avail.max() - avail.min()) if len(avail) else 0.0
        grouped_rows.append({
            "ean_sku_ref": str(ean_ref),
            "mlb_count": int(len(item_ids)),
            "mlb_ids": " | ".join(item_ids[:20]),
            "full_mlb_count": int(full_g["item_id"].nunique()),
            "non_full_mlb_count": int(non_full_g["item_id"].nunique()),
            "estoque_ml_total": float(avail.sum()),
            "estoque_full": float(full_av.sum()),
            "estoque_outros": float(non_full_av.sum()),
            "sold_quantity_ml_total": float(sold.sum()),
            "logistic_types": " | ".join(sorted({x for x in g["logistic_type"].astype(str).tolist() if x})),
            "price_min": float(pd.to_numeric(g["price"], errors="coerce").min()) if "price" in g.columns and pd.to_numeric(g["price"], errors="coerce").notna().any() else np.nan,
            "price_max": float(pd.to_numeric(g["price"], errors="coerce").max()) if "price" in g.columns and pd.to_numeric(g["price"], errors="coerce").notna().any() else np.nan,
            "estoque_spread": float(spread),
            "estoque_gap_full_vs_outros": float(abs(full_av.sum() - non_full_av.sum())),
        })
    grouped = pd.DataFrame(grouped_rows)
    if grouped.empty:
        return empty | {"ok": True}

    duplicados_df = grouped[grouped["mlb_count"] > 1].copy().sort_values(["mlb_count", "estoque_ml_total", "sold_quantity_ml_total"], ascending=[False, False, False])
    full_flex_df = grouped[(grouped["full_mlb_count"] > 0) & (grouped["non_full_mlb_count"] > 0)].copy().sort_values(["estoque_gap_full_vs_outros", "estoque_ml_total"], ascending=[False, False])
    divergencia_df = grouped[(grouped["mlb_count"] > 1) & ((grouped["estoque_spread"] >= 20) | (grouped["estoque_gap_full_vs_outros"] >= 20))].copy().sort_values(["estoque_spread", "estoque_gap_full_vs_outros"], ascending=[False, False])

    return {
        "ativos": int(len(ativos)),
        "ativos_sem_sku": int(len(sem_sku)),
        "eans_duplicados": int(len(duplicados_df)),
        "skus_full_flex": int(len(full_flex_df)),
        "estoque_divergente": int(len(divergencia_df)),
        "sem_sku_df": sem_sku_df,
        "duplicados_df": duplicados_df,
        "full_flex_df": full_flex_df,
        "divergencia_df": divergencia_df,
        "ok": True,
    }


# =========================
# DASH OPS / REPOSIÇÃO COLABORATIVA (POSTGRES)
# =========================
@st.cache_data(show_spinner=False, ttl=60)
def load_dash_ops_estado() -> pd.DataFrame:
    sql = """
        SELECT *
        FROM dash_ops.vw_reposicao_estado_ean
    """
    res = _run_query_with_fallback(sql, fetch="all")
    if not res["ok"]:
        return pd.DataFrame()
    rows, cols = res["data"]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows, columns=cols)
    if "ean" in df.columns and "EAN" not in df.columns:
        df = df.rename(columns={"ean": "EAN"})
    return df


def merge_dash_ops_estado(base_df: pd.DataFrame, estado_df: pd.DataFrame) -> pd.DataFrame:
    if base_df is None or base_df.empty:
        return pd.DataFrame()
    out = base_df.copy()
    if estado_df is None or estado_df.empty or "EAN" not in out.columns or "EAN" not in estado_df.columns:
        return out

    estado_cols = [
        "EAN",
        "ocultar_na_reposicao",
        "prioridade_comercial",
        "owner_categoria",
        "observacao_geral",
        "updated_by",
        "estado_created_at",
        "estado_updated_at",
        "qtd_tags_ativas",
        "tags_lista",
        "tags_detalhadas",
        "fl_nao_repor",
        "fl_fora_de_linha",
        "fl_sazonal",
        "qtd_sugestoes_ativas",
        "qtd_sugerida_total_info",
        "ultima_sugestao_em",
        "ultima_atualizacao_em",
        "ultimo_analista",
        "ultima_qtd_sugerida",
        "ultimo_motivo",
        "ultima_observacao",
        "ultimo_canal_contexto",
    ]
    estado_use = estado_df[[c for c in estado_cols if c in estado_df.columns]].copy()
    out["EAN"] = out["EAN"].astype(str)
    estado_use["EAN"] = estado_use["EAN"].astype(str)
    out = out.merge(estado_use, how="left", on="EAN")

    fill_zero_cols = [
        "qtd_tags_ativas", "fl_nao_repor", "fl_fora_de_linha", "fl_sazonal",
        "qtd_sugestoes_ativas", "qtd_sugerida_total_info", "ultima_qtd_sugerida"
    ]
    for c in fill_zero_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    if "ocultar_na_reposicao" in out.columns:
        out["ocultar_na_reposicao"] = out["ocultar_na_reposicao"].fillna(False).astype(bool)

    text_cols = [
        "tags_lista", "tags_detalhadas", "prioridade_comercial", "owner_categoria",
        "observacao_geral", "ultimo_analista", "ultimo_motivo", "ultima_observacao",
        "ultimo_canal_contexto"
    ]
    for c in text_cols:
        if c in out.columns:
            out[c] = out[c].fillna("")

    return out


def build_status_dash(row) -> str:
    badges = []
    try:
        if float(row.get("fl_sazonal", 0)) == 1:
            badges.append("🌸 SAZONAL")
    except Exception:
        pass
    try:
        if float(row.get("fl_fora_de_linha", 0)) == 1:
            badges.append("⛔ FORA DE LINHA")
    except Exception:
        pass
    try:
        if float(row.get("fl_nao_repor", 0)) == 1:
            badges.append("🚫 NÃO REPOR")
    except Exception:
        pass
    try:
        qtd = int(float(row.get("qtd_sugestoes_ativas", 0) or 0))
        if qtd > 0:
            badges.append(f"💡 {qtd} sugest{'ão' if qtd == 1 else 'ões'}")
    except Exception:
        pass
    return " | ".join(badges)


@st.cache_data(show_spinner=False, ttl=30)
def load_dash_ops_sugestoes_raw(ean: str) -> pd.DataFrame:
    sql = """
        SELECT id, ean, sku, descricao_snapshot, marca_snapshot, analista,
               qtd_sugerida, motivo, observacao, canal_contexto, status,
               sessao_origem, created_at, updated_at
        FROM dash_ops.reposicao_sugestoes
        WHERE ean = %s
        ORDER BY created_at DESC, id DESC
    """
    res = _run_query_with_fallback(sql, params=(str(ean),), fetch="all")
    if not res["ok"]:
        return pd.DataFrame()
    rows, cols = res["data"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


@st.cache_data(show_spinner=False, ttl=30)
def load_dash_ops_tags_raw(ean: str) -> pd.DataFrame:
    sql = """
        SELECT id, ean, tag, valor, observacao, ativo, created_by, created_at, updated_at
        FROM dash_ops.produto_tags
        WHERE ean = %s
        ORDER BY ativo DESC, created_at DESC, id DESC
    """
    res = _run_query_with_fallback(sql, params=(str(ean),), fetch="all")
    if not res["ok"]:
        return pd.DataFrame()
    rows, cols = res["data"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


def clear_dash_ops_cache():
    try:
        load_dash_ops_estado.clear()
    except Exception:
        pass
    try:
        load_dash_ops_sugestoes_raw.clear()
    except Exception:
        pass
    try:
        load_dash_ops_tags_raw.clear()
    except Exception:
        pass


def save_dash_ops_sugestao(ean: str, sku: str, descricao: str, marca: str, analista: str,
                            qtd_sugerida: float, motivo: str, observacao: str = "",
                            canal_contexto: str = "", sessao_origem: str = "streamlit"):
    sql = """
        INSERT INTO dash_ops.reposicao_sugestoes (
            ean, sku, descricao_snapshot, marca_snapshot, analista,
            qtd_sugerida, motivo, observacao, canal_contexto, sessao_origem
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        str(ean),
        str(sku or ""),
        str(descricao or ""),
        str(marca or ""),
        str(analista or ""),
        float(qtd_sugerida or 0),
        str(motivo or ""),
        str(observacao or ""),
        str(canal_contexto or ""),
        str(sessao_origem or "streamlit"),
    )
    res = _run_query_with_fallback(sql, params=params, fetch=None)
    if res.get("ok"):
        clear_dash_ops_cache()
    return res


def save_dash_ops_tag(ean: str, tag: str, valor: str = "", observacao: str = "", created_by: str = ""):
    sql = """
        WITH desativa AS (
            UPDATE dash_ops.produto_tags
            SET ativo = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE ean = %s
              AND tag = %s
              AND COALESCE(valor, '') = COALESCE(%s, '')
              AND ativo = TRUE
            RETURNING id
        )
        INSERT INTO dash_ops.produto_tags (ean, tag, valor, observacao, created_by)
        VALUES (%s, %s, %s, %s, %s)
    """
    params = (
        str(ean), str(tag), str(valor or ""),
        str(ean), str(tag), str(valor or ""), str(observacao or ""), str(created_by or "")
    )
    res = _run_query_with_fallback(sql, params=params, fetch=None)
    if res.get("ok"):
        clear_dash_ops_cache()
    return res


def upsert_dash_ops_estado(ean: str, ocultar_na_reposicao: bool = False,
                           prioridade_comercial: str = "", owner_categoria: str = "",
                           observacao_geral: str = "", updated_by: str = ""):
    sql = """
        INSERT INTO dash_ops.produto_estado (
            ean, ocultar_na_reposicao, prioridade_comercial,
            owner_categoria, observacao_geral, updated_by
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (ean)
        DO UPDATE SET
            ocultar_na_reposicao = EXCLUDED.ocultar_na_reposicao,
            prioridade_comercial = EXCLUDED.prioridade_comercial,
            owner_categoria = EXCLUDED.owner_categoria,
            observacao_geral = EXCLUDED.observacao_geral,
            updated_by = EXCLUDED.updated_by,
            updated_at = CURRENT_TIMESTAMP
    """
    params = (
        str(ean), bool(ocultar_na_reposicao), str(prioridade_comercial or ""),
        str(owner_categoria or ""), str(observacao_geral or ""), str(updated_by or "")
    )
    res = _run_query_with_fallback(sql, params=params, fetch=None)
    if res.get("ok"):
        clear_dash_ops_cache()
    return res

# =========================
# DEBUG
# =========================

db_url_show = (os.getenv("DATABASE_URL") or "").strip()
if db_url_show:
    try:
        _p = urllib.parse.urlparse(db_url_show)
        env_db_show = (_p.path or "/").lstrip("/") or "(db-url)"
        env_host_show = _p.hostname or "(db-url)"
        env_port_show = str(_p.port or 5432)
        env_user_show = _p.username or "(db-url)"
        env_pass_show = "OK"
    except Exception:
        env_db_show = "(db-url)"
        env_host_show = "(db-url)"
        env_port_show = "5432"
        env_user_show = "(db-url)"
        env_pass_show = "OK"
else:
    env_db_show = os.getenv("DB_NAME", "(auto: postgres/n8n)")
    env_host_show = os.getenv("DB_HOST", "127.0.0.1")
    env_port_show = os.getenv("DB_PORT", "5433")
    env_user_show = os.getenv("DB_USER", "n8n")
    env_pass_show = 'OK' if (os.getenv('DB_POSTGRESDB_PASSWORD') or os.getenv('POSTGRES_PASSWORD') or os.getenv('DB_PASSWORD')) else 'VAZIA'

st.caption(
    "DB ENV: "
    f"host={env_host_show} | "
    f"port={env_port_show} | "
    f"db={env_db_show} | "
    f"user={env_user_show} | "
    f"pass={env_pass_show}"
)


# =========================
# ALERTA BETA (Cobertura ML Sync)
# =========================
cov_result = load_ml_full_coverage_kpi() or {"ok": False, "error": "load_ml_full_coverage_kpi retornou None", "data": None}

if not cov_result["ok"]:
    st.error(f"Erro ao ler cobertura no Postgres:\n\n{cov_result['error']}")
else:
    data = cov_result["data"]
    if not data:
        st.info("ℹ️ Cobertura ML (BETA): ainda não existe registro na tabela `dashboard_kpi_coverage`.")
    else:
        catalog = data["catalog"]
        queue = data["queue"]
        coverage = data["coverage"]
        last_upd = data["last_catalog_update"]

        st.warning(
            f"⚠️ Versão BETA: base parcial sincronizada (Mercado Livre).\n\n"
            f"Cobertura atual: {catalog:,} / {queue:,} SKUs ({coverage:.1%}).\n"
            f"Última atualização do catálogo: {last_upd}"
        )


# =========================
# Helpers (formatação)
# =========================
def money_br(v) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    s = f"{v:,.2f}"
    return ("R$ " + s).replace(",", "X").replace(".", ",").replace("X", ".")


def metric_int(n) -> str:
    try:
        n = int(n)
    except Exception:
        n = 0
    return f"{n:,}".replace(",", ".")


def pct_br(v) -> str:
    try:
        return f"{float(v):.2f}%".replace(".", ",")
    except Exception:
        return "0,00%"


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def safe_int(v, default=0):
    try:
        return int(float(v))
    except Exception:
        return default


# =========================
# Helpers (dados)
# =========================
def parse_date(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def load_csv_safe(path: str) -> pd.DataFrame:
    if use_database_tables():
        remote = load_artifact_or_empty(path)
        if remote is not None and not remote.empty:
            return remote

    if not file_exists(path):
        return pd.DataFrame()

    try:
        df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
        if len(df.columns) > 1:
            return df
    except Exception:
        pass

    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
        if len(df.columns) > 1:
            return df
    except Exception:
        pass

    try:
        df = pd.read_csv(path, sep=",", encoding="utf-8-sig")
        if len(df.columns) > 1:
            return df
    except Exception:
        pass

    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


load_csv = load_csv_safe


@st.cache_data(show_spinner=True)
def load_master_parquet(path: str) -> pd.DataFrame:
    if use_database_tables():
        remote = load_artifact_or_empty(path)
        if remote is not None and not remote.empty:
            return remote
    if not file_exists(path):
        return pd.DataFrame()
    return pd.read_parquet(path)


def ensure_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def get_receita_col(df: pd.DataFrame) -> str | None:
    candidates = [
        "receita", "Total_num", "Total", "Valor Total", "Valor_Total", "Total do Item", "TotalItem",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def get_qtde_col(df: pd.DataFrame) -> str | None:
    for c in ["Qtde", "Quantidade", "Qtd", "QTD", "itens", "quantidade"]:
        if c in df.columns:
            return c
    return None


def get_canal_col(df: pd.DataFrame) -> str | None:
    candidates = ["Canal", "canal", "Marketplace", "marketplace", "Plataforma", "plataforma"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def get_brand_col(df: pd.DataFrame) -> str | None:
    candidates = ["Marca", "marca", "Brand", "brand", "Linha", "linha"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def get_desc_col(df: pd.DataFrame) -> str | None:
    candidates = ["Descricao", "Descrição", "descricao", "descrição"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def get_sku_col(df: pd.DataFrame) -> str | None:
    candidates = ["SKU", "sku", "Sku", "seller_sku", "Seller_SKU", "codigo_sku", "Seller SKU", "SellerSKU"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def get_fulfillment_col(df: pd.DataFrame) -> str | None:
    candidates = [
        "is_fulfillment", "Is_Fulfillment", "fulfillment", "Fulfillment",
        "full", "FULL", "eh_full", "is_full"
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def normalize_text_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .fillna("")
         .str.strip()
         .str.upper()
         .str.replace(r"\s+", " ", regex=True)
    )


def normalize_text_value(x) -> str:
    try:
        return re.sub(r"\s+", " ", str(x).strip().upper())
    except Exception:
        return ""


def safe_multiselect_filter(df: pd.DataFrame, col: str, selected: list[str]) -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns or not selected:
        return df.copy()

    out = df.copy()
    base_norm = normalize_text_series(out[col])
    sel_norm = {normalize_text_value(v) for v in selected if str(v).strip()}
    if not sel_norm:
        return out

    mask = base_norm.isin(sel_norm)
    return out[mask].copy()


def make_anomaly_flags(df: pd.DataFrame, value_col: str, roll_col: str, std_col: str, z_threshold: float) -> pd.DataFrame:
    df = df.copy()
    df["z"] = (df[value_col] - df[roll_col]) / df[std_col].replace({0: np.nan})
    df["anomalia"] = df["z"].abs() >= z_threshold
    df["anomalia"] = df["anomalia"].fillna(False)
    return df


def agg_timeseries(df: pd.DataFrame, dt_col: str, freq: str) -> pd.DataFrame:
    out = df.copy()
    out["X"] = out[dt_col].dt.to_period(freq).dt.to_timestamp()
    out = out.groupby("X", as_index=False)[["receita", "pedidos"]].sum().sort_values("X")
    return out


def rolling_params(freq: str) -> tuple[int, int]:
    if freq == "D":
        return 90, 10
    if freq == "W":
        return 12, 6
    if freq == "M":
        return 3, 2
    if freq == "Q":
        return 4, 2
    return 3, 2


def calc_abc(df: pd.DataFrame, prod_col="EAN", receita_col="receita", qtde_col=None, pedido_col="Pedido") -> pd.DataFrame:
    base = df.copy()
    if prod_col not in base.columns:
        return pd.DataFrame()

    base[receita_col] = pd.to_numeric(base[receita_col], errors="coerce").fillna(0.0)

    agg = {
        "receita": (receita_col, "sum"),
        "pedidos": (pedido_col, "nunique") if pedido_col in base.columns else (receita_col, "count"),
    }

    if qtde_col and qtde_col in base.columns:
        agg["itens"] = (qtde_col, "sum")
    else:
        agg["itens"] = (receita_col, "count")

    out = (
        base.groupby(prod_col)
            .agg(**agg)
            .reset_index()
            .sort_values("receita", ascending=False)
    )

    total = float(out["receita"].sum()) if len(out) else 0.0
    out["pct"] = (out["receita"] / total) if total else 0.0
    out["pct_acum"] = out["pct"].cumsum()

    out["classe_abc"] = np.select(
        [out["pct_acum"] <= 0.80, out["pct_acum"] <= 0.95],
        ["A", "B"],
        default="C"
    )
    return out


def normalize_status_cobertura(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()


def normalize_trend(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()


def ensure_full_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    rename_map = {}
    if "descricao" in out.columns and "Descricao" not in out.columns:
        rename_map["descricao"] = "Descricao"
    if "marca" in out.columns and "Marca" not in out.columns:
        rename_map["marca"] = "Marca"
    if "unidades30d" in out.columns and "unidades_30d" not in out.columns:
        rename_map["unidades30d"] = "unidades_30d"
    if "vendas_30d" in out.columns and "unidades_30d" not in out.columns:
        rename_map["vendas_30d"] = "unidades_30d"

    if rename_map:
        out = out.rename(columns=rename_map)

    return out






def normalize_full_numeric_display(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    int_cols = [
        "unidades_30d", "unidades_30d_brutas", "unidades_30d_ajustadas",
        "forecast_unid_35d", "forecast_bruto_35d", "forecast_cap_35d",
        "safety_stock", "estoque_alvo_full_35d", "recomendacao_envio_full",
        "qtd_segura_envio_full", "unidades_90d", "unidades_90d_ajustadas",
        "dias_com_venda_90d", "unidades_30d_full", "unidades_30d_ml_fora_full",
        "unidades_30d_total_ml", "unidades_60d_full", "unidades_60d_ml_fora_full",
        "unidades_60d_total_ml", "unidades_90d_full", "unidades_90d_ml_fora_full",
        "unidades_90d_total_ml",
    ]
    for c in int_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).apply(np.ceil).astype(int)
    return out

def get_margin_col(df: pd.DataFrame) -> str | None:
    candidates = [
        "margem", "Margem", "lucro", "Lucro", "Margem_Contribuicao", "margem_contribuicao",
        "profit", "Profit", "lucro_bruto", "margem_bruta"
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def clamp_value(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def score_linear(value, bad: float, good: float, neutral: float = 50.0) -> float:
    try:
        value = float(value)
    except Exception:
        return neutral
    if good == bad:
        return neutral
    score = ((value - bad) / (good - bad)) * 100.0
    return clamp_value(score, 0.0, 100.0)


def pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def build_exec_item_label(df: pd.DataFrame) -> tuple[str | None, pd.Series | None]:
    if df is None or df.empty:
        return None, None
    sku_col = get_sku_col(df)
    ean_col = COL_EAN if COL_EAN in df.columns else None
    desc_col = get_desc_col(df)

    if sku_col and desc_col:
        label = (
            df[sku_col].astype(str).fillna("").str.strip() + " — " +
            df[desc_col].astype(str).fillna("").str.strip()
        ).str.strip(" —")
        return "item_label", label
    if ean_col and desc_col:
        label = (
            df[ean_col].astype(str).fillna("").str.strip() + " — " +
            df[desc_col].astype(str).fillna("").str.strip()
        ).str.strip(" —")
        return "item_label", label
    if sku_col:
        return sku_col, df[sku_col].astype(str)
    if ean_col:
        return ean_col, df[ean_col].astype(str)
    if desc_col:
        return desc_col, df[desc_col].astype(str)
    return None, None


def compute_health_score(
    var_receita,
    var_pedidos,
    var_ticket,
    master_df: pd.DataFrame,
    repl_df: pd.DataFrame,
    alertas_df: pd.DataFrame,
    receita_col: str,
    margin_col: str | None = None,
) -> dict:
    growth_score = score_linear(var_receita, -15, 15)
    volume_score = score_linear(var_pedidos, -15, 15)

    if margin_col and margin_col in master_df.columns and receita_col in master_df.columns and len(master_df):
        receita_total = pd.to_numeric(master_df[receita_col], errors="coerce").fillna(0).sum()
        margem_total = pd.to_numeric(master_df[margin_col], errors="coerce").fillna(0).sum()
        margem_pct = (margem_total / receita_total) * 100 if receita_total else np.nan
        margin_score = score_linear(margem_pct, 5, 25)
        margin_label = "Margem"
        margin_value = margem_pct
        margin_display = pct_br(margem_pct) if pd.notna(margem_pct) else "N/D"
    else:
        margin_score = score_linear(var_ticket, -10, 10)
        margin_label = "Ticket / mix"
        margin_value = var_ticket
        margin_display = pct_br(var_ticket) if pd.notna(var_ticket) else "N/D"

    total_rep = len(repl_df) if repl_df is not None else 0
    criticos = 0
    urgentes = 0
    if repl_df is not None and not repl_df.empty and "status_cobertura_90d" in repl_df.columns:
        status = repl_df["status_cobertura_90d"].astype(str).str.upper().str.strip()
        criticos = int((status == "CRÍTICO").sum())
        urgentes = int((status == "URGENTE").sum())
    stock_risk = ((criticos * 1.0) + (urgentes * 0.6)) / total_rep if total_rep else 0.0
    stock_score = clamp_value(100 - (stock_risk * 220), 0, 100)

    alert_count = len(alertas_df) if alertas_df is not None else 0
    alert_score = clamp_value(100 - alert_count * 6, 0, 100)

    components = [
        ("Crescimento", growth_score),
        ("Volume", volume_score),
        (margin_label, margin_score),
        ("Estoque", stock_score),
        ("Alertas", alert_score),
    ]
    health_score = round(sum(score for _, score in components) / len(components), 1)

    if health_score >= 80:
        status = "Saudável"
        color = "#16a34a"
    elif health_score >= 60:
        status = "Atenção"
        color = "#f59e0b"
    else:
        status = "Crítico"
        color = "#dc2626"

    comp_df = pd.DataFrame(components, columns=["componente", "score"])
    comp_df["score"] = comp_df["score"].round(1)

    return {
        "health_score": health_score,
        "status": status,
        "color": color,
        "components_df": comp_df,
        "criticos_estoque": criticos,
        "urgentes_estoque": urgentes,
        "alert_count": alert_count,
        "margin_label": margin_label,
        "margin_display": margin_display,
        "margin_value": margin_value,
    }


def compute_month_forecast(
    master_df: pd.DataFrame,
    end_vis: pd.Timestamp,
    receita_col: str,
    canal_col: str | None,
    canais_sel: list[str],
    brand_col: str | None,
    marcas_sel: list[str],
    fulfillment_sel_dash: str,
    sku_busca_dash: str,
    sku_col_master: str | None,
    desc_col: str | None,
) -> dict:
    month_start = end_vis.to_period("M").start_time
    month_end = end_vis.to_period("M").end_time.normalize()
    base = master_df[(master_df[COL_DATA] >= month_start) & (master_df[COL_DATA] <= end_vis)].copy()

    if canal_col and len(canais_sel) > 0:
        base = safe_multiselect_filter(base, canal_col, canais_sel)
    if brand_col and len(marcas_sel) > 0:
        base = safe_multiselect_filter(base, brand_col, marcas_sel)
    if "_fulfillment_flag" in base.columns and pd.Series(base["_fulfillment_flag"]).notna().any():
        flag = pd.Series(base["_fulfillment_flag"]).map(lambda x: True if x is True else False if x is False else np.nan)
        if fulfillment_sel_dash == "Somente Fulfillment":
            base = base[flag.fillna(False)].copy()
        elif fulfillment_sel_dash == "Somente Não Fulfillment":
            base = base[~flag.fillna(False)].copy()
    if sku_busca_dash:
        cols_busca = []
        if sku_col_master and sku_col_master in base.columns:
            cols_busca.append(sku_col_master)
        if COL_EAN in base.columns:
            cols_busca.append(COL_EAN)
        if desc_col and desc_col in base.columns:
            cols_busca.append(desc_col)
        base = apply_multi_text_search(base, sku_busca_dash, cols_busca)

    receita_mtd = float(pd.to_numeric(base[receita_col], errors="coerce").fillna(0).sum()) if len(base) else 0.0
    elapsed_days = max((end_vis.normalize() - month_start.normalize()).days + 1, 1)
    days_in_month = month_end.day
    avg_daily = receita_mtd / elapsed_days if elapsed_days else 0.0
    projected_month = avg_daily * days_in_month

    prev_month_start = (month_start - pd.offsets.MonthBegin(1)).normalize()
    prev_month_end = (month_start - pd.Timedelta(days=1)).normalize()
    prev_full = master_df[(master_df[COL_DATA] >= prev_month_start) & (master_df[COL_DATA] <= prev_month_end)].copy()
    if canal_col and len(canais_sel) > 0:
        prev_full = safe_multiselect_filter(prev_full, canal_col, canais_sel)
    if brand_col and len(marcas_sel) > 0:
        prev_full = safe_multiselect_filter(prev_full, brand_col, marcas_sel)
    if sku_busca_dash:
        cols_busca_prev = []
        if sku_col_master and sku_col_master in prev_full.columns:
            cols_busca_prev.append(sku_col_master)
        if COL_EAN in prev_full.columns:
            cols_busca_prev.append(COL_EAN)
        if desc_col and desc_col in prev_full.columns:
            cols_busca_prev.append(desc_col)
        prev_full = apply_multi_text_search(prev_full, sku_busca_dash, cols_busca_prev)
    receita_prev_full = float(pd.to_numeric(prev_full[receita_col], errors="coerce").fillna(0).sum()) if len(prev_full) else 0.0
    delta_projection = ((projected_month / receita_prev_full) - 1) * 100 if receita_prev_full else np.nan

    return {
        "month_start": month_start,
        "month_end": month_end,
        "receita_mtd": receita_mtd,
        "avg_daily": avg_daily,
        "projected_month": projected_month,
        "prev_month_full": receita_prev_full,
        "delta_projection": delta_projection,
        "days_elapsed": elapsed_days,
        "days_in_month": days_in_month,
    }


def build_risk_radar(
    mp: pd.DataFrame,
    mp_prev: pd.DataFrame,
    receita_col: str,
    canal_col: str | None,
    sku_dim: str | None,
    repl_df: pd.DataFrame,
) -> dict:
    canais_risco = pd.DataFrame()
    if canal_col and canal_col in mp.columns and canal_col in mp_prev.columns:
        curr = mp.groupby(canal_col, as_index=False)[receita_col].sum().rename(columns={receita_col: "receita_atual"})
        prev = mp_prev.groupby(canal_col, as_index=False)[receita_col].sum().rename(columns={receita_col: "receita_prev"})
        canais_risco = curr.merge(prev, on=canal_col, how="outer").fillna(0)
        canais_risco["var_pct"] = np.where(canais_risco["receita_prev"] > 0, ((canais_risco["receita_atual"] / canais_risco["receita_prev"]) - 1) * 100, np.nan)
        base_cut = canais_risco["receita_prev"].quantile(0.60) if len(canais_risco) else 0
        canais_risco = canais_risco[(canais_risco["receita_prev"] >= base_cut) & (canais_risco["var_pct"] <= -10)].copy()
        canais_risco = canais_risco.sort_values(["var_pct", "receita_prev"], ascending=[True, False]).head(10)

    skus_risco = pd.DataFrame()
    if sku_dim and sku_dim in mp.columns and sku_dim in mp_prev.columns:
        curr = mp.groupby(sku_dim, as_index=False)[receita_col].sum().rename(columns={receita_col: "receita_atual"})
        prev = mp_prev.groupby(sku_dim, as_index=False)[receita_col].sum().rename(columns={receita_col: "receita_prev"})
        skus_risco = curr.merge(prev, on=sku_dim, how="outer").fillna(0)
        skus_risco["var_pct"] = np.where(skus_risco["receita_prev"] > 0, ((skus_risco["receita_atual"] / skus_risco["receita_prev"]) - 1) * 100, np.nan)
        base_cut = skus_risco["receita_prev"].quantile(0.85) if len(skus_risco) else 0
        skus_risco = skus_risco[(skus_risco["receita_prev"] >= base_cut) & (skus_risco["var_pct"] <= -15)].copy()
        skus_risco = skus_risco.sort_values(["var_pct", "receita_prev"], ascending=[True, False]).head(15)

    ruptura = pd.DataFrame()
    if repl_df is not None and not repl_df.empty and "status_cobertura_90d" in repl_df.columns:
        ruptura = repl_df.copy()
        ruptura["status_cobertura_90d"] = ruptura["status_cobertura_90d"].astype(str).str.upper().str.strip()
        ruptura = ruptura[ruptura["status_cobertura_90d"].isin(["CRÍTICO", "URGENTE"])]
        sort_col = pick_first_existing(ruptura, ["score_urgencia", "reposicao_sugerida_90d", "forecast_unid_90d", "vendas_90d", "cobertura_90d"])
        if sort_col:
            ruptura = ruptura.sort_values(sort_col, ascending=(sort_col == "cobertura_90d"))
        ruptura = ruptura.head(15)

    radar_counts = pd.DataFrame([
        {"frente": "Canais desacelerando", "qtd": int(len(canais_risco))},
        {"frente": "SKUs perdendo venda", "qtd": int(len(skus_risco))},
        {"frente": "Ruptura próxima", "qtd": int(len(ruptura))},
    ])

    return {
        "counts": radar_counts,
        "canais_risco": canais_risco,
        "skus_risco": skus_risco,
        "ruptura": ruptura,
    }


def build_copiloto_views(full_cand_df: pd.DataFrame, repl_df: pd.DataFrame, accel_df: pd.DataFrame) -> dict:
    full_cand_df = ensure_full_columns(full_cand_df)
    repl_df = ensure_full_columns(repl_df)
    accel_df = ensure_full_columns(accel_df)

    entrar_full = pd.DataFrame()
    if full_cand_df is not None and not full_cand_df.empty:
        entrar_full = full_cand_df.copy()
        sort_col = pick_first_existing(entrar_full, ["score_full", "score_urgencia", "receita", "vendas_30d", "unidades_30d", "forecast_unid_90d"])
        if sort_col:
            entrar_full = entrar_full.sort_values(sort_col, ascending=False)
        entrar_full = entrar_full.head(20)

    agir_hoje = pd.DataFrame()
    comprar = pd.DataFrame()
    ruptura = pd.DataFrame()
    anuncio = pd.DataFrame()

    if repl_df is not None and not repl_df.empty:
        base = repl_df.copy()

        bucket_col = base.get("bucket_prioridade", pd.Series(index=base.index, dtype=object)).astype(str).str.upper()
        status_col = base.get("status_cobertura_90d", pd.Series(index=base.index, dtype=object)).astype(str).str.upper().str.strip()
        compra_col = base.get("recomendacao_compra", pd.Series(index=base.index, dtype=object)).astype(str).str.upper()

        agir_mask = bucket_col.str.contains("AGIR_HOJE", na=False)
        if agir_mask.sum() == 0:
            agir_mask = status_col.str.contains("CRÍTICO", na=False)
        agir_hoje = base.loc[agir_mask].copy()
        sort_col = pick_first_existing(agir_hoje, ["impacto_prioridade", "score_prioridade_sku", "score_urgencia", "reposicao_sugerida_90d", "cobertura_90d"])
        if sort_col:
            agir_hoje = agir_hoje.sort_values(sort_col, ascending=(sort_col == "cobertura_90d"))
        agir_hoje = agir_hoje.head(20)

        comprar_mask = compra_col.str.contains("COMPRAR", na=False)
        if comprar_mask.sum() == 0 and "reposicao_sugerida_90d" in base.columns:
            comprar_mask = pd.to_numeric(base["reposicao_sugerida_90d"], errors="coerce").fillna(0).gt(0)
        comprar = base.loc[comprar_mask].copy()
        sort_col = pick_first_existing(comprar, ["impacto_prioridade", "qtd_compra_sugerida", "reposicao_sugerida_90d", "score_prioridade_sku"])
        if sort_col:
            comprar = comprar.sort_values(sort_col, ascending=False)
        comprar = comprar.head(20)

        ruptura = base.copy()
        ruptura = ruptura[status_col.isin(["CRÍTICO", "URGENTE"])].copy()
        sort_col = pick_first_existing(ruptura, ["impacto_prioridade", "score_urgencia", "score_prioridade_sku", "reposicao_sugerida_90d", "cobertura_90d"])
        if sort_col:
            ruptura = ruptura.sort_values(sort_col, ascending=(sort_col == "cobertura_90d"))
        ruptura = ruptura.head(20)

        anuncio = base.copy()
        status_ok = status_col.isin(["SAUDÁVEL", "EXCESSO", "ALTO"])
        if "trend_status" in anuncio.columns:
            trend_low = anuncio["trend_status"].astype(str).str.upper().str.contains("SEM_GIRO|DESACELER|BAIXA", regex=True, na=False)
            anuncio = anuncio[status_ok & trend_low].copy()
        else:
            anuncio = anuncio[status_ok].copy()
        sort_col = pick_first_existing(anuncio, ["impacto_receita_30d_est", "reposicao_sugerida_90d", "forecast_unid_90d", "vendas_90d"])
        if sort_col:
            anuncio = anuncio.sort_values(sort_col, ascending=False)
        anuncio = anuncio.head(20)

    acelerar = pd.DataFrame()
    if accel_df is not None and not accel_df.empty:
        acelerar = accel_df.copy()
        sort_col = pick_first_existing(acelerar, ["impacto_prioridade", "score_urgencia", "crescimento_ult30_vs_prev30_pct", "forecast_unid_90d", "vendas_90d"])
        if sort_col:
            acelerar = acelerar.sort_values(sort_col, ascending=False)
        acelerar = acelerar.head(20)

    summary = pd.DataFrame([
        {"oportunidade": "Agir hoje", "qtd": int(len(agir_hoje))},
        {"oportunidade": "Comprar", "qtd": int(len(comprar))},
        {"oportunidade": "Entrar em FULL", "qtd": int(len(entrar_full))},
        {"oportunidade": "Reposição urgente", "qtd": int(len(ruptura))},
        {"oportunidade": "Potencial de escala", "qtd": int(len(acelerar))},
        {"oportunidade": "Precisa anúncio", "qtd": int(len(anuncio))},
    ])

    return {
        "summary": summary,
        "agir_hoje": agir_hoje,
        "comprar": comprar,
        "entrar_full": entrar_full,
        "ruptura": ruptura,
        "acelerar": acelerar,
        "anuncio": anuncio,
    }


def get_csv_last_update_str(path: str) -> str:
    try:
        ts = Path(path).stat().st_mtime
        return datetime.fromtimestamp(ts).strftime("%d/%m %H:%M")
    except Exception:
        return "N/A"


def render_copiloto_validadores(base_df: pd.DataFrame, csv_path: str = ARQ_REPOSICAO_DECISAO):
    base_df = ensure_full_columns(base_df)
    agir_hoje_count = 0
    if base_df is not None and not base_df.empty:
        if "bucket_prioridade" in base_df.columns:
            agir_hoje_count = int(base_df["bucket_prioridade"].astype(str).str.upper().str.contains("AGIR_HOJE", na=False).sum())
        if agir_hoje_count == 0 and "status_cobertura_90d" in base_df.columns:
            agir_hoje_count = int(base_df["status_cobertura_90d"].astype(str).str.upper().str.contains("CRÍTICO", na=False).sum())

    ultima = get_csv_last_update_str(csv_path)
    if base_df is None or base_df.empty:
        status = "🔴 sem base"
        status_detail = "Pipeline sem base de decisão carregada."
    elif file_exists(csv_path):
        if agir_hoje_count > 0:
            status = "🟢 OK"
            status_detail = "CSV encontrado e leitura operacional válida."
        else:
            status = "🟡 sem AGIR_HOJE"
            status_detail = "CSV encontrado, mas sem itens em prioridade máxima."
    else:
        status = "🔴 CSV ausente"
        status_detail = "Arquivo de decisão não encontrado na pasta do projeto."

    v1, v2, v3 = st.columns(3)
    with v1:
        st.metric("🔥 AGIR HOJE (total detectado)", agir_hoje_count, help="Contagem total encontrada no CSV de decisão, sem limite de exibição.")
    with v2:
        st.metric("🕒 Última atualização do CSV", ultima, help="Data/hora do arquivo reposicao_decisao_sku.csv usado pela dashboard.")
    with v3:
        st.metric("⚙️ Status do pipeline", status)
    st.caption(status_detail)




def ensure_alertas_operacionais(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "tipo_alerta","severidade","frente","EAN","SKU","Descricao","Marca","mensagem",
        "acao_recomendada","score_prioridade_sku","impacto_prioridade","bucket_prioridade",
        "status_cobertura_90d","recomendacao_compra","recomendacao_logistica","data_ref","status_alerta"
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = np.nan
    for c in ["score_prioridade_sku", "impacto_prioridade"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out[cols].copy()


def render_alertas_summary(alertas_df: pd.DataFrame):
    df = ensure_alertas_operacionais(alertas_df)
    if df.empty:
        a = m = b = 0
    else:
        sev = df["severidade"].astype(str).str.upper()
        a = int((sev == "ALTA").sum())
        m = int((sev == "MEDIA").sum())
        b = int((sev == "BAIXA").sum())
    c1, c2, c3 = st.columns(3)
    with c1:
        render_metric_card("Alertas ALTA", metric_int(a), "Ruptura, agir hoje e riscos imediatos", "#dc2626")
    with c2:
        render_metric_card("Alertas MÉDIA", metric_int(m), "Oportunidades relevantes e riscos controláveis", "#f59e0b")
    with c3:
        render_metric_card("Alertas BAIXA", metric_int(b), "Monitoramento e ajustes finos", "#2563eb")

def add_fulfillment_helper_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    fcol = get_fulfillment_col(out)

    out["_fulfillment_flag"] = np.nan

    if fcol and fcol in out.columns:
        serie = out[fcol]

        if pd.api.types.is_bool_dtype(serie):
            out["_fulfillment_flag"] = serie.astype(object)

        elif pd.api.types.is_numeric_dtype(serie):
            out["_fulfillment_flag"] = pd.to_numeric(serie, errors="coerce").eq(1).astype(object)

        else:
            s = serie.astype(str).str.strip().str.lower()
            true_vals = {"1", "true", "t", "sim", "s", "yes", "y", "full", "fulfillment"}
            false_vals = {"0", "false", "f", "nao", "não", "n", "no", "normal", "me1", "cross_docking"}

            out["_fulfillment_flag"] = np.where(
                s.isin(true_vals),
                True,
                np.where(s.isin(false_vals), False, np.nan)
            )

    out["_fulfillment_label"] = np.where(
        pd.Series(out["_fulfillment_flag"]).astype(str) == "True",
        "Fulfillment",
        np.where(pd.Series(out["_fulfillment_flag"]).astype(str) == "False", "Não Fulfillment", "N/I")
    )

    return out


def apply_multi_text_search(df: pd.DataFrame, termo: str, cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if not termo:
        return df.copy()

    mask = pd.Series(False, index=df.index)
    for c in cols:
        if c in df.columns:
            mask = mask | df[c].astype(str).str.contains(termo, case=False, na=False)
    return df[mask].copy()


def apply_full_filters(
    df: pd.DataFrame,
    sku_termo: str = "",
    marcas_sel: list[str] | None = None,
    fulfillment_sel: str = "Todos",
    ean_termo: str = "",
    desc_termo: str = "",
    canal_sel: str = "Todos"
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    view = df.copy()
    marcas_sel = marcas_sel or []

    sku_col = get_sku_col(view)
    brand_col_local = get_brand_col(view)

    if sku_termo and sku_col and sku_col in view.columns:
        view = view[view[sku_col].astype(str).str.contains(sku_termo, case=False, na=False)].copy()

    if ean_termo and "EAN" in view.columns:
        view = view[view["EAN"].astype(str).str.contains(ean_termo, case=False, na=False)].copy()

    if desc_termo:
        desc_col_local = get_desc_col(view)
        if desc_col_local and desc_col_local in view.columns:
            view = view[view[desc_col_local].astype(str).str.contains(desc_termo, case=False, na=False)].copy()

    if brand_col_local and len(marcas_sel) > 0:
        view = safe_multiselect_filter(view, brand_col_local, marcas_sel)

    view = add_fulfillment_helper_col(view)

    if "_fulfillment_flag" in view.columns and pd.Series(view["_fulfillment_flag"]).notna().any():
        flag_series = pd.Series(view["_fulfillment_flag"]).map(lambda x: True if x is True else False if x is False else np.nan)
        if fulfillment_sel == "Somente Fulfillment":
            view = view[flag_series.fillna(False)].copy()
        elif fulfillment_sel == "Somente Não Fulfillment":
            view = view[~flag_series.fillna(False)].copy()

    canal_col_local = get_canal_col(view)
    if canal_col_local and canal_sel != "Todos":
        canal_sel_norm = normalize_text_value(canal_sel)
        view = view[normalize_text_series(view[canal_col_local]) == canal_sel_norm].copy()

    return view


def build_manual_suggestion_df(base_df: pd.DataFrame, manual_map: dict) -> pd.DataFrame:
    if base_df is None or base_df.empty:
        return pd.DataFrame()

    out = base_df.copy()
    if "EAN" not in out.columns:
        out["reposicao_manual_comercial"] = np.nan
        return out

    out["reposicao_manual_comercial"] = out["EAN"].astype(str).map(manual_map)
    out["reposicao_manual_comercial"] = pd.to_numeric(out["reposicao_manual_comercial"], errors="coerce")
    return out


# =========================
# Exec helper (IA)
# =========================
def run_cmd(cmd: list[str]) -> tuple[int, str]:
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="ignore",
        env=env
    )
    return p.returncode, (p.stdout or "")


# =========================
# Session state
# =========================
if "manual_reposicoes" not in st.session_state:
    st.session_state["manual_reposicoes"] = {}

if "ml_full_df" not in st.session_state:
    st.session_state["ml_full_df"] = pd.DataFrame()

if "ml_full_loaded" not in st.session_state:
    st.session_state["ml_full_loaded"] = False

if "ml_full_error" not in st.session_state:
    st.session_state["ml_full_error"] = None

if "ml_full_source" not in st.session_state:
    st.session_state["ml_full_source"] = None

if "ml_full_dbname" not in st.session_state:
    st.session_state["ml_full_dbname"] = None

if "ia_last_run" not in st.session_state:
    st.session_state["ia_last_run"] = None

if "ia_last_log" not in st.session_state:
    st.session_state["ia_last_log"] = ""

if "ia_last_ok" not in st.session_state:
    st.session_state["ia_last_ok"] = None


# =========================
# Top controls
# =========================
open_modern_card("Controles Globais", "Ajustes rápidos da sessão e sensibilidade visual")

colA, colB, colC, colD = st.columns([1, 1, 1, 1.8])

with colA:
    if st.button("🔄 Recarregar dados (limpar cache)", use_container_width=True):
        st.cache_data.clear()
        st.success("Cache limpo! Se necessário, pressione F5 no navegador.")

with colB:
    topN = st.slider("Top N (tabelas)", min_value=5, max_value=50, value=10, step=1, key="global_topn")

with colC:
    z_thr = st.slider("Sensibilidade anomalia (z)", 1.5, 4.0, Z_THRESHOLD_DEFAULT, 0.1, key="global_z")

with colD:
    st.markdown('<span class="soft-chip">Painel executivo</span>', unsafe_allow_html=True)
    st.caption("KPIs e filtros dinâmicos calculados do parquet e CSVs gerados pelo pipeline.")

close_card()


# =========================
# Load master + daily
# =========================
master = load_master_parquet(ARQ_MASTER)
if master is None or master.empty:
    origem = "banco" if use_database_tables() else "arquivo local"
    st.error(f"Não consegui carregar a base principal ({origem}). Verifique `{ARQ_MASTER}` ou a tabela `dash_base_vendas`.")
    st.stop()
if COL_DATA not in master.columns:
    st.error(f"Base principal não tem coluna `{COL_DATA}`.")
    st.stop()

master = parse_date(master, COL_DATA).dropna(subset=[COL_DATA]).copy()

receita_col = get_receita_col(master)
if not receita_col:
    st.error("Não encontrei coluna de receita no parquet (ex: `receita`, `Total`).")
    st.stop()

qtde_col = get_qtde_col(master)
has_pedido = COL_PEDIDO in master.columns
canal_col = get_canal_col(master)
brand_col = get_brand_col(master)
desc_col = get_desc_col(master)
sku_col_master = get_sku_col(master)

master = add_fulfillment_helper_col(master)

daily = pd.DataFrame()
if file_exists(ARQ_DAILY):
    daily = load_csv(ARQ_DAILY)
    daily = parse_date(daily, COL_DATA).dropna(subset=[COL_DATA]).sort_values(COL_DATA)
    daily = ensure_numeric(daily, ["receita", "pedidos"])

canal_legacy = load_csv(ARQ_CANAL) if file_exists(ARQ_CANAL) else pd.DataFrame()
abc_legacy = load_csv(ARQ_ABC) if file_exists(ARQ_ABC) else pd.DataFrame()
alertas = load_csv(ARQ_ALERTAS) if file_exists(ARQ_ALERTAS) else pd.DataFrame()
alertas_operacionais = load_csv(ARQ_ALERTAS_OPERACIONAIS) if file_exists(ARQ_ALERTAS_OPERACIONAIS) else pd.DataFrame()
alertas_operacionais = ensure_alertas_operacionais(alertas_operacionais)
repl_geral = load_csv(ARQ_REPOSICAO_GERAL) if file_exists(ARQ_REPOSICAO_GERAL) else pd.DataFrame()
repl_geral_accel = load_csv(ARQ_REPOSICAO_GERAL_ACCEL) if file_exists(ARQ_REPOSICAO_GERAL_ACCEL) else pd.DataFrame()
repl_decisao = load_csv(ARQ_REPOSICAO_DECISAO) if file_exists(ARQ_REPOSICAO_DECISAO) else pd.DataFrame()
alertas_tracking = load_csv(ARQ_ALERTAS_TRACKING) if file_exists(ARQ_ALERTAS_TRACKING) else pd.DataFrame()

resumo = try_read_json(ARQ_RESUMO_JSON) or {}
full_reposicao_home = ensure_full_columns(load_csv(ARQ_FULL_REPOSICAO)) if file_exists(ARQ_FULL_REPOSICAO) else pd.DataFrame()
full_candidatos_home = ensure_full_columns(load_csv(ARQ_FULL_CANDIDATOS)) if file_exists(ARQ_FULL_CANDIDATOS) else pd.DataFrame()

estado_dash_ops = load_dash_ops_estado()
repl_geral = merge_dash_ops_estado(repl_geral, estado_dash_ops)
if not repl_geral_accel.empty:
    repl_geral_accel = merge_dash_ops_estado(repl_geral_accel, estado_dash_ops)
if not repl_decisao.empty:
    repl_decisao = merge_dash_ops_estado(repl_decisao, estado_dash_ops)
if "EAN" in repl_geral.columns:
    repl_geral["status_dash"] = repl_geral.apply(build_status_dash, axis=1)
if not repl_geral_accel.empty and "EAN" in repl_geral_accel.columns:
    repl_geral_accel["status_dash"] = repl_geral_accel.apply(build_status_dash, axis=1)
if not repl_decisao.empty and "EAN" in repl_decisao.columns:
    repl_decisao["status_dash"] = repl_decisao.apply(build_status_dash, axis=1)

min_master = master[COL_DATA].min()
max_master = master[COL_DATA].max()




# =========================
# TRACKING / PERFORMANCE OPERACIONAL
# =========================
def ensure_alertas_tracking(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["EAN", "tipo_alerta", "status", "owner", "data_inicio", "data_conclusao"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    out["status"] = out["status"].astype(str).replace({"": "ABERTO"}).fillna("ABERTO")
    out["owner"] = out["owner"].fillna("").astype(str)
    out["tipo_alerta"] = out["tipo_alerta"].fillna("").astype(str)
    out["EAN"] = out["EAN"].fillna("").astype(str)
    out["data_inicio"] = pd.to_datetime(out["data_inicio"], errors="coerce")
    out["data_conclusao"] = pd.to_datetime(out["data_conclusao"], errors="coerce")
    return out[cols].copy()


def build_tracking_perf(df: pd.DataFrame) -> dict:
    df = ensure_alertas_tracking(df)
    if df.empty:
        return {
            "df": df,
            "abertos": 0,
            "concluidos": 0,
            "sla_medio_h": 0.0,
            "vencidos": 0,
            "por_owner": pd.DataFrame(columns=["owner", "resolvidos", "tempo_medio_h"]),
        }
    now = pd.Timestamp.now()
    work = df.copy()
    work["tempo_aberto_h"] = ((work["data_conclusao"].fillna(now) - work["data_inicio"]).dt.total_seconds() / 3600.0).fillna(0.0)
    work["tempo_resolucao_h"] = ((work["data_conclusao"] - work["data_inicio"]).dt.total_seconds() / 3600.0)
    abertos = int(work["status"].astype(str).str.upper().eq("ABERTO").sum())
    concluidos = int(work["status"].astype(str).str.upper().eq("CONCLUIDO").sum())
    sla_vals = pd.to_numeric(work.loc[work["status"].astype(str).str.upper().eq("CONCLUIDO"), "tempo_resolucao_h"], errors="coerce")
    sla_medio_h = float(round(sla_vals.mean(), 1)) if sla_vals.notna().any() else 0.0
    vencidos = int(((work["status"].astype(str).str.upper().eq("ABERTO")) & (work["tempo_aberto_h"] > 24)).sum())
    por_owner = (
        work[work["status"].astype(str).str.upper().eq("CONCLUIDO")]
        .assign(owner=work["owner"].replace({"": "Sem owner"}))
        .groupby("owner", as_index=False)
        .agg(resolvidos=("EAN", "count"), tempo_medio_h=("tempo_resolucao_h", "mean"))
        .sort_values(["resolvidos", "tempo_medio_h"], ascending=[False, True])
    ) if concluidos > 0 else pd.DataFrame(columns=["owner", "resolvidos", "tempo_medio_h"])
    return {"df": work, "abertos": abertos, "concluidos": concluidos, "sla_medio_h": sla_medio_h, "vencidos": vencidos, "por_owner": por_owner}

# =========================
# BLOCO 1 — TABS PRINCIPAIS
# =========================
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📈 Painel Executivo",
    "🧠 Resumo Executivo (IA)",
    "🧾 Relatório Técnico",
    "📦 ML FULL Operacional",
    "🏬 Reposição Geral",
    "🧠 Copiloto de Vendas",
    "🚨 Centro de Alertas"
])


# =========================
# TAB 1: Painel Executivo
# =========================
with tab1:
    render_section_header(
        "📈 Painel Executivo de Vendas",
        "Leitura gerencial do negócio: status, tendência, drivers, alertas e ação"
    )

    st.markdown('<hr class="divider-modern">', unsafe_allow_html=True)

    # =========================
    # BLOCO 6 — FILTROS EXECUTIVOS
    # =========================
    render_section_header(
        "Período & Filtros",
        "Base histórica do parquet com refinamento por canal, marca, fulfillment e busca"
    )
    open_modern_card(
        "Filtros executivos",
        "Defina o recorte que será contado nos indicadores e gráficos"
    )

    cF1, cF2, cF3 = st.columns([2, 1, 2])

    with cF1:
        start, end = st.date_input(
            "Selecione o período",
            value=(min_master.date(), max_master.date()),
            min_value=min_master.date(),
            max_value=max_master.date(),
            key="dash_periodo"
        )
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

    with cF2:
        ano_min = st.number_input(
            "Ano mínimo (visualização)",
            min_value=2000,
            max_value=datetime.now().year + 1,
            value=max(2024, int(min_master.year)),
            step=1,
            key="dash_ano_min"
        )

    with cF3:
        modo = st.radio("Modo", ["Mensal", "Diário"], horizontal=True, index=0, key="dash_modo")
        if modo == "Diário":
            gran = st.selectbox("Granularidade (eixo X)", ["Dia", "Semana", "Mês"], index=2, key="dash_gran_d")
        else:
            gran = st.selectbox("Granularidade (eixo X)", ["Mês", "Trimestre"], index=0, key="dash_gran_m")

    start_vis = max(start, pd.Timestamp(year=int(ano_min), month=1, day=1))
    end_vis = end

    cF4, cF5, cF6, cF7 = st.columns([2, 2, 1.5, 2])

    with cF4:
        if canal_col:
            canais_opts = sorted(master[canal_col].dropna().astype(str).unique().tolist())
            canais_sel = st.multiselect("Marketplace (Canal)", canais_opts, default=canais_opts, key="dash_canais")
        else:
            canais_sel = []
            st.info("Coluna de canal/marketplace não encontrada no parquet.")

    with cF5:
        if brand_col:
            marcas_opts = sorted(master[brand_col].dropna().astype(str).unique().tolist())
            marcas_sel = st.multiselect("Marca", marcas_opts, default=marcas_opts, key="dash_marcas")
        else:
            marcas_sel = []
            st.info("Coluna de marca não encontrada no parquet.")

    with cF6:
        fulfillment_opts_dash = ["Todos", "Somente Fulfillment", "Somente Não Fulfillment"]
        fulfillment_sel_dash = st.selectbox("Fulfillment", fulfillment_opts_dash, index=0, key="dash_fulfillment")

    with cF7:
        sku_busca_dash = st.text_input("Buscar SKU / EAN / descrição", value="", key="dash_sku_busca").strip()

    close_card()

    with st.expander("🧠 IA — gerar resumo com os filtros atuais", expanded=False):
        filtros_ia = {
            "periodo_inicio": str(start_vis.date()),
            "periodo_fim": str(end_vis.date()),
            "canais": canais_sel if canal_col else [],
            "marcas": marcas_sel if brand_col else [],
            "fulfillment": fulfillment_sel_dash,
            "busca": sku_busca_dash,
        }

        cIA1, cIA2, cIA3, cIA4 = st.columns([1, 1, 1, 2])

        with cIA1:
            if st.button("🧠 Exportar filtros", key="btn_export_filters"):
                Path("ia_filtros.json").write_text(
                    json.dumps(filtros_ia, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )
                st.success("Gerado ia_filtros.json")

        with cIA2:
            gerar_agora = st.button("⚡ Gerar Resumo IA agora", type="primary", key="btn_run_ia")

        with cIA3:
            validar_ia = st.checkbox("Verificar números", value=True, key="chk_validar_ia")

        with cIA4:
            st.caption("Roda `ia_resumo_local.py` no seu PC. Pode demorar dependendo do modelo/RAM.")

        if gerar_agora:
            Path("ia_filtros.json").write_text(
                json.dumps(filtros_ia, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )

            with st.spinner("Executando IA..."):
                rc1, out1 = run_cmd([sys.executable, "ia_resumo_local.py"])
                rc2, out2 = (0, "")
                if validar_ia:
                    rc2, out2 = run_cmd([sys.executable, "verifica_resumo_ia.py"])

            st.session_state["ia_last_run"] = datetime.now().strftime("%d/%m/%Y %H:%M")
            st.session_state["ia_last_log"] = (out1 or "") + ("\n\n" + out2 if validar_ia else "")
            st.session_state["ia_last_ok"] = (rc1 == 0 and (rc2 == 0 if validar_ia else True))

        if st.session_state["ia_last_run"]:
            if st.session_state["ia_last_ok"]:
                st.success(f"Última execução OK: {st.session_state['ia_last_run']}")
            else:
                st.error(f"Última execução com falha: {st.session_state['ia_last_run']}")
            with st.expander("Ver log da última execução", expanded=False):
                st.code(st.session_state["ia_last_log"][-6000:] if st.session_state["ia_last_log"] else "(sem log)")

    mp = master[(master[COL_DATA] >= start_vis) & (master[COL_DATA] <= end_vis)].copy()

    if canal_col and len(canais_sel) > 0:
        mp = safe_multiselect_filter(mp, canal_col, canais_sel)

    if brand_col and len(marcas_sel) > 0:
        mp = safe_multiselect_filter(mp, brand_col, marcas_sel)

    fulfillment_warn = False
    if "_fulfillment_flag" in mp.columns and pd.Series(mp["_fulfillment_flag"]).notna().any():
        flag_mp = pd.Series(mp["_fulfillment_flag"]).map(lambda x: True if x is True else False if x is False else np.nan)
        if fulfillment_sel_dash == "Somente Fulfillment":
            mp = mp[flag_mp.fillna(False)].copy()
        elif fulfillment_sel_dash == "Somente Não Fulfillment":
            mp = mp[~flag_mp.fillna(False)].copy()
    else:
        if fulfillment_sel_dash != "Todos":
            fulfillment_warn = True

    if sku_busca_dash:
        cols_busca_dash = []
        if sku_col_master and sku_col_master in mp.columns:
            cols_busca_dash.append(sku_col_master)
        if COL_EAN in mp.columns:
            cols_busca_dash.append(COL_EAN)
        if desc_col and desc_col in mp.columns:
            cols_busca_dash.append(desc_col)
        mp = apply_multi_text_search(mp, sku_busca_dash, cols_busca_dash)

    receita_periodo = float(pd.to_numeric(mp[receita_col], errors="coerce").fillna(0).sum()) if len(mp) else 0.0
    pedidos_periodo = mp[COL_PEDIDO].astype(str).nunique() if (len(mp) and has_pedido) else len(mp)
    itens_periodo = float(pd.to_numeric(mp[qtde_col], errors="coerce").fillna(0).sum()) if qtde_col else float(len(mp))
    ticket_medio_periodo = (receita_periodo / pedidos_periodo) if pedidos_periodo else 0.0

    # =========================
    # BLOCO 7 — KPIs PRINCIPAIS
    # =========================
    pk1, pk2, pk3, pk4 = st.columns(4)
    with pk1:
        render_metric_card("Receita (período)", money_br(receita_periodo), "Quanto o negócio faturou", "#2563eb")
    with pk2:
        render_metric_card("Pedidos (únicos)", metric_int(pedidos_periodo), "Volume real de compras", "#7c3aed")
    with pk3:
        render_metric_card("Ticket médio", money_br(ticket_medio_periodo), "Valor médio por pedido", "#0f766e")
    with pk4:
        render_metric_card("Itens vendidos", metric_int(int(itens_periodo)), "Giro do período", "#16a34a")

    # =========================
    # BLOCO 8 — COMPARAÇÃO VS PERÍODO ANTERIOR
    # =========================
    dias_periodo = max((end_vis - start_vis).days + 1, 1)
    prev_end = start_vis - pd.Timedelta(days=1)
    prev_start = prev_end - pd.Timedelta(days=dias_periodo - 1)

    mp_prev = master[(master[COL_DATA] >= prev_start) & (master[COL_DATA] <= prev_end)].copy()

    if canal_col and len(canais_sel) > 0:
        mp_prev = safe_multiselect_filter(mp_prev, canal_col, canais_sel)
    if brand_col and len(marcas_sel) > 0:
        mp_prev = safe_multiselect_filter(mp_prev, brand_col, marcas_sel)
    if "_fulfillment_flag" in mp_prev.columns and pd.Series(mp_prev["_fulfillment_flag"]).notna().any():
        flag_prev = pd.Series(mp_prev["_fulfillment_flag"]).map(lambda x: True if x is True else False if x is False else np.nan)
        if fulfillment_sel_dash == "Somente Fulfillment":
            mp_prev = mp_prev[flag_prev.fillna(False)].copy()
        elif fulfillment_sel_dash == "Somente Não Fulfillment":
            mp_prev = mp_prev[~flag_prev.fillna(False)].copy()
    if sku_busca_dash:
        cols_busca_prev = []
        if sku_col_master and sku_col_master in mp_prev.columns:
            cols_busca_prev.append(sku_col_master)
        if COL_EAN in mp_prev.columns:
            cols_busca_prev.append(COL_EAN)
        if desc_col and desc_col in mp_prev.columns:
            cols_busca_prev.append(desc_col)
        mp_prev = apply_multi_text_search(mp_prev, sku_busca_dash, cols_busca_prev)

    receita_prev = float(pd.to_numeric(mp_prev[receita_col], errors="coerce").fillna(0).sum()) if len(mp_prev) else 0.0
    pedidos_prev = mp_prev[COL_PEDIDO].astype(str).nunique() if (len(mp_prev) and has_pedido) else len(mp_prev)
    ticket_prev = (receita_prev / pedidos_prev) if pedidos_prev else 0.0

    var_receita = ((receita_periodo / receita_prev) - 1) * 100 if receita_prev else np.nan
    var_pedidos = ((pedidos_periodo / pedidos_prev) - 1) * 100 if pedidos_prev else np.nan
    var_ticket = ((ticket_medio_periodo / ticket_prev) - 1) * 100 if ticket_prev else np.nan

    vc1, vc2, vc3 = st.columns(3)
    with vc1:
        render_metric_card("Receita vs período anterior", pct_br(var_receita) if pd.notna(var_receita) else "N/D", f"Comparação com {prev_start.date()} → {prev_end.date()}", "#1d4ed8")
    with vc2:
        render_metric_card("Pedidos vs período anterior", pct_br(var_pedidos) if pd.notna(var_pedidos) else "N/D", "Leitura de volume", "#7c3aed")
    with vc3:
        render_metric_card("Ticket vs período anterior", pct_br(var_ticket) if pd.notna(var_ticket) else "N/D", "Leitura de qualidade da venda", "#0f766e")

    # =========================
    # BLOCO 8.1 — HEALTH SCORE + PREVISÃO
    # =========================
    margin_col_master = get_margin_col(master)
    health = compute_health_score(
        var_receita=var_receita,
        var_pedidos=var_pedidos,
        var_ticket=var_ticket,
        master_df=mp,
        repl_df=repl_geral,
        alertas_df=alertas,
        receita_col=receita_col,
        margin_col=margin_col_master,
    )
    forecast_month = compute_month_forecast(
        master_df=master,
        end_vis=end_vis,
        receita_col=receita_col,
        canal_col=canal_col,
        canais_sel=canais_sel,
        brand_col=brand_col,
        marcas_sel=marcas_sel,
        fulfillment_sel_dash=fulfillment_sel_dash,
        sku_busca_dash=sku_busca_dash,
        sku_col_master=sku_col_master,
        desc_col=desc_col,
    )

    hs1, hs2 = st.columns([1.1, 1.9])
    with hs1:
        open_modern_card("Health Score", "Índice sintético do negócio para leitura de diretoria")
        st.markdown(f"<div class='soft-chip' style='background:{health['color']}15;color:{health['color']};'>Status: {health['status']}</div>", unsafe_allow_html=True)
        st.markdown(
            f"<div style='font-size:44px;font-weight:800;color:{health['color']};line-height:1;'>{health['health_score']:.0f}/100</div>",
            unsafe_allow_html=True
        )
        st.caption(f"Baseado em crescimento, volume, {health['margin_label'].lower()}, estoque e alertas.")
        st.dataframe(health["components_df"], use_container_width=True, hide_index=True)
        close_card()

    with hs2:
        open_modern_card("Previsão de receita do mês", "Planejamento com base no run-rate do mês corrente")
        f1, f2, f3 = st.columns(3)
        with f1:
            render_metric_card("Receita acumulada no mês", money_br(forecast_month["receita_mtd"]), f"{forecast_month['days_elapsed']} dias corridos", "#2563eb")
        with f2:
            render_metric_card("Projeção de fechamento", money_br(forecast_month["projected_month"]), f"Média diária: {money_br(forecast_month['avg_daily'])}", "#7c3aed")
        with f3:
            delta_proj_txt = pct_br(forecast_month["delta_projection"]) if pd.notna(forecast_month["delta_projection"]) else "N/D"
            render_metric_card("Vs mês anterior", delta_proj_txt, f"Comparação com {forecast_month['month_start'].strftime('%m/%Y')}", "#0f766e")
        st.caption(
            f"Projeção simples: receita acumulada de {forecast_month['month_start'].date()} até {end_vis.date()} dividida pelos dias corridos e anualizada para {forecast_month['days_in_month']} dias."
        )
        close_card()

    st.caption(
        f"Período em tela: {start_vis.date()} → {end_vis.date()} | Ano mínimo aplicado: {ano_min} "
        f"| Filtros: {('Canal' if canal_col else 'Canal N/A')}, {('Marca' if brand_col else 'Marca N/A')}, Fulfillment, Busca SKU/EAN"
    )

    if fulfillment_warn:
        st.warning(
            "O filtro de Fulfillment da página inicial não foi aplicado porque o parquet base não possui essa informação de forma confiável."
        )

    st.markdown('<hr class="divider-modern">', unsafe_allow_html=True)

    if len(mp) == 0:
        st.info("Sem dados no período/filtros selecionados (parquet).")
    else:
        if modo == "Diário":
            freq = {"Dia": "D", "Semana": "W", "Mês": "M"}[gran]
        else:
            freq = {"Mês": "M", "Trimestre": "Q"}[gran]

        pedidos_series = (COL_PEDIDO, "nunique") if has_pedido else (receita_col, "count")

        ts_base = (
            mp.groupby(COL_DATA, as_index=False)
              .agg(receita=(receita_col, "sum"), pedidos=pedidos_series)
              .sort_values(COL_DATA)
        )
        ts_base = ts_base.set_index(COL_DATA).asfreq("D").fillna(0).reset_index()
        ts = agg_timeseries(ts_base, COL_DATA, freq=freq)
        ts = ts.rename(columns={COL_DATA: "X"}) if COL_DATA in ts.columns else ts
        if "X" not in ts.columns:
            ts = ts.rename(columns={ts.columns[0]: "X"})
        ts["receita_mm"] = ts["receita"].rolling(3, min_periods=1).mean()
        ts["pedidos_mm"] = ts["pedidos"].rolling(3, min_periods=1).mean()

        g1, g2 = st.columns([1.8, 1.2])

        with g1:
            open_chart_card("Receita no tempo", "O gráfico principal da história de vendas")
            receita_chart = alt.Chart(ts).transform_fold(
                ["receita", "receita_mm"],
                as_=["serie", "valor"]
            ).mark_line(point=True).encode(
                x=alt.X("X:T", title="Tempo"),
                y=alt.Y("valor:Q", title="Receita"),
                color=alt.Color("serie:N", title="Série"),
                tooltip=["X:T", "serie:N", alt.Tooltip("valor:Q", format=",.2f")]
            ).interactive()
            st.altair_chart(receita_chart, use_container_width=True)
            close_card()

        with g2:
            open_chart_card("Pedidos no tempo", "Volume de pedidos no mesmo recorte")
            pedidos_chart = alt.Chart(ts).transform_fold(
                ["pedidos", "pedidos_mm"],
                as_=["serie", "valor"]
            ).mark_line(point=True).encode(
                x=alt.X("X:T", title="Tempo"),
                y=alt.Y("valor:Q", title="Pedidos"),
                color=alt.Color("serie:N", title="Série"),
                tooltip=["X:T", "serie:N", alt.Tooltip("valor:Q", format=",.0f")]
            ).interactive()
            st.altair_chart(pedidos_chart, use_container_width=True)
            close_card()

        # =========================
        # BLOCO 9 — DRIVERS
        # =========================
        d1, d2 = st.columns(2)

        with d1:
            open_chart_card(f"Top {topN} canais", "Quem está puxando a receita no recorte atual")
            if canal_col:
                top_canais = (
                    mp.groupby(canal_col, as_index=False)
                      .agg(receita=(receita_col, "sum"), pedidos=((COL_PEDIDO, "nunique") if has_pedido else (receita_col, "count")))
                      .sort_values("receita", ascending=False)
                      .head(topN)
                )
                chart_canais = alt.Chart(top_canais).mark_bar().encode(
                    x=alt.X("receita:Q", title="Receita"),
                    y=alt.Y(f"{canal_col}:N", sort="-x", title="Canal"),
                    tooltip=[alt.Tooltip(f"{canal_col}:N", title="Canal"), alt.Tooltip("receita:Q", format=",.2f"), alt.Tooltip("pedidos:Q", format=",.0f")]
                )
                st.altair_chart(chart_canais, use_container_width=True)
                st.dataframe(top_canais.head(min(topN, 10)), use_container_width=True)
            else:
                st.info("Coluna de canal não encontrada no parquet.")
            close_card()

        with d2:
            open_chart_card(f"Top {topN} SKUs", "Produtos que mais puxam a venda no recorte atual")
            sku_dim = None
            for candidate in [sku_col_master, desc_col, COL_EAN]:
                if candidate and candidate in mp.columns:
                    sku_dim = candidate
                    break
            if sku_dim:
                top_skus = (
                    mp.groupby(sku_dim, as_index=False)
                      .agg(receita=(receita_col, "sum"), itens=((qtde_col, "sum") if qtde_col else (receita_col, "count")))
                      .sort_values("receita", ascending=False)
                      .head(topN)
                )
                chart_skus = alt.Chart(top_skus).mark_bar().encode(
                    x=alt.X("receita:Q", title="Receita"),
                    y=alt.Y(f"{sku_dim}:N", sort="-x", title="SKU / Produto"),
                    tooltip=[alt.Tooltip(f"{sku_dim}:N", title="Item"), alt.Tooltip("receita:Q", format=",.2f"), alt.Tooltip("itens:Q", format=",.0f")]
                )
                st.altair_chart(chart_skus, use_container_width=True)
            else:
                st.info("Nenhuma coluna de SKU / descrição / EAN encontrada para o ranking.")
            close_card()

        b1, b2 = st.columns(2)
        with b1:
            open_modern_card("Curva ABC resumida", "Visão simplificada para gestão")
            if COL_EAN in mp.columns:
                abc_dyn = calc_abc(mp, prod_col=COL_EAN, receita_col=receita_col, qtde_col=qtde_col, pedido_col=COL_PEDIDO)
                if len(abc_dyn):
                    resumo_abc = abc_dyn["classe_abc"].value_counts(dropna=False).rename_axis("classe").reset_index(name="qtd")
                    abc_chart = alt.Chart(resumo_abc).mark_bar().encode(
                        x=alt.X("classe:N", title="Classe"),
                        y=alt.Y("qtd:Q", title="Quantidade de itens"),
                        tooltip=["classe:N", "qtd:Q"]
                    )
                    st.altair_chart(abc_chart, use_container_width=True)
                    cols_show = [c for c in [COL_EAN, "receita", "itens", "pedidos", "classe_abc"] if c in abc_dyn.columns]
                    st.dataframe(abc_dyn[cols_show].head(10), use_container_width=True)
                else:
                    st.info("Curva ABC indisponível.")
            else:
                st.info("Curva ABC indisponível: coluna EAN não encontrada.")
            close_card()

        with b2:
            open_modern_card("Alertas", "Sinais operacionais extraídos do pipeline")
            if len(alertas) == 0:
                st.info("Nenhum alerta disponível.")
            else:
                st.dataframe(alertas.head(15), use_container_width=True)
            close_card()

        # =========================
        # BLOCO 9.1 — RADAR DE RISCO AUTOMÁTICO
        # =========================
        radar = build_risk_radar(
            mp=mp,
            mp_prev=mp_prev,
            receita_col=receita_col,
            canal_col=canal_col,
            sku_dim=sku_dim,
            repl_df=repl_geral,
        )

        rr1, rr2 = st.columns([1.0, 2.0])
        with rr1:
            open_modern_card("Radar de risco", "Onde a diretoria precisa olhar primeiro")
            radar_chart = alt.Chart(radar["counts"]).mark_bar().encode(
                x=alt.X("qtd:Q", title="Quantidade"),
                y=alt.Y("frente:N", sort="-x", title="Frente"),
                tooltip=["frente:N", "qtd:Q"]
            )
            st.altair_chart(radar_chart, use_container_width=True)
            close_card()

        with rr2:
            open_modern_card("Sinais críticos detectados", "Leitura automática por canal, SKU e estoque")
            rcol1, rcol2, rcol3 = st.columns(3)
            with rcol1:
                st.markdown("**Canais desacelerando**")
                if len(radar["canais_risco"]):
                    cols = [c for c in [canal_col, "receita_prev", "receita_atual", "var_pct"] if c and c in radar["canais_risco"].columns]
                    st.dataframe(radar["canais_risco"][cols].head(5), use_container_width=True, hide_index=True)
                else:
                    st.success("Sem desaceleração relevante")
            with rcol2:
                st.markdown("**SKUs perdendo venda**")
                if len(radar["skus_risco"]):
                    cols = [c for c in [sku_dim, "receita_prev", "receita_atual", "var_pct"] if c and c in radar["skus_risco"].columns]
                    st.dataframe(radar["skus_risco"][cols].head(5), use_container_width=True, hide_index=True)
                else:
                    st.success("Sem perda estrutural relevante")
            with rcol3:
                st.markdown("**Ruptura próxima**")
                if len(radar["ruptura"]):
                    cols = [c for c in ["EAN", "Descricao", "Marca", "status_cobertura_90d", "reposicao_sugerida_90d", "cobertura_90d"] if c in radar["ruptura"].columns]
                    st.dataframe(radar["ruptura"][cols].head(5), use_container_width=True, hide_index=True)
                else:
                    st.success("Sem ruptura crítica")
            close_card()

        # =========================
        # BLOCO 9.2 — COPILOTO DE VENDAS (RESUMO)
        # =========================
        base_copiloto_home = repl_decisao.copy() if repl_decisao is not None and not repl_decisao.empty else repl_geral.copy()
        copiloto = build_copiloto_views(full_candidatos_home, base_copiloto_home, repl_geral_accel)
        cp1, cp2 = st.columns([1.0, 2.0])
        with cp1:
            open_modern_card("Copiloto de Vendas", "Recomendações automáticas de execução")
            cop_chart = alt.Chart(copiloto["summary"]).mark_bar().encode(
                x=alt.X("qtd:Q", title="Quantidade"),
                y=alt.Y("oportunidade:N", sort="-x", title="Ação"),
                tooltip=["oportunidade:N", "qtd:Q"]
            )
            st.altair_chart(cop_chart, use_container_width=True)
            close_card()
        with cp2:
            open_modern_card("Onde agir primeiro", "Resumo gerencial do Copiloto")
            render_copiloto_validadores(base_copiloto_home)
            st.caption("Home executiva mostra o recorte de execução imediata. A contagem acima representa o total detectado no CSV; os cards abaixo mostram o TOP 20 priorizado para ação.")
            cpa, cpb, cpc, cpd = st.columns(4)
            with cpa:
                render_metric_card("Agir hoje (Top 20)", metric_int(len(copiloto["agir_hoje"])), "Fila executiva imediata", "#dc2626")
            with cpb:
                render_metric_card("Comprar (Top 20)", metric_int(len(copiloto["comprar"])), "SKUs com compra sugerida", "#16a34a")
            with cpc:
                render_metric_card("Entrar em FULL (Top 20)", metric_int(len(copiloto["entrar_full"])), "Candidatos priorizados", "#2563eb")
            with cpd:
                render_metric_card("Potencial de escala (Top 20)", metric_int(len(copiloto["acelerar"])), "Itens acelerando", "#f59e0b")
            if alertas_operacionais is not None and not alertas_operacionais.empty:
                totais_alertas = int(len(alertas_operacionais))
                altas_alertas = int(alertas_operacionais["severidade"].astype(str).str.upper().eq("ALTA").sum())
                st.warning(f"🚨 Centro de Alertas ativo: {metric_int(totais_alertas)} alertas operacionais gerados, sendo {metric_int(altas_alertas)} de severidade ALTA.")
            st.info(f"Visão executiva: mostrando até 20 itens por frente. Total detectado em AGIR HOJE no pipeline: {metric_int(int(base_copiloto_home['bucket_prioridade'].astype(str).str.upper().str.contains('AGIR_HOJE', na=False).sum()) if base_copiloto_home is not None and not base_copiloto_home.empty and 'bucket_prioridade' in base_copiloto_home.columns else 0)}.")
            st.caption("Os gestores enxergam a história na home; a operação detalha e executa na aba 'Copiloto de Vendas'.")
            close_card()

        # =========================
        # BLOCO 10 — O QUE FAZER AGORA
        # =========================
        open_modern_card("O que fazer agora", "Leitura rápida para um gestor agir sem entrar em tabelas técnicas")
        acoes = []
        if pd.notna(var_receita) and var_receita < -10:
            acoes.append("🔴 Receita em queda relevante vs período anterior: revisar canais, sortimento e pricing.")
        if pd.notna(var_pedidos) and var_pedidos < -10:
            acoes.append("🟠 Queda de pedidos: investigar tráfego, conversão e ruptura.")
        if pd.notna(var_ticket) and var_ticket < -5:
            acoes.append("🟡 Ticket médio caiu: avaliar desconto excessivo ou mix mais barato.")
        if canal_col and len(mp):
            canal_lider = mp.groupby(canal_col)[receita_col].sum().sort_values(ascending=False)
            if len(canal_lider):
                acoes.append(f"📈 Canal líder no recorte atual: {canal_lider.index[0]}.")
        if len(alertas_operacionais) > 0:
            altas = int(alertas_operacionais["severidade"].astype(str).str.upper().eq("ALTA").sum()) if "severidade" in alertas_operacionais.columns else 0
            acoes.append(f"⚠️ Existem {len(alertas_operacionais)} alertas operacionais no pipeline, sendo {altas} de severidade alta.")
        elif len(alertas) > 0:
            acoes.append(f"⚠️ Existem {len(alertas)} alertas analíticos no pipeline que merecem revisão.")
        if len(repl_geral) > 0 and 'status_cobertura_90d' in repl_geral.columns:
            criticos = int((repl_geral['status_cobertura_90d'].astype(str).str.upper() == 'CRÍTICO').sum())
            if criticos > 0:
                acoes.append(f"📦 Há {criticos} itens críticos de cobertura na reposição geral.")
        if health['health_score'] < 60:
            acoes.append("🧭 Health Score abaixo de 60: tratar risco de crescimento, estoque e alertas antes de escalar mídia.")
        if pd.notna(forecast_month['delta_projection']) and forecast_month['delta_projection'] < 0:
            acoes.append("🔮 A projeção do mês está abaixo do mês anterior: revisar plano comercial para recuperar fechamento.")
        if len(copiloto['entrar_full']) > 0:
            acoes.append(f"🚚 Existem {len(copiloto['entrar_full'])} candidatos para FULL que podem melhorar nível de serviço.")
        if len(copiloto['anuncio']) > 0:
            acoes.append(f"📣 Existem {len(copiloto['anuncio'])} itens com estoque saudável e baixa tração: priorizar campanha e conteúdo.")
        if len(acoes) == 0:
            st.success("✅ Cenário sem alertas graves no recorte atual. Próximo passo: explorar drivers e oportunidades.")
        else:
            for acao in acoes[:5]:
                st.markdown(f"- {acao}")
        close_card()


# =========================
# TAB 2: Resumo Executivo IA
# =========================
with tab2:
    render_section_header("🧠 Resumo Executivo (IA)", "Leitura automatizada do resultado consolidado")

    resumo_ia_status = try_read_json(ARQ_RESUMO_IA_STATUS) or {}
    decisao_ia_status = try_read_json(ARQ_DECISAO_IA_STATUS) or {}
    decisao_ia_json = try_read_json(ARQ_DECISAO_IA_JSON) or {}

    c_status_ia_1, c_status_ia_2, c_status_ia_3, c_status_ia_4 = st.columns(4)
    with c_status_ia_1:
        st.metric("Status resumo IA", str(resumo_ia_status.get("status", "N/D")))
    with c_status_ia_2:
        st.metric("Status decisão IA", str(decisao_ia_status.get("status", "N/D")))
    with c_status_ia_3:
        st.metric("Último sucesso IA", str((resumo_ia_status.get("updated_at") or resumo_ia_status.get("ultima_atualizacao") or decisao_ia_status.get("updated_at") or "N/D"))[:19])
    with c_status_ia_4:
        st.metric("Modelo IA", str(decisao_ia_status.get("model") or resumo_ia_status.get("model") or os.getenv("OLLAMA_MODEL", "N/D")))

    open_modern_card("Resumo Executivo", "Arquivo `resumo_executivo_ia.md`")
    texto_ia = read_text(ARQ_RESUMO_IA_MD)
    if not texto_ia.strip():
        st.warning(
            f"Não encontrei `{ARQ_RESUMO_IA_MD}` ou está vazio.\n\n"
            "Gere com:\n"
            "`python gerar_resumo_ia.py`"
        )
    else:
        st.markdown(texto_ia)
    close_card()

    open_modern_card("Decisão IA", "Ações recomendadas geradas a partir dos tops operacionais")
    if not decisao_ia_json:
        st.info(
            f"`{ARQ_DECISAO_IA_JSON}` não encontrado ou inválido.\n\n"
            "Gere com:\n"
            "`python gerar_decisao_ia.py`"
        )
    else:
        resumo_exec_decisao = decisao_ia_json.get("resumo_executivo", [])
        if resumo_exec_decisao:
            st.markdown("**Resumo decisório**")
            for linha in resumo_exec_decisao:
                st.markdown(f"- {linha}")

        ac1, ac2, ac3 = st.columns(3)
        with ac1:
            st.metric("Ações do dia", len(decisao_ia_json.get("acoes_do_dia", [])))
        with ac2:
            st.metric("Oportunidades FULL", len(decisao_ia_json.get("oportunidades_full", [])))
        with ac3:
            st.metric("Ações comerciais", len(decisao_ia_json.get("acoes_comerciais", [])))

        for bloco, titulo in [("acoes_do_dia", "Ações do dia"), ("oportunidades_full", "Oportunidades FULL"), ("acoes_comerciais", "Ações comerciais")]:
            itens = decisao_ia_json.get(bloco, [])
            if itens:
                st.markdown(f"**{titulo}**")
                st.dataframe(pd.DataFrame(itens), use_container_width=True, hide_index=True)

        texto_decisao_md = read_text(ARQ_DECISAO_IA_MD)
        if texto_decisao_md.strip():
            with st.expander("Ver versão markdown da decisão IA", expanded=False):
                st.markdown(texto_decisao_md)
    close_card()

    open_modern_card("Resumo JSON", "Status consolidado do pipeline")
    resumo = try_read_json(ARQ_RESUMO_JSON) or {}
    if not resumo:
        st.info("`resumo.json` não encontrado ou inválido.")
    else:
        st.json(resumo)
    close_card()


# =========================
# TAB 3: Relatório Técnico
# =========================
with tab3:
    render_section_header("🧾 Relatório Técnico", "Visão técnica dos artefatos e arquivos gerados")

    open_modern_card("Relatório Técnico", "Arquivo `relatorio.md`")
    texto_rel = read_text(ARQ_RELATORIO_MD)
    if not texto_rel.strip():
        st.warning(
            f"Não encontrei `{ARQ_RELATORIO_MD}` ou está vazio.\n\n"
            "Gere com:\n"
            "`python copiloto_vendas_v3.py`"
        )
    else:
        st.markdown(texto_rel)
    close_card()

    open_modern_card("Arquivos disponíveis", "Inventário rápido dos artefatos locais")
    files = [
        ARQ_MASTER,
        ARQ_DAILY,
        ARQ_CANAL,
        ARQ_ABC,
        ARQ_ALERTAS,
        ARQ_RESUMO_JSON,
        ARQ_RELATORIO_MD,
        ARQ_RESUMO_IA_MD,
        ARQ_FULL_REPOSICAO,
        ARQ_FULL_CANDIDATOS,
        ARQ_REPOSICAO_GERAL,
        ARQ_REPOSICAO_GERAL_ACCEL,
    ]
    rows = []
    for f in files:
        p = Path(f)
        rows.append({
            "arquivo": f,
            "existe": p.exists(),
            "tamanho_kb": round(p.stat().st_size / 1024, 1) if p.exists() else None,
            "modificado_em": datetime.fromtimestamp(p.stat().st_mtime).strftime("%d/%m/%Y %H:%M") if p.exists() else None
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True)
    close_card()


# =========================
# TAB 4: ML FULL Operacional
# =========================
with tab4:
    render_section_header(
        "📦 ML FULL Operacional (Postgres)",
        "Consulta da view sincronizada, qualidade da carga e leitura operacional do catálogo"
    )

    # =========================
    # BLOCO 13 — ESTADO INICIAL ML FULL
    # =========================
    df_ml = st.session_state.get("ml_full_df", pd.DataFrame()).copy()
    if "ml_full_loaded" not in st.session_state:
        st.session_state["ml_full_loaded"] = False
    if "ml_full_error" not in st.session_state:
        st.session_state["ml_full_error"] = None
    if "ml_full_source" not in st.session_state:
        st.session_state["ml_full_source"] = None
    if "ml_full_dbname" not in st.session_state:
        st.session_state["ml_full_dbname"] = None

    open_modern_card("Consulta da base FULL", "Parâmetros da leitura da view pública no Postgres")
    cML1, cML2, cML3, cML4 = st.columns([1.2, 1.2, 1, 1])

    with cML1:
        seller_default = int(os.getenv("SELLER_ID", "168873882") or "168873882")
        seller_id_dash = st.number_input("Seller ID (ML)", min_value=1, value=seller_default, step=1, key="ml_seller")

    with cML2:
        limit_ml = st.number_input("Limite de linhas (view)", min_value=100, max_value=50000, value=5000, step=100, key="ml_limit")

    with cML3:
        carregar_ml = st.button("📥 Carregar ML FULL", type="primary", key="ml_load", use_container_width=True)

    with cML4:
        source_info = discover_ml_full_source()
        st.markdown('<span class="soft-chip">Fonte</span>', unsafe_allow_html=True)
        if source_info["ok"]:
            st.caption(f"`{source_info['qualified']}`")
            st.caption(f"DB: `{source_info.get('dbname_used','-')}`")
        else:
            st.caption("Fonte ainda não localizada")
    close_card()

    if carregar_ml:
        with st.spinner("Consultando Postgres (ML FULL)..."):
            ml_res = load_ml_full_current(int(seller_id_dash), int(limit_ml))
        if not ml_res["ok"]:
            st.session_state["ml_full_df"] = pd.DataFrame()
            st.session_state["ml_full_loaded"] = False
            st.session_state["ml_full_error"] = ml_res["error"]
            st.session_state["ml_full_source"] = ml_res.get("source_used")
            st.session_state["ml_full_dbname"] = ml_res.get("dbname_used")
        else:
            df_ml_loaded = ml_res["data"]
            st.session_state["ml_full_source"] = ml_res.get("source_used")
            st.session_state["ml_full_dbname"] = ml_res.get("dbname_used")
            st.session_state["ml_full_df"] = df_ml_loaded.copy() if df_ml_loaded is not None and not df_ml_loaded.empty else pd.DataFrame()
            st.session_state["ml_full_loaded"] = True
            st.session_state["ml_full_error"] = None
            df_ml = st.session_state["ml_full_df"].copy()

    if st.session_state["ml_full_error"]:
        st.error(f"Erro ao ler ML FULL no Postgres:\n\n{st.session_state['ml_full_error']}")
    elif st.session_state["ml_full_loaded"]:
        df_ml = st.session_state["ml_full_df"].copy()

    if st.session_state.get("ml_full_source"):
        st.info(
            f"Fonte usada: `{st.session_state['ml_full_source']}`"
            + (f" | Banco: `{st.session_state['ml_full_dbname']}`" if st.session_state.get("ml_full_dbname") else "")
        )

    if st.session_state["ml_full_loaded"]:
        if df_ml is None or df_ml.empty:
            st.info("Nenhum registro retornado da view (verifique seller_id / snapshot_date de hoje).")
        else:
            df_ml = add_fulfillment_helper_col(df_ml)
            total_rows = len(df_ml)
            is_full = df_ml["is_fulfillment"].fillna(False) if "is_fulfillment" in df_ml.columns else pd.Series(dtype=bool)
            pct_full = float(is_full.mean()) if total_rows and len(is_full) else 0.0

            mk1, mk2, mk3, mk4 = st.columns(4)
            with mk1:
                render_metric_card("SKUs (hoje) — ML FULL", metric_int(total_rows), "Volume retornado da view", "#2563eb")
            with mk2:
                render_metric_card("% Fulfillment", f"{pct_full:.1%}", "Participação do FULL na amostra", "#7c3aed")
            with mk3:
                render_metric_card("Preço NULL", metric_int(int(df_ml["price"].isna().sum()) if "price" in df_ml.columns else 0), "Itens sem preço", "#f59e0b")
            with mk4:
                render_metric_card("Estoque NULL", metric_int(int(df_ml["available_quantity"].isna().sum()) if "available_quantity" in df_ml.columns else 0), "Itens sem estoque", "#ef4444")

            cols_crit = [c for c in ["status", "price", "available_quantity", "sold_quantity", "logistic_type", "health"] if c in df_ml.columns]
            df_ml["_completude"] = df_ml[cols_crit].notna().sum(axis=1) if cols_crit else 0
            linhas_com_info = int((df_ml["_completude"] > 0).sum())
            linhas_sem_info = int((df_ml["_completude"] == 0).sum())

            mk5, mk6, mk7 = st.columns(3)
            with mk5:
                render_metric_card("Linhas com alguma informação", metric_int(linhas_com_info), "Registros utilizáveis", "#16a34a")
            with mk6:
                render_metric_card("Linhas zeradas / nulas", metric_int(linhas_sem_info), "Registros sem completude", "#f97316")
            with mk7:
                render_metric_card("% linhas com alguma informação", f"{(linhas_com_info / len(df_ml)):.1%}" if len(df_ml) else "0,0%", "Qualidade geral da visão", "#0ea5e9")

            open_modern_card("Filtros da amostra ML FULL", "Visualização operacional da view sincronizada")
            fml1, fml2, fml3, fml4 = st.columns([1.2, 1.2, 1.2, 2])
            with fml1:
                filtro_info = st.selectbox("Filtro de completude", ["Todos", "Somente com informação", "Somente nulos"], index=0, key="ml_filtro_info")
            with fml2:
                filtro_fulfillment_ml = st.selectbox("Fulfillment", ["Todos", "Somente Fulfillment", "Somente Não Fulfillment"], index=0, key="ml_filtro_fulfillment")
            with fml3:
                top_ml = st.slider("Qtde linhas exibidas", 20, 500, 100, 20, key="ml_top_linhas")
            with fml4:
                busca_ml = st.text_input("Buscar item_id / status / logistic_type", value="", key="ml_busca").strip()

            view_ml = df_ml.copy()
            if filtro_info == "Somente com informação":
                view_ml = view_ml[view_ml["_completude"] > 0].copy()
            elif filtro_info == "Somente nulos":
                view_ml = view_ml[view_ml["_completude"] == 0].copy()
            if "_fulfillment_flag" in view_ml.columns and pd.Series(view_ml["_fulfillment_flag"]).notna().any():
                flag_ml = pd.Series(view_ml["_fulfillment_flag"]).map(lambda x: True if x is True else False if x is False else np.nan)
                if filtro_fulfillment_ml == "Somente Fulfillment":
                    view_ml = view_ml[flag_ml.fillna(False)].copy()
                elif filtro_fulfillment_ml == "Somente Não Fulfillment":
                    view_ml = view_ml[~flag_ml.fillna(False)].copy()
            if busca_ml:
                mask = pd.Series(False, index=view_ml.index)
                for c in [col for col in ["item_id", "status", "logistic_type"] if col in view_ml.columns]:
                    mask = mask | view_ml[c].astype(str).str.contains(busca_ml, case=False, na=False)
                view_ml = view_ml[mask].copy()
            sort_cols = [c for c in ["_completude", "item_id"] if c in view_ml.columns]
            if sort_cols:
                ascending = [False] + [True] * (len(sort_cols) - 1)
                view_ml = view_ml.sort_values(sort_cols, ascending=ascending)
            st.markdown("**Amostra filtrável da view ML FULL**")
            st.dataframe(view_ml.head(top_ml), use_container_width=True)
            with st.expander("Ver colunas e % nulos", expanded=False):
                nulls = (df_ml.isna().mean() * 100).sort_values(ascending=False).round(1)
                st.dataframe(nulls.reset_index().rename(columns={"index": "coluna", 0: "% nulos"}), use_container_width=True)
            close_card()

    st.markdown('<hr class="divider-modern">', unsafe_allow_html=True)
    render_section_header(
        "📦 Mercado Livre FULL — Reposição (35 dias)",
        "Visão operacional de envio ao FULL e candidatos com quantidade segura"
    )

    repl = ensure_full_columns(load_csv_safe(ARQ_FULL_REPOSICAO))
    cand = ensure_full_columns(load_csv_safe(ARQ_FULL_CANDIDATOS))
    audit_full = load_csv_safe(ARQ_FULL_AUDITORIA) if file_exists(ARQ_FULL_AUDITORIA) else pd.DataFrame()
    repl = normalize_full_numeric_display(repl)
    cand = normalize_full_numeric_display(cand)
    audit_full = normalize_full_numeric_display(audit_full)
    repl = add_fulfillment_helper_col(repl) if not repl.empty else repl
    cand = add_fulfillment_helper_col(cand) if not cand.empty else cand

    if repl.empty and cand.empty:
        st.warning(
            "Ainda não encontrei os arquivos de FULL.\n\n"
            f"- `{ARQ_FULL_REPOSICAO}`\n"
            f"- `{ARQ_FULL_CANDIDATOS}`\n"
            f"- `{ARQ_FULL_AUDITORIA}`\n\n"
            "Rode: `python copiloto_vendas_v3.py` (ou seu BAT de atualização)."
        )
    else:
        total_envio = 0
        if not repl.empty and "recomendacao_envio_full" in repl.columns:
            total_envio = int(pd.to_numeric(repl["recomendacao_envio_full"], errors="coerce").fillna(0).sum())

        fk1, fk2, fk3 = st.columns(3)
        with fk1:
            render_metric_card("SKUs com reposição", metric_int(int(len(repl)) if not repl.empty else 0), "Itens com recomendação de envio", "#2563eb")
        with fk2:
            render_metric_card("Unidades sugeridas", metric_int(total_envio), "Reposição total projetada", "#7c3aed")
        with fk3:
            render_metric_card("Candidatos a FULL", metric_int(int(len(cand)) if not cand.empty else 0), "Itens elegíveis para envio", "#16a34a")

        st.caption("Nesta aba, o racional do FULL usa somente vendas FULL do parquet como série principal, com auditoria separando ML fora do FULL e forecast com trava de sanidade.")

        open_modern_card("🧮 Como a reposição FULL é calculada", "Leitura executiva da lógica usada no CSV de FULL")
        st.markdown("""
        - **Base do cálculo:** vendas históricas **somente dos canais FULL** agregadas por **EAN**.
        - **Horizonte padrão:** **35 dias** de cobertura alvo.
        - **Forecast:** projeção de 35 dias a partir da série diária **bruta do parquet**, com **trava de sanidade** para evitar inflação.
        - **Safety stock:** colchão calculado com base na variabilidade da demanda (`coeficiente_variacao_60d` / `fator_risco_demanda`).
        - **Estoque alvo FULL:** `forecast_unid_35d + safety_stock`.
        - **Reposição real sugerida:** `max(estoque_alvo_full_35d - estoque_full_atual, 0)`.
        - **Auditoria FULL:** o arquivo `full_auditoria_35d.csv` separa FULL, ML fora do FULL e sinaliza possível cross-listing.
        """)
        close_card()

        open_modern_card("🕵️ Auditoria FULL", "Separação entre FULL e ML fora do FULL, com flags de sanity/cross-listing")
        if audit_full.empty:
            st.info("Arquivo de auditoria FULL ainda não encontrado.")
        else:
            ak1, ak2, ak3 = st.columns(3)
            with ak1:
                inflados = int((audit_full["flag_inflado"].astype(str) == "SIM").sum()) if "flag_inflado" in audit_full.columns else 0
                render_metric_card("Possível inflação", metric_int(inflados), "EANs com ML fora do FULL dominante", "#dc2626")
            with ak2:
                caps = int((audit_full["sanity_cap_aplicado"].astype(str) == "SIM").sum()) if "sanity_cap_aplicado" in audit_full.columns else 0
                render_metric_card("Sanity cap aplicado", metric_int(caps), "Forecast limitado por baseline recente", "#d97706")
            with ak3:
                audited = int(len(audit_full))
                render_metric_card("EANs auditados", metric_int(audited), "Itens com leitura FULL auditável", "#2563eb")

            cols_audit = [c for c in [
                "EAN", "SKU", "Descricao", "Marca",
                "unidades_30d_full", "unidades_30d_ml_fora_full", "unidades_30d_total_ml",
                "forecast_unid_35d", "forecast_bruto_35d", "forecast_cap_35d",
                "recomendacao_envio_full", "sanity_cap_aplicado", "flag_cross_listing", "flag_inflado", "ratio_full_30d"
            ] if c in audit_full.columns]
            st.dataframe(audit_full[cols_audit], use_container_width=True)
        close_card()

        open_modern_card("🔁 Reposição FULL (estoque alvo 35d)", "Itens que já estão no FULL com recomendação de envio")
        if repl.empty:
            st.info("Nenhuma reposição calculada.")
        else:
            sort_opts = [c for c in ["forecast_unid_35d", "estoque_alvo_full_35d", "recomendacao_envio_full", "cv_60d", "unidades_30d"] if c in repl.columns]
            sort_col = st.selectbox("Ordenar reposição por", options=sort_opts, index=0, key="full_sort_repl")
            asc = st.checkbox("Ordem crescente", value=False, key="full_sort_repl_asc")
            fr1, fr2, fr3, fr4, fr5 = st.columns([1.2, 2, 1.4, 1.4, 1.6])
            with fr1:
                topn = st.slider("Top N (reposição)", 10, 200, 50, 10, key="full_topn_repl")
            with fr2:
                sku_busca_repl = st.text_input("Filtro SKU", value="", key="full_repl_sku").strip()
            with fr3:
                ean_busca_repl = st.text_input("Filtro EAN", value="", key="full_repl_ean").strip()
            with fr4:
                desc_busca_repl = st.text_input("Filtro descrição", value="", key="full_repl_desc").strip()
            with fr5:
                fulfillment_sel_repl = st.selectbox("Fulfillment", ["Todos", "Somente Fulfillment", "Somente Não Fulfillment"], index=0, key="full_repl_fulfillment")
            brand_col_repl = get_brand_col(repl)
            marca_opts_repl = sorted(repl[brand_col_repl].dropna().astype(str).unique().tolist()) if brand_col_repl and brand_col_repl in repl.columns else []
            marcas_sel_repl = st.multiselect("Marca (reposição FULL)", options=marca_opts_repl, default=marca_opts_repl, key="full_repl_marcas") if len(marca_opts_repl) else []
            view = apply_full_filters(repl, sku_termo=sku_busca_repl, marcas_sel=marcas_sel_repl, fulfillment_sel=fulfillment_sel_repl, ean_termo=ean_busca_repl, desc_termo=desc_busca_repl, canal_sel="Todos")
            if sort_col in view.columns:
                view[sort_col] = pd.to_numeric(view[sort_col], errors="coerce")
                view = view.sort_values(sort_col, ascending=asc)
            cols_pref = [c for c in [
                "EAN", "SKU", "Descricao", "Marca", "_fulfillment_label", "classe_abc",
                "mlb_count", "mlb_ids",
                "unidades_30d", "forecast_unid_35d", "safety_stock", "estoque_alvo_full_35d", "estoque_full_atual", "reposicao_real_sugerida", "recomendacao_envio_full",
                "lead_time_dias", "dias_seguranca", "dias_cobertura_alvo",
                "desvio_padrao_diario_60d", "coeficiente_variacao_60d", "fator_risco_demanda", "score_prioridade_full",
                "sold_quantity_ml_total", "available_quantity_ml_total", "available_quantity_full_atual", "available_quantity_outros_ml",
                "media_diaria_60d", "cv_60d", "dias_com_venda_90d", "unidades_90d", "metodo_forecast", "data_ref"
            ] if c in view.columns]
            df_full_show = view[cols_pref].head(topn).copy()
            styler_full = df_full_show.style
            for c in [col for col in ["reposicao_real_sugerida", "recomendacao_envio_full", "estoque_alvo_full_35d", "forecast_unid_35d"] if col in df_full_show.columns]:
                styler_full = styler_full.applymap(highlight_reposicao, subset=[c])
            for c in [col for col in ["score_prioridade_full"] if col in df_full_show.columns]:
                styler_full = styler_full.applymap(highlight_urgency_score, subset=[c])
            st.dataframe(styler_full, use_container_width=True)

            with st.expander("🔎 Auditoria rápida de um EAN da reposição FULL", expanded=False):
                ean_opts_full = view["EAN"].astype(str).dropna().unique().tolist() if "EAN" in view.columns else []
                if ean_opts_full:
                    ean_audit = st.selectbox("Selecione o EAN para auditar", options=ean_opts_full, key="full_audit_ean")
                    row_audit = view[view["EAN"].astype(str) == str(ean_audit)].head(1)
                    if len(row_audit):
                        ra = row_audit.iloc[0]
                        audit_cols = st.columns(4)
                        audit_cols[0].metric("Forecast 35d", metric_int(int(float(ra.get("forecast_unid_35d", 0) or 0))))
                        audit_cols[1].metric("Safety stock", metric_int(int(float(ra.get("safety_stock", 0) or 0))))
                        audit_cols[2].metric("Estoque alvo", metric_int(int(float(ra.get("estoque_alvo_full_35d", 0) or 0))))
                        audit_cols[3].metric("Reposição real", metric_int(int(float(ra.get("reposicao_real_sugerida", ra.get("recomendacao_envio_full", 0)) or 0))))
                        st.markdown(f"**MLBs vinculados:** `{ra.get('mlb_ids', '') or 'N/D'}`")
                        st.markdown(f"**Qtd de MLBs:** {int(float(ra.get('mlb_count', 0) or 0))}")
                        st.markdown(f"**Lead time / segurança:** {int(float(ra.get('lead_time_dias', 0) or 0))}d + {int(float(ra.get('dias_seguranca', 0) or 0))}d")
                        st.markdown(f"**Variabilidade da demanda:** desvio padrão diário 60d = `{ra.get('desvio_padrao_diario_60d', 'N/D')}`, CV 60d = `{ra.get('coeficiente_variacao_60d', ra.get('cv_60d', 'N/D'))}`")
                        st.markdown(f"**Fator de risco:** `{ra.get('fator_risco_demanda', 'N/D')}` | **Score prioridade:** `{ra.get('score_prioridade_full', 'N/D')}`")
                else:
                    st.info("Sem EANs disponíveis para auditoria.")
            st.download_button("⬇️ Baixar reposição (CSV)", data=view.to_csv(index=False, sep=";").encode("utf-8-sig"), file_name=ARQ_FULL_REPOSICAO, mime="text/csv", key="full_download_repl")
        close_card()

        open_modern_card("🚀 Candidatos para enviar ao FULL", "Itens com quantidade segura para primeiro envio")
        if cand.empty:
            st.info("Nenhum candidato calculado.")
        else:
            sort_opts2 = [c for c in ["score_full", "forecast_unid_35d", "unidades_30d", "unidades_90d", "pedidos_90d", "cv_60d"] if c in cand.columns]
            sort_col2 = st.selectbox("Ordenar candidatos por", options=sort_opts2, index=0, key="full_sort_cand")
            asc2 = st.checkbox("Ordem crescente (candidatos)", value=False, key="full_sort_cand_asc")
            fc1, fc2, fc3, fc4, fc5 = st.columns([1.2, 2, 1.4, 1.4, 1.6])
            with fc1:
                topn2 = st.slider("Top N (candidatos)", 10, 200, 50, 10, key="full_topn_cand")
            with fc2:
                sku_busca_cand = st.text_input("Filtro SKU", value="", key="full_cand_sku").strip()
            with fc3:
                ean_busca_cand = st.text_input("Filtro EAN", value="", key="full_cand_ean").strip()
            with fc4:
                desc_busca_cand = st.text_input("Filtro descrição", value="", key="full_cand_desc").strip()
            with fc5:
                fulfillment_sel_cand = st.selectbox("Fulfillment", ["Todos", "Somente Fulfillment", "Somente Não Fulfillment"], index=0, key="full_cand_fulfillment")
            brand_col_cand = get_brand_col(cand)
            marca_opts_cand = sorted(cand[brand_col_cand].dropna().astype(str).unique().tolist()) if brand_col_cand and brand_col_cand in cand.columns else []
            marcas_sel_cand = st.multiselect("Marca (candidatos FULL)", options=marca_opts_cand, default=marca_opts_cand, key="full_cand_marcas") if len(marca_opts_cand) else []
            view2 = apply_full_filters(cand, sku_termo=sku_busca_cand, marcas_sel=marcas_sel_cand, fulfillment_sel=fulfillment_sel_cand, ean_termo=ean_busca_cand, desc_termo=desc_busca_cand, canal_sel="Todos")
            if sort_col2 in view2.columns:
                view2[sort_col2] = pd.to_numeric(view2[sort_col2], errors="coerce")
                view2 = view2.sort_values(sort_col2, ascending=asc2)
            cols_pref2 = [c for c in [
                "EAN", "SKU", "Descricao", "Marca", "_fulfillment_label", "classe_abc", "score_full",
                "mlb_count", "mlb_ids",
                "unidades_30d", "forecast_unid_35d", "qtd_segura_envio_full",
                "lead_time_dias", "dias_seguranca", "dias_cobertura_alvo",
                "desvio_padrao_diario_60d", "coeficiente_variacao_60d", "fator_risco_demanda",
                "pedidos_90d", "dias_com_venda_90d",
                "unidades_90d", "receita_90d", "media_diaria_60d", "cv_60d", "metodo_forecast", "data_ref"
            ] if c in view2.columns]
            st.dataframe(view2[cols_pref2].head(topn2), use_container_width=True)
            st.download_button("⬇️ Baixar candidatos (CSV)", data=view2.to_csv(index=False, sep=";").encode("utf-8-sig"), file_name=ARQ_FULL_CANDIDATOS, mime="text/csv", key="full_download_cand")
        close_card()

        st.markdown('<hr class="divider-modern">', unsafe_allow_html=True)
        render_section_header(
            "🛡️ Governança automática do catálogo ML",
            "Auditoria operacional dos anúncios ativos do snapshot: sem SKU/EAN, duplicidades e convivência FULL vs FLEX"
        )

        gov_res = load_ml_catalog_snapshot(int(seller_id_dash), 20000)
        if not gov_res.get("ok"):
            st.info("Governança do catálogo indisponível no momento. A fonte operacional do Postgres não respondeu para leitura detalhada do snapshot.")
        else:
            gov = build_ml_catalog_governance(gov_res.get("data"))
            gk1, gk2, gk3, gk4, gk5 = st.columns(5)
            with gk1:
                render_metric_card("Anúncios ativos", metric_int(gov.get("ativos", 0)), "Itens ativos no snapshot do catálogo", "#2563eb")
            with gk2:
                render_metric_card("Ativos sem SKU/EAN", metric_int(gov.get("ativos_sem_sku", 0)), "Anúncios que não amarram com EAN/SKU", "#ef4444")
            with gk3:
                render_metric_card("EANs duplicados", metric_int(gov.get("eans_duplicados", 0)), "Mesmo EAN com múltiplos MLBs ativos", "#7c3aed")
            with gk4:
                render_metric_card("FULL + FLEX por SKU", metric_int(gov.get("skus_full_flex", 0)), "SKU convivendo em logísticas diferentes", "#16a34a")
            with gk5:
                render_metric_card("Estoque divergente", metric_int(gov.get("estoque_divergente", 0)), "Diferenças relevantes entre anúncios do mesmo SKU", "#f59e0b")

            open_modern_card("Anúncios ativos sem SKU/EAN", "Itens ativos que hoje não podem ser amarrados automaticamente ao EAN/SKU da operação")
            sem_sku_df = gov.get("sem_sku_df", pd.DataFrame())
            if sem_sku_df.empty:
                st.success("✅ Nenhum anúncio ativo sem SKU/EAN no snapshot atual.")
            else:
                st.dataframe(sem_sku_df.head(300), use_container_width=True)
            close_card()

            open_modern_card("EANs duplicados por anúncios ativos", "Governança de catálogo para detectar múltiplos MLBs no mesmo EAN/SKU")
            duplicados_df = gov.get("duplicados_df", pd.DataFrame())
            if duplicados_df.empty:
                st.success("✅ Nenhum EAN duplicado detectado no snapshot atual.")
            else:
                cols_dup = [c for c in ["ean_sku_ref", "mlb_count", "full_mlb_count", "non_full_mlb_count", "estoque_ml_total", "sold_quantity_ml_total", "logistic_types", "mlb_ids"] if c in duplicados_df.columns]
                st.dataframe(duplicados_df[cols_dup].head(300), use_container_width=True)
            close_card()

            open_modern_card("FULL vs FLEX / Drop-off por SKU", "SKU com anúncios simultâneos no FULL e fora do FULL")
            full_flex_df = gov.get("full_flex_df", pd.DataFrame())
            if full_flex_df.empty:
                st.info("Nenhum SKU com convivência FULL + FLEX / Drop-off no snapshot atual.")
            else:
                cols_ff = [c for c in ["ean_sku_ref", "mlb_count", "full_mlb_count", "non_full_mlb_count", "estoque_full", "estoque_outros", "estoque_ml_total", "sold_quantity_ml_total", "mlb_ids"] if c in full_flex_df.columns]
                st.dataframe(full_flex_df[cols_ff].head(300), use_container_width=True)
            close_card()

            open_modern_card("Estoque divergente entre anúncios do mesmo SKU", "Diferença material de estoque entre MLBs ativos do mesmo EAN/SKU")
            divergencia_df = gov.get("divergencia_df", pd.DataFrame())
            if divergencia_df.empty:
                st.info("Nenhuma divergência material de estoque detectada no snapshot atual.")
            else:
                cols_div = [c for c in ["ean_sku_ref", "mlb_count", "estoque_ml_total", "estoque_full", "estoque_outros", "estoque_spread", "estoque_gap_full_vs_outros", "mlb_ids"] if c in divergencia_df.columns]
                st.dataframe(divergencia_df[cols_div].head(300), use_container_width=True)
            close_card()

    st.caption("Obs.: esta aba combina a leitura operacional do Postgres com os CSVs gerados pelo `copiloto_vendas_v3.py`.")


# =========================
# TAB 5: REPOSIÇÃO GERAL
# =========================
with tab5:
    render_section_header(
        "🏬 Reposição Geral — Estoque Interno por EAN",
        "Painel executivo de ruptura, excesso, aceleração de demanda e recomendação de reposição"
    )

    if repl_geral.empty:
        st.warning(
            f"Ainda não encontrei os arquivos da reposição geral.\n\n- `{ARQ_REPOSICAO_GERAL}`\n- `{ARQ_REPOSICAO_GERAL_ACCEL}`\n\nRode: `python copiloto_vendas_v3.py`."
        )
    else:
        repl_geral = ensure_numeric(
            repl_geral,
            [
                "estoque_atual",
                "vendas_30d", "vendas_60d", "vendas_90d",
                "receita_90d", "pedidos_90d", "dias_com_venda_90d",
                "media_diaria_30d", "media_diaria_60d", "media_diaria_90d",
                "cobertura_30d", "cobertura_60d", "cobertura_90d",
                "forecast_unid_30d", "forecast_unid_60d", "forecast_unid_90d",
                "safety_stock_30d", "safety_stock_60d", "safety_stock_90d",
                "reposicao_sugerida_30d", "reposicao_sugerida_60d", "reposicao_sugerida_90d",
                "media_prev_30d", "media_ult_30d",
                "crescimento_ult30_vs_prev30_pct",
                "trend_score", "score_urgencia", "ordem_urgencia"
            ]
        )

        if "status_cobertura_30d" in repl_geral.columns:
            repl_geral["status_cobertura_30d"] = normalize_status_cobertura(repl_geral["status_cobertura_30d"])
        if "status_cobertura_60d" in repl_geral.columns:
            repl_geral["status_cobertura_60d"] = normalize_status_cobertura(repl_geral["status_cobertura_60d"])
        if "status_cobertura_90d" in repl_geral.columns:
            repl_geral["status_cobertura_90d"] = normalize_status_cobertura(repl_geral["status_cobertura_90d"])
        if "trend_status" in repl_geral.columns:
            repl_geral["trend_status"] = normalize_trend(repl_geral["trend_status"])

        if not repl_geral_accel.empty:
            repl_geral_accel = ensure_numeric(
                repl_geral_accel,
                [
                    "estoque_atual",
                    "vendas_30d", "vendas_60d", "vendas_90d",
                    "forecast_unid_30d", "forecast_unid_60d", "forecast_unid_90d",
                    "cobertura_90d",
                    "media_prev_30d", "media_ult_30d",
                    "crescimento_ult30_vs_prev30_pct",
                    "trend_score", "score_urgencia",
                    "reposicao_sugerida_90d"
                ]
            )
            if "status_cobertura_90d" in repl_geral_accel.columns:
                repl_geral_accel["status_cobertura_90d"] = normalize_status_cobertura(repl_geral_accel["status_cobertura_90d"])
            if "trend_status" in repl_geral_accel.columns:
                repl_geral_accel["trend_status"] = normalize_trend(repl_geral_accel["trend_status"])

        repl_geral = merge_dash_ops_estado(repl_geral, estado_dash_ops)
        if not repl_geral_accel.empty:
            repl_geral_accel = merge_dash_ops_estado(repl_geral_accel, estado_dash_ops)

        if "EAN" in repl_geral.columns:
            repl_geral["status_dash"] = repl_geral.apply(build_status_dash, axis=1)
        if not repl_geral_accel.empty and "EAN" in repl_geral_accel.columns:
            repl_geral_accel["status_dash"] = repl_geral_accel.apply(build_status_dash, axis=1)

        criticos_90 = int((repl_geral["status_cobertura_90d"] == "CRÍTICO").sum()) if "status_cobertura_90d" in repl_geral.columns else 0
        urgentes_90 = int((repl_geral["status_cobertura_90d"] == "URGENTE").sum()) if "status_cobertura_90d" in repl_geral.columns else 0
        total_accel = int(len(repl_geral_accel)) if not repl_geral_accel.empty else 0
        total_rep_90_exec = int(pd.to_numeric(repl_geral.get("reposicao_sugerida_90d", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if "reposicao_sugerida_90d" in repl_geral.columns else 0
        total_sugestoes = int(pd.to_numeric(repl_geral.get("qtd_sugestoes_ativas", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if "qtd_sugestoes_ativas" in repl_geral.columns else 0
        itens_taggeados = int((pd.to_numeric(repl_geral.get("qtd_tags_ativas", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0).sum()) if "qtd_tags_ativas" in repl_geral.columns else 0

        st.markdown("### Visão Executiva")
        ec1, ec2, ec3, ec4, ec5, ec6 = st.columns(6)
        with ec1:
            render_exec_card("Itens críticos (90d)", metric_int(criticos_90), "Risco imediato de ruptura", "#ff4d4f")
        with ec2:
            render_exec_card("Itens urgentes (90d)", metric_int(urgentes_90), "Cobertura baixa, ação prioritária", "#fa8c16")
        with ec3:
            render_exec_card("Produtos acelerando", metric_int(total_accel), "Demanda recente em aceleração", "#52c41a")
        with ec4:
            render_exec_card("Reposição sugerida 90d", metric_int(total_rep_90_exec), "Volume total projetado para recompor", "#1677ff")
        with ec5:
            render_exec_card("Sugestões colaborativas", metric_int(total_sugestoes), "Sugestões ativas gravadas no Postgres", "#7c3aed")
        with ec6:
            render_exec_card("Produtos com tag", metric_int(itens_taggeados), "Itens com contexto operacional salvo", "#0f766e")

        st.markdown("### Leitura rápida por criticidade")
        render_criticidade_legend()

        kpi_strip_items = [
            ("Críticos 30d", metric_int(int((repl_geral["status_cobertura_30d"] == "CRÍTICO").sum())) if "status_cobertura_30d" in repl_geral.columns else 0, "#b42318"),
            ("Críticos 60d", metric_int(int((repl_geral["status_cobertura_60d"] == "CRÍTICO").sum())) if "status_cobertura_60d" in repl_geral.columns else 0, "#d92d20"),
            ("Críticos 90d", metric_int(int((repl_geral["status_cobertura_90d"] == "CRÍTICO").sum())) if "status_cobertura_90d" in repl_geral.columns else 0, "#f04438"),
            ("Acelerando", metric_int(total_accel), "#16a34a"),
        ]
        render_kpi_strip(kpi_strip_items)

        st.markdown("")
        k5, k6, k7, k8 = st.columns(4)
        total_rep_30 = int(pd.to_numeric(repl_geral.get("reposicao_sugerida_30d", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if "reposicao_sugerida_30d" in repl_geral.columns else 0
        total_rep_60 = int(pd.to_numeric(repl_geral.get("reposicao_sugerida_60d", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if "reposicao_sugerida_60d" in repl_geral.columns else 0
        total_rep_90 = int(pd.to_numeric(repl_geral.get("reposicao_sugerida_90d", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if "reposicao_sugerida_90d" in repl_geral.columns else 0
        data_ref_geral = repl_geral["data_ref_global"].dropna().astype(str).iloc[0] if "data_ref_global" in repl_geral.columns and repl_geral["data_ref_global"].notna().any() else "-"

        k5.metric("Reposição sugerida 30d", metric_int(total_rep_30))
        k6.metric("Reposição sugerida 60d", metric_int(total_rep_60))
        k7.metric("Reposição sugerida 90d", metric_int(total_rep_90))
        k8.metric("Data ref.", data_ref_geral)

        st.divider()
        st.markdown("### Filtros")

        f1, f2, f3, f4, f5 = st.columns(5)
        with f1:
            if "Marca" in repl_geral.columns:
                marcas = sorted(repl_geral["Marca"].dropna().astype(str).unique().tolist())
                marcas_sel = st.multiselect("Marca", marcas, default=marcas, key="geral_marcas")
            else:
                marcas_sel = []
        with f2:
            if "status_cobertura_90d" in repl_geral.columns:
                status_opts = ["CRÍTICO", "URGENTE", "ATENÇÃO", "SAUDÁVEL", "ALTO", "EXCESSO", "SEM_GIRO"]
                present = [s for s in status_opts if s in set(repl_geral["status_cobertura_90d"].dropna().astype(str).tolist())]
                status_sel = st.multiselect("Cobertura 90d", present, default=present, key="geral_status")
            else:
                status_sel = []
        with f3:
            if "trend_status" in repl_geral.columns:
                trend_opts = sorted(repl_geral["trend_status"].dropna().astype(str).unique().tolist())
                trend_sel = st.multiselect("Trend", trend_opts, default=trend_opts, key="geral_trend")
            else:
                trend_sel = []
        with f4:
            min_rep90 = st.number_input("Reposição mínima (90d)", min_value=0, value=0, step=1, key="geral_minrep")
        with f5:
            sku_col_geral = get_sku_col(repl_geral)
            sku_busca_geral = st.text_input("Filtro SKU", value="", key="geral_sku_busca").strip()

        f6, f7, f8, f9 = st.columns(4)
        with f6:
            somente_acelerando = st.checkbox("Mostrar só acelerando venda", value=False, key="geral_so_acel")
        with f7:
            termo_busca = st.text_input("Buscar EAN / descrição", value="", key="geral_busca").strip()
        with f8:
            mostrar_ocultos = st.checkbox("Mostrar itens ocultos", value=False, key="geral_mostrar_ocultos")
        with f9:
            so_com_sugestao = st.checkbox("Somente com sugestão colaborativa", value=False, key="geral_so_com_sug")

        f10, f11, f12 = st.columns(3)
        tag_opts = [
            ("fl_sazonal", "🌸 Sazonal"),
            ("fl_fora_de_linha", "⛔ Fora de linha"),
            ("fl_nao_repor", "🚫 Não repor"),
        ]
        with f10:
            tag_labels = [label for key, label in tag_opts if key in repl_geral.columns]
            tags_sel = st.multiselect("Filtrar por tags", tag_labels, default=[], key="geral_tags_sel")
        with f11:
            sort_geral = st.selectbox(
                "Ordenar por",
                options=["urgencia", "reposicao_90d", "cobertura_90d", "forecast_90d", "vendas_90d", "crescimento_30d", "sugestoes_colaborativas"],
                index=0,
                key="geral_sort"
            )
        with f12:
            topn_geral = st.slider("Top N (reposição geral)", 10, 500, 100, 10, key="geral_topn")

        view = repl_geral.copy()
        if "Marca" in view.columns and len(marcas_sel) > 0:
            view = safe_multiselect_filter(view, "Marca", marcas_sel)
        if "status_cobertura_90d" in view.columns and len(status_sel) > 0:
            view = safe_multiselect_filter(view, "status_cobertura_90d", status_sel)
        if "trend_status" in view.columns and len(trend_sel) > 0:
            view = safe_multiselect_filter(view, "trend_status", trend_sel)
        if "reposicao_sugerida_90d" in view.columns:
            view = view[pd.to_numeric(view["reposicao_sugerida_90d"], errors="coerce").fillna(0) >= int(min_rep90)].copy()
        if somente_acelerando and "trend_status" in view.columns:
            view = view[normalize_text_series(view["trend_status"]).isin(["ACELERANDO", "ACELERANDO_FORTE"])].copy()
        if sku_busca_geral and sku_col_geral and sku_col_geral in view.columns:
            view = view[view[sku_col_geral].astype(str).str.contains(sku_busca_geral, case=False, na=False)].copy()
        if termo_busca:
            mask = pd.Series(False, index=view.index)
            if "EAN" in view.columns:
                mask = mask | view["EAN"].astype(str).str.contains(termo_busca, case=False, na=False)
            if "Descricao" in view.columns:
                mask = mask | view["Descricao"].astype(str).str.contains(termo_busca, case=False, na=False)
            view = view[mask].copy()
        if not mostrar_ocultos:
            if "fl_nao_repor" in view.columns:
                view = view[pd.to_numeric(view["fl_nao_repor"], errors="coerce").fillna(0) == 0].copy()
            if "fl_fora_de_linha" in view.columns:
                view = view[pd.to_numeric(view["fl_fora_de_linha"], errors="coerce").fillna(0) == 0].copy()
            if "ocultar_na_reposicao" in view.columns:
                view = view[~view["ocultar_na_reposicao"].fillna(False)].copy()
        if so_com_sugestao and "qtd_sugestoes_ativas" in view.columns:
            view = view[pd.to_numeric(view["qtd_sugestoes_ativas"], errors="coerce").fillna(0) > 0].copy()
        selected_tag_keys = []
        for key, label in tag_opts:
            if label in tags_sel and key in view.columns:
                selected_tag_keys.append(key)
        for key in selected_tag_keys:
            view = view[pd.to_numeric(view[key], errors="coerce").fillna(0) == 1].copy()

        if sort_geral == "urgencia":
            sort_cols = [c for c in ["ordem_urgencia", "score_urgencia", "reposicao_sugerida_90d", "forecast_unid_90d", "vendas_90d"] if c in view.columns]
            sort_asc = [True, False, False, False, False][:len(sort_cols)]
            if sort_cols:
                view = view.sort_values(sort_cols, ascending=sort_asc)
        elif sort_geral == "reposicao_90d" and "reposicao_sugerida_90d" in view.columns:
            view = view.sort_values("reposicao_sugerida_90d", ascending=False)
        elif sort_geral == "cobertura_90d" and "cobertura_90d" in view.columns:
            view = view.sort_values("cobertura_90d", ascending=True)
        elif sort_geral == "forecast_90d" and "forecast_unid_90d" in view.columns:
            view = view.sort_values("forecast_unid_90d", ascending=False)
        elif sort_geral == "vendas_90d" and "vendas_90d" in view.columns:
            view = view.sort_values("vendas_90d", ascending=False)
        elif sort_geral == "crescimento_30d" and "crescimento_ult30_vs_prev30_pct" in view.columns:
            view = view.sort_values("crescimento_ult30_vs_prev30_pct", ascending=False)
        elif sort_geral == "sugestoes_colaborativas" and "qtd_sugestoes_ativas" in view.columns:
            if "ultima_sugestao_em" in view.columns:
                view = view.sort_values(["qtd_sugestoes_ativas", "ultima_sugestao_em"], ascending=[False, False])
            else:
                view = view.sort_values(["qtd_sugestoes_ativas"], ascending=[False])

        st.divider()
        st.markdown("### Camada colaborativa (Postgres)")
        if estado_dash_ops.empty:
            st.warning("Não foi possível ler `dash_ops.vw_reposicao_estado_ean`. A aba continua funcionando só com a base analítica.")
        else:
            cpg1, cpg2, cpg3 = st.columns(3)
            cpg1.metric("EANs com sugestão ativa", metric_int(int((pd.to_numeric(repl_geral.get("qtd_sugestoes_ativas", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0).sum())))
            cpg2.metric("EANs com tag ativa", metric_int(int((pd.to_numeric(repl_geral.get("qtd_tags_ativas", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0).sum())))
            cpg3.metric("Itens ocultos por regra", metric_int(int((repl_geral.get("ocultar_na_reposicao", pd.Series(dtype=bool)).fillna(False)).sum())) if "ocultar_na_reposicao" in repl_geral.columns else "0")
            st.caption("Leitura atual: sugestões, tags e estado consolidado vindos do Postgres via `dash_ops.vw_reposicao_estado_ean`.")

        st.divider()
        st.markdown("### Editor colaborativo — sugestões, tags e estado")

        view_editor = view.copy()
        if len(view_editor) == 0:
            st.info("Nenhum item disponível no filtro atual para edição colaborativa.")
        else:
            def _make_editor_label(r):
                ean = str(r.get("EAN", ""))
                sku = str(r.get("SKU", ""))
                desc = str(r.get("Descricao", ""))
                marca = str(r.get("Marca", ""))
                return f"{ean} | {sku} | {desc[:70]} | {marca}"

            editor_df = view_editor.copy()
            editor_df["_editor_label"] = editor_df.apply(_make_editor_label, axis=1)
            editor_options = editor_df["_editor_label"].tolist()
            selected_editor_label = st.selectbox(
                "Selecione o produto para editar",
                options=editor_options,
                index=0,
                key="repos_editor_produto"
            )
            selected_row = editor_df.loc[editor_df["_editor_label"] == selected_editor_label].iloc[0].copy()
            selected_ean = str(selected_row.get("EAN", ""))
            selected_sku = str(selected_row.get("SKU", ""))
            selected_desc = str(selected_row.get("Descricao", ""))
            selected_marca = str(selected_row.get("Marca", ""))

            info1, info2, info3, info4 = st.columns(4)
            info1.metric("EAN", selected_ean or "-")
            info2.metric("SKU", selected_sku or "-")
            info3.metric("Sugestões ativas", metric_int(int(float(selected_row.get("qtd_sugestoes_ativas", 0) or 0))))
            info4.metric("Tags ativas", metric_int(int(float(selected_row.get("qtd_tags_ativas", 0) or 0))))
            st.caption(f"Produto: {selected_desc} | Marca: {selected_marca}")
            if str(selected_row.get("status_dash", "")).strip():
                st.info(f"Status colaborativo atual: {selected_row.get('status_dash', '')}")

            ed1, ed2, ed3 = st.columns([1.3, 1.2, 1.5])

            with ed1:
                st.markdown("#### Nova sugestão de reposição")
                with st.form(f"form_sugestao_{selected_ean}"):
                    analista_sug = st.text_input("Analista", value="", key=f"analista_sug_{selected_ean}")
                    qtd_sug = st.number_input("Qtd sugerida", min_value=0.0, value=float(selected_row.get("reposicao_sugerida_90d", 0) or 0), step=1.0, key=f"qtd_sug_{selected_ean}")
                    motivo_sug = st.text_input("Motivo", value="", key=f"motivo_sug_{selected_ean}")
                    obs_sug = st.text_area("Observação", value="", height=90, key=f"obs_sug_{selected_ean}")
                    canal_ctx = st.text_input("Canal / contexto", value="", key=f"canal_sug_{selected_ean}")
                    submit_sug = st.form_submit_button("💾 Salvar sugestão", use_container_width=True)

                if submit_sug:
                    if not analista_sug.strip():
                        st.error("Informe o nome do analista para salvar a sugestão.")
                    elif qtd_sug <= 0:
                        st.error("A quantidade sugerida precisa ser maior que zero.")
                    elif not motivo_sug.strip():
                        st.error("Informe o motivo da sugestão.")
                    else:
                        res = save_dash_ops_sugestao(
                            ean=selected_ean,
                            sku=selected_sku,
                            descricao=selected_desc,
                            marca=selected_marca,
                            analista=analista_sug.strip(),
                            qtd_sugerida=float(qtd_sug),
                            motivo=motivo_sug.strip(),
                            observacao=obs_sug.strip(),
                            canal_contexto=canal_ctx.strip(),
                            sessao_origem="streamlit_reposicao"
                        )
                        if res.get("ok"):
                            st.success("Sugestão gravada no Postgres com sucesso.")
                            st.rerun()
                        else:
                            st.error(f"Falha ao salvar sugestão: {res.get('error')}")

            with ed2:
                st.markdown("#### Nova tag do produto")
                with st.form(f"form_tag_{selected_ean}"):
                    analista_tag = st.text_input("Analista / responsável", value="", key=f"analista_tag_{selected_ean}")
                    tag_sel = st.selectbox("Tag", options=["sazonal", "fora_de_linha", "nao_repor", "produto_estrategico", "alto_giro_eventual", "dependente_de_campanha", "aguardando_importacao", "bloqueado_comercial"], key=f"tag_sel_{selected_ean}")
                    valor_tag = st.text_input("Valor da tag", value="", placeholder="Ex.: pascoa", key=f"valor_tag_{selected_ean}")
                    obs_tag = st.text_area("Observação da tag", value="", height=90, key=f"obs_tag_{selected_ean}")
                    submit_tag = st.form_submit_button("🏷️ Salvar tag", use_container_width=True)

                if submit_tag:
                    if not analista_tag.strip():
                        st.error("Informe o nome do responsável para salvar a tag.")
                    else:
                        res = save_dash_ops_tag(
                            ean=selected_ean,
                            tag=tag_sel,
                            valor=valor_tag.strip(),
                            observacao=obs_tag.strip(),
                            created_by=analista_tag.strip()
                        )
                        if res.get("ok"):
                            st.success("Tag gravada no Postgres com sucesso.")
                            st.rerun()
                        else:
                            st.error(f"Falha ao salvar tag: {res.get('error')}")

            with ed3:
                st.markdown("#### Estado consolidado do produto")
                with st.form(f"form_estado_{selected_ean}"):
                    responsavel_estado = st.text_input("Responsável pela atualização", value=str(selected_row.get("updated_by", "")), key=f"resp_estado_{selected_ean}")
                    ocultar_estado = st.checkbox("Ocultar na reposição", value=bool(selected_row.get("ocultar_na_reposicao", False)), key=f"ocultar_estado_{selected_ean}")
                    prioridade_estado = st.selectbox("Prioridade comercial", options=["", "baixa", "media", "alta", "estrategica"], index=["", "baixa", "media", "alta", "estrategica"].index(str(selected_row.get("prioridade_comercial", "")).lower()) if str(selected_row.get("prioridade_comercial", "")).lower() in ["", "baixa", "media", "alta", "estrategica"] else 0, key=f"prioridade_estado_{selected_ean}")
                    owner_estado = st.text_input("Owner da categoria", value=str(selected_row.get("owner_categoria", "")), key=f"owner_estado_{selected_ean}")
                    obs_estado = st.text_area("Observação geral", value=str(selected_row.get("observacao_geral", "")), height=120, key=f"obs_estado_{selected_ean}")
                    submit_estado = st.form_submit_button("🧩 Salvar estado", use_container_width=True)

                if submit_estado:
                    if not responsavel_estado.strip():
                        st.error("Informe quem está atualizando o estado do produto.")
                    else:
                        res = upsert_dash_ops_estado(
                            ean=selected_ean,
                            ocultar_na_reposicao=bool(ocultar_estado),
                            prioridade_comercial=prioridade_estado,
                            owner_categoria=owner_estado.strip(),
                            observacao_geral=obs_estado.strip(),
                            updated_by=responsavel_estado.strip()
                        )
                        if res.get("ok"):
                            st.success("Estado consolidado atualizado com sucesso.")
                            st.rerun()
                        else:
                            st.error(f"Falha ao salvar estado: {res.get('error')}")

            hist1, hist2 = st.columns(2)
            with hist1:
                st.markdown("#### Histórico de sugestões")
                hist_sug = load_dash_ops_sugestoes_raw(selected_ean)
                if hist_sug.empty:
                    st.info("Sem histórico de sugestões para este EAN.")
                else:
                    hist_sug_show = hist_sug[[c for c in ["created_at", "analista", "qtd_sugerida", "motivo", "observacao", "canal_contexto", "status"] if c in hist_sug.columns]].copy()
                    st.dataframe(hist_sug_show, use_container_width=True, height=260)
            with hist2:
                st.markdown("#### Histórico de tags")
                hist_tag = load_dash_ops_tags_raw(selected_ean)
                if hist_tag.empty:
                    st.info("Sem histórico de tags para este EAN.")
                else:
                    hist_tag_show = hist_tag[[c for c in ["created_at", "created_by", "tag", "valor", "observacao", "ativo"] if c in hist_tag.columns]].copy()
                    st.dataframe(hist_tag_show, use_container_width=True, height=260)

        st.divider()
        st.markdown("### Resumo visual — Ruptura vs Excesso")
        rv1, rv2 = st.columns(2)
        with rv1:
            if "status_cobertura_90d" in view.columns and len(view):
                ruptura_df = pd.DataFrame({
                    "categoria": ["CRÍTICO", "URGENTE", "ATENÇÃO"],
                    "qtd": [
                        int((normalize_text_series(view["status_cobertura_90d"]) == "CRÍTICO").sum()),
                        int((normalize_text_series(view["status_cobertura_90d"]) == "URGENTE").sum()),
                        int((normalize_text_series(view["status_cobertura_90d"]) == "ATENÇÃO").sum()),
                    ]
                })
                chart_ruptura = alt.Chart(ruptura_df).mark_bar().encode(
                    x=alt.X("categoria:N", title="Faixa de risco"),
                    y=alt.Y("qtd:Q", title="EANs"),
                    tooltip=["categoria", "qtd"]
                )
                st.altair_chart(chart_ruptura, use_container_width=True)
            else:
                st.info("Sem dados para risco de ruptura.")
        with rv2:
            if "status_cobertura_90d" in view.columns and len(view):
                excesso_df = pd.DataFrame({
                    "categoria": ["SAUDÁVEL", "ALTO", "EXCESSO", "SEM_GIRO"],
                    "qtd": [
                        int((normalize_text_series(view["status_cobertura_90d"]) == "SAUDÁVEL").sum()),
                        int((normalize_text_series(view["status_cobertura_90d"]) == "ALTO").sum()),
                        int((normalize_text_series(view["status_cobertura_90d"]) == "EXCESSO").sum()),
                        int((normalize_text_series(view["status_cobertura_90d"]) == "SEM_GIRO").sum()),
                    ]
                })
                chart_excesso = alt.Chart(excesso_df).mark_bar().encode(
                    x=alt.X("categoria:N", title="Faixa de sobra"),
                    y=alt.Y("qtd:Q", title="EANs"),
                    tooltip=["categoria", "qtd"]
                )
                st.altair_chart(chart_excesso, use_container_width=True)
            else:
                st.info("Sem dados para excesso.")

        if "status_cobertura_90d" in view.columns and len(view):
            status_norm = normalize_text_series(view["status_cobertura_90d"])
            qtd_ruptura = int(status_norm.isin(["CRÍTICO", "URGENTE", "ATENÇÃO"]).sum())
            qtd_excesso = int(status_norm.isin(["ALTO", "EXCESSO", "SEM_GIRO"]).sum())
            qtd_saudavel = int((status_norm == "SAUDÁVEL").sum())
            st.info(
                f"Leitura executiva: {metric_int(qtd_ruptura)} EANs em zona de possível ruptura, {metric_int(qtd_saudavel)} em faixa saudável e {metric_int(qtd_excesso)} com indício de excesso / baixo giro."
            )

        st.divider()
        g1, g2 = st.columns(2)
        with g1:
            st.markdown("### Distribuição por cobertura 90d")
            if "status_cobertura_90d" in view.columns and len(view):
                dist = view.groupby("status_cobertura_90d", as_index=False).agg(qtd=("EAN", "count"))
                chart = alt.Chart(dist).mark_bar().encode(
                    x=alt.X("status_cobertura_90d:N", title="Status"),
                    y=alt.Y("qtd:Q", title="EANs"),
                    tooltip=["status_cobertura_90d", "qtd"]
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("Sem dados para o gráfico.")
        with g2:
            st.markdown("### Top reposição sugerida 90d")
            if len(view) and "reposicao_sugerida_90d" in view.columns:
                top_rep = view.nlargest(15, "reposicao_sugerida_90d").copy()
                label_col = "Descricao" if "Descricao" in top_rep.columns else "EAN"
                chart = alt.Chart(top_rep).mark_bar().encode(
                    x=alt.X("reposicao_sugerida_90d:Q", title="Reposição 90d"),
                    y=alt.Y(f"{label_col}:N", sort="-x", title="EAN / Descrição"),
                    tooltip=[c for c in ["EAN", "Descricao", "Marca", "reposicao_sugerida_90d", "cobertura_90d", "trend_status", "status_dash", "qtd_sugestoes_ativas", "ultima_qtd_sugerida", "ultimo_analista"] if c in top_rep.columns]
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("Sem dados para o gráfico.")

        st.divider()
        st.markdown("### Ranking visual — EANs críticos")
        if "status_cobertura_90d" in view.columns and "reposicao_sugerida_90d" in view.columns:
            criticos = view[normalize_text_series(view["status_cobertura_90d"]).isin(["CRÍTICO", "URGENTE"])].copy()
            if len(criticos):
                criticos = criticos.nlargest(20, "reposicao_sugerida_90d")
                label_col = "Descricao" if "Descricao" in criticos.columns else "EAN"
                chart_crit = alt.Chart(criticos).mark_bar().encode(
                    x=alt.X("reposicao_sugerida_90d:Q", title="Reposição sugerida 90d"),
                    y=alt.Y(f"{label_col}:N", sort="-x", title="Produto"),
                    tooltip=[c for c in ["EAN", "Descricao", "Marca", "status_cobertura_90d", "reposicao_sugerida_90d", "cobertura_90d", "status_dash", "qtd_sugestoes_ativas", "ultima_qtd_sugerida", "ultimo_analista"] if c in criticos.columns]
                )
                st.altair_chart(chart_crit, use_container_width=True)
            else:
                st.info("Nenhum item crítico/urgente no filtro atual.")
        else:
            st.info("Sem dados para ranking crítico.")

        st.divider()
        open_modern_card("🧮 Como ler a lógica da reposição geral", "Explicação rápida para auditoria dos números")
        st.markdown("""
        - **Base histórica:** vendas da base mestre agregadas por **EAN**.
        - **Estoque atual:** último `Estoque Local` conhecido por EAN.
        - **Forecasts 30/60/90d:** projeções de unidades com fallback robusto para séries curtas.
        - **Safety stock:** ajustado pela variabilidade recente da demanda (`coeficiente_variacao_60d` e `fator_risco_demanda`).
        - **Reposição sugerida:** diferença entre o estoque alvo e o estoque atual no horizonte analisado.
        - **Reposição real sugerida:** mesma lógica operacional do horizonte principal da tabela.
        """)
        close_card()

        st.markdown("### Tabela principal — Reposição Geral")
        cols_main = [c for c in [
            "EAN", "SKU", "Descricao", "Marca", "status_dash", "tags_lista", "qtd_sugestoes_ativas", "ultima_qtd_sugerida", "ultimo_analista",
            "data_ult_estoque_ean", "estoque_atual", "vendas_30d", "vendas_60d", "vendas_90d", "cobertura_30d", "cobertura_60d", "cobertura_90d",
            "status_cobertura_30d", "status_cobertura_60d", "status_cobertura_90d", "forecast_unid_30d", "forecast_unid_60d", "forecast_unid_90d",
            "reposicao_sugerida_30d", "reposicao_sugerida_60d", "reposicao_sugerida_90d", "reposicao_real_sugerida", "qtd_sugerida_total_info", "crescimento_ult30_vs_prev30_pct",
            "trend_status", "classe_abc", "score_urgencia", "desvio_padrao_diario_60d", "coeficiente_variacao_60d", "fator_risco_demanda", "lead_time_dias", "dias_seguranca", "dias_cobertura_alvo", "metodo_forecast", "observacao_geral", "data_ref_global"
        ] if c in view.columns]
        df_main_show = view[cols_main].head(topn_geral).copy()
        styler_main = df_main_show.style
        status_cols = [c for c in ["status_cobertura_30d", "status_cobertura_60d", "status_cobertura_90d"] if c in df_main_show.columns]
        if status_cols:
            styler_main = styler_main.applymap(color_status_bg, subset=status_cols)
        for c in [col for col in ["cobertura_30d", "cobertura_60d", "cobertura_90d"] if col in df_main_show.columns]:
            styler_main = styler_main.applymap(highlight_cobertura, subset=[c])
        for c in [col for col in ["reposicao_sugerida_30d", "reposicao_sugerida_60d", "reposicao_sugerida_90d", "reposicao_real_sugerida", "qtd_sugerida_total_info", "ultima_qtd_sugerida"] if col in df_main_show.columns]:
            styler_main = styler_main.applymap(highlight_reposicao, subset=[c])
        if "crescimento_ult30_vs_prev30_pct" in df_main_show.columns:
            styler_main = styler_main.applymap(highlight_growth, subset=["crescimento_ult30_vs_prev30_pct"])
        if "score_urgencia" in df_main_show.columns:
            styler_main = styler_main.applymap(highlight_urgency_score, subset=["score_urgencia"])
        st.dataframe(styler_main, use_container_width=True)
        st.download_button(
            "⬇️ Baixar reposição geral (CSV)",
            data=view.to_csv(index=False, sep=";").encode("utf-8-sig"),
            file_name=ARQ_REPOSICAO_GERAL,
            mime="text/csv",
            key="geral_download_repl"
        )

        st.divider()
        st.markdown("### Produtos acelerando venda")
        if repl_geral_accel.empty:
            st.info("Nenhum produto acelerando venda no arquivo atual.")
        else:
            accel_view = repl_geral_accel.copy()
            if "Marca" in accel_view.columns and len(marcas_sel) > 0:
                accel_view = safe_multiselect_filter(accel_view, "Marca", marcas_sel)
            sku_col_accel = get_sku_col(accel_view)
            if sku_busca_geral and sku_col_accel and sku_col_accel in accel_view.columns:
                accel_view = accel_view[accel_view[sku_col_accel].astype(str).str.contains(sku_busca_geral, case=False, na=False)].copy()
            if termo_busca:
                mask2 = pd.Series(False, index=accel_view.index)
                if "EAN" in accel_view.columns:
                    mask2 = mask2 | accel_view["EAN"].astype(str).str.contains(termo_busca, case=False, na=False)
                if "Descricao" in accel_view.columns:
                    mask2 = mask2 | accel_view["Descricao"].astype(str).str.contains(termo_busca, case=False, na=False)
                accel_view = accel_view[mask2].copy()
            if not mostrar_ocultos:
                if "fl_nao_repor" in accel_view.columns:
                    accel_view = accel_view[pd.to_numeric(accel_view["fl_nao_repor"], errors="coerce").fillna(0) == 0].copy()
                if "fl_fora_de_linha" in accel_view.columns:
                    accel_view = accel_view[pd.to_numeric(accel_view["fl_fora_de_linha"], errors="coerce").fillna(0) == 0].copy()
                if "ocultar_na_reposicao" in accel_view.columns:
                    accel_view = accel_view[~accel_view["ocultar_na_reposicao"].fillna(False)].copy()
            if so_com_sugestao and "qtd_sugestoes_ativas" in accel_view.columns:
                accel_view = accel_view[pd.to_numeric(accel_view["qtd_sugestoes_ativas"], errors="coerce").fillna(0) > 0].copy()
            for key in selected_tag_keys:
                if key in accel_view.columns:
                    accel_view = accel_view[pd.to_numeric(accel_view[key], errors="coerce").fillna(0) == 1].copy()
            if "score_urgencia" in accel_view.columns:
                accel_view = accel_view.sort_values(["score_urgencia", "crescimento_ult30_vs_prev30_pct"], ascending=[False, False])
            topn_accel = st.slider("Top N (acelerando)", 10, 300, 50, 10, key="geral_topn_accel")
            cols_accel = [c for c in [
                "EAN", "SKU", "Descricao", "Marca", "status_dash", "tags_lista", "qtd_sugestoes_ativas", "ultima_qtd_sugerida", "ultimo_analista",
                "estoque_atual", "vendas_30d", "vendas_60d", "vendas_90d", "forecast_unid_30d", "forecast_unid_60d", "forecast_unid_90d",
                "cobertura_90d", "status_cobertura_90d", "media_prev_30d", "media_ult_30d", "crescimento_ult30_vs_prev30_pct", "trend_status",
                "trend_score", "classe_abc", "score_urgencia", "reposicao_sugerida_90d", "reposicao_real_sugerida", "desvio_padrao_diario_60d", "coeficiente_variacao_60d", "fator_risco_demanda", "lead_time_dias", "dias_seguranca", "dias_cobertura_alvo", "data_ult_estoque_ean", "data_ref_global"
            ] if c in accel_view.columns]
            df_accel_show = accel_view[cols_accel].head(topn_accel).copy()
            styler_accel = df_accel_show.style
            if "status_cobertura_90d" in df_accel_show.columns:
                styler_accel = styler_accel.applymap(color_status_bg, subset=["status_cobertura_90d"])
            if "trend_status" in df_accel_show.columns:
                styler_accel = styler_accel.applymap(color_status_text, subset=["trend_status"])
            for c in [col for col in ["cobertura_90d"] if col in df_accel_show.columns]:
                styler_accel = styler_accel.applymap(highlight_cobertura, subset=[c])
            for c in [col for col in ["reposicao_sugerida_90d", "reposicao_real_sugerida", "ultima_qtd_sugerida"] if col in df_accel_show.columns]:
                styler_accel = styler_accel.applymap(highlight_reposicao, subset=[c])
            if "crescimento_ult30_vs_prev30_pct" in df_accel_show.columns:
                styler_accel = styler_accel.applymap(highlight_growth, subset=["crescimento_ult30_vs_prev30_pct"])
            if "score_urgencia" in df_accel_show.columns:
                styler_accel = styler_accel.applymap(highlight_urgency_score, subset=["score_urgencia"])
            st.dataframe(styler_accel, use_container_width=True)
            st.download_button(
                "⬇️ Baixar acelerando venda (CSV)",
                data=accel_view.to_csv(index=False, sep=";").encode("utf-8-sig"),
                file_name=ARQ_REPOSICAO_GERAL_ACCEL,
                mime="text/csv",
                key="geral_download_accel"
            )

        st.caption("Obs.: esta aba agora combina a base analítica do `copiloto_vendas_v3.py` com a camada colaborativa do Postgres (`dash_ops.vw_reposicao_estado_ean`).")

# =========================
# TAB 6: Copiloto de Vendas
# =========================
with tab6:
    render_section_header(
        "🧠 Copiloto de Vendas",
        "Motor de decisão comercial: FULL, reposição, escala e ativação de anúncios"
    )

    base_copiloto = repl_decisao.copy() if repl_decisao is not None and not repl_decisao.empty else repl_geral.copy()
    copiloto = build_copiloto_views(full_candidatos_home, base_copiloto, repl_geral_accel)

    open_modern_card("Validador do Copiloto", "Saúde do pipeline e leitura da base de decisão")
    render_copiloto_validadores(base_copiloto)
    st.caption("Nesta aba, as tabelas abaixo continuam limitadas aos TOP 20 por frente para manter foco operacional e velocidade de leitura.")
    close_card()

    open_modern_card("Mapa executivo do Copiloto", "Distribuição das oportunidades encontradas automaticamente")
    st.altair_chart(
        alt.Chart(copiloto["summary"]).mark_bar().encode(
            x=alt.X("qtd:Q", title="Quantidade"),
            y=alt.Y("oportunidade:N", sort="-x", title="Frente"),
            tooltip=["oportunidade:N", "qtd:Q"]
        ),
        use_container_width=True
    )
    close_card()

    c1, c2 = st.columns(2)
    with c1:
        open_modern_card("Fila operacional — AGIR HOJE", "Itens com prioridade máxima para tratamento imediato")
        df = copiloto["agir_hoje"].copy()
        if df.empty:
            st.info("Nenhum item em AGIR_HOJE detectado no CSV atual.")
        else:
            cols = [c for c in ["EAN", "SKU", "Descricao", "Marca", "bucket_prioridade", "status_cobertura_90d", "qtd_compra_sugerida", "recomendacao_logistica", "impacto_prioridade"] if c in df.columns]
            st.dataframe(df[cols].head(20), use_container_width=True, hide_index=True)
        close_card()

    with c2:
        open_modern_card("Fila operacional — COMPRAR", "SKUs com compra sugerida e maior impacto")
        df = copiloto["comprar"].copy()
        if df.empty:
            st.info("Nenhum SKU com compra sugerida no CSV atual.")
        else:
            cols = [c for c in ["EAN", "SKU", "Descricao", "Marca", "qtd_compra_sugerida", "reposicao_sugerida_90d", "impacto_receita_30d_est", "impacto_prioridade"] if c in df.columns]
            st.dataframe(df[cols].head(20), use_container_width=True, hide_index=True)
        close_card()

    c3, c4 = st.columns(2)
    with c3:
        open_modern_card("Produtos com potencial de escalar", "Itens acelerando que merecem reforço comercial")
        df = copiloto["acelerar"].copy()
        if df.empty:
            st.info("Nenhum item acelerando disponível no recorte atual.")
        else:
            cols = [c for c in ["EAN", "SKU", "Descricao", "Marca", "vendas_30d", "forecast_unid_90d", "crescimento_ult30_vs_prev30_pct", "score_urgencia"] if c in df.columns]
            st.dataframe(df[cols].head(20), use_container_width=True, hide_index=True)
        close_card()

    with c4:
        open_modern_card("Produtos que precisam de anúncio", "Itens com estoque saudável e baixa tração")
        df = copiloto["anuncio"].copy()
        if df.empty:
            st.info("Nenhum item com necessidade clara de ativação de anúncio.")
        else:
            cols = [c for c in ["EAN", "SKU", "Descricao", "Marca", "status_cobertura_90d", "trend_status", "reposicao_sugerida_90d", "forecast_unid_90d"] if c in df.columns]
            st.dataframe(df[cols].head(20), use_container_width=True, hide_index=True)
        close_card()

    open_modern_card("Como usar o Copiloto", "Fluxo simples para o time comercial e operacional")
    st.markdown(
        """
        1. **AGIR HOJE**: trate primeiro os itens com bucket de prioridade máxima ou cobertura crítica.
        2. **COMPRAR**: execute compras sugeridas com maior impacto comercial estimado.
        3. **Entrar em FULL**: proteger itens que já mostraram tração e merecem melhora de serviço.
        4. **Potencial de escala**: reforçar mídia, preço e estoque dos itens acelerando.
        5. **Precisa anúncio**: ativar tráfego em itens com estoque saudável e baixa rotação.
        """
    )
    close_card()


# =========================
# TAB 7: Centro de Alertas
# =========================
with tab7:
    render_section_header(
        "🚨 Centro de Alertas",
        "Fila consolidada de risco e oportunidade para compras, FULL, escala e ativação comercial"
    )

    df_alert = ensure_alertas_operacionais(alertas_operacionais)

    open_modern_card("Resumo dos alertas", "Leitura rápida por severidade")
    render_alertas_summary(df_alert)
    if df_alert.empty:
        st.info("Nenhum alerta operacional encontrado. Rode o copiloto para gerar alertas_operacionais.csv.")
    else:
        st.caption(f"Arquivo base: {ARQ_ALERTAS_OPERACIONAIS} | Atualizado em {get_csv_last_update_str(ARQ_ALERTAS_OPERACIONAIS)}")
    close_card()

    if not df_alert.empty:
        f1, f2, f3 = st.columns([1.2, 1.2, 1.6])
        with f1:
            sev_opts = ["Todos"] + sorted(df_alert["severidade"].dropna().astype(str).str.upper().unique().tolist())
            sev_sel = st.selectbox("Severidade", sev_opts, index=0, key="alert_sev")
        with f2:
            frente_opts = ["Todos"] + sorted(df_alert["frente"].dropna().astype(str).unique().tolist())
            frente_sel = st.selectbox("Frente", frente_opts, index=0, key="alert_frente")
        with f3:
            busca_alerta = st.text_input("Buscar EAN / SKU / descrição / mensagem", key="alert_busca").strip().lower()

        alert_filtrado = df_alert.copy()
        if sev_sel != "Todos":
            alert_filtrado = alert_filtrado[alert_filtrado["severidade"].astype(str).str.upper() == sev_sel]
        if frente_sel != "Todos":
            alert_filtrado = alert_filtrado[alert_filtrado["frente"].astype(str) == frente_sel]
        if busca_alerta:
            blob = (
                alert_filtrado["EAN"].astype(str) + " " +
                alert_filtrado["SKU"].astype(str) + " " +
                alert_filtrado["Descricao"].astype(str) + " " +
                alert_filtrado["mensagem"].astype(str)
            ).str.lower()
            alert_filtrado = alert_filtrado[blob.str.contains(re.escape(busca_alerta), na=False)]

        open_modern_card("Tabela operacional de alertas", "Priorizada por severidade, impacto e score")
        cols_show = [c for c in ["severidade","frente","tipo_alerta","EAN","SKU","Descricao","mensagem","acao_recomendada","impacto_prioridade","score_prioridade_sku","status_alerta"] if c in alert_filtrado.columns]
        st.dataframe(alert_filtrado[cols_show], use_container_width=True, hide_index=True)
        st.download_button(
            "⬇️ Baixar alertas_operacionais.csv",
            data=Path(ARQ_ALERTAS_OPERACIONAIS).read_bytes() if file_exists(ARQ_ALERTAS_OPERACIONAIS) else b"",
            file_name=ARQ_ALERTAS_OPERACIONAIS,
            mime="text/csv",
            key="dl_alertas_operacionais"
        )
        close_card()

        open_modern_card("Leitura de ação", "Como usar os alertas na rotina")
        st.markdown(
            """
            1. **ALTA**: tratar no mesmo dia, priorizando ruptura iminente e itens em **AGIR_HOJE**.
            2. **MÉDIA**: resolver na semana, com foco em oportunidades de **FULL** e itens acelerando com risco.
            3. **BAIXA**: monitorar e acionar frente comercial quando o estoque estiver saudável e a tração baixa.
            """
        )
        close_card()


# =========================
# TAB 7: Performance Operacional
# =========================
with tab7:
    render_section_header(
        "📊 Performance Operacional",
        "SLA, alertas abertos, tempo médio de resolução e accountability do time"
    )

    tracking_perf = build_tracking_perf(alertas_tracking)
    df_track = tracking_perf["df"]

    open_modern_card("Resumo da operação", "Visão de SLA e execução dos alertas")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_metric_card("Alertas abertos", metric_int(tracking_perf["abertos"]), "Fila atual em aberto", "#dc2626")
    with c2:
        render_metric_card("Concluídos", metric_int(tracking_perf["concluidos"]), "Histórico concluído", "#16a34a")
    with c3:
        render_metric_card("SLA médio (h)", f"{tracking_perf['sla_medio_h']:.1f}", "Tempo médio para concluir", "#2563eb")
    with c4:
        render_metric_card("Vencidos > 24h", metric_int(tracking_perf["vencidos"]), "Alertas abertos fora do SLA", "#f59e0b")
    if df_track.empty:
        st.info("Ainda não há tracking de alertas. Rode o copiloto para gerar ou atualizar o arquivo alertas_tracking.csv.")
    else:
        st.caption(f"Arquivo base: {ARQ_ALERTAS_TRACKING} | Atualizado em {get_csv_last_update_str(ARQ_ALERTAS_TRACKING)}")
    close_card()

    if not df_track.empty:
        f1, f2, f3 = st.columns([1.2, 1.2, 1.6])
        with f1:
            status_opts = ["Todos"] + sorted(df_track["status"].dropna().astype(str).str.upper().unique().tolist())
            status_sel = st.selectbox("Status", status_opts, index=0, key="perf_status")
        with f2:
            owner_opts = ["Todos"] + sorted([x for x in df_track["owner"].fillna("").astype(str).replace({"": "Sem owner"}).unique().tolist()])
            owner_sel = st.selectbox("Owner", owner_opts, index=0, key="perf_owner")
        with f3:
            busca_perf = st.text_input("Buscar EAN / tipo de alerta", key="perf_busca").strip().lower()

        perf_f = df_track.copy()
        perf_f["owner_exib"] = perf_f["owner"].fillna("").astype(str).replace({"": "Sem owner"})
        if status_sel != "Todos":
            perf_f = perf_f[perf_f["status"].astype(str).str.upper() == status_sel]
        if owner_sel != "Todos":
            perf_f = perf_f[perf_f["owner_exib"] == owner_sel]
        if busca_perf:
            blob = (perf_f["EAN"].astype(str) + " " + perf_f["tipo_alerta"].astype(str) + " " + perf_f["owner_exib"].astype(str)).str.lower()
            perf_f = perf_f[blob.str.contains(re.escape(busca_perf), na=False)]

        open_modern_card("Tracking de alertas", "Base operacional para acompanhamento de dono, status e tempo")
        cols_show = [c for c in ["EAN", "tipo_alerta", "status", "owner_exib", "data_inicio", "data_conclusao", "tempo_aberto_h"] if c in perf_f.columns]
        df_show = perf_f[cols_show].copy().rename(columns={"owner_exib": "owner"})
        st.dataframe(df_show.sort_values(["status", "data_inicio"], ascending=[True, False]), use_container_width=True, hide_index=True)
        close_card()

        open_modern_card("Performance por responsável", "Resolvidos e tempo médio de conclusão")
        if tracking_perf["por_owner"].empty:
            st.info("Sem alertas concluídos ainda para cálculo de performance por owner.")
        else:
            st.dataframe(tracking_perf["por_owner"], use_container_width=True, hide_index=True)
        close_card()
