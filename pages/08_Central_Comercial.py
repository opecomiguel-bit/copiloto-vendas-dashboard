from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st

from shared_db import run_query_df, write_query

st.set_page_config(page_title="Central Comercial", layout="wide", page_icon="📈")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    :root {
        --brand:#2563eb;--green:#16a34a;--red:#dc2626;--amber:#d97706;
        --gray50:#f8fafc;--gray200:#e2e8f0;--text:#0f172a;--soft:#64748b;
        --shadow:0 4px 18px rgba(15,23,42,0.07);
    }
    .block-container {padding-top:.9rem;padding-bottom:2rem;max-width:100rem;}
    .cc-hero {
        background:linear-gradient(135deg,#0f172a 0%,#134e4a 60%,#059669 100%);
        border-radius:20px;padding:18px 26px;margin-bottom:14px;
        box-shadow:0 10px 30px rgba(15,23,42,0.16);
    }
    .cc-hero-title {font-size:24px;font-weight:900;color:#fff;line-height:1.2;margin:0 0 4px;}
    .cc-hero-sub   {font-size:13px;color:#a7f3d0;margin:0;}
    .cc-kpi {
        background:#fff;border:1px solid var(--gray200);
        border-left:5px solid var(--brand);border-radius:16px;
        padding:14px 16px 12px;box-shadow:var(--shadow);
    }
    .cc-kpi-label {font-size:11px;color:var(--soft);font-weight:700;
                   letter-spacing:.06em;text-transform:uppercase;margin-bottom:5px;}
    .cc-kpi-value {font-size:26px;font-weight:900;color:var(--text);line-height:1.1;}
    .cc-kpi-sub   {font-size:11px;color:var(--soft);margin-top:3px;}
    .cc-card {
        background:#fff;border:1px solid var(--gray200);border-radius:16px;
        padding:16px 16px 10px;box-shadow:var(--shadow);margin-bottom:12px;
    }
    .cc-card-title {font-size:17px;font-weight:800;color:var(--text);margin-bottom:2px;}
    .cc-card-sub   {font-size:12px;color:var(--soft);margin-bottom:12px;}
    div[data-testid="stDataFrame"] {
        border-radius:14px;overflow:hidden;
        border:1px solid var(--gray200);box-shadow:var(--shadow);
    }
    div[data-testid="stMetric"] {
        background:#fff;border-radius:14px;
        border:1px solid var(--gray200);
        box-shadow:var(--shadow);padding:12px 16px;
    }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_series(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    if col in df.columns:
        return df[col].fillna(default).astype(str)
    return pd.Series([default] * len(df), index=df.index, dtype="object")


def _norm_key(v: object) -> str:
    s = str(v or "").strip()
    return s[:-2].upper() if s.endswith(".0") else s.upper()


@st.cache_data(ttl=600, show_spinner=False)
def _csv_sep(path: str) -> str:
    try:
        first_line = Path(path).read_text(encoding="utf-8-sig", errors="ignore").splitlines()[0]
        return ";" if first_line.count(";") > first_line.count(",") else ","
    except Exception:
        return ";"


@st.cache_data(ttl=600, show_spinner=False)
def _read_local_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, sep=_csv_sep(path), encoding="utf-8-sig", low_memory=False)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _load_channel30() -> pd.DataFrame:
    p_master = Path("base_vendas_master.parquet")
    if not p_master.exists():
        return pd.DataFrame(columns=["marketplace", "vendas_30d", "marketplace_key"])

    qty_candidates = ["Qtde", "Quantidade", "Qtd"]
    base_cols = ["Data", "Canal"]
    m = None
    for qty_col in qty_candidates:
        try:
            m = pd.read_parquet(p_master, columns=base_cols + [qty_col])
            break
        except Exception:
            continue
    if m is None or m.empty or "Data" not in m.columns or "Canal" not in m.columns:
        return pd.DataFrame(columns=["marketplace", "vendas_30d", "marketplace_key"])

    qty_col = next((c for c in qty_candidates if c in m.columns), None)
    if not qty_col:
        return pd.DataFrame(columns=["marketplace", "vendas_30d", "marketplace_key"])

    m["Data"] = pd.to_datetime(m["Data"], errors="coerce")
    m = m.dropna(subset=["Data"])
    if m.empty:
        return pd.DataFrame(columns=["marketplace", "vendas_30d", "marketplace_key"])

    max_d = m["Data"].max()
    if pd.isna(max_d):
        return pd.DataFrame(columns=["marketplace", "vendas_30d", "marketplace_key"])

    m30 = m.loc[m["Data"] >= max_d - pd.Timedelta(days=29), ["Canal", qty_col]].copy()
    m30[qty_col] = pd.to_numeric(m30[qty_col], errors="coerce").fillna(0)
    channel30 = m30.groupby("Canal", as_index=False)[qty_col].sum().rename(
        columns={"Canal": "marketplace", qty_col: "vendas_30d"}
    )
    channel30["marketplace_key"] = channel30["marketplace"].astype(str).str.strip().str.upper()
    return channel30


@st.cache_data(ttl=600, show_spinner=False)
def _load_operational_sources() -> tuple[pd.DataFrame, pd.DataFrame]:
    rg = _read_local_csv("reposicao_geral_estoque.csv")
    rd = _read_local_csv("reposicao_decisao_sku.csv")
    base = pd.DataFrame()

    if not rg.empty:
        base = rg.copy()
    if not rd.empty:
        if base.empty:
            base = rd.copy()
        else:
            merge_key = next((k for k in ["EAN", "SKU"] if k in base.columns and k in rd.columns), None)
            if merge_key:
                preferred = {
                    "impacto_prioridade",
                    "bucket_prioridade",
                    "recomendacao_compra",
                    "recomendacao_logistica",
                    "motivo_recomendacao",
                    "responsavel",
                }
                extras = [c for c in rd.columns if c not in base.columns or c in preferred]
                if merge_key not in extras:
                    extras.append(merge_key)
                base = base.merge(rd[extras], on=merge_key, how="left")

    if not base.empty:
        for col in [
            "EAN", "SKU", "Marca", "Descricao", "trend_status",
            "status_cobertura_90d", "bucket_prioridade",
            "recomendacao_compra", "recomendacao_logistica",
        ]:
            if col in base.columns:
                base[col] = base[col].astype(str)

        for col in [
            "estoque_atual", "vendas_30d", "impacto_prioridade",
            "score_prioridade_sku", "crescimento_ult30_vs_prev30_pct",
            "qtd_compra_sugerida",
        ]:
            if col in base.columns:
                base[col] = pd.to_numeric(base[col], errors="coerce")

        base["ean_key"] = base["EAN"].map(_norm_key) if "EAN" in base.columns else ""
        base["sku_key"] = base["SKU"].map(_norm_key) if "SKU" in base.columns else ""
        base["marca_key"] = _safe_series(base, "Marca").str.strip().str.upper()
        cat_col = next((c for c in ["categoria", "Categoria"] if c in base.columns), None)
        base["categoria_key"] = _safe_series(base, cat_col).str.strip().str.upper() if cat_col else ""

    return base, _load_channel30()


def _classificar(vendas_30d: float, estoque_atual: float, has_critico: bool, has_accel: bool, score: float):
    v = float(vendas_30d or 0)
    s = float(score or 0)
    if has_critico and v > 0:
        return "risco_ruptura", "Risco de ruptura", "Priorizar reposição/ajuste da campanha.", 95 + min(s * 5, 5)
    if v <= 0:
        return "campanha_sem_tracao", "Campanha sem tração", "Revisar verba, oferta e criativo.", 55
    if has_accel or s >= 0.55:
        return "oportunidade_escalar", "Oportunidade de escalar", "Reforçar comercialmente.", 80 + min(s * 10, 10)
    return "acao_sem_insight", "Sem insight associado", "Nenhum sinal operacional forte.", 20


# ── Banco ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=180, show_spinner=False)
def load_base_data():
    queries = {
        "active": """
            SELECT id, nome_acao, tipo_acao, escopo_tipo, escopo_valor, canal,
                   percentual_desconto, data_inicio, data_fim, prioridade, status
            FROM vw_commercial_actions_active
        """,
        "week": """
            SELECT id, nome_acao, tipo_acao, escopo_tipo, escopo_valor, canal,
                   percentual_desconto, data_inicio, data_fim, prioridade, status
            FROM vw_commercial_actions_week
        """,
        "upcoming": """
            SELECT id, nome_acao, tipo_acao, escopo_tipo, escopo_valor, canal,
                   percentual_desconto, data_inicio, data_fim, prioridade, status
            FROM vw_commercial_actions_upcoming
        """,
        "all": """
            SELECT id, nome_acao, tipo_acao, escopo_tipo, escopo_valor, canal, marca,
                   categoria, sku, percentual_desconto, data_inicio, data_fim,
                   origem_decisao, responsavel, prioridade, status, observacoes, created_at
            FROM commercial_actions
            ORDER BY created_at DESC
        """,
    }
    dfs: dict[str, pd.DataFrame] = {}
    dbname = None
    for key, sql in queries.items():
        df, db = run_query_df(sql)
        dfs[key] = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        if db:
            dbname = db

    all_df = dfs.get("all", pd.DataFrame()).copy()
    if not all_df.empty:
        for c in ["data_inicio", "data_fim", "created_at"]:
            if c in all_df.columns:
                all_df[c] = pd.to_datetime(all_df[c], errors="coerce")
        today = pd.Timestamp(date.today())

        if dfs.get("active", pd.DataFrame()).empty:
            active = all_df.copy()
            if "status" in active.columns:
                active = active[active["status"].astype(str).str.lower().isin(["ativa", "planejada"])]
            if "data_inicio" in active.columns and "data_fim" in active.columns:
                active = active[(active["data_inicio"].fillna(today) <= today) & (active["data_fim"].fillna(today) >= today)]
            dfs["active"] = active

        if dfs.get("week", pd.DataFrame()).empty:
            week = all_df.copy()
            start = today.normalize()
            end = start + pd.Timedelta(days=7)
            if "data_inicio" in week.columns:
                week = week[(week["data_inicio"].fillna(end) <= end)]
            if "data_fim" in week.columns:
                week = week[(week["data_fim"].fillna(start) >= start)]
            dfs["week"] = week

        if dfs.get("upcoming", pd.DataFrame()).empty:
            upcoming = all_df.copy()
            if "data_inicio" in upcoming.columns:
                upcoming = upcoming[upcoming["data_inicio"] > today]
            dfs["upcoming"] = upcoming

    return (
        dfs.get("active", pd.DataFrame()),
        dfs.get("week", pd.DataFrame()),
        dfs.get("upcoming", pd.DataFrame()),
        dfs.get("all", pd.DataFrame()),
        dbname,
    )


@st.cache_data(ttl=180, show_spinner=False)
def load_remote_opportunities():
    sql_opp = """
        SELECT action_id, nome_acao, escopo_tipo, escopo_valor, marketplace, sku,
               produto, estoque_atual, vendas_30d, insight_titulo,
               classificacao_comercial, score_oportunidade, acao_sugerida
        FROM vw_commercial_opportunities
        ORDER BY score_oportunidade DESC, action_id DESC
    """
    sql_sum = """
        SELECT classificacao_comercial, COUNT(*) AS total
        FROM vw_commercial_opportunities
        GROUP BY classificacao_comercial
        ORDER BY total DESC
    """
    df_opp, _ = run_query_df(sql_opp)
    df_sum, _ = run_query_df(sql_sum)
    return (
        df_opp if isinstance(df_opp, pd.DataFrame) else pd.DataFrame(),
        df_sum if isinstance(df_sum, pd.DataFrame) else pd.DataFrame(),
    )


# ── Inteligência local ────────────────────────────────────────────────────────

def _aggregate_scope(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    if df.empty or key_col not in df.columns:
        return pd.DataFrame(columns=[key_col, "vendas_30d", "estoque_atual", "score_prioridade_sku", "has_critico", "has_accel", "produto", "sku_ref"])

    work = df.copy()
    work["vendas_30d"] = pd.to_numeric(work.get("vendas_30d"), errors="coerce").fillna(0)
    work["estoque_atual"] = pd.to_numeric(work.get("estoque_atual"), errors="coerce").fillna(0)
    work["score_prioridade_sku"] = pd.to_numeric(work.get("score_prioridade_sku"), errors="coerce").fillna(0)
    work["has_critico"] = _safe_series(work, "status_cobertura_90d").str.upper().isin(["CRÍTICO", "URGENTE"])
    work["has_accel"] = _safe_series(work, "trend_status").str.upper().str.contains("ACELER", na=False)
    work["produto"] = _safe_series(work, "Descricao")
    work["sku_ref"] = _safe_series(work, "SKU")

    grp = work.groupby(key_col, dropna=False, as_index=False).agg(
        vendas_30d=("vendas_30d", "sum"),
        estoque_atual=("estoque_atual", "sum"),
        score_prioridade_sku=("score_prioridade_sku", "max"),
        has_critico=("has_critico", "max"),
        has_accel=("has_accel", "max"),
        produto=("produto", "first"),
        sku_ref=("sku_ref", "first"),
    )
    return grp


@st.cache_data(ttl=300, show_spinner=False)
def build_local_commercial_opportunities(df_actions: pd.DataFrame) -> pd.DataFrame:
    if df_actions is None or df_actions.empty:
        return pd.DataFrame()

    base, channel30 = _load_operational_sources()
    if base.empty and channel30.empty:
        return pd.DataFrame()

    acts = df_actions.copy()
    acts["_sku_key"] = _safe_series(acts, "sku").map(_norm_key)
    acts["_marca_key"] = _safe_series(acts, "marca").str.strip().str.upper()
    acts["_cat_key"] = _safe_series(acts, "categoria").str.strip().str.upper()
    acts["_canal_key"] = _safe_series(acts, "canal").str.strip().str.upper()
    acts["_escopo"] = _safe_series(acts, "escopo_tipo").str.strip().str.lower()
    acts["_escval"] = _safe_series(acts, "escopo_valor").str.strip()

    sku_agg = _aggregate_scope(base, "sku_key") if not base.empty else pd.DataFrame()
    ean_agg = _aggregate_scope(base, "ean_key") if not base.empty else pd.DataFrame()
    marca_agg = _aggregate_scope(base, "marca_key") if not base.empty else pd.DataFrame()
    categoria_agg = _aggregate_scope(base, "categoria_key") if not base.empty else pd.DataFrame()

    def _to_map(df: pd.DataFrame, key_col: str) -> dict[str, dict]:
        return {} if df.empty else df.set_index(key_col).to_dict("index")

    sku_map = _to_map(sku_agg, "sku_key")
    sku_map.update({k: v for k, v in _to_map(ean_agg, "ean_key").items() if k not in sku_map})
    marca_map = _to_map(marca_agg, "marca_key")
    categoria_map = _to_map(categoria_agg, "categoria_key")
    canal_map = {} if channel30.empty else channel30.set_index("marketplace_key")["vendas_30d"].to_dict()

    geral = {
        "vendas_30d": float(pd.to_numeric(base.get("vendas_30d"), errors="coerce").fillna(0).sum()) if not base.empty else 0.0,
        "estoque_atual": float(pd.to_numeric(base.get("estoque_atual"), errors="coerce").fillna(0).sum()) if not base.empty else 0.0,
        "score_prioridade_sku": float(pd.to_numeric(base.get("score_prioridade_sku"), errors="coerce").fillna(0).max()) if not base.empty else 0.0,
        "has_critico": bool(_safe_series(base, "status_cobertura_90d").str.upper().isin(["CRÍTICO", "URGENTE"]).any()) if not base.empty else False,
        "has_accel": bool(_safe_series(base, "trend_status").str.upper().str.contains("ACELER", na=False).any()) if not base.empty else False,
        "produto": "",
        "sku_ref": "",
    }

    rows: list[dict] = []
    for _, row in acts.iterrows():
        escopo = row["_escopo"]
        action_id = row.get("id", row.get("action_id", ""))
        nome_acao = str(row.get("nome_acao", ""))
        canal = str(row.get("canal", "")).strip()
        escopo_valor = row["_escval"]

        if escopo == "canal":
            key = row["_canal_key"] or escopo_valor.upper()
            v30 = float(canal_map.get(key, 0.0))
            cl, ins, ac, sc = _classificar(v30, 0, False, v30 > 0, 0)
            rows.append({
                "action_id": action_id, "nome_acao": nome_acao, "escopo_tipo": escopo,
                "escopo_valor": escopo_valor, "marketplace": canal or escopo_valor,
                "sku": "", "produto": "", "estoque_atual": 0.0, "vendas_30d": round(v30, 2),
                "insight_titulo": ins, "classificacao_comercial": cl,
                "score_oportunidade": round(sc, 2), "acao_sugerida": ac,
            })
            continue

        if escopo == "sku":
            agg = sku_map.get(row["_sku_key"] or _norm_key(escopo_valor))
        elif escopo == "marca":
            agg = marca_map.get(row["_marca_key"] or escopo_valor.upper())
        elif escopo == "categoria":
            agg = categoria_map.get(row["_cat_key"] or escopo_valor.upper())
        elif escopo == "geral":
            agg = geral
        else:
            agg = None

        if not agg:
            rows.append({
                "action_id": action_id, "nome_acao": nome_acao, "escopo_tipo": escopo,
                "escopo_valor": escopo_valor, "marketplace": canal,
                "sku": str(row.get("sku", "")) or escopo_valor, "produto": "",
                "estoque_atual": 0.0, "vendas_30d": 0.0,
                "insight_titulo": "Sem insight associado",
                "classificacao_comercial": "acao_sem_insight",
                "score_oportunidade": 0.0, "acao_sugerida": "Mapear base operacional",
            })
            continue

        v = float(agg.get("vendas_30d", 0.0))
        e = float(agg.get("estoque_atual", 0.0))
        s = float(agg.get("score_prioridade_sku", 0.0))
        hc = bool(agg.get("has_critico", False))
        ha = bool(agg.get("has_accel", False))
        cl, ins, ac, sc = _classificar(v, e, hc, ha, s)
        rows.append({
            "action_id": action_id, "nome_acao": nome_acao, "escopo_tipo": escopo,
            "escopo_valor": escopo_valor, "marketplace": canal,
            "sku": str(agg.get("sku_ref", "")) or str(row.get("sku", "")) or escopo_valor,
            "produto": str(agg.get("produto", "")),
            "estoque_atual": round(e, 2), "vendas_30d": round(v, 2),
            "insight_titulo": ins, "classificacao_comercial": cl,
            "score_oportunidade": round(sc, 2), "acao_sugerida": ac,
        })

    return pd.DataFrame(rows)


def load_intelligence_data(df_all: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_local = build_local_commercial_opportunities(df_all)
    if not df_local.empty:
        df_sum = (
            df_local.groupby("classificacao_comercial", as_index=False)
            .size()
            .rename(columns={"size": "total"})
            .sort_values("total", ascending=False)
        )
        return df_local, df_sum
    return load_remote_opportunities()


# ── Writes ────────────────────────────────────────────────────────────────────

def insert_action(nome_acao, tipo_acao, escopo_tipo, escopo_valor, canal, marca,
                  categoria, sku, percentual_desconto, data_inicio, data_fim,
                  origem_decisao, responsavel, prioridade, status, observacoes):
    sql = """
        INSERT INTO commercial_actions (
            nome_acao, tipo_acao, escopo_tipo, escopo_valor, canal, marca, categoria, sku,
            percentual_desconto, data_inicio, data_fim, origem_decisao, responsavel,
            prioridade, status, observacoes
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    params = (
        nome_acao, tipo_acao, escopo_tipo,
        escopo_valor or None, canal or None, marca or None,
        categoria or None, sku or None,
        float(percentual_desconto or 0),
        data_inicio, data_fim,
        origem_decisao or None, responsavel or None,
        prioridade, status, observacoes or None,
    )
    ok, err = write_query(sql, params)
    return {"ok": ok, "error": err}


def update_action_status(action_id, status, responsavel, observacoes):
    sql = """
        UPDATE commercial_actions
        SET status=%s, responsavel=%s, observacoes=%s
        WHERE id=%s
    """
    ok, err = write_query(sql, (status, responsavel or None, observacoes or None, int(action_id)))
    return {"ok": ok, "error": err}


# ── MAIN ──────────────────────────────────────────────────────────────────────
with st.spinner("Carregando central comercial..."):
    df_active, df_week, df_upcoming, df_all, dbname = load_base_data()
    df_opportunities, df_op_summary = load_intelligence_data(df_all)

st.markdown(f"""
<div class="cc-hero">
    <div class="cc-hero-title">📈 Central Comercial & Oportunidades</div>
    <p class="cc-hero-sub">Banco: {dbname or 'N/D'} · Inteligência enriquecida com dados operacionais locais</p>
</div>
""", unsafe_allow_html=True)

if df_all.empty:
    st.info("ℹ️ A base comercial ainda não foi migrada para este banco. A página entrou em fallback seguro e ficará funcional assim que as tabelas/views forem criadas.")

k1, k2, k3 = st.columns(3)
with k1:
    st.markdown(
        f'<div class="cc-kpi" style="border-left-color:#dc2626;">'
        f'<div class="cc-kpi-label">🔥 Ações Ativas</div>'
        f'<div class="cc-kpi-value">{len(df_active)}</div>'
        f'<div class="cc-kpi-sub">campanhas em andamento</div></div>',
        unsafe_allow_html=True,
    )
with k2:
    st.markdown(
        f'<div class="cc-kpi" style="border-left-color:#7c3aed;">'
        f'<div class="cc-kpi-label">📅 Semana (7 dias)</div>'
        f'<div class="cc-kpi-value">{len(df_week)}</div>'
        f'<div class="cc-kpi-sub">ações nesta semana</div></div>',
        unsafe_allow_html=True,
    )
with k3:
    st.markdown(
        f'<div class="cc-kpi" style="border-left-color:#2563eb;">'
        f'<div class="cc-kpi-label">🚀 Próximas</div>'
        f'<div class="cc-kpi-value">{len(df_upcoming)}</div>'
        f'<div class="cc-kpi-sub">ações planejadas</div></div>',
        unsafe_allow_html=True,
    )

st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

aba_visao, aba_cadastro, aba_gestao, aba_inteligencia = st.tabs([
    "📆 Visão da Semana", "➕ Nova Campanha", "🛠️ Gestão", "🧠 Inteligência Comercial",
])

with aba_visao:
    st.markdown('<div class="cc-card"><div class="cc-card-title">📆 Linha do Tempo</div>'
                '<div class="cc-card-sub">Campanhas desta semana</div>',
                unsafe_allow_html=True)
    if df_week.empty:
        st.info("Nenhuma ação encontrada para a semana.")
    else:
        cols_show = [c for c in [
            "nome_acao", "tipo_acao", "escopo_tipo", "escopo_valor", "canal",
            "percentual_desconto", "data_inicio", "data_fim", "prioridade", "status",
        ] if c in df_week.columns]
        st.dataframe(df_week.sort_values("data_inicio")[cols_show], use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="cc-card"><div class="cc-card-title">🔎 Filtros — Todas as Campanhas</div>',
                unsafe_allow_html=True)
    if df_all.empty:
        st.warning("Ainda não há ações comerciais.")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            status_opts = ["Todos"] + sorted([x for x in df_all["status"].dropna().unique() if str(x).strip()])
            fs = st.selectbox("Status", status_opts, key="fs")
        with c2:
            canal_opts = ["Todos"] + sorted([x for x in df_all["canal"].dropna().unique() if str(x).strip()])
            fc = st.selectbox("Canal", canal_opts, key="fc")
        with c3:
            marca_opts = ["Todos"] + sorted([x for x in df_all["marca"].dropna().unique() if str(x).strip()])
            fm = st.selectbox("Marca", marca_opts, key="fm")

        filt = df_all
        if fs != "Todos":
            filt = filt[filt["status"].astype(str) == fs]
        if fc != "Todos":
            filt = filt[filt["canal"].astype(str) == fc]
        if fm != "Todos":
            filt = filt[filt["marca"].astype(str) == fm]

        cols_all = [c for c in [
            "id", "nome_acao", "tipo_acao", "escopo_tipo", "escopo_valor", "canal", "marca",
            "categoria", "sku", "percentual_desconto", "data_inicio", "data_fim",
            "prioridade", "status", "responsavel",
        ] if c in filt.columns]
        st.dataframe(filt[cols_all].sort_values("data_inicio", ascending=False), use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

with aba_cadastro:
    st.markdown('<div class="cc-card"><div class="cc-card-title">➕ Cadastrar nova campanha</div>',
                unsafe_allow_html=True)
    with st.form("form_nova_campanha", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            nome_acao = st.text_input("Nome da ação *")
            tipo_acao = st.selectbox("Tipo da ação *", ["desconto", "campanha", "sazonal", "combo", "push"])
            escopo_tipo = st.selectbox("Escopo *", ["geral", "marca", "categoria", "canal", "sku"])
            escopo_valor = st.text_input("Valor do escopo")
            canal = st.text_input("Canal")
            marca = st.text_input("Marca")
            categoria = st.text_input("Categoria")
            sku = st.text_input("SKU")
        with col2:
            percentual_desconto = st.number_input("% Desconto", 0.0, 100.0, 0.0, 0.5)
            data_inicio = st.date_input("Data início *", value=date.today())
            data_fim = st.date_input("Data fim *", value=date.today())
            origem_decisao = st.text_input("Origem da decisão")
            responsavel = st.text_input("Responsável")
            prioridade = st.selectbox("Prioridade", ["alta", "media", "baixa"])
            status = st.selectbox("Status", ["planejada", "ativa", "encerrada", "cancelada"])
        observacoes = st.text_area("Observações")
        submitted = st.form_submit_button("Salvar campanha")

        if submitted:
            erros = []
            if not nome_acao.strip():
                erros.append("Informe o nome da ação.")
            if data_fim < data_inicio:
                erros.append("Data fim não pode ser menor que data início.")
            for e in erros:
                st.error(e)
            if not erros:
                r = insert_action(
                    nome_acao.strip(), tipo_acao.lower(), escopo_tipo.lower(),
                    escopo_valor.strip(), canal.strip(), marca.strip(), categoria.strip(), sku.strip(),
                    percentual_desconto, data_inicio, data_fim, origem_decisao.strip(),
                    responsavel.strip(), prioridade.lower(), status.lower(), observacoes.strip(),
                )
                if r["ok"]:
                    st.success("Campanha cadastrada com sucesso.")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"Erro ao salvar: {r['error']}")
    st.markdown("</div>", unsafe_allow_html=True)

with aba_gestao:
    st.markdown('<div class="cc-card"><div class="cc-card-title">🛠️ Atualização rápida de status</div>',
                unsafe_allow_html=True)
    if df_all.empty:
        st.info("Nenhuma campanha cadastrada.")
    else:
        with st.form("form_gestao_campanha"):
            ids = df_all["id"].dropna().astype(int).tolist()
            action_id = st.selectbox("Selecione o ID", ids)
            current_row = df_all[df_all["id"].astype(int) == int(action_id)].iloc[0]
            c1, c2 = st.columns(2)
            status_list = ["planejada", "ativa", "encerrada", "cancelada"]
            cur_status = str(current_row.get("status", "")).lower()
            idx = status_list.index(cur_status) if cur_status in status_list else 0
            with c1:
                novo_status = st.selectbox("Novo status", status_list, index=idx)
            with c2:
                novo_responsavel = st.text_input("Responsável", value=str(current_row.get("responsavel", "")))
            nova_obs = st.text_area("Observações", value=str(current_row.get("observacoes", "")))
            if st.form_submit_button("Atualizar campanha"):
                r = update_action_status(action_id, novo_status.lower(), novo_responsavel.strip(), nova_obs.strip())
                if r["ok"]:
                    st.success("Campanha atualizada.")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"Erro: {r['error']}")
    st.markdown("</div>", unsafe_allow_html=True)

with aba_inteligencia:
    st.markdown('<div class="cc-card"><div class="cc-card-title">🧠 Inteligência Comercial</div>'
                '<div class="cc-card-sub">Cruzamento entre campanhas e sinais operacionais</div>',
                unsafe_allow_html=True)

    if not df_op_summary.empty:
        resumo_map = {r["classificacao_comercial"]: int(r["total"]) for _, r in df_op_summary.iterrows()}
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🔴 Risco Ruptura", resumo_map.get("risco_ruptura", 0))
        c2.metric("🟢 Oportunidade Escalar", resumo_map.get("oportunidade_escalar", 0))
        c3.metric("🟡 Sem Tração", resumo_map.get("campanha_sem_tracao", 0))
        c4.metric("⚪ Sem Insight", resumo_map.get("acao_sem_insight", 0))

    if not df_opportunities.empty:
        filtro_class = st.selectbox(
            "Classificação",
            ["Todos"] + sorted(df_opportunities["classificacao_comercial"].dropna().astype(str).unique().tolist()),
            key="filtro_class",
        )
        df_op_f = df_opportunities if filtro_class == "Todos" else df_opportunities[
            df_opportunities["classificacao_comercial"].astype(str) == filtro_class
        ]

        cols_op = [c for c in [
            "action_id", "nome_acao", "escopo_tipo", "escopo_valor", "marketplace", "sku", "produto",
            "estoque_atual", "vendas_30d", "insight_titulo", "classificacao_comercial",
            "score_oportunidade", "acao_sugerida",
        ] if c in df_op_f.columns]
        st.dataframe(df_op_f[cols_op].sort_values("score_oportunidade", ascending=False), use_container_width=True, hide_index=True)

        classif = df_op_f["classificacao_comercial"].astype(str) if "classificacao_comercial" in df_op_f.columns else pd.Series(dtype=str)
        total = len(df_op_f)
        risco = int((classif == "risco_ruptura").sum())
        escalar = int((classif == "oportunidade_escalar").sum())
        tracao = int((classif == "campanha_sem_tracao").sum())
        sem_i = int((classif == "acao_sem_insight").sum())
        st.info(
            f"**{total} cruzamentos comerciais analisados.** "
            f"{risco} riscos de ruptura · {escalar} oportunidades de escala · "
            f"{tracao} campanhas sem tração · {sem_i} sem insight. "
            f"Priorize primeiros os riscos, depois as oportunidades com maior score."
        )
    else:
        st.warning("Nenhuma oportunidade comercial encontrada.")

    st.markdown("</div>", unsafe_allow_html=True)
