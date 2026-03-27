from __future__ import annotations

"""
07_Insights_IA_otimizado.py
===========================
Otimizações aplicadas sem alterar a UX:
  • Normalização de score/tipo/prioridade feita uma única vez
  • Filtros reutilizam colunas auxiliares, evitando astype/str repetidos
  • Ranking top usa colunas já numéricas, sem assign/copy desnecessários
  • Opções de filtro cacheadas
  • Resumo executivo preparado com parsing estável e barato
  • Mantém visual e storytelling originais
"""

import json
from typing import Any

import pandas as pd
import streamlit as st

from shared_db import run_query, run_query_df

st.set_page_config(page_title="Insights IA", layout="wide")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    :root {
        color-scheme: light !important;
        --bg-main:#f4f7fb; --bg-card:#ffffff; --bg-card-soft:#f8fafc;
        --line-soft:#e5e7eb; --text-main:#0f172a; --text-soft:#64748b;
        --brand:#2563eb; --brand-2:#7c3aed; --danger:#dc2626;
        --warning:#d97706; --success:#059669;
        --shadow-soft:0 10px 30px rgba(15,23,42,0.08);
    }
    html,body,[data-testid="stAppViewContainer"],.stApp {
        background:var(--bg-main)!important; color:var(--text-main)!important;
        forced-color-adjust:none;
    }
    [data-testid="stHeader"] {
        background:rgba(244,247,251,0.92)!important; backdrop-filter:blur(8px);
    }
    .block-container {
        padding-top:1rem; padding-bottom:2rem;
        padding-left:1.15rem; padding-right:1.15rem; max-width:108rem;
    }
    section[data-testid="stSidebar"] {
        background:linear-gradient(180deg,#f8fbff 0%,#eef4ff 100%)!important;
        border-right:1px solid rgba(148,163,184,0.18);
        width:17rem!important; min-width:17rem!important;
    }
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stSlider label,
    section[data-testid="stSidebar"] .stRadio label {
        color:#0f172a!important; font-weight:700;
    }
    .hero-wrap {
        background:linear-gradient(135deg,#0f172a 0%,#172554 55%,#1d4ed8 100%);
        border-radius:24px; padding:1.35rem 1.5rem; color:white!important;
        box-shadow:var(--shadow-soft); border:1px solid rgba(255,255,255,0.08);
        margin-bottom:1rem;
    }
    .hero-title {font-size:2.2rem;font-weight:900;line-height:1.1;
                  color:white!important;margin:0 0 .35rem 0;}
    .hero-sub   {color:rgba(255,255,255,.84)!important;font-size:.98rem;
                  margin:0;line-height:1.55;}
    .pillbar {display:flex;gap:.6rem;flex-wrap:wrap;margin:.85rem 0 1rem 0;}
    .pill {background:rgba(37,99,235,.08);color:var(--brand)!important;
           border:1px solid rgba(37,99,235,.16);border-radius:999px;
           padding:.35rem .75rem;font-size:.82rem;font-weight:700;}
    .kpi-card {background:linear-gradient(180deg,#ffffff 0%,#fbfdff 100%);
               border:1px solid rgba(148,163,184,0.16);border-radius:18px;
               padding:1rem 1rem .9rem 1rem;box-shadow:var(--shadow-soft);
               min-height:120px;}
    .kpi-label {color:var(--text-soft)!important;font-size:.9rem;font-weight:700;
                margin-bottom:.45rem;}
    .kpi-value {color:var(--text-main)!important;font-size:2rem;font-weight:900;
                line-height:1;margin-bottom:.25rem;}
    .kpi-sub   {color:var(--text-soft)!important;font-size:.82rem;}
    .section-card {background:var(--bg-card);
                   border:1px solid rgba(148,163,184,0.16);border-radius:20px;
                   padding:1rem 1rem .4rem 1rem;box-shadow:var(--shadow-soft);
                   margin:1rem 0;}
    .section-title {font-size:1.55rem;font-weight:900;
                    color:var(--text-main)!important;margin-bottom:.2rem;}
    .section-sub   {color:var(--text-soft)!important;font-size:.92rem;
                    margin-bottom:.75rem;}
    .summary-box {background:linear-gradient(135deg,#eff6ff 0%,#eef2ff 100%);
                  border:1px solid rgba(59,130,246,0.18);border-radius:18px;
                  padding:1rem 1.1rem;color:#1e3a8a!important;line-height:1.65;
                  box-shadow:var(--shadow-soft);}
    .summary-item {background:#fff;border:1px solid rgba(148,163,184,0.14);
                   border-radius:16px;padding:.9rem 1rem;
                   box-shadow:var(--shadow-soft);margin-bottom:.75rem;}
    .summary-item-title {font-weight:900;color:var(--text-main)!important;
                          margin-bottom:.2rem;}
    .summary-item-text  {color:var(--text-soft)!important;line-height:1.55;}
    div[data-testid="stDataFrame"] {border-radius:18px;overflow:hidden;
        border:1px solid rgba(148,163,184,0.16);box-shadow:var(--shadow-soft);
        background:#fff;}
    .stTabs [data-baseweb="tab-list"] {
        gap:.5rem;overflow-x:auto;overflow-y:hidden;
        flex-wrap:nowrap!important;scrollbar-width:thin;padding-bottom:.2rem;
    }
    .stTabs [data-baseweb="tab"] {
        white-space:nowrap;border-radius:12px;padding:.5rem .9rem;
        background:#f8fafc;border:1px solid rgba(148,163,184,0.16);
        color:var(--text-main)!important;font-weight:700;
    }
    .stTabs [aria-selected="true"] {
        background:linear-gradient(135deg,#eff6ff 0%,#eef2ff 100%)!important;
        border-color:rgba(37,99,235,.25)!important;color:var(--brand)!important;
    }
    .stSelectbox>div>div,.stMultiSelect>div>div {
        background:#fff!important;border-radius:14px!important;
        border:1px solid rgba(148,163,184,0.22)!important;
    }
    .muted-note {color:var(--text-soft)!important;font-size:.86rem;}
</style>
""", unsafe_allow_html=True)


# ── Carregamento ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=180, show_spinner=False)
def load_insights() -> tuple[pd.DataFrame, str | None]:
    # 1) tenta a view legada
    df, dbname = run_query_df("SELECT * FROM vw_ai_insights_abertos")
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df, dbname

    # 2) fallback para tabela exportada pelo copiloto
    df2, dbname2 = run_query_df("SELECT * FROM dash_alertas")
    if isinstance(df2, pd.DataFrame) and not df2.empty:
        rename_map = {
            "tipo": "tipo_insight",
            "severidade": "prioridade",
            "EAN": "sku",
        }
        out = df2.rename(columns={k: v for k, v in rename_map.items() if k in df2.columns}).copy()
        if "titulo" not in out.columns:
            if "tipo_insight" in out.columns:
                out["titulo"] = out["tipo_insight"].astype(str)
            else:
                out["titulo"] = "Insight"
        if "descricao" not in out.columns:
            out["descricao"] = out["variacao_pct"].astype(str).radd("Variação: ").add("%") if "variacao_pct" in out.columns else ""
        if "acao_sugerida" not in out.columns:
            out["acao_sugerida"] = "Verificar causa raiz e plano de ação."
        if "score" not in out.columns:
            sev = out["prioridade"].astype(str).str.upper() if "prioridade" in out.columns else pd.Series(dtype=str)
            out["score"] = sev.map({"ALTA": 95, "MÉDIA": 75, "MEDIA": 75, "BAIXA": 55}).fillna(60)
        if "data_ref" not in out.columns:
            out["data_ref"] = ""
        if "tipo_insight" not in out.columns:
            out["tipo_insight"] = "alerta"
        return out, (dbname2 or dbname)

    return pd.DataFrame(), (dbname2 or dbname)


@st.cache_data(ttl=180, show_spinner=False)
def load_exec_summary() -> tuple[dict[str, Any], str | None]:
    # 1) tenta a view legada do resumo
    res = run_query("SELECT * FROM vw_ai_exec_summary_latest LIMIT 1", fetch="one")
    if res.get("ok") and res.get("data"):
        row, cols = res["data"]
        if row:
            return dict(zip(cols, row)), res.get("dbname")

    # 2) fallback: monta resumo executivo a partir da própria base de alertas
    df, dbname = run_query_df("SELECT * FROM dash_alertas")
    if df is None or df.empty:
        return {}, dbname

    sev = df["severidade"].astype(str).str.upper() if "severidade" in df.columns else pd.Series(dtype=str)
    alta = int(sev.isin(["ALTA"]).sum())
    media = int(sev.isin(["MÉDIA", "MEDIA"]).sum())
    total = len(df)
    tipos = df["tipo"].astype(str).value_counts().head(3).index.tolist() if "tipo" in df.columns else []
    resumo = {
        "resumo_texto": (
            f"Foram identificados {total} sinais no recorte atual, com {alta} alertas de alta severidade "
            f"e {media} de severidade média. "
            + (f"Principais tipos observados: {', '.join(tipos)}. " if tipos else "")
            + "Priorize a investigação dos alertas de alta severidade e acompanhe os itens reincidentes."
        ),
        "forcas_json": [],
        "fraquezas_json": [],
        "oportunidades_json": [],
        "riscos_json": [
            {
                "titulo": str(r.get("tipo", "Alerta")),
                "descricao": f"Severidade: {r.get('severidade', '')} | variação: {r.get('variacao_pct', '')}",
                "score": 95 if str(r.get("severidade", "")).upper() == "ALTA" else 75,
            }
            for _, r in df.head(10).iterrows()
        ],
        "data_ref": str(df["data_ref"].dropna().astype(str).iloc[0]) if "data_ref" in df.columns and not df["data_ref"].dropna().empty else "",
        "modo_resumo": "fallback_dash_alertas",
        "origem_modelo": "database_fallback",
    }
    return resumo, dbname


@st.cache_data(ttl=180, show_spinner=False)
def prepare_insights(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    if "score" in out.columns:
        out["score"] = pd.to_numeric(out["score"], errors="coerce")
    else:
        out["score"] = pd.Series([pd.NA] * len(out), dtype="Float64")

    if "tipo_insight" in out.columns:
        out["_tipo_norm"] = out["tipo_insight"].fillna("").astype(str).str.strip()
    else:
        out["tipo_insight"] = ""
        out["_tipo_norm"] = ""

    if "prioridade" in out.columns:
        out["_prio_norm"] = out["prioridade"].fillna("").astype(str).str.strip()
    else:
        out["prioridade"] = ""
        out["_prio_norm"] = ""

    return out


@st.cache_data(ttl=180, show_spinner=False)
def get_filter_options(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    if df is None or df.empty:
        return ["Todos"], ["Todos"]

    prioridades = ["Todos"] + sorted(x for x in df["_prio_norm"].dropna().unique().tolist() if str(x).strip())
    tipos = ["Todos"] + sorted(x for x in df["_tipo_norm"].dropna().unique().tolist() if str(x).strip())
    return prioridades, tipos


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]

    text = str(value).strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
        return []
    except Exception:
        return [text]


def kpi_card(title: str, value: Any, subtitle: str = "") -> None:
    st.markdown(
        f'<div class="kpi-card"><div class="kpi-label">{title}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-sub">{subtitle}</div></div>',
        unsafe_allow_html=True,
    )


def rank_top(df: pd.DataFrame, tipo: str, topn: int = 10) -> pd.DataFrame:
    if df.empty:
        return df

    sub = df.loc[df["_tipo_norm"].eq(tipo)]
    if sub.empty:
        return sub

    if "score" in sub.columns:
        return sub.nlargest(topn, "score")
    return sub.head(topn)


def render_summary_items(items: list[Any], title: str, empty_text: str) -> None:
    if not items:
        st.markdown(
            f"<div class='summary-item'><div class='summary-item-title'>{title}</div>"
            f"<div class='summary-item-text'>{empty_text}</div></div>",
            unsafe_allow_html=True,
        )
        return

    content = [f"<div class='summary-item'><div class='summary-item-title'>{title}</div>"]
    for item in items:
        if isinstance(item, dict):
            main = item.get("titulo") or item.get("sku") or "Sem título"
            desc = item.get("descricao") or item.get("acao_sugerida") or ""
            score = item.get("score")
            stxt = f" <b>(score {score})</b>" if score not in (None, "") else ""
            text = f"• <b>{main}</b>{stxt}" + (f": {desc}" if desc else "")
        else:
            text = f"• {item}"
        content.append(f"<div class='summary-item-text'>{text}</div>")
    content.append("</div>")
    st.markdown("".join(content), unsafe_allow_html=True)


# ── Resumo Executivo ──────────────────────────────────────────────────────────

def render_exec_summary(summary: dict[str, Any], filtered: pd.DataFrame, top_n: int) -> None:
    resumo_texto = str(summary.get("resumo_texto") or "").strip()
    forcas = parse_json_list(summary.get("forcas_json"))
    fraquezas = parse_json_list(summary.get("fraquezas_json"))
    oportunidades_j = parse_json_list(summary.get("oportunidades_json"))
    riscos = parse_json_list(summary.get("riscos_json"))

    if not resumo_texto:
        total = len(filtered)
        alta = int(filtered["_prio_norm"].eq("alta").sum()) if "_prio_norm" in filtered.columns else 0
        alert = int(filtered["_tipo_norm"].eq("alerta").sum()) if "_tipo_norm" in filtered.columns else 0
        opor = int(filtered["_tipo_norm"].eq("oportunidade").sum()) if "_tipo_norm" in filtered.columns else 0
        resumo_texto = (
            f"Foram identificados {total} insights no recorte atual, sendo {alta} de alta prioridade. "
            f"Há {alert} alertas operacionais e {opor} oportunidades mapeadas. "
            f"Recomendação imediata: atuar primeiro nos itens com maior score e risco operacional."
        )

    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">🧠 Resumo Executivo IA</div>
            <div class="section-sub">
                Leitura persistida em banco — menos custo e mais estabilidade.
            </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(f"<div class='summary-box'>{resumo_texto}</div>", unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        render_summary_items(forcas, "Forças", "Nenhuma força destacada.")
        render_summary_items(
            oportunidades_j[:top_n],
            "Oportunidades prioritárias",
            "Nenhuma oportunidade no resumo persistido.",
        )
    with c2:
        render_summary_items(fraquezas, "Fraquezas", "Nenhuma fraqueza destacada.")
        render_summary_items(
            riscos[:top_n],
            "Riscos prioritários",
            "Nenhum risco no resumo persistido.",
        )

    meta = [
        f"Data ref: {str(summary['data_ref'])[:10]}" if summary.get("data_ref") else "",
        f"Modo: {summary['modo_resumo']}" if summary.get("modo_resumo") else "",
        f"Origem: {summary['origem_modelo']}" if summary.get("origem_modelo") else "",
    ]
    meta = [m for m in meta if m]
    if meta:
        st.markdown(f"<div class='muted-note'>{' • '.join(meta)}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ── Painel de Prioridades ─────────────────────────────────────────────────────

def render_action_board(filtered: pd.DataFrame, top_n: int) -> None:
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">📊 Painel de Prioridades</div>
            <div class="section-sub">Base completa ordenada para leitura operacional.</div>
        """,
        unsafe_allow_html=True,
    )

    top_all = filtered.nlargest(top_n, "score") if "score" in filtered.columns else filtered.head(top_n)
    cols = [
        "data_ref",
        "sku",
        "tipo_insight",
        "titulo",
        "prioridade",
        "score",
        "impacto_estimado",
        "acao_sugerida",
    ]
    show = [c for c in cols if c in top_all.columns]
    st.dataframe(top_all[show], use_container_width=True, hide_index=True)
    st.markdown("<div class='muted-note'>Itens mais prioritários do recorte atual.</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ── Tabs ──────────────────────────────────────────────────────────────────────

def render_tabs(filtered: pd.DataFrame, top_n: int) -> None:
    t1, t2, t3, t4 = st.tabs([
        "🚨 Top Alertas",
        "📈 Top Oportunidades",
        "🗂️ Detalhe Analítico",
        "🧾 Base do Resumo",
    ])

    with t1:
        st.markdown(
            "<div class='section-card'><div class='section-sub'>Ranking dos principais riscos operacionais.</div>",
            unsafe_allow_html=True,
        )
        ta = rank_top(filtered, "alerta", top_n)
        cols = [x for x in ["sku", "titulo", "descricao", "acao_sugerida", "score"] if x in ta.columns]
        st.dataframe(ta[cols], use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with t2:
        st.markdown(
            "<div class='section-card'><div class='section-sub'>Itens com maior potencial de ganho ou destravamento.</div>",
            unsafe_allow_html=True,
        )
        to = rank_top(filtered, "oportunidade", top_n)
        cols = [x for x in ["sku", "titulo", "descricao", "acao_sugerida", "score"] if x in to.columns]
        st.dataframe(to[cols], use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with t3:
        st.markdown(
            "<div class='section-card'><div class='section-sub'>Visão ampla dos campos para auditoria.</div>",
            unsafe_allow_html=True,
        )
        hidden = [c for c in ["_tipo_norm", "_prio_norm"] if c in filtered.columns]
        detalhe = filtered.drop(columns=hidden, errors="ignore")
        st.dataframe(detalhe, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with t4:
        st.markdown(
            "<div class='section-card'><div class='section-sub'>Amostra dos insights usados para o resumo persistido.</div>",
            unsafe_allow_html=True,
        )
        cols = [
            x for x in ["sku", "tipo_insight", "titulo", "descricao", "acao_sugerida", "score", "prioridade"]
            if x in filtered.columns
        ]
        base = filtered.nlargest(max(10, top_n), "score") if "score" in filtered.columns else filtered.head(max(10, top_n))
        st.dataframe(base[cols], use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)


# ── MAIN ──────────────────────────────────────────────────────────────────────

with st.spinner("Carregando insights..."):
    raw_insights_df, dbname = load_insights()
    summary_data, _ = load_exec_summary()
    insights_df = prepare_insights(raw_insights_df)

st.markdown(
    """
    <div class="hero-wrap">
        <div class="hero-title">🧠 Insights IA · Copiloto de Decisão</div>
        <p class="hero-sub">Conectado ao resumo executivo persistido em banco.
        Leitura estável, rápida e confiável.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    f"<div class='pillbar'>"
    f"<div class='pill'>Banco: {dbname or 'N/D'}</div>"
    f"<div class='pill'>Insights: vw_ai_insights_abertos</div>"
    f"<div class='pill'>Resumo: vw_ai_exec_summary_latest</div>"
    f"</div>",
    unsafe_allow_html=True,
)

if insights_df.empty:
    st.warning("⚠️ Insights IA ainda não disponíveis no banco.")
    st.info("👉 A página entrou em fallback seguro. Quando a base de insights for migrada, ela aparecerá aqui automaticamente.")
    st.stop()

# KPIs globais
ntotal = len(insights_df)
alta = int(insights_df["_prio_norm"].eq("alta").sum())
alert = int(insights_df["_tipo_norm"].eq("alerta").sum())
opor = int(insights_df["_tipo_norm"].eq("oportunidade").sum())

c1, c2, c3, c4 = st.columns(4)
with c1:
    kpi_card("Total Insights", f"{ntotal:,}".replace(",", "."), "Itens acionáveis")
with c2:
    kpi_card("Alta Prioridade", f"{alta:,}".replace(",", "."), "Risco com urgência")
with c3:
    kpi_card("Alertas", f"{alert:,}".replace(",", "."), "Pontos críticos")
with c4:
    kpi_card("Oportunidades", f"{opor:,}".replace(",", "."), "Ganho potencial")

# Filtros
st.sidebar.markdown("## Filtros")
prioridade_opts, tipo_opts = get_filter_options(insights_df)
prioridade = st.sidebar.selectbox("Prioridade", prioridade_opts)
tipo = st.sidebar.selectbox("Tipo Insight", tipo_opts)
top_n = st.sidebar.slider("Top itens por bloco", 5, 30, 12)

filtered = insights_df
if prioridade != "Todos":
    filtered = filtered.loc[filtered["_prio_norm"].eq(prioridade)]
if tipo != "Todos":
    filtered = filtered.loc[filtered["_tipo_norm"].eq(tipo)]

main_tab, exec_tab = st.tabs(["📌 Painel de Prioridades", "🧠 Resumo Executivo IA"])

with main_tab:
    render_action_board(filtered, top_n)
    render_tabs(filtered, top_n)

with exec_tab:
    render_exec_summary(summary_data, filtered, min(5, top_n))
    st.markdown(
        "<div class='muted-note'>Resumo persistido em banco — mais estável e eficiente "
        "que o fluxo anterior que processava direto na base bruta.</div>",
        unsafe_allow_html=True,
    )
