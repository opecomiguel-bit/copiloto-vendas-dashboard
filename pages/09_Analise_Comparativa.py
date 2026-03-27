"""
09_Analise_Comparativa.py — Análise Comparativa de Vendas
==========================================================
Página de análise temporal para gestores:
  • Compare vendas por Marca, Produto ou Canal
  • Mês vs mesmo mês do ano anterior (YoY)
  • Mês vs mês imediatamente anterior (MoM)
  • Linha de tendência dos últimos 24 meses (dois anos lado a lado)

Fonte de dados: base_vendas_master.parquet
Sem dependência de banco — leitura direta do Parquet local.
"""

from __future__ import annotations

from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Análise Comparativa", layout="wide", page_icon="📊")

# ── Constantes ────────────────────────────────────────────────────────────────
PARQUET_PATH = "base_vendas_master.parquet"

MESES_PT = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    :root {
        --brand:   #2563eb;
        --green:   #16a34a;
        --red:     #dc2626;
        --amber:   #d97706;
        --gray50:  #f8fafc;
        --gray200: #e2e8f0;
        --text:    #0f172a;
        --soft:    #64748b;
        --shadow:  0 4px 18px rgba(15,23,42,0.07);
    }
    .block-container { padding-top:.9rem; padding-bottom:2rem; max-width:100rem; }

    .cmp-hero {
        background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 60%, #1d4ed8 100%);
        border-radius: 20px; padding: 20px 28px; margin-bottom: 18px;
        box-shadow: 0 10px 30px rgba(15,23,42,0.18);
    }
    .cmp-hero-title { font-size:26px; font-weight:900; color:#fff; line-height:1.2; margin:0 0 4px 0; }
    .cmp-hero-sub   { font-size:13px; color:#cbd5e1; margin:0; }

    .cmp-kpi {
        background: #fff; border: 1px solid var(--gray200);
        border-left: 5px solid var(--brand);
        border-radius: 16px; padding: 16px 18px 14px;
        box-shadow: var(--shadow); min-height: 106px;
    }
    .cmp-kpi-label { font-size:11px; color:var(--soft); font-weight:700;
                      letter-spacing:.06em; text-transform:uppercase; margin-bottom:6px; }
    .cmp-kpi-value { font-size:24px; font-weight:900; color:var(--text);
                      line-height:1.1; letter-spacing:-.02em; }
    .cmp-kpi-delta { font-size:12px; margin-top:5px; font-weight:700; }
    .cmp-kpi-sub   { font-size:11px; color:var(--soft); margin-top:3px; }

    .cmp-section {
        background:#fff; border:1px solid var(--gray200);
        border-radius:18px; padding:18px 18px 10px;
        box-shadow:var(--shadow); margin-bottom:14px;
    }
    .cmp-section-title { font-size:17px; font-weight:800; color:var(--text);
                          letter-spacing:-.01em; margin-bottom:2px; }
    .cmp-section-sub   { font-size:12px; color:var(--soft); margin-bottom:12px; }

    .narrative-box {
        background:linear-gradient(135deg,#eff6ff,#eef2ff);
        border:1px solid #bfdbfe; border-radius:14px;
        padding:14px 18px; font-size:14px; color:#1e3a8a; line-height:1.7;
    }
    .pill-tag {
        display:inline-block; background:#eff6ff; color:#1d4ed8;
        border:1px solid #bfdbfe; border-radius:999px;
        padding:3px 10px; font-size:11px; font-weight:700; margin:2px;
    }
    .up   { color:var(--green) !important; }
    .down { color:var(--red)   !important; }
    .flat { color:var(--soft)  !important; }

    div[data-testid="stDataFrame"] {
        border-radius:14px; overflow:hidden;
        border:1px solid var(--gray200); box-shadow:var(--shadow);
    }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _money(v) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    s = f"{v:,.2f}"
    return ("R$ " + s).replace(",", "X").replace(".", ",").replace("X", ".")


def _int_br(v) -> str:
    try:
        return f"{int(v):,}".replace(",", ".")
    except Exception:
        return "0"


def _pct(v, casas: int = 1) -> str:
    try:
        return f"{float(v):+.{casas}f}%"
    except Exception:
        return "N/D"


def _delta_cls(v) -> str:
    try:
        f = float(v)
        if f > 0:
            return "up"
        if f < 0:
            return "down"
    except Exception:
        pass
    return "flat"


def _delta_icon(v) -> str:
    try:
        f = float(v)
        if f > 0:
            return "▲"
        if f < 0:
            return "▼"
    except Exception:
        pass
    return "—"


def _month_label(year: int, month: int) -> str:
    return f"{MESES_PT[month]}/{str(year)[-2:]}"


def _fmt_var(v):
    if v is None or pd.isna(v):
        return "—"
    try:
        f = float(v)
        icon = "▲" if f > 0 else ("▼" if f < 0 else "—")
        return f"{icon} {f:+.1f}%"
    except Exception:
        return "—"


# ── Carregamento do Parquet ───────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=600)
def _read_parquet_base() -> pd.DataFrame:
    p = Path(PARQUET_PATH)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


@st.cache_data(show_spinner=False, ttl=600)
def _prepare() -> pd.DataFrame:
    """Lê e normaliza o parquet uma única vez, mantendo só colunas úteis."""
    raw = _read_parquet_base()
    if raw.empty:
        return raw

    date_col = next((c for c in ["Data", "data", "DATA"] if c in raw.columns), None)
    if date_col is None:
        return pd.DataFrame()

    rec_candidates = ["receita", "Total_num", "Total", "Valor Total", "Valor_Total", "Total do Item", "TotalItem"]
    qty_candidates = ["Qtde", "Quantidade", "Qtd", "QTD", "itens", "quantidade"]
    ped_candidates = ["Pedido", "pedido", "order_id"]
    marca_candidates = ["Marca", "marca", "Brand", "brand"]
    ean_candidates = ["EAN", "ean"]
    sku_candidates = ["SKU", "sku"]
    desc_candidates = ["Descricao", "Descrição", "descricao"]
    canal_candidates = ["Canal", "canal", "Marketplace"]

    rec_col = next((c for c in rec_candidates if c in raw.columns), None)
    qty_col = next((c for c in qty_candidates if c in raw.columns), None)
    ped_col = next((c for c in ped_candidates if c in raw.columns), None)
    marca_col = next((c for c in marca_candidates if c in raw.columns), None)
    ean_col = next((c for c in ean_candidates if c in raw.columns), None)
    sku_col = next((c for c in sku_candidates if c in raw.columns), None)
    desc_col = next((c for c in desc_candidates if c in raw.columns), None)
    canal_col = next((c for c in canal_candidates if c in raw.columns), None)

    cols = [date_col]
    for c in [rec_col, qty_col, ped_col, marca_col, ean_col, sku_col, desc_col, canal_col]:
        if c and c not in cols:
            cols.append(c)

    df = raw[cols].copy()
    del raw

    df["_data"] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["_data"])
    if df.empty:
        return pd.DataFrame()

    df["_ano"] = df["_data"].dt.year.astype("int16")
    df["_mes"] = df["_data"].dt.month.astype("int8")
    df["_ym"] = (df["_ano"].astype("int32") * 100 + df["_mes"].astype("int32")).astype("int32")

    df["_receita"] = pd.to_numeric(df[rec_col], errors="coerce").fillna(0.0).astype("float64") if rec_col else 0.0
    df["_qtde"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0.0).astype("float64") if qty_col else 0.0
    df["_pedido"] = df[ped_col].astype(str) if ped_col else df.index.astype(str)
    df["_marca"] = df[marca_col].astype(str).str.strip().str.upper() if marca_col else "SEM MARCA"
    df["_ean"] = df[ean_col].astype(str).str.strip() if ean_col else ""
    df["_sku"] = df[sku_col].astype(str).str.strip() if sku_col else ""
    df["_desc"] = df[desc_col].astype(str).str.strip() if desc_col else ""
    df["_produto_label"] = (df["_sku"].where(df["_sku"] != "", df["_ean"]) + " — " + df["_desc"]).str.strip(" —").str[:80]
    df["_canal"] = df[canal_col].astype(str).str.strip().str.upper() if canal_col else "TODOS"

    keep = ["_ano", "_mes", "_ym", "_receita", "_qtde", "_pedido", "_marca", "_produto_label", "_canal"]
    return df[keep]


@st.cache_data(show_spinner=False, ttl=600)
def _get_filter_options(df: pd.DataFrame) -> tuple[list[str], list[str], list[int], dict[str, list[str]]]:
    marcas = sorted(df["_marca"].dropna().unique().tolist()) if not df.empty else []
    canais = sorted(df["_canal"].dropna().unique().tolist()) if not df.empty else []
    anos = sorted(df["_ano"].dropna().astype(int).unique().tolist()) if not df.empty else []

    marca_produtos: dict[str, list[str]] = {}
    if not df.empty:
        grp = (
            df.loc[df["_produto_label"].notna() & df["_produto_label"].ne(""), ["_marca", "_produto_label"]]
            .drop_duplicates()
            .groupby("_marca")["_produto_label"]
            .agg(lambda s: sorted(s.tolist())[:800])
        )
        marca_produtos = grp.to_dict()

    return marcas, canais, anos, marca_produtos


# ── Agregação mensal ──────────────────────────────────────────────────────────

def _agg_month(df: pd.DataFrame, marcas: list[str], produtos: list[str], canais: list[str]) -> pd.DataFrame:
    """Agrega receita, qtde e pedidos por mês com filtros leves e sem regex."""
    if df.empty:
        return pd.DataFrame(columns=["ano", "mes", "ym", "receita", "qtde", "pedidos"])

    mask = pd.Series(True, index=df.index)

    if marcas:
        marcas_up = {m.upper() for m in marcas}
        mask &= df["_marca"].isin(marcas_up)
    if produtos:
        mask &= df["_produto_label"].isin(produtos)
    if canais:
        canais_up = {c.upper() for c in canais}
        mask &= df["_canal"].isin(canais_up)

    sub = df.loc[mask, ["_ano", "_mes", "_ym", "_receita", "_qtde", "_pedido"]]
    if sub.empty:
        return pd.DataFrame(columns=["ano", "mes", "ym", "receita", "qtde", "pedidos"])

    agg = (
        sub.groupby(["_ano", "_mes", "_ym"], sort=True, observed=True, as_index=False)
        .agg(receita=("_receita", "sum"), qtde=("_qtde", "sum"), pedidos=("_pedido", "nunique"))
        .rename(columns={"_ano": "ano", "_mes": "mes", "_ym": "ym"})
        .sort_values("ym")
        .reset_index(drop=True)
    )
    return agg


# ── KPI tile ──────────────────────────────────────────────────────────────────

def _kpi(label: str, value: str, delta: float | None = None, delta_label: str = "", border: str = "#2563eb") -> None:
    delta_html = ""
    if delta is not None:
        cls = _delta_cls(delta)
        icon = _delta_icon(delta)
        delta_html = (
            f'<div class="cmp-kpi-delta">'
            f'<span class="{cls}">{icon} {_pct(delta)}</span>'
            f'<span style="color:#94a3b8;font-weight:400;"> {delta_label}</span>'
            f'</div>'
        )
    st.markdown(
        f'<div class="cmp-kpi" style="border-left-color:{border};">'
        f'<div class="cmp-kpi-label">{label}</div>'
        f'<div class="cmp-kpi-value">{value}</div>'
        f'{delta_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


# ── Narrativa automática ──────────────────────────────────────────────────────

def _narrative(
    sel_label: str,
    sel_ano: int,
    sel_mes: int,
    rec_sel: float,
    rec_yoy: float,
    rec_mom: float,
    qty_sel: float,
    qty_yoy: float,
    qty_mom: float,
    peds_sel: int,
) -> str:
    mes_nome = MESES_PT[sel_mes]

    parts = [
        f"Em <strong>{mes_nome}/{sel_ano}</strong>, {sel_label} gerou "
        f"<strong>{_money(rec_sel)}</strong> em receita "
        f"com <strong>{_int_br(peds_sel)} pedidos</strong> e <strong>{_int_br(qty_sel)} unidades</strong> vendidas."
    ]

    if not np.isnan(rec_yoy):
        dir_yoy = "cresceu" if rec_yoy > 0 else "caiu"
        force_yoy = "forte" if abs(rec_yoy) >= 30 else ("moderada" if abs(rec_yoy) >= 10 else "leve")
        parts.append(
            f"Comparando com <strong>{mes_nome}/{sel_ano - 1}</strong> (mesmo mês do ano anterior), "
            f"a receita <strong>{dir_yoy} {_pct(rec_yoy)}</strong> — variação {force_yoy}."
        )
        if not np.isnan(qty_yoy):
            dir_qty = "cresceu" if qty_yoy > 0 else "caiu"
            parts.append(f"O volume de unidades também {dir_qty} {_pct(qty_yoy)} no comparativo anual.")
    else:
        parts.append(f"Não há dados de {mes_nome}/{sel_ano - 1} para comparação anual.")

    prev_mes = sel_mes - 1 if sel_mes > 1 else 12
    prev_ano = sel_ano if sel_mes > 1 else sel_ano - 1
    if not np.isnan(rec_mom):
        dir_mom = "cresceu" if rec_mom > 0 else "recuou"
        parts.append(
            f"Em relação ao mês anterior ({MESES_PT[prev_mes]}/{prev_ano}), "
            f"a receita <strong>{dir_mom} {_pct(rec_mom)}</strong>."
        )
    else:
        parts.append(f"Não há dados de {MESES_PT[prev_mes]}/{prev_ano} para comparação mensal.")

    if not np.isnan(rec_yoy):
        if rec_yoy < -20:
            parts.append(
                "⚠️ <strong>Atenção:</strong> queda acentuada vs ano anterior. "
                "Verifique ruptura de estoque, mudanças de campanha ou perda de canal."
            )
        elif rec_yoy < 0:
            parts.append("📌 Leve retração vs ano anterior. Monitore tendência nos próximos meses.")
        elif rec_yoy >= 20:
            parts.append(
                "✅ Crescimento expressivo vs ano anterior. "
                "Avalie se o estoque está preparado para manter o ritmo."
            )

    return " ".join(parts)


# ── Tabela mês a mês ──────────────────────────────────────────────────────────

def _build_comparison_table(agg: pd.DataFrame) -> pd.DataFrame:
    """Calcula YoY e MoM vetorizados, evitando loop linha a linha."""
    if agg.empty:
        return pd.DataFrame()

    out = agg.sort_values("ym").copy()
    out["yoy_base"] = out["receita"].shift(12)
    out["mom_base"] = out["receita"].shift(1)
    out["vs Ano Ant. (%)"] = np.where(out["yoy_base"].fillna(0) != 0, ((out["receita"] / out["yoy_base"]) - 1) * 100, np.nan)
    out["vs Mês Ant. (%)"] = np.where(out["mom_base"].fillna(0) != 0, ((out["receita"] / out["mom_base"]) - 1) * 100, np.nan)
    out["Ticket Médio"] = np.where(out["pedidos"].fillna(0) != 0, out["receita"] / out["pedidos"], 0.0)
    out["Mês"] = out.apply(lambda r: _month_label(int(r["ano"]), int(r["mes"])), axis=1)
    out = out[["ym", "Mês", "receita", "qtde", "pedidos", "Ticket Médio", "vs Ano Ant. (%)", "vs Mês Ant. (%)"]].rename(
        columns={"receita": "Receita", "qtde": "Qtde", "pedidos": "Pedidos"}
    )
    return out


# ── Charts ────────────────────────────────────────────────────────────────────

def _chart_trend(agg: pd.DataFrame) -> alt.Chart | None:
    if agg.empty:
        return None

    chart_df = agg.copy()
    chart_df["periodo"] = chart_df["mes"].map(MESES_PT) + "/" + chart_df["ano"].astype(str).str[-2:]
    chart_df["ano_label"] = chart_df["ano"].astype(str)

    return (
        alt.Chart(chart_df)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X(
                "mes:O",
                title="Mês",
                axis=alt.Axis(labelExpr="['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'][datum.value-1]"),
            ),
            y=alt.Y("receita:Q", title="Receita (R$)", axis=alt.Axis(format="~s")),
            color=alt.Color("ano_label:N", title="Ano", scale=alt.Scale(range=["#94a3b8", "#2563eb"])),
            tooltip=[
                alt.Tooltip("periodo:N", title="Período"),
                alt.Tooltip("receita:Q", title="Receita", format=",.2f"),
                alt.Tooltip("qtde:Q", title="Unidades"),
                alt.Tooltip("pedidos:Q", title="Pedidos"),
            ],
        )
        .properties(height=300)
    )


def _chart_bar_compare(agg: pd.DataFrame, sel_mes: int, anos: list[int]) -> alt.Chart | None:
    sub = agg.loc[(agg["mes"] == sel_mes) & (agg["ano"].isin(anos))].copy()
    if sub.empty:
        return None
    sub["ano_label"] = sub["ano"].astype(str)

    return (
        alt.Chart(sub)
        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(
            x=alt.X("ano_label:N", title="Ano", sort=[str(a) for a in anos]),
            y=alt.Y("receita:Q", title="Receita (R$)", axis=alt.Axis(format="~s")),
            color=alt.Color("ano_label:N", title="Ano", scale=alt.Scale(range=["#94a3b8", "#2563eb"])),
            tooltip=[
                alt.Tooltip("ano_label:N", title="Ano"),
                alt.Tooltip("receita:Q", title="Receita", format=",.2f"),
                alt.Tooltip("qtde:Q", title="Unidades"),
                alt.Tooltip("pedidos:Q", title="Pedidos"),
            ],
        )
        .properties(height=260)
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="cmp-hero">
    <div class="cmp-hero-title">📊 Análise Comparativa de Vendas</div>
    <p class="cmp-hero-sub">Compare marcas e produtos mês a mês — YoY (mesmo mês ano anterior) e MoM (mês anterior).
    Fonte: base_vendas_master.parquet</p>
</div>
""", unsafe_allow_html=True)

with st.spinner("Carregando base de vendas..."):
    df = _prepare()

if df.empty:
    st.error(
        f"Arquivo '{PARQUET_PATH}' não encontrado, vazio ou sem coluna de data válida. "
        "Certifique-se de que o copiloto foi executado e o arquivo existe na mesma pasta."
    )
    st.stop()

marcas_disp, canais_disp, anos_disp, marca_produtos = _get_filter_options(df)

st.sidebar.markdown("## Filtros da Comparação")

sel_marcas = st.sidebar.multiselect(
    "Marca(s)",
    marcas_disp,
    placeholder="Todas as marcas",
    help="Ex: SILVERPLASTIC — deixe vazio para consolidar tudo",
)

if sel_marcas:
    produtos_base = []
    for marca in sel_marcas:
        produtos_base.extend(marca_produtos.get(marca.upper(), marca_produtos.get(marca, [])))
    produtos_disp = sorted(set(produtos_base))
else:
    produtos_disp = sorted(df.loc[df["_produto_label"].notna() & df["_produto_label"].ne(""), "_produto_label"].drop_duplicates().tolist())

sel_produtos = st.sidebar.multiselect(
    "Produto(s)",
    produtos_disp[:300],
    placeholder="Todos os produtos",
    help="Filtre por SKU / EAN específico",
)

sel_canais = st.sidebar.multiselect("Canal(is)", canais_disp, placeholder="Todos os canais")

st.sidebar.markdown("---")
st.sidebar.markdown("### Período de análise")

sel_ano = st.sidebar.selectbox("Ano de referência", sorted(anos_disp, reverse=True), index=0)
sel_mes = st.sidebar.selectbox(
    "Mês de referência",
    list(range(1, 13)),
    index=0,
    format_func=lambda m: f"{MESES_PT[m]} ({m:02d})",
)

anos_comparar = sorted({sel_ano - 1, sel_ano})

with st.spinner("Calculando..."):
    agg = _agg_month(df, sel_marcas, sel_produtos, sel_canais)

if agg.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados. Tente ampliar a seleção.")
    st.stop()

metrics_by_ym = agg.set_index("ym")[["receita", "qtde", "pedidos"]]
ym_sel = sel_ano * 100 + sel_mes
ym_yoy = (sel_ano - 1) * 100 + sel_mes
ym_mom = (sel_ano * 100 + sel_mes - 1) if sel_mes > 1 else ((sel_ano - 1) * 100 + 12)


def _get_metrics(ym: int) -> pd.Series:
    if ym in metrics_by_ym.index:
        return metrics_by_ym.loc[ym]
    return pd.Series({"receita": np.nan, "qtde": np.nan, "pedidos": np.nan})


def _var(a, b) -> float:
    try:
        if pd.isna(b) or float(b) == 0:
            return np.nan
        return ((float(a) / float(b)) - 1) * 100
    except Exception:
        return np.nan


r_sel = _get_metrics(ym_sel)
r_yoy = _get_metrics(ym_yoy)
r_mom = _get_metrics(ym_mom)

rec_sel = float(r_sel["receita"]) if not pd.isna(r_sel["receita"]) else 0.0
qty_sel = float(r_sel["qtde"]) if not pd.isna(r_sel["qtde"]) else 0.0
peds_sel = int(r_sel["pedidos"]) if not pd.isna(r_sel["pedidos"]) else 0
tick_sel = rec_sel / peds_sel if peds_sel else 0.0

var_yoy_rec = _var(r_sel["receita"], r_yoy["receita"])
var_mom_rec = _var(r_sel["receita"], r_mom["receita"])
var_yoy_qty = _var(r_sel["qtde"], r_yoy["qtde"])
var_mom_qty = _var(r_sel["qtde"], r_mom["qtde"])

sel_parts = []
if sel_marcas:
    sel_parts.append(", ".join(sel_marcas[:3]) + ("…" if len(sel_marcas) > 3 else ""))
if sel_produtos:
    sel_parts.append(f"{len(sel_produtos)} produto(s)")
if sel_canais:
    sel_parts.append(", ".join(sel_canais[:2]) + ("…" if len(sel_canais) > 2 else ""))
sel_label = " · ".join(sel_parts) if sel_parts else "Toda a base"

pills_html = (
    f'<span class="pill-tag">{sel_label}</span>'
    f'<span class="pill-tag">{MESES_PT[sel_mes]}/{sel_ano}</span>'
    f'<span class="pill-tag">YoY: {MESES_PT[sel_mes]}/{sel_ano-1}</span>'
)
if sel_mes > 1:
    pills_html += f'<span class="pill-tag">MoM: {MESES_PT[sel_mes-1]}/{sel_ano}</span>'
else:
    pills_html += f'<span class="pill-tag">MoM: Dez/{sel_ano-1}</span>'
st.markdown(pills_html, unsafe_allow_html=True)

st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    _kpi("Receita", _money(rec_sel), border="#2563eb")
with k2:
    _kpi(
        "vs Ano Anterior",
        _pct(var_yoy_rec) if not np.isnan(var_yoy_rec) else "N/D",
        delta=var_yoy_rec if not np.isnan(var_yoy_rec) else None,
        delta_label=f"{MESES_PT[sel_mes]}/{sel_ano-1}",
        border="#16a34a" if not np.isnan(var_yoy_rec) and var_yoy_rec >= 0 else "#dc2626",
    )
with k3:
    _kpi(
        "vs Mês Anterior",
        _pct(var_mom_rec) if not np.isnan(var_mom_rec) else "N/D",
        delta=var_mom_rec if not np.isnan(var_mom_rec) else None,
        delta_label="MoM",
        border="#7c3aed",
    )
with k4:
    _kpi(
        "Pedidos",
        _int_br(peds_sel),
        delta=_var(r_sel["pedidos"], r_yoy["pedidos"]) if not pd.isna(r_yoy["pedidos"]) else None,
        delta_label="vs ano ant.",
        border="#0ea5e9",
    )
with k5:
    _kpi("Ticket Médio", _money(tick_sel), border="#f59e0b")

st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

narrative = _narrative(
    sel_label=sel_label,
    sel_ano=sel_ano,
    sel_mes=sel_mes,
    rec_sel=rec_sel,
    rec_yoy=var_yoy_rec,
    rec_mom=var_mom_rec,
    qty_sel=qty_sel,
    qty_yoy=var_yoy_qty,
    qty_mom=var_mom_qty,
    peds_sel=peds_sel,
)
st.markdown(f'<div class="narrative-box">📋 {narrative}</div>', unsafe_allow_html=True)
st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

col_chart1, col_chart2 = st.columns([3, 2])

with col_chart1:
    st.markdown("""
    <div class="cmp-section">
        <div class="cmp-section-title">📈 Tendência de Receita — 2 anos</div>
        <div class="cmp-section-sub">Linha mensal para cada ano — compare sazonalidade e crescimento</div>
    """, unsafe_allow_html=True)

    agg_chart = agg.loc[agg["ano"].isin(anos_comparar)]
    chart = _chart_trend(agg_chart)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Dados insuficientes para o gráfico de tendência.")
    st.markdown("</div>", unsafe_allow_html=True)

with col_chart2:
    st.markdown("""
    <div class="cmp-section">
        <div class="cmp-section-title">📊 Comparativo do Mês</div>
        <div class="cmp-section-sub">Receita do mês selecionado: ano atual vs anterior</div>
    """, unsafe_allow_html=True)

    bar = _chart_bar_compare(agg, sel_mes, anos_comparar)
    if bar is not None:
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info("Dados insuficientes para o gráfico de barras.")
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("""
<div class="cmp-section">
    <div class="cmp-section-title">📋 Histórico mês a mês</div>
    <div class="cmp-section-sub">
        Receita, volume, pedidos e variações YoY/MoM para o recorte selecionado
    </div>
""", unsafe_allow_html=True)

table = _build_comparison_table(agg)
if not table.empty:
    table_show = table.copy()
    table_show["Receita"] = table_show["Receita"].apply(_money)
    table_show["Ticket Médio"] = table_show["Ticket Médio"].apply(_money)
    table_show["Qtde"] = table_show["Qtde"].apply(_int_br)
    table_show["Pedidos"] = table_show["Pedidos"].apply(_int_br)
    table_show["vs Ano Ant. (%)"] = table_show["vs Ano Ant. (%)"].apply(_fmt_var)
    table_show["vs Mês Ant. (%)"] = table_show["vs Mês Ant. (%)"].apply(_fmt_var)

    st.dataframe(
        table_show.sort_values("ym", ascending=False).drop(columns=["ym"]).reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("Nenhum dado para exibir na tabela.")

st.markdown("</div>", unsafe_allow_html=True)

if not sel_marcas and not sel_produtos:
    st.markdown("""
    <div class="cmp-section">
        <div class="cmp-section-title">🏷️ Top Marcas — Comparativo do Mês</div>
        <div class="cmp-section-sub">
            Receita por marca no mês selecionado vs mesmo mês ano anterior
        </div>
    """, unsafe_allow_html=True)

    top_base = df
    if sel_canais:
        top_base = top_base.loc[top_base["_canal"].isin({c.upper() for c in sel_canais})]

    top_cur = (
        top_base.loc[(top_base["_ano"] == sel_ano) & (top_base["_mes"] == sel_mes)]
        .groupby("_marca", as_index=False, observed=True)["_receita"].sum()
        .rename(columns={"_receita": "receita_atual"})
    )
    top_prev = (
        top_base.loc[(top_base["_ano"] == sel_ano - 1) & (top_base["_mes"] == sel_mes)]
        .groupby("_marca", as_index=False, observed=True)["_receita"].sum()
        .rename(columns={"_receita": "receita_ant"})
    )

    if not top_cur.empty:
        merged = top_cur.merge(top_prev, on="_marca", how="left")
        merged["var_yoy"] = np.where(
            merged["receita_ant"].fillna(0) != 0,
            ((merged["receita_atual"] / merged["receita_ant"]) - 1) * 100,
            np.nan,
        )
        merged = merged.sort_values("receita_atual", ascending=False).head(15)
        merged = merged.rename(columns={"_marca": "Marca", "receita_atual": "Receita Atual", "receita_ant": "Receita Ano Ant.", "var_yoy": "YoY (%)"})
        merged["Receita Atual"] = merged["Receita Atual"].apply(_money)
        merged["Receita Ano Ant."] = merged["Receita Ano Ant."].apply(lambda v: _money(v) if not pd.isna(v) else "—")
        merged["YoY (%)"] = merged["YoY (%)"].apply(_fmt_var)
        st.dataframe(merged, use_container_width=True, hide_index=True)
    else:
        st.info(f"Sem dados de marcas para {MESES_PT[sel_mes]}/{sel_ano}.")

    st.markdown("</div>", unsafe_allow_html=True)
