import pandas as pd
import numpy as np
import re
import json
import os
import requests
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

from statsmodels.tsa.holtwinters import ExponentialSmoothing

from db_writer import write_bundle

# Evita erro de encoding no CMD/PowerShell do Windows com emojis em prints
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# Compatibilidade de ambiente para local/Render
def _bootstrap_db_env():
    db_url = (os.getenv("DATABASE_URL") or "").strip()

    # Se vier DATABASE_URL, espelha variáveis legadas para compatibilidade
    if db_url:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(db_url.replace("postgresql+psycopg2://", "postgresql://"))
            if parsed.hostname and not os.getenv("DB_HOST"):
                os.environ["DB_HOST"] = parsed.hostname
            if parsed.port and not os.getenv("DB_PORT"):
                os.environ["DB_PORT"] = str(parsed.port)
            db_name = (parsed.path or "/").lstrip("/")
            if db_name and not os.getenv("DB_NAME"):
                os.environ["DB_NAME"] = db_name
            if parsed.username and not os.getenv("DB_USER"):
                os.environ["DB_USER"] = parsed.username
            if parsed.password:
                if not os.getenv("DB_PASSWORD"):
                    os.environ["DB_PASSWORD"] = parsed.password
                if not os.getenv("DB_POSTGRESDB_PASSWORD"):
                    os.environ["DB_POSTGRESDB_PASSWORD"] = parsed.password
        except Exception:
            pass

    # Compatibilidade entre nomes de senha
    if os.getenv("DB_POSTGRESDB_PASSWORD") and not os.getenv("DB_PASSWORD"):
        os.environ["DB_PASSWORD"] = os.getenv("DB_POSTGRESDB_PASSWORD")
    if os.getenv("DB_PASSWORD") and not os.getenv("DB_POSTGRESDB_PASSWORD"):
        os.environ["DB_POSTGRESDB_PASSWORD"] = os.getenv("DB_PASSWORD")

    # Fallback local solicitado pelo usuário
    if not os.getenv("DB_PASSWORD") and not os.getenv("DB_POSTGRESDB_PASSWORD"):
        os.environ["DB_PASSWORD"] = "123456"
        os.environ["DB_POSTGRESDB_PASSWORD"] = "123456"

_bootstrap_db_env()


# =========================
# CONFIG
# =========================
ARQUIVO_BASE = "base_vendas_master.parquet"   # mestre completo
KEY_COL = "Pedido"
DATE_COL = "Data"
CANAL_COL = "Canal"
PROD_COL = "Produto"   # NÃO usar como base de cálculo de reposição
EAN_COL = "EAN"
ESTOQUE_COL = "Estoque Local"
ESTOQUE_FULL_COL = "Estoque Full"
ESTOQUE_TOTAL_COL = "Estoque Total"

# Ingestão automática
INPUT_DIR = "input"
INPUT_GLOB = "AdmPedidos*.xlsx"
AUTO_UPDATE_FROM_INPUT = True
KIT_RULES_FILE = "regras_kit.csv"

# Horizonte padrão (mantido para suas saídas existentes)
HORIZON_DIAS = 30

# FULL: regra do ML = 35 dias
FULL_CANAL_EXATO = "MELI FULL"
FULL_HORIZON_DIAS = 35

# Guardrails de forecast (confiabilidade)
MIN_DIAS_PARA_FORECAST = 60
MIN_DIAS_NAO_ZERO = 10

# FULL guardrails (mais “pé no chão”)
FULL_MIN_DIAS_HIST = 45
FULL_MIN_DIAS_NAO_ZERO = 8

# Reposição geral guardrails
GERAL_MIN_DIAS_HIST = 45
GERAL_MIN_DIAS_NAO_ZERO = 8

# Janela de métricas
JAN30 = 30
JAN60 = 60
JAN90 = 90

# Saúde de estoque / cobertura
COBERTURA_CRITICA = 15
COBERTURA_URGENTE = 30
COBERTURA_ATENCAO = 60
COBERTURA_SAUDAVEL = 90
COBERTURA_EXCESSO = 120

# Trend / aceleração
TREND_ACCEL_THRESHOLD = 0.20   # +20%
TREND_FORTE_THRESHOLD = 0.40   # +40%

# Saídas (usadas pela Dash)
OUT_SAIDA_DAILY = "saida_daily.csv"
OUT_SAIDA_MONTHLY = "saida_monthly.csv"
OUT_SAIDA_POR_CANAL = "saida_por_canal.csv"
OUT_SAIDA_ABC = "saida_abc.csv"
OUT_FORECAST_TOTAL = "saida_forecast_30d.csv"
OUT_FORECAST_CANAL = "forecast_por_canal_30d.csv"
OUT_FORECAST_SKU_A = "forecast_sku_A_30d.csv"
OUT_ALERTAS_CANAL = "alertas_por_canal.csv"
OUT_ALERTAS_SKU_A = "alertas_sku_A.csv"
OUT_ALERTAS_ALL = "alertas.csv"
OUT_RESUMO_JSON = "resumo.json"
OUT_RELATORIO_MD = "relatorio.md"

# FULL outputs
OUT_FULL_REPOSICAO_35D = "full_reposicao_35d.csv"
OUT_FULL_CANDIDATOS_35D = "full_candidatos_envio_35d.csv"
OUT_FULL_AUDITORIA_35D = "full_auditoria_35d.csv"

# Reposição geral
OUT_REPOSICAO_GERAL = "reposicao_geral_estoque.csv"
OUT_REPOSICAO_GERAL_ACCEL = "reposicao_geral_acelerando.csv"
OUT_ALERTAS_TRACKING = "alertas_tracking.csv"
DASH_RENDER_BASE_WINDOW_DAYS = int(os.getenv("DASH_RENDER_BASE_WINDOW_DAYS", "90"))


# =========================
# UTIL
# =========================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()
    return df


def parse_br_number(x):
    """
    Converte valores BR (R$ 1.234,56) em float.
    Também trata inteiros / floats / strings com separador.
    """
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).strip()
    s = re.sub(r"[Rr]\$|\s", "", s)
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return np.nan


def money_br(v: float) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        v = 0.0
    s = f"{v:,.2f}"
    return ("R$ " + s).replace(",", "X").replace(".", ",").replace("X", ".")


def safe_to_datetime(series: pd.Series) -> pd.Series:
    """
    Converte 'Data' com alta robustez:
    - datetime
    - string pt-BR
    - timestamp ms/s
    - serial Excel (dias desde 1899-12-30)
    """
    s = series
    if pd.api.types.is_datetime64_any_dtype(s):
        return s

    if pd.api.types.is_numeric_dtype(s):
        x = pd.to_numeric(s, errors="coerce")
        if x.dropna().empty:
            return pd.to_datetime(x, errors="coerce")

        med = x.dropna().median()
        if med > 1e12:
            return pd.to_datetime(x, unit="ms", errors="coerce")
        if med > 1e9:
            return pd.to_datetime(x, unit="s", errors="coerce")

        return pd.to_datetime(x, unit="D", origin="1899-12-30", errors="coerce")

    return pd.to_datetime(s, dayfirst=True, errors="coerce")


def pct_change(a, b):
    if b == 0 or pd.isna(b) or pd.isna(a):
        return np.nan
    return (a - b) / b


def ensure_daily_index(ts: pd.Series) -> pd.Series:
    ts = ts.copy()
    ts = ts.asfreq("D")
    ts = ts.fillna(0)
    return ts


def classify_cobertura(dias):
    if pd.isna(dias):
        return "SEM_GIRO"
    if dias <= COBERTURA_CRITICA:
        return "CRÍTICO"
    if dias <= COBERTURA_URGENTE:
        return "URGENTE"
    if dias <= COBERTURA_ATENCAO:
        return "ATENÇÃO"
    if dias <= COBERTURA_SAUDAVEL:
        return "SAUDÁVEL"
    if dias <= COBERTURA_EXCESSO:
        return "ALTO"
    return "EXCESSO"


def classify_trend(growth_pct):
    if pd.isna(growth_pct):
        return "SEM_DADOS"
    if growth_pct >= TREND_FORTE_THRESHOLD:
        return "ACELERANDO_FORTE"
    if growth_pct >= TREND_ACCEL_THRESHOLD:
        return "ACELERANDO"
    if growth_pct <= -0.30:
        return "DESACELERANDO_FORTE"
    if growth_pct <= -0.15:
        return "DESACELERANDO"
    return "ESTÁVEL"


def trend_score(growth_pct):
    if pd.isna(growth_pct):
        return 0.0
    return float(max(growth_pct, 0.0))


def urgency_rank(cobertura_cat: str) -> int:
    order = {
        "CRÍTICO": 1,
        "URGENTE": 2,
        "ATENÇÃO": 3,
        "SAUDÁVEL": 4,
        "ALTO": 5,
        "EXCESSO": 6,
        "SEM_GIRO": 7,
    }
    return order.get(str(cobertura_cat), 99)




SLACK_DAILY_STATE_FILE = "slack_resumo_diario_estado.json"

def enviar_alertas_slack(df_alertas: pd.DataFrame) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        print("[WARN] Slack webhook não configurado; alertas não enviados.")
        return
    if df_alertas is None or df_alertas.empty:
        print("[INFO] Sem alertas para enviar ao Slack.")
        return

    severidade_col = "severidade" if "severidade" in df_alertas.columns else None
    df_envio = df_alertas.copy()
    if severidade_col:
        df_envio = df_envio[df_envio[severidade_col].astype(str).str.upper().isin(["ALTA", "MÉDIA", "MEDIA"])].copy()
    df_envio = df_envio.head(10)
    if df_envio.empty:
        print("[INFO] Sem alertas relevantes para envio ao Slack.")
        return

    linhas = []
    for _, row in df_envio.iterrows():
        linhas.append(f"• *{row.get('tipo', 'ALERTA')}* | {row.get('severidade', '')} | {row.get('EAN', row.get('Canal', ''))}\n  variação: {row.get('variacao_pct', '')}%")

    texto = "🚨 *COPILOTO DE VENDAS — ALERTAS RELEVANTES*\n\n" + "\n".join(linhas)
    try:
        resp = requests.post(webhook, json={"text": texto}, timeout=10)
        if resp.status_code == 200:
            print("[OK] Alertas enviados para Slack.")
        else:
            print(f"[ERRO] Slack alertas retornou status {resp.status_code}.")
    except Exception as e:
        print(f"[ERRO] Falha ao enviar alertas para Slack: {e}")


def _load_slack_daily_state() -> dict:
    p = Path(SLACK_DAILY_STATE_FILE)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _save_slack_daily_state(state: dict) -> None:
    try:
        Path(SLACK_DAILY_STATE_FILE).write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        pass


def enviar_resumo_diario_slack(repos_geral: pd.DataFrame, cand_full: pd.DataFrame, repos_geral_accel: pd.DataFrame, alertas_all: pd.DataFrame, data_ref: str | None = None, force: bool = False) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        print("[WARN] Slack webhook não configurado; resumo diário não enviado.")
        return
    if repos_geral is None or repos_geral.empty:
        print("[INFO] Base de reposição geral vazia; resumo diário não enviado.")
        return

    today_key = str(data_ref or datetime.now().date())
    state = _load_slack_daily_state()
    if state.get("last_daily_summary_date") == today_key and not force:
        print(f"[INFO] Resumo diário já enviado em {today_key}; pulando novo envio.")
        return

    rg = repos_geral.copy()
    agir_hoje = int(rg["status_cobertura_90d"].astype(str).str.upper().eq("CRÍTICO").sum()) if "status_cobertura_90d" in rg.columns else 0
    comprar_total = int((pd.to_numeric(rg.get("reposicao_sugerida_90d"), errors="coerce").fillna(0) > 0).sum()) if "reposicao_sugerida_90d" in rg.columns else 0
    full_total = int(len(cand_full)) if cand_full is not None else 0
    escala_total = int(len(repos_geral_accel)) if repos_geral_accel is not None else 0
    alertas_alta = int(alertas_all["severidade"].astype(str).str.upper().eq("ALTA").sum()) if alertas_all is not None and not alertas_all.empty and "severidade" in alertas_all.columns else 0

    top_compras = pd.DataFrame()
    if "reposicao_sugerida_90d" in rg.columns:
        top_compras = rg.copy()
        top_compras["reposicao_sugerida_90d"] = pd.to_numeric(top_compras["reposicao_sugerida_90d"], errors="coerce").fillna(0)
        if "receita_90d" in top_compras.columns:
            top_compras["receita_90d"] = pd.to_numeric(top_compras["receita_90d"], errors="coerce").fillna(0.0)
            top_compras = top_compras.sort_values(["reposicao_sugerida_90d", "receita_90d"], ascending=[False, False])
        else:
            top_compras = top_compras.sort_values(["reposicao_sugerida_90d"], ascending=[False])
        top_compras = top_compras[top_compras["reposicao_sugerida_90d"] > 0].head(5)

    top_full = cand_full.head(5).copy() if cand_full is not None and not cand_full.empty else pd.DataFrame()

    linhas = [
        "📊 *COPILOTO DE VENDAS — RESUMO DIÁRIO*",
        f"Data de referência: *{today_key}*",
        "",
        f"🔥 Itens críticos (agir hoje): *{agir_hoje}*",
        f"🛒 Itens para comprar: *{comprar_total}*",
        f"📦 Candidatos FULL: *{full_total}*",
        f"🚀 Itens acelerando: *{escala_total}*",
        f"🚨 Alertas ALTA: *{alertas_alta}*",
    ]

    if not top_compras.empty:
        linhas.append("")
        linhas.append("*Top 5 compras do dia*")
        for _, row in top_compras.iterrows():
            linhas.append(
                f"• {row.get('EAN', '')} | {str(row.get('Descricao', ''))[:55]} | repor {int(row.get('reposicao_sugerida_90d', 0))} un."
            )

    if not top_full.empty:
        linhas.append("")
        linhas.append("*Top 5 oportunidades FULL*")
        for _, row in top_full.iterrows():
            linhas.append(
                f"• {row.get('EAN', '')} | {str(row.get('Descricao', ''))[:55]} | score {float(row.get('score_full', 0.0)):.2f}"
            )

    texto = "\n".join(linhas)
    try:
        resp = requests.post(webhook, json={"text": texto}, timeout=10)
        if resp.status_code == 200:
            state["last_daily_summary_date"] = today_key
            state["last_daily_summary_sent_at"] = datetime.now().isoformat()
            _save_slack_daily_state(state)
            print(f"[OK] Resumo diário enviado para Slack ({today_key}).")
        else:
            print(f"[ERRO] Slack resumo diário retornou status {resp.status_code}.")
    except Exception as e:
        print(f"[ERRO] Falha ao enviar resumo diário para Slack: {e}")


def atualizar_tracking_alertas(alertas_df: pd.DataFrame, path: str = OUT_ALERTAS_TRACKING) -> pd.DataFrame:
    cols = ["EAN", "tipo_alerta", "status", "owner", "data_inicio", "data_conclusao"]
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if alertas_df is None or alertas_df.empty:
        if Path(path).exists():
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.DataFrame(columns=cols)
        return pd.DataFrame(columns=cols)

    base = alertas_df.copy()
    tipo_col = "tipo_alerta" if "tipo_alerta" in base.columns else "tipo" if "tipo" in base.columns else None
    ean_col = "EAN" if "EAN" in base.columns else None
    if tipo_col is None or ean_col is None:
        return pd.DataFrame(columns=cols)

    tracking_base = pd.DataFrame({
        "EAN": base[ean_col].astype(str).fillna(""),
        "tipo_alerta": base[tipo_col].astype(str).fillna(""),
        "status": "ABERTO",
        "owner": "",
        "data_inicio": agora,
        "data_conclusao": "",
    }).drop_duplicates(subset=["EAN", "tipo_alerta"])

    try:
        hist = pd.read_csv(path)
    except Exception:
        hist = pd.DataFrame(columns=cols)

    for c in cols:
        if c not in hist.columns:
            hist[c] = ""
    hist = hist[cols].copy()
    hist["EAN"] = hist["EAN"].astype(str)
    hist["tipo_alerta"] = hist["tipo_alerta"].astype(str)

    chaves_exist = set(zip(hist["EAN"], hist["tipo_alerta"]))
    novos = tracking_base[~tracking_base.apply(lambda r: (r["EAN"], r["tipo_alerta"]) in chaves_exist, axis=1)].copy()
    final = pd.concat([hist, novos], ignore_index=True)
    final.to_csv(path, index=False, encoding="utf-8-sig")
    print("[OK] Tracking de alertas atualizado.")
    return final

def clean_ean_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.replace(".0", "", regex=False)
         .replace({"nan": "", "None": "", "NONE": ""})
    )


def clean_sku_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.replace(".0", "", regex=False)
         .replace({"nan": "", "None": "", "NONE": ""})
    )



def _norm_colname(x: str) -> str:
    s = unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


_CANONICAL_ALIASES = {
    "Seller": ["seller"],
    "Pedido": ["pedido", "numeropedido", "numped"],
    "Data": ["data", "datavenda", "emissao"],
    "Cliente": ["cliente", "nomecliente"],
    "Canal": ["canal", "marketplace", "canalvenda"],
    "Produto": ["produto", "sku", "codigo", "codigoproduto"],
    "EAN": ["ean", "gtin", "codigobarras"],
    "Descricao": ["descricao", "descricao", "descricaoproduto"],
    "Marca": ["marca"],
    "Qtde": ["qtde", "qtd", "quantidade", "itens"],
    "Preço": ["preco", "precounitario", "valorunitario", "valorunit", "precounit"],
    "Total": ["total", "valor", "valortotal"],
    "Estoque Local": ["estoquelocal", "estoque"],
    "Estoque Full": ["estoquefull"],
    "Estoque Total": ["estoquetotal"],
}

_REQUIRED_INPUT_COLS = [
    "Pedido", "Data", "Canal", "Produto", "EAN", "Descricao",
    "Marca", "Qtde", "Preço", "Total", "Estoque Local"
]
_OPTIONAL_INPUT_COLS = ["Seller", "Cliente", "Estoque Full", "Estoque Total"]


def standardize_input_sales_frame(df_raw: pd.DataFrame, source_name: str = None) -> pd.DataFrame:
    df = normalize_columns(df_raw)
    norm_map = {_norm_colname(c): c for c in df.columns}

    out = pd.DataFrame(index=df.index)
    for canonical, aliases in _CANONICAL_ALIASES.items():
        src = None
        for alias in aliases:
            if alias in norm_map:
                src = norm_map[alias]
                break
        if src is not None:
            out[canonical] = df[src]
        elif canonical in _REQUIRED_INPUT_COLS:
            out[canonical] = np.nan
        elif canonical in _OPTIONAL_INPUT_COLS:
            out[canonical] = np.nan

    out = out[[c for c in (_REQUIRED_INPUT_COLS + _OPTIONAL_INPUT_COLS) if c in out.columns]].copy()
    out["arquivo_origem"] = source_name or ""
    return out


def _row_signature(df: pd.DataFrame) -> pd.Series:
    tmp = df.copy()
    for c in ["Pedido", "Canal", "Produto", "EAN", "Descricao", "Marca", "Seller", "Cliente"]:
        if c not in tmp.columns:
            tmp[c] = ""
        tmp[c] = tmp[c].astype(str).str.strip().str.upper()

    if "Data" not in tmp.columns:
        tmp["Data"] = pd.NaT
    dt = safe_to_datetime(tmp["Data"]).dt.strftime("%Y-%m-%d").fillna("")

    for c in ["Qtde", "Preço", "Total", "Estoque Local"]:
        if c not in tmp.columns:
            tmp[c] = np.nan
        tmp[c] = tmp[c].apply(parse_br_number).fillna(0).round(4).astype(str)

    parts = [
        tmp["Pedido"], dt, tmp["Canal"], tmp["Produto"], tmp["EAN"],
        tmp["Descricao"], tmp["Marca"], tmp["Qtde"], tmp["Preço"], tmp["Total"]
    ]
    sig = parts[0]
    for p in parts[1:]:
        sig = sig + "||" + p
    return sig


def update_master_from_input(base_path: Path, input_dir: str = INPUT_DIR) -> dict:
    input_path = Path(input_dir)
    files = sorted(input_path.glob(INPUT_GLOB))
    if not files:
        return {"modo": "sem_input", "arquivos_lidos": 0, "novos_inseridos": 0, "total_master_agora": int(pd.read_parquet(base_path).shape[0]) if base_path.exists() else 0}

    frames = []
    lidos = []
    for fp in files:
        try:
            raw = pd.read_excel(fp)
            std = standardize_input_sales_frame(raw, source_name=fp.name)
            std = std.dropna(how="all")
            if len(std):
                frames.append(std)
                lidos.append(fp.name)
        except Exception:
            continue

    if not frames:
        return {"modo": "input_vazio", "arquivos_lidos": len(files), "novos_inseridos": 0, "total_master_agora": int(pd.read_parquet(base_path).shape[0]) if base_path.exists() else 0}

    new_df = pd.concat(frames, ignore_index=True, sort=False)
    new_df = normalize_columns(new_df)

    if base_path.exists():
        master = pd.read_parquet(base_path)
        master = normalize_columns(master)
    else:
        master = pd.DataFrame(columns=new_df.columns)

    for col in new_df.columns:
        if col not in master.columns:
            master[col] = np.nan
    for col in master.columns:
        if col not in new_df.columns:
            new_df[col] = np.nan

    master = master[new_df.columns.tolist()].copy()

    new_sig = _row_signature(new_df)
    master_sig = _row_signature(master) if len(master) else pd.Series(dtype=str)

    new_df = new_df.assign(_sig__=new_sig)
    master = master.assign(_sig__=master_sig if len(master) else pd.Series([], dtype=str))

    existing = set(master["_sig__"].astype(str).tolist())
    append_df = new_df[~new_df["_sig__"].astype(str).isin(existing)].copy()

    combined = pd.concat([master, append_df], ignore_index=True, sort=False)
    combined = combined.drop(columns=["_sig__"], errors="ignore")

    # =========================
    # NORMALIZAÇÃO FINAL ANTES DO PARQUET
    # =========================
    if "Data" in combined.columns:
        combined["Data"] = safe_to_datetime(combined["Data"])
        combined = combined[combined["Data"].notna()].copy()

    for col in ["Pedido", "Canal", "Produto", "EAN", "Descricao", "Marca", "SKU"]:
        if col in combined.columns:
            combined[col] = combined[col].astype(str).str.strip()

    for col in ["Qtde", "Preço", "Preco", "Total", "Estoque Local"]:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    combined.to_parquet(base_path, index=False)

    return {
        "modo": "input_auto_merge",
        "arquivos_lidos": len(lidos),
        "arquivos": lidos,
        "linhas_input": int(len(new_df)),
        "novos_inseridos": int(len(append_df)),
        "total_master_agora": int(len(combined)),
    }


def load_kit_rules(path: str = KIT_RULES_FILE) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["EAN", "kit_multiplier", "kit_observacao", "kit_ativo"])

    sep = ";"
    try:
        sample = p.read_text(encoding="utf-8-sig")[:2048]
        if sample.count(";") < sample.count(","):
            sep = ","
    except Exception:
        pass

    try:
        df = pd.read_csv(p, sep=sep, encoding="utf-8-sig")
    except Exception:
        try:
            df = pd.read_excel(p)
        except Exception:
            return pd.DataFrame(columns=["EAN", "kit_multiplier", "kit_observacao", "kit_ativo"])

    df = normalize_columns(df)
    norm_map = {_norm_colname(c): c for c in df.columns}

    def pick(*aliases):
        for a in aliases:
            if a in norm_map:
                return norm_map[a]
        return None

    ean_col = pick("ean", "gtin", "codigobarras")
    mult_col = pick("multiplicador", "kitmultiplier", "multiplicadorkit", "fator", "multiplier")
    obs_col = pick("observacao", "obs", "descricao", "comentario")
    ativo_col = pick("ativo", "status", "habilitado")

    if ean_col is None or mult_col is None:
        return pd.DataFrame(columns=["EAN", "kit_multiplier", "kit_observacao", "kit_ativo"])

    out = pd.DataFrame()
    out["EAN"] = clean_ean_series(df[ean_col])
    out["kit_multiplier"] = pd.to_numeric(df[mult_col], errors="coerce").fillna(1.0)
    out["kit_multiplier"] = out["kit_multiplier"].clip(lower=1.0)
    out["kit_observacao"] = df[obs_col].astype(str).str.strip() if obs_col else ""
    if ativo_col:
        raw = df[ativo_col].astype(str).str.strip().str.upper()
        out["kit_ativo"] = ~raw.isin(["0", "FALSE", "FALSO", "N", "NAO", "NÃO", "INATIVO"])
    else:
        out["kit_ativo"] = True

    out = out[(out["EAN"].astype(str).str.len() > 0) & (out["kit_ativo"]) & (out["kit_multiplier"] > 1)]
    out = out.drop_duplicates(subset=["EAN"], keep="last")
    return out.reset_index(drop=True)


def apply_kit_rules(df: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # BUGFIX 1: preserva Qtde_original já salvo no parquet para evitar dupla multiplicação de kit
    if "Qtde_original" not in out.columns or out["Qtde_original"].isna().all():
        out["Qtde_original"] = pd.to_numeric(out.get("Qtde", 0), errors="coerce").fillna(0.0)
    else:
        out["Qtde_original"] = pd.to_numeric(out["Qtde_original"], errors="coerce").fillna(0.0)

    out["kit_multiplier"] = 1.0
    out["is_kit"] = False
    out["kit_observacao"] = ""

    if rules is None or len(rules) == 0:
        out["Qtde"] = out["Qtde_original"]
        return out

    rule_map = rules.set_index("EAN")[["kit_multiplier", "kit_observacao"]].to_dict(orient="index")
    eans = clean_ean_series(out["EAN"]) if "EAN" in out.columns else pd.Series("", index=out.index)
    mults = []
    obs = []
    flags = []
    for e in eans.astype(str):
        rule = rule_map.get(e)
        if rule:
            mults.append(float(rule.get("kit_multiplier", 1.0) or 1.0))
            obs.append(str(rule.get("kit_observacao", "")))
            flags.append(True)
        else:
            mults.append(1.0)
            obs.append("")
            flags.append(False)

    out["kit_multiplier"] = pd.to_numeric(pd.Series(mults, index=out.index), errors="coerce").fillna(1.0)
    out["is_kit"] = pd.Series(flags, index=out.index).astype(bool)
    out["kit_observacao"] = pd.Series(obs, index=out.index).astype(str)
    out["Qtde"] = out["Qtde_original"] * out["kit_multiplier"]
    return out


def coalesce_first_existing_column(df: pd.DataFrame, options: list[str], default_name: str = None):
    for c in options:
        if c in df.columns:
            return c
    return default_name


def resolve_ean_column(df: pd.DataFrame) -> str:
    """
    Prioridade:
    1) EAN
    2) SKU / sku / Seller SKU / variações
    """
    options = [
        "EAN",
        "ean",
        "SKU",
        "sku",
        "Sku",
        "Seller SKU",
        "SellerSKU",
        "seller_sku",
        "Código SKU",
        "Codigo SKU",
    ]
    for c in options:
        if c in df.columns:
            return c
    raise ValueError(
        f"Não encontrei coluna de EAN/SKU. Colunas disponíveis: {df.columns.tolist()}"
    )


def resolve_sku_column(df: pd.DataFrame) -> str | None:
    options = [
        "SKU",
        "sku",
        "Sku",
        "Seller SKU",
        "SellerSKU",
        "seller_sku",
        "Código SKU",
        "Codigo SKU",
    ]
    for c in options:
        if c in df.columns:
            return c
    return None


def forecast_series_units(ts: pd.Series, horizon: int, min_hist: int, min_nonzero: int) -> tuple[pd.DataFrame, str]:
    """
    Forecast de UNIDADES com ExponentialSmoothing (tendência aditiva).
    Fallback: média diária recente * horizonte.
    Retorna (df_forecast, metodo).
    """
    ts = ensure_daily_index(ts)

    def fallback_mean():
        recent = ts.tail(min(60, len(ts)))
        mu = float(recent.mean()) if len(recent) else 0.0
        idx = pd.date_range(ts.index.max() + pd.Timedelta(days=1), periods=horizon, freq="D")
        vals = np.repeat(mu, horizon)
        return pd.DataFrame({"data": idx, "valor_previsto": vals}), "media_60d"

    if len(ts) < min_hist:
        return fallback_mean()

    nonzero_days = int((ts != 0).sum())
    if nonzero_days < min_nonzero:
        return fallback_mean()

    try:
        # BUGFIX 2: usa damped_trend para amortecer slope e aplica hard cap diário defensivo
        model = ExponentialSmoothing(
            ts, trend="add", damped_trend=True, seasonal=None
        ).fit(optimized=True)
        fc = model.forecast(horizon)

        mu60_cap = float(ts.mean()) if len(ts) else 0.0
        max_daily = max(mu60_cap * 3.0, 1.0)
        fc = np.maximum(np.minimum(fc.values, max_daily), 0)

        idx = pd.date_range(ts.index.max() + pd.Timedelta(days=1), periods=horizon, freq="D")
        return pd.DataFrame({"data": idx, "valor_previsto": fc}), "exp_smoothing_damped"
    except Exception:
        return fallback_mean()


def alertas_7d_por_grupo(base: pd.DataFrame, group_col: str, value_col: str, label: str,
                         QUEDA_7D_ALTA=-0.20, QUEDA_7D_MEDIA=-0.12):
    base = base.copy()
    base[DATE_COL] = pd.to_datetime(base[DATE_COL], errors="coerce")
    max_d = base[DATE_COL].max()
    if pd.isna(max_d):
        return pd.DataFrame()

    last_7_start = max_d - pd.Timedelta(days=7)
    prev_7_start = max_d - pd.Timedelta(days=14)

    alert_list = []
    for name, g in base.groupby(group_col):
        g = g.sort_values(DATE_COL)
        last_7 = g[g[DATE_COL] > last_7_start][value_col].sum()
        prev_7 = g[(g[DATE_COL] <= last_7_start) & (g[DATE_COL] > prev_7_start)][value_col].sum()
        var = pct_change(last_7, prev_7)
        if pd.isna(var):
            continue

        sev = None
        if var <= QUEDA_7D_ALTA:
            sev = "ALTA"
        elif var <= QUEDA_7D_MEDIA:
            sev = "MÉDIA"

        if sev:
            alert_list.append({
                "tipo": f"Queda 7d vs 7d anterior ({label})",
                group_col: name,
                "valor_7d": float(last_7),
                "valor_prev7d": float(prev_7),
                "variacao_pct": round(float(var) * 100, 2),
                "severidade": sev,
                "data_ref": str(max_d.date())
            })

    return pd.DataFrame(alert_list)


def _norm_channel(x: str) -> str:
    try:
        s = str(x).upper().strip()
        s = re.sub(r"\s+", " ", s)
        return s
    except Exception:
        return ""


def _is_meli_channel(x: str) -> bool:
    s = _norm_channel(x)
    return any(k in s for k in ["MELI", "MERCADO LIVRE", "ML"])


def _is_full_channel(x: str) -> bool:
    s = _norm_channel(x)
    return _is_meli_channel(s) and ("FULL" in s)


def _infer_is_fulfillment_from_channel(x: str) -> bool:
    return _is_full_channel(x)


# =========================
# FULL (35 dias)
# =========================

def compute_full_35d(df: pd.DataFrame, receita_col: str, ean_col: str, sku_col: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Reposição FULL corrigida:
    - FULL usa SOMENTE vendas de canais/anúncios FULL
    - candidatos usam SOMENTE ML fora do FULL
    - série principal: Qtde_original (bruta do parquet)
    - aplica sanity cap para evitar forecast inflado
    - principais saídas inteiras/arredondadas para cima
    """
    out_empty_cols_repl = [
        "EAN", "SKU", "Descricao", "Marca", "is_fulfillment", "classe_abc",
        "unidades_30d", "unidades_30d_ajustadas", "unidades_30d_brutas",
        "forecast_bruto_35d", "forecast_cap_35d", "sanity_cap_aplicado",
        "forecast_unid_35d", "safety_stock",
        "estoque_alvo_full_35d", "recomendacao_envio_full",
        "media_diaria_60d", "cv_60d", "dias_com_venda_90d", "unidades_90d",
        "unidades_90d_ajustadas", "metodo_forecast", "data_ref"
    ]
    out_empty_cols_cand = [
        "EAN", "SKU", "Descricao", "Marca", "is_fulfillment", "classe_abc", "score_full",
        "unidades_30d", "unidades_30d_ajustadas", "unidades_30d_brutas",
        "forecast_bruto_35d", "forecast_cap_35d", "sanity_cap_aplicado",
        "forecast_unid_35d", "qtd_segura_envio_full",
        "pedidos_90d", "dias_com_venda_90d",
        "unidades_90d", "unidades_90d_ajustadas", "receita_90d",
        "media_diaria_60d", "cv_60d", "metodo_forecast", "data_ref"
    ]

    if len(df) == 0 or ean_col not in df.columns or DATE_COL not in df.columns:
        return pd.DataFrame(columns=out_empty_cols_repl), pd.DataFrame(columns=out_empty_cols_cand), {"ok": False, "motivo": "df vazio/colunas ausentes"}

    base = df.copy()

    if "Qtde" not in base.columns:
        for alt in ["Quantidade", "Qtd", "QTD", "itens"]:
            if alt in base.columns:
                base["Qtde"] = pd.to_numeric(base[alt], errors="coerce").fillna(0)
                break
        else:
            base["Qtde"] = 0.0
    else:
        base["Qtde"] = pd.to_numeric(base["Qtde"], errors="coerce").fillna(0)

    if "Qtde_original" in base.columns:
        base["Qtde_original"] = pd.to_numeric(base["Qtde_original"], errors="coerce").fillna(base["Qtde"])
    else:
        base["Qtde_original"] = pd.to_numeric(base["Qtde"], errors="coerce").fillna(0)

    if CANAL_COL not in base.columns:
        base[CANAL_COL] = "SEM_CANAL"
    base[CANAL_COL] = base[CANAL_COL].astype(str).str.strip()

    base["EAN"] = clean_ean_series(base[ean_col])
    if sku_col and sku_col in base.columns:
        base["SKU"] = clean_sku_series(base[sku_col])
    else:
        base["SKU"] = base["EAN"].astype(str)

    desc_col = coalesce_first_existing_column(base, ["Descricao", "Descrição"], None)
    marca_col = coalesce_first_existing_column(base, ["Marca"], None)
    if desc_col is None:
        base["Descricao"] = "SEM_DESCRICAO"
        desc_col = "Descricao"
    if marca_col is None:
        base["Marca"] = "SEM_MARCA"
        marca_col = "Marca"

    base[desc_col] = base[desc_col].astype(str).str.strip()
    base[marca_col] = base[marca_col].astype(str).str.strip()
    base[DATE_COL] = pd.to_datetime(base[DATE_COL], errors="coerce")
    base = base[(base["EAN"].astype(str).str.len() > 0) & (base[DATE_COL].notna())].copy()

    if len(base) == 0:
        return pd.DataFrame(columns=out_empty_cols_repl), pd.DataFrame(columns=out_empty_cols_cand), {"ok": False, "motivo": "sem EAN válido"}

    base["is_full"] = base[CANAL_COL].apply(_is_full_channel)
    base["is_meli"] = base[CANAL_COL].apply(_is_meli_channel)

    base_full = base[base["is_full"]].copy()
    base_non_full = base[(base["is_meli"]) & (~base["is_full"])].copy()
    base_meli = base[base["is_meli"]].copy()

    if len(base_meli) == 0:
        return pd.DataFrame(columns=out_empty_cols_repl), pd.DataFrame(columns=out_empty_cols_cand), {"ok": False, "motivo": "sem vendas Mercado Livre"}

    data_ref = base_meli[DATE_COL].max()
    data_ref_str = str(pd.to_datetime(data_ref).date())
    d30_ini = data_ref - pd.Timedelta(days=JAN30 - 1)
    d60_ini = data_ref - pd.Timedelta(days=JAN60 - 1)
    d90_ini = data_ref - pd.Timedelta(days=JAN90 - 1)

    def _snap(df_src: pd.DataFrame):
        if len(df_src) == 0:
            return {}
        tmp = df_src.reset_index(drop=True).copy()
        tmp["_rowid__"] = np.arange(len(tmp))
        snap = (
            tmp.sort_values(["EAN", DATE_COL, "_rowid__"])
               .groupby("EAN", as_index=False)
               .tail(1)[["EAN", "SKU", desc_col, marca_col]]
               .rename(columns={desc_col: "Descricao", marca_col: "Marca"})
        )
        return {
            "desc": dict(zip(snap["EAN"].astype(str), snap["Descricao"].astype(str))),
            "marca": dict(zip(snap["EAN"].astype(str), snap["Marca"].astype(str))),
            "sku": dict(zip(snap["EAN"].astype(str), snap["SKU"].astype(str))),
        }

    snap_full = _snap(base_full)
    snap_non = _snap(base_non_full)
    snap_all = _snap(base_meli)

    def _map_value(primary, fallback, key, default):
        return str(primary.get(key, fallback.get(key, default)))

    def _abc_map(df_src: pd.DataFrame):
        if len(df_src) == 0:
            return {}
        tmp = df_src.copy()
        tmp[receita_col] = pd.to_numeric(tmp[receita_col], errors="coerce").fillna(0.0)
        abc_agg = tmp.groupby("EAN", as_index=False).agg(receita=(receita_col, "sum")).sort_values("receita", ascending=False)
        total_rec = float(abc_agg["receita"].sum()) if len(abc_agg) else 0.0
        abc_agg["pct"] = (abc_agg["receita"] / total_rec) if total_rec else 0.0
        abc_agg["pct_acum"] = abc_agg["pct"].cumsum()
        abc_agg["classe_abc"] = np.select([abc_agg["pct_acum"] <= 0.80, abc_agg["pct_acum"] <= 0.95], ["A", "B"], default="C")
        return dict(zip(abc_agg["EAN"].astype(str), abc_agg["classe_abc"].astype(str)))

    abc_full_map = _abc_map(base_full)
    abc_non_map = _abc_map(base_non_full)

    def _build_metrics(df_src: pd.DataFrame):
        b30 = df_src[(df_src[DATE_COL] >= d30_ini) & (df_src[DATE_COL] <= data_ref)].copy()
        b60 = df_src[(df_src[DATE_COL] >= d60_ini) & (df_src[DATE_COL] <= data_ref)].copy()
        b90 = df_src[(df_src[DATE_COL] >= d90_ini) & (df_src[DATE_COL] <= data_ref)].copy()

        m30 = b30.groupby("EAN", as_index=False).agg(
            unidades_30d=("Qtde_original", "sum"),
            unidades_30d_brutas=("Qtde_original", "sum"),
            unidades_30d_ajustadas=("Qtde", "sum"),
        ) if len(b30) else pd.DataFrame(columns=["EAN", "unidades_30d", "unidades_30d_brutas", "unidades_30d_ajustadas"])

        pedidos_agg = (KEY_COL, "nunique") if KEY_COL in b90.columns else (receita_col, "count")
        m90 = b90.groupby("EAN", as_index=False).agg(
            unidades_90d=("Qtde_original", "sum"),
            unidades_90d_ajustadas=("Qtde", "sum"),
            receita_90d=(receita_col, lambda x: pd.to_numeric(x, errors="coerce").fillna(0).sum()),
            pedidos_90d=pedidos_agg,
        ) if len(b90) else pd.DataFrame(columns=["EAN", "unidades_90d", "unidades_90d_ajustadas", "receita_90d", "pedidos_90d"])

        if len(b90):
            tmp = b90.copy()
            tmp["_d"] = pd.to_datetime(tmp[DATE_COL], errors="coerce").dt.date
            tmp["_has"] = pd.to_numeric(tmp["Qtde_original"], errors="coerce").fillna(0) > 0
            days_sell = tmp[tmp["_has"]].groupby("EAN", as_index=False).agg(dias_com_venda_90d=("_d", "nunique"))
            m90 = m90.merge(days_sell, on="EAN", how="left")
            m90["dias_com_venda_90d"] = m90["dias_com_venda_90d"].fillna(0).astype(int)
        else:
            m90["dias_com_venda_90d"] = []

        day = b60.groupby([DATE_COL, "EAN"], as_index=False).agg(unidades=("Qtde_original", "sum")) if len(b60) else pd.DataFrame(columns=[DATE_COL, "EAN", "unidades"])
        return m30, m90, day

    m30_full, m90_full, day_full = _build_metrics(base_full)
    m30_non, m90_non, day_non = _build_metrics(base_non_full)

    full_eans_sorted = sorted(m90_full["EAN"].astype(str).unique().tolist()) if len(m90_full) else []
    non_eans = set(m90_non["EAN"].astype(str).unique().tolist()) if len(m90_non) else set()
    full_eans = set(full_eans_sorted)
    cand_eans = sorted(list(non_eans - full_eans))

    all_days_60 = pd.date_range(pd.to_datetime(d60_ini).normalize(), pd.to_datetime(data_ref).normalize(), freq="D")

    def _metric_scalar(df_src, ean, col, default=0.0, cast=float):
        if df_src is None or len(df_src) == 0 or col not in df_src.columns:
            return cast(default)
        r = df_src[df_src["EAN"].astype(str) == str(ean)]
        if len(r) == 0:
            return cast(default)
        try:
            return cast(r.iloc[0][col])
        except Exception:
            return cast(default)

    def _calc_series_forecast(day_df: pd.DataFrame, ean: str):
        g = day_df[day_df["EAN"].astype(str) == str(ean)].copy()
        ts = pd.Series(0.0, index=all_days_60)
        if len(g):
            g[DATE_COL] = pd.to_datetime(g[DATE_COL], errors="coerce")
            g = g.dropna(subset=[DATE_COL])
            s = g.set_index(DATE_COL)["unidades"].astype(float)
            s = s.groupby(s.index).sum()
            idx_ok = s.index.intersection(ts.index)
            ts.loc[idx_ok] = s.loc[idx_ok].values
        ts = ensure_daily_index(ts)

        mu60 = float(ts.mean()) if len(ts) else 0.0
        sd60 = float(ts.std(ddof=0)) if len(ts) else 0.0
        cv60 = sd60 / mu60 if mu60 > 0 else np.nan

        fc_df, metodo = forecast_series_units(ts, FULL_HORIZON_DIAS, FULL_MIN_DIAS_HIST, FULL_MIN_DIAS_NAO_ZERO)
        forecast_bruto = float(pd.to_numeric(fc_df["valor_previsto"], errors="coerce").fillna(0).sum()) if len(fc_df) else 0.0

        # BUGFIX 3: mantém sanity cap com barreira dupla defensiva
        vendas_30 = float(ts.tail(JAN30).sum()) if len(ts) else 0.0
        linear_35 = (vendas_30 / JAN30) * FULL_HORIZON_DIAS if JAN30 else 0.0
        cap_35 = float(np.ceil(max(linear_35 * 1.8, linear_35, 0.0)))
        cap_absoluto = float(np.ceil(max(linear_35 * 3.0, linear_35, 0.0)))
        cap_35 = min(cap_35, cap_absoluto)
        cap_aplicado = bool(forecast_bruto > cap_35 and cap_35 > 0)
        forecast_final = min(forecast_bruto, cap_35) if cap_35 > 0 else forecast_bruto

        if np.isnan(cv60):
            z = 1.10
        elif cv60 <= 0.35:
            z = 1.05
        elif cv60 <= 0.75:
            z = 1.20
        else:
            z = 1.35

        # BUGFIX 4: limita safety stock a 30% do forecast final
        safety_raw = max(float(z * sd60 * np.sqrt(FULL_HORIZON_DIAS)), 0.0)
        safety_max = forecast_final * 0.30
        safety = min(safety_raw, safety_max)
        return forecast_bruto, cap_35, cap_aplicado, forecast_final, safety, mu60, cv60, metodo

    repl_rows = []
    for ean in full_eans_sorted:
        forecast_bruto, forecast_cap, cap_aplicado, forecast_35, safety, mu60, cv60, metodo = _calc_series_forecast(day_full, ean)
        unidades_30 = _metric_scalar(m30_full, ean, "unidades_30d", 0.0, float)
        unidades_30_brutas = _metric_scalar(m30_full, ean, "unidades_30d_brutas", 0.0, float)
        unidades_30_aj = _metric_scalar(m30_full, ean, "unidades_30d_ajustadas", 0.0, float)
        unidades_90 = _metric_scalar(m90_full, ean, "unidades_90d", 0.0, float)
        unidades_90_aj = _metric_scalar(m90_full, ean, "unidades_90d_ajustadas", 0.0, float)
        dias_venda_90 = _metric_scalar(m90_full, ean, "dias_com_venda_90d", 0, int)

        estoque_alvo = float(forecast_35 + safety)
        recomendacao_envio = int(np.ceil(estoque_alvo))

        repl_rows.append({
            "EAN": str(ean),
            "SKU": _map_value(snap_full.get("sku", {}), snap_all.get("sku", {}), str(ean), str(ean)),
            "Descricao": _map_value(snap_full.get("desc", {}), snap_all.get("desc", {}), str(ean), "SEM_DESCRICAO"),
            "Marca": _map_value(snap_full.get("marca", {}), snap_all.get("marca", {}), str(ean), "SEM_MARCA"),
            "is_fulfillment": True,
            "classe_abc": str(abc_full_map.get(str(ean), "C")),
            "unidades_30d": int(np.ceil(unidades_30)),
            "unidades_30d_brutas": int(np.ceil(unidades_30_brutas)),
            "unidades_30d_ajustadas": int(np.ceil(unidades_30_aj)),
            "forecast_bruto_35d": int(np.ceil(forecast_bruto)),
            "forecast_cap_35d": int(np.ceil(forecast_cap)),
            "sanity_cap_aplicado": bool(cap_aplicado),
            "forecast_unid_35d": int(np.ceil(forecast_35)),
            "safety_stock": int(np.ceil(safety)),
            "estoque_alvo_full_35d": int(np.ceil(estoque_alvo)),
            "recomendacao_envio_full": int(max(recomendacao_envio, 0)),
            "media_diaria_60d": float(round(mu60, 6)),
            "cv_60d": float(round(cv60, 6)) if not np.isnan(cv60) else np.nan,
            "dias_com_venda_90d": int(dias_venda_90),
            "unidades_90d": int(np.ceil(unidades_90)),
            "unidades_90d_ajustadas": int(np.ceil(unidades_90_aj)),
            "metodo_forecast": metodo,
            "data_ref": data_ref_str,
        })

    repl = pd.DataFrame(repl_rows)
    if len(repl):
        repl = repl.sort_values(["recomendacao_envio_full", "forecast_unid_35d"], ascending=False)

    cand_rows = []
    for ean in cand_eans:
        forecast_bruto, forecast_cap, cap_aplicado, forecast_35, safety, mu60, cv60, metodo = _calc_series_forecast(day_non, ean)
        unidades_30 = _metric_scalar(m30_non, ean, "unidades_30d", 0.0, float)
        unidades_30_brutas = _metric_scalar(m30_non, ean, "unidades_30d_brutas", 0.0, float)
        unidades_30_aj = _metric_scalar(m30_non, ean, "unidades_30d_ajustadas", 0.0, float)
        unidades_90 = _metric_scalar(m90_non, ean, "unidades_90d", 0.0, float)
        unidades_90_aj = _metric_scalar(m90_non, ean, "unidades_90d_ajustadas", 0.0, float)
        receita_90 = _metric_scalar(m90_non, ean, "receita_90d", 0.0, float)
        pedidos_90 = _metric_scalar(m90_non, ean, "pedidos_90d", 0, int)
        dias_venda_90 = _metric_scalar(m90_non, ean, "dias_com_venda_90d", 0, int)

        if dias_venda_90 < 5 or unidades_90 < 8 or pedidos_90 < 3 or forecast_35 < 4:
            continue

        classe = str(abc_non_map.get(str(ean), "C"))
        w_abc = {"A": 1.00, "B": 0.70, "C": 0.45}.get(classe, 0.45)
        stab = 0.50 if np.isnan(cv60) else float(1.0 / (1.0 + max(cv60, 0)))
        dem = float(np.log1p(max(forecast_35, 0)))
        score = (dem * 0.60 + stab * 0.40) * w_abc

        qtd_base = 0.60 * forecast_35 + 0.50 * safety
        teto = max(0.50 * unidades_90, 1.0)
        qtd_segura = int(np.ceil(min(qtd_base, teto)))
        qtd_segura = max(qtd_segura, 1)

        cand_rows.append({
            "EAN": str(ean),
            "SKU": _map_value(snap_non.get("sku", {}), snap_all.get("sku", {}), str(ean), str(ean)),
            "Descricao": _map_value(snap_non.get("desc", {}), snap_all.get("desc", {}), str(ean), "SEM_DESCRICAO"),
            "Marca": _map_value(snap_non.get("marca", {}), snap_all.get("marca", {}), str(ean), "SEM_MARCA"),
            "is_fulfillment": False,
            "classe_abc": classe,
            "score_full": float(round(score, 6)),
            "unidades_30d": int(np.ceil(unidades_30)),
            "unidades_30d_brutas": int(np.ceil(unidades_30_brutas)),
            "unidades_30d_ajustadas": int(np.ceil(unidades_30_aj)),
            "forecast_bruto_35d": int(np.ceil(forecast_bruto)),
            "forecast_cap_35d": int(np.ceil(forecast_cap)),
            "sanity_cap_aplicado": bool(cap_aplicado),
            "forecast_unid_35d": int(np.ceil(forecast_35)),
            "qtd_segura_envio_full": int(qtd_segura),
            "pedidos_90d": int(pedidos_90),
            "dias_com_venda_90d": int(dias_venda_90),
            "unidades_90d": int(np.ceil(unidades_90)),
            "unidades_90d_ajustadas": int(np.ceil(unidades_90_aj)),
            "receita_90d": float(round(receita_90, 4)),
            "media_diaria_60d": float(round(mu60, 6)),
            "cv_60d": float(round(cv60, 6)) if not np.isnan(cv60) else np.nan,
            "metodo_forecast": metodo,
            "data_ref": data_ref_str,
        })

    cand = pd.DataFrame(cand_rows)
    if len(cand):
        cand = cand.sort_values(["score_full", "forecast_unid_35d"], ascending=False)

    resumo_full = {
        "ok": True,
        "data_ref": data_ref_str,
        "horizonte_dias": FULL_HORIZON_DIAS,
        "base_calculo": "somente_anuncios_full_do_parquet_serie_bruta",
        "serie_principal": "Qtde_original",
        "eans_meli_90d": int(len(set(base_meli[(base_meli[DATE_COL] >= d90_ini) & (base_meli[DATE_COL] <= data_ref)]["EAN"].astype(str).unique().tolist()))),
        "eans_full_90d": int(len(full_eans)),
        "eans_ml_fora_full_90d": int(len(non_eans)),
        "candidatos_brutos": int(len(cand_eans)),
        "candidatos_filtrados": int(len(cand)),
    }

    return repl.reindex(columns=out_empty_cols_repl), cand.reindex(columns=out_empty_cols_cand), resumo_full


def gerar_auditoria_full(df: pd.DataFrame, repl: pd.DataFrame, receita_col: str, ean_col: str) -> pd.DataFrame:
    if len(df) == 0 or ean_col not in df.columns or DATE_COL not in df.columns:
        return pd.DataFrame()

    base = df.copy()
    base["EAN"] = clean_ean_series(base[ean_col])
    base[DATE_COL] = pd.to_datetime(base[DATE_COL], errors="coerce")
    base = base[base["EAN"].astype(str).str.len() > 0].copy()
    base = base[base[DATE_COL].notna()].copy()

    if "Qtde" not in base.columns:
        for alt in ["Quantidade", "Qtd", "QTD", "itens"]:
            if alt in base.columns:
                base["Qtde"] = pd.to_numeric(base[alt], errors="coerce").fillna(0)
                break
        else:
            base["Qtde"] = 0.0
    else:
        base["Qtde"] = pd.to_numeric(base["Qtde"], errors="coerce").fillna(0)

    if "Qtde_original" in base.columns:
        base["Qtde_original"] = pd.to_numeric(base["Qtde_original"], errors="coerce").fillna(base["Qtde"])
    else:
        base["Qtde_original"] = base["Qtde"]

    if CANAL_COL not in base.columns:
        base[CANAL_COL] = "SEM_CANAL"
    base[CANAL_COL] = base[CANAL_COL].astype(str).str.strip()
    base["is_full"] = base[CANAL_COL].apply(_is_full_channel)
    base["is_meli"] = base[CANAL_COL].apply(_is_meli_channel)

    data_ref = base[DATE_COL].max()
    d30_ini = data_ref - pd.Timedelta(days=JAN30 - 1)
    d60_ini = data_ref - pd.Timedelta(days=JAN60 - 1)
    d90_ini = data_ref - pd.Timedelta(days=JAN90 - 1)

    def _agg(df_src: pd.DataFrame, label: str):
        if len(df_src) == 0:
            return pd.DataFrame(columns=["EAN"])
        out = df_src[df_src[DATE_COL] >= d90_ini].groupby("EAN", as_index=False).agg(
            **{
                f"unidades_30d_{label}": ("Qtde_original", lambda x: float(df_src.loc[x.index][df_src.loc[x.index, DATE_COL] >= d30_ini]["Qtde_original"].sum())),
                f"unidades_60d_{label}": ("Qtde_original", lambda x: float(df_src.loc[x.index][df_src.loc[x.index, DATE_COL] >= d60_ini]["Qtde_original"].sum())),
                f"unidades_90d_{label}": ("Qtde_original", "sum"),
            }
        )
        return out

    full_df = base[base["is_full"]].copy()
    out_df = base[(base["is_meli"]) & (~base["is_full"])].copy()

    a_full = _agg(full_df, "full")
    a_out = _agg(out_df, "ml_fora_full")
    audit = a_full.merge(a_out, on="EAN", how="outer").fillna(0)

    if repl is not None and len(repl):
        cols = [c for c in ["EAN", "forecast_bruto_35d", "forecast_cap_35d", "sanity_cap_aplicado", "forecast_unid_35d", "recomendacao_envio_full"] if c in repl.columns]
        audit = audit.merge(repl[cols], on="EAN", how="left")

    audit["unidades_30d_total_ml"] = audit.get("unidades_30d_full", 0) + audit.get("unidades_30d_ml_fora_full", 0)
    audit["ratio_full_vs_total_ml"] = np.where(audit["unidades_30d_total_ml"] > 0, audit["unidades_30d_full"] / audit["unidades_30d_total_ml"], 0.0)
    audit["flag_cross_listing_inflado"] = np.where(audit.get("unidades_30d_ml_fora_full", 0) > audit.get("unidades_30d_full", 0) * 1.5, "SIM", "OK")
    audit["data_ref"] = str(pd.to_datetime(data_ref).date())

    # normalização visual
    int_cols = [c for c in audit.columns if c.startswith("unidades_") or c in ["forecast_bruto_35d", "forecast_cap_35d", "forecast_unid_35d", "recomendacao_envio_full"]]
    for c in int_cols:
        audit[c] = pd.to_numeric(audit[c], errors="coerce").fillna(0).apply(lambda v: int(np.ceil(v)))

    return audit

# =========================
# REPOSIÇÃO GERAL (ESTOQUE INTERNO)
# =========================
def compute_geral_reposicao(df: pd.DataFrame, receita_col: str, ean_col: str, sku_col: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Análise geral de reposição por EAN:
    - base de cálculo: EAN
    - estoque atual: valor do Estoque Local na última data válida de cada EAN
    - vendas 30/60/90d
    - cobertura 30/60/90d
    - forecast 30/60/90d em UNIDADES
    - tendência / aceleração
    - reposição sugerida para 30/60/90 dias
    - ordenação por urgência
    """

    out_cols = [
        "EAN",
        "SKU",
        "Descricao",
        "Marca",
        "is_kit", "kit_multiplier", "kit_observacao",
        "data_ref_global",
        "data_ult_estoque_ean",
        "estoque_atual",
        "vendas_30d", "vendas_30d_brutas",
        "vendas_60d",
        "vendas_90d",
        "receita_90d",
        "pedidos_90d",
        "dias_com_venda_90d",
        "media_diaria_30d",
        "media_diaria_60d",
        "media_diaria_90d",
        "cobertura_30d",
        "cobertura_60d",
        "cobertura_90d",
        "status_cobertura_30d",
        "status_cobertura_60d",
        "status_cobertura_90d",
        "forecast_unid_30d",
        "forecast_unid_60d",
        "forecast_unid_90d",
        "metodo_forecast",
        "safety_stock_30d",
        "safety_stock_60d",
        "safety_stock_90d",
        "reposicao_sugerida_30d",
        "reposicao_sugerida_60d",
        "reposicao_sugerida_90d",
        "media_prev_30d",
        "media_ult_30d",
        "crescimento_ult30_vs_prev30_pct",
        "trend_status",
        "trend_score",
        "classe_abc",
        "score_urgencia",
        "ordem_urgencia"
    ]

    out_cols_accel = [
        "EAN",
        "SKU",
        "Descricao",
        "Marca",
        "is_kit", "kit_multiplier", "kit_observacao",
        "estoque_atual",
        "vendas_30d", "vendas_30d_brutas",
        "vendas_60d",
        "vendas_90d",
        "forecast_unid_30d",
        "forecast_unid_60d",
        "forecast_unid_90d",
        "cobertura_90d",
        "status_cobertura_90d",
        "media_prev_30d",
        "media_ult_30d",
        "crescimento_ult30_vs_prev30_pct",
        "trend_status",
        "trend_score",
        "classe_abc",
        "score_urgencia",
        "reposicao_sugerida_90d",
        "data_ult_estoque_ean",
        "data_ref_global"
    ]

    if len(df) == 0 or ean_col not in df.columns or DATE_COL not in df.columns:
        return (
            pd.DataFrame(columns=out_cols),
            pd.DataFrame(columns=out_cols_accel),
            {"ok": False, "motivo": "df vazio/colunas ausentes"}
        )

    base = df.copy()

    if "Qtde" not in base.columns:
        for alt in ["Quantidade", "Qtd", "QTD", "itens"]:
            if alt in base.columns:
                base["Qtde"] = pd.to_numeric(base[alt], errors="coerce").fillna(0)
                break
        else:
            base["Qtde"] = 0
    else:
        base["Qtde"] = pd.to_numeric(base["Qtde"], errors="coerce").fillna(0)

    base["EAN"] = clean_ean_series(base[ean_col])

    if sku_col and sku_col in base.columns:
        base["SKU"] = clean_sku_series(base[sku_col])
    else:
        base["SKU"] = base["EAN"].astype(str)

    desc_col = coalesce_first_existing_column(base, ["Descricao", "Descrição"], None)
    marca_col = coalesce_first_existing_column(base, ["Marca"], None)

    if desc_col is None:
        base["Descricao"] = "SEM_DESCRICAO"
        desc_col = "Descricao"
    else:
        base[desc_col] = base[desc_col].astype(str).str.strip().replace({"nan": "SEM_DESCRICAO"})

    if marca_col is None:
        base["Marca"] = "SEM_MARCA"
        marca_col = "Marca"
    else:
        base[marca_col] = base[marca_col].astype(str).str.strip().replace({"nan": "SEM_MARCA"})

    base = base[base["EAN"].astype(str).str.len() > 0].copy()
    if len(base) == 0:
        return (
            pd.DataFrame(columns=out_cols),
            pd.DataFrame(columns=out_cols_accel),
            {"ok": False, "motivo": "sem EAN válido"}
        )

    if ESTOQUE_COL not in base.columns:
        base[ESTOQUE_COL] = np.nan
    base[ESTOQUE_COL] = base[ESTOQUE_COL].apply(parse_br_number)
    base[ESTOQUE_COL] = pd.to_numeric(base[ESTOQUE_COL], errors="coerce")

    base[receita_col] = pd.to_numeric(base[receita_col], errors="coerce").fillna(0.0)

    data_ref = pd.to_datetime(base[DATE_COL], errors="coerce").max()
    if pd.isna(data_ref):
        return (
            pd.DataFrame(columns=out_cols),
            pd.DataFrame(columns=out_cols_accel),
            {"ok": False, "motivo": "data_ref inválida"}
        )

    data_ref_str = str(data_ref.date())

    d30_ini = data_ref - pd.Timedelta(days=JAN30 - 1)
    d60_ini = data_ref - pd.Timedelta(days=JAN60 - 1)
    d90_ini = data_ref - pd.Timedelta(days=JAN90 - 1)

    b30 = base[(base[DATE_COL] >= d30_ini) & (base[DATE_COL] <= data_ref)].copy()
    b60 = base[(base[DATE_COL] >= d60_ini) & (base[DATE_COL] <= data_ref)].copy()
    b90 = base[(base[DATE_COL] >= d90_ini) & (base[DATE_COL] <= data_ref)].copy()

    abc = (
        base.groupby("EAN", as_index=False)
            .agg(receita=(receita_col, "sum"))
            .sort_values("receita", ascending=False)
    )
    total_rec = float(abc["receita"].sum()) if len(abc) else 0.0
    abc["pct"] = (abc["receita"] / total_rec) if total_rec else 0.0
    abc["pct_acum"] = abc["pct"].cumsum()
    abc["classe_abc"] = np.select(
        [abc["pct_acum"] <= 0.80, abc["pct_acum"] <= 0.95],
        ["A", "B"],
        default="C"
    )
    abc_map = dict(zip(abc["EAN"].astype(str), abc["classe_abc"].astype(str)))

    base = base.reset_index(drop=True).copy()
    base["_rowid__"] = np.arange(len(base))
    snap = (
        base.sort_values(["EAN", DATE_COL, "_rowid__"])
            .groupby("EAN", as_index=False)
            .tail(1)
            [["EAN", "SKU", DATE_COL, ESTOQUE_COL, desc_col, marca_col, "is_kit", "kit_multiplier", "kit_observacao"]]
            .rename(columns={
                DATE_COL: "data_ult_estoque_ean",
                ESTOQUE_COL: "estoque_atual",
                desc_col: "Descricao",
                marca_col: "Marca"
            })
            .copy()
    )
    snap["estoque_atual"] = pd.to_numeric(snap["estoque_atual"], errors="coerce").fillna(0)
    snap["data_ult_estoque_ean"] = pd.to_datetime(snap["data_ult_estoque_ean"], errors="coerce")

    met_90 = (
        b90.groupby("EAN", as_index=False)
           .agg(
               vendas_90d=("Qtde", "sum"),
               vendas_90d_brutas=("Qtde_original", "sum"),
               receita_90d=(receita_col, "sum"),
               pedidos_90d=(KEY_COL, "nunique")
           )
    )

    tmp90 = b90.copy()
    tmp90["_dia"] = pd.to_datetime(tmp90[DATE_COL], errors="coerce").dt.date
    tmp90 = tmp90[tmp90["Qtde"] > 0]
    dias_sell_90 = (
        tmp90.groupby("EAN", as_index=False)
             .agg(dias_com_venda_90d=("_dia", "nunique"))
    )

    day_ean = (
        base.groupby([DATE_COL, "EAN"], as_index=False)
            .agg(unidades=("Qtde", "sum"))
    )

    all_days_hist = pd.date_range(base[DATE_COL].min().normalize(), data_ref.normalize(), freq="D")
    eans_all = sorted(base["EAN"].astype(str).dropna().unique().tolist())

    rows = []

    for ean in eans_all:
        g = day_ean[day_ean["EAN"].astype(str) == str(ean)].copy()

        ts = pd.Series(0.0, index=all_days_hist)
        if len(g):
            g[DATE_COL] = pd.to_datetime(g[DATE_COL], errors="coerce")
            g = g.dropna(subset=[DATE_COL])
            s = g.set_index(DATE_COL)["unidades"].astype(float)
            s = s.groupby(s.index).sum()
            idx_ok = s.index.intersection(ts.index)
            ts.loc[idx_ok] = s.loc[idx_ok].values

        ts = ensure_daily_index(ts)

        ts30 = ts.loc[ts.index >= d30_ini.normalize()]
        ts60 = ts.loc[ts.index >= d60_ini.normalize()]
        ts90 = ts.loc[ts.index >= d90_ini.normalize()]

        vendas_30 = float(ts30.sum()) if len(ts30) else 0.0
        vendas_60 = float(ts60.sum()) if len(ts60) else 0.0
        vendas_90 = float(ts90.sum()) if len(ts90) else 0.0

        ts_raw = pd.Series(base.loc[base["EAN"].astype(str) == str(ean), "Qtde_original"].values,
                           index=base.loc[base["EAN"].astype(str) == str(ean), DATE_COL]).groupby(level=0).sum() if len(base.loc[base["EAN"].astype(str) == str(ean)]) else pd.Series(dtype=float)
        ts_raw_full = pd.Series(0.0, index=all_days_hist)
        if len(ts_raw):
            idx_ok_raw = ts_raw.index.intersection(ts_raw_full.index)
            ts_raw_full.loc[idx_ok_raw] = ts_raw.loc[idx_ok_raw].values
        ts30_raw = ts_raw_full.loc[ts_raw_full.index >= d30_ini.normalize()]
        vendas_30_brutas = float(ts30_raw.sum()) if len(ts30_raw) else 0.0

        media_30 = float(vendas_30 / JAN30)
        media_60 = float(vendas_60 / JAN60)
        media_90 = float(vendas_90 / JAN90)

        snap_row = snap[snap["EAN"].astype(str) == str(ean)]
        estoque_atual = float(snap_row["estoque_atual"].iloc[0]) if len(snap_row) else 0.0
        data_ult_estoque_ean = snap_row["data_ult_estoque_ean"].iloc[0] if len(snap_row) else pd.NaT
        descricao = str(snap_row["Descricao"].iloc[0]) if len(snap_row) else "SEM_DESCRICAO"
        marca = str(snap_row["Marca"].iloc[0]) if len(snap_row) else "SEM_MARCA"
        sku = str(snap_row["SKU"].iloc[0]) if len(snap_row) else str(ean)
        is_kit = bool(snap_row["is_kit"].iloc[0]) if len(snap_row) else False
        kit_multiplier = float(snap_row["kit_multiplier"].iloc[0]) if len(snap_row) else 1.0
        kit_observacao = str(snap_row["kit_observacao"].iloc[0]) if len(snap_row) else ""

        cobertura_30 = float(estoque_atual / media_30) if media_30 > 0 else np.nan
        cobertura_60 = float(estoque_atual / media_60) if media_60 > 0 else np.nan
        cobertura_90 = float(estoque_atual / media_90) if media_90 > 0 else np.nan

        prev30_start = d60_ini.normalize()
        prev30_end = (d30_ini - pd.Timedelta(days=1)).normalize()
        ts_prev30 = ts.loc[(ts.index >= prev30_start) & (ts.index <= prev30_end)]
        media_prev_30 = float(ts_prev30.mean()) if len(ts_prev30) else 0.0
        media_ult_30 = float(ts30.mean()) if len(ts30) else 0.0

        growth_30 = pct_change(media_ult_30, media_prev_30)
        trend_label = classify_trend(growth_30)
        tr_score = trend_score(growth_30)

        fc30_df, metodo = forecast_series_units(ts, 30, GERAL_MIN_DIAS_HIST, GERAL_MIN_DIAS_NAO_ZERO)
        fc60_df, _ = forecast_series_units(ts, 60, GERAL_MIN_DIAS_HIST, GERAL_MIN_DIAS_NAO_ZERO)
        fc90_df, _ = forecast_series_units(ts, 90, GERAL_MIN_DIAS_HIST, GERAL_MIN_DIAS_NAO_ZERO)

        forecast_30 = float(pd.to_numeric(fc30_df["valor_previsto"], errors="coerce").fillna(0).sum()) if len(fc30_df) else 0.0
        forecast_60 = float(pd.to_numeric(fc60_df["valor_previsto"], errors="coerce").fillna(0).sum()) if len(fc60_df) else 0.0
        forecast_90 = float(pd.to_numeric(fc90_df["valor_previsto"], errors="coerce").fillna(0).sum()) if len(fc90_df) else 0.0

        ts60_for_sd = ts.loc[ts.index >= d60_ini.normalize()]
        sd60 = float(ts60_for_sd.std(ddof=0)) if len(ts60_for_sd) else 0.0
        mu60 = float(ts60_for_sd.mean()) if len(ts60_for_sd) else 0.0
        cv60 = float(sd60 / mu60) if mu60 > 0 else np.nan

        if np.isnan(cv60):
            z = 1.10
        else:
            if cv60 <= 0.35:
                z = 1.05
            elif cv60 <= 0.75:
                z = 1.20
            else:
                z = 1.35

        safety_30 = float(max(z * sd60 * np.sqrt(30), 0.0))
        safety_60 = float(max(z * sd60 * np.sqrt(60), 0.0))
        safety_90 = float(max(z * sd60 * np.sqrt(90), 0.0))

        alvo_30_base = max(forecast_30, media_ult_30 * 30)
        alvo_60_base = max(forecast_60, media_ult_30 * 60)
        alvo_90_base = max(forecast_90, media_ult_30 * 90)

        alvo_30 = alvo_30_base + safety_30
        alvo_60 = alvo_60_base + safety_60
        alvo_90 = alvo_90_base + safety_90

        repos_30 = int(max(np.ceil(alvo_30 - estoque_atual), 0))
        repos_60 = int(max(np.ceil(alvo_60 - estoque_atual), 0))
        repos_90 = int(max(np.ceil(alvo_90 - estoque_atual), 0))

        r90 = met_90[met_90["EAN"].astype(str) == str(ean)]
        rd90 = dias_sell_90[dias_sell_90["EAN"].astype(str) == str(ean)]

        receita_90d = float(r90["receita_90d"].iloc[0]) if len(r90) else 0.0
        pedidos_90d = int(r90["pedidos_90d"].iloc[0]) if len(r90) else 0
        dias_com_venda_90d = int(rd90["dias_com_venda_90d"].iloc[0]) if len(rd90) else 0

        abc_cls = str(abc_map.get(str(ean), "C"))
        abc_weight = {"A": 1.00, "B": 0.70, "C": 0.45}.get(abc_cls, 0.45)

        cobertura_base = cobertura_90 if pd.notna(cobertura_90) else 9999.0
        if cobertura_base <= 15:
            cobertura_factor = 1.00
        elif cobertura_base <= 30:
            cobertura_factor = 0.85
        elif cobertura_base <= 60:
            cobertura_factor = 0.65
        elif cobertura_base <= 90:
            cobertura_factor = 0.40
        elif cobertura_base <= 120:
            cobertura_factor = 0.20
        else:
            cobertura_factor = 0.05

        repos_factor = float(np.log1p(max(repos_90, 0)))
        demanda_factor = float(np.log1p(max(vendas_90, 0)))
        urg_score = (
            cobertura_factor * 0.50
            + min(tr_score, 1.5) * 0.20
            + abc_weight * 0.15
            + min(repos_factor / 5.0, 1.0) * 0.10
            + min(demanda_factor / 5.0, 1.0) * 0.05
        )

        row = {
            "EAN": str(ean),
            "SKU": sku,
            "Descricao": descricao,
            "Marca": marca,
            "is_kit": bool(is_kit),
            "kit_multiplier": float(round(kit_multiplier, 4)),
            "kit_observacao": kit_observacao,
            "data_ref_global": data_ref_str,
            "data_ult_estoque_ean": str(pd.to_datetime(data_ult_estoque_ean).date()) if pd.notna(data_ult_estoque_ean) else None,
            "estoque_atual": float(round(estoque_atual, 4)),
            "vendas_30d": float(round(vendas_30, 4)),
            "vendas_30d_brutas": float(round(vendas_30_brutas, 4)),
            "vendas_60d": float(round(vendas_60, 4)),
            "vendas_90d": float(round(vendas_90, 4)),
            "receita_90d": float(round(receita_90d, 4)),
            "pedidos_90d": int(pedidos_90d),
            "dias_com_venda_90d": int(dias_com_venda_90d),
            "media_diaria_30d": float(round(media_30, 6)),
            "media_diaria_60d": float(round(media_60, 6)),
            "media_diaria_90d": float(round(media_90, 6)),
            "cobertura_30d": float(round(cobertura_30, 4)) if pd.notna(cobertura_30) else np.nan,
            "cobertura_60d": float(round(cobertura_60, 4)) if pd.notna(cobertura_60) else np.nan,
            "cobertura_90d": float(round(cobertura_90, 4)) if pd.notna(cobertura_90) else np.nan,
            "status_cobertura_30d": classify_cobertura(cobertura_30),
            "status_cobertura_60d": classify_cobertura(cobertura_60),
            "status_cobertura_90d": classify_cobertura(cobertura_90),
            "forecast_unid_30d": float(round(forecast_30, 4)),
            "forecast_unid_60d": float(round(forecast_60, 4)),
            "forecast_unid_90d": float(round(forecast_90, 4)),
            "metodo_forecast": metodo,
            "safety_stock_30d": float(round(safety_30, 4)),
            "safety_stock_60d": float(round(safety_60, 4)),
            "safety_stock_90d": float(round(safety_90, 4)),
            "reposicao_sugerida_30d": int(repos_30),
            "reposicao_sugerida_60d": int(repos_60),
            "reposicao_sugerida_90d": int(repos_90),
            "media_prev_30d": float(round(media_prev_30, 6)),
            "media_ult_30d": float(round(media_ult_30, 6)),
            "crescimento_ult30_vs_prev30_pct": float(round(growth_30 * 100, 2)) if pd.notna(growth_30) else np.nan,
            "trend_status": trend_label,
            "trend_score": float(round(tr_score, 6)),
            "classe_abc": abc_cls,
            "score_urgencia": float(round(urg_score, 6)),
            "ordem_urgencia": int(urgency_rank(classify_cobertura(cobertura_90))),
        }
        rows.append(row)

    repos = pd.DataFrame(rows)
    if len(repos) == 0:
        return (
            pd.DataFrame(columns=out_cols),
            pd.DataFrame(columns=out_cols_accel),
            {"ok": False, "motivo": "sem linhas após processamento"}
        )

    repos = repos.sort_values(
        by=[
            "ordem_urgencia",
            "score_urgencia",
            "reposicao_sugerida_90d",
            "forecast_unid_90d",
            "vendas_90d"
        ],
        ascending=[True, False, False, False, False]
    ).reset_index(drop=True)

    accel = repos[
        repos["trend_status"].isin(["ACELERANDO", "ACELERANDO_FORTE"])
    ].copy()

    if len(accel):
        accel = accel.sort_values(
            by=[
                "score_urgencia",
                "crescimento_ult30_vs_prev30_pct",
                "reposicao_sugerida_90d",
                "forecast_unid_90d"
            ],
            ascending=[False, False, False, False]
        ).reset_index(drop=True)

    resumo = {
        "ok": True,
        "data_ref": data_ref_str,
        "eans_analisados": int(repos["EAN"].nunique()),
        "acelerando": int(len(accel)),
        "criticos_90d": int((repos["status_cobertura_90d"] == "CRÍTICO").sum()),
        "urgentes_90d": int((repos["status_cobertura_90d"] == "URGENTE").sum()),
        "atencao_90d": int((repos["status_cobertura_90d"] == "ATENÇÃO").sum()),
        "sem_giro_90d": int((repos["status_cobertura_90d"] == "SEM_GIRO").sum()),
    }

    return repos.reindex(columns=out_cols), accel.reindex(columns=out_cols_accel), resumo




def export_dash_tables_to_db(bundle: dict[str, pd.DataFrame]) -> None:
    try:
        # Gera uma base mais leve para a dashboard online
        if "dash_base_vendas" in bundle and isinstance(bundle["dash_base_vendas"], pd.DataFrame):
            base_dash = bundle["dash_base_vendas"].copy()
            if DATE_COL in base_dash.columns:
                base_dash[DATE_COL] = pd.to_datetime(base_dash[DATE_COL], errors="coerce")
                base_dash = base_dash.dropna(subset=[DATE_COL]).copy()
                if not base_dash.empty:
                    max_dt = base_dash[DATE_COL].max()
                    cut = max_dt.normalize() - pd.Timedelta(days=DASH_RENDER_BASE_WINDOW_DAYS)
                    base_dash = base_dash[base_dash[DATE_COL] >= cut].copy()
            bundle["dash_base_vendas_render"] = base_dash.reset_index(drop=True)

        write_bundle(bundle, mode="replace")
        print("[OK] Banco atualizado com sucesso")
    except Exception as e:
        print(f"[ERRO DB] {e}")
# =========================
# MAIN
# =========================
def main():
    base_path = Path(ARQUIVO_BASE)

    ingest_info = None
    if AUTO_UPDATE_FROM_INPUT:
        if not base_path.exists():
            base_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(columns=_REQUIRED_INPUT_COLS + _OPTIONAL_INPUT_COLS + ["arquivo_origem"]).to_parquet(base_path, index=False)
        ingest_info = update_master_from_input(base_path, INPUT_DIR)

    if not base_path.exists():
        raise FileNotFoundError(f"Não encontrei o arquivo: {base_path.resolve()}")

    df = pd.read_parquet(base_path)
    df = normalize_columns(df)

    for col in [KEY_COL, DATE_COL]:
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {col}. Colunas disponíveis: {df.columns.tolist()}")

    # resolve EAN / SKU fallback
    ean_source_col = resolve_ean_column(df)
    sku_source_col = resolve_sku_column(df)

    df[KEY_COL] = df[KEY_COL].astype(str).str.strip()
    df[DATE_COL] = safe_to_datetime(df[DATE_COL])
    df = df[df[DATE_COL].notna()].copy()

    if CANAL_COL not in df.columns:
        df[CANAL_COL] = "SEM_CANAL"

    df[CANAL_COL] = df[CANAL_COL].astype(str).str.strip()
    df["EAN"] = clean_ean_series(df[ean_source_col])

    if sku_source_col and sku_source_col in df.columns:
        df["SKU"] = clean_sku_series(df[sku_source_col])
    else:
        df["SKU"] = df["EAN"].astype(str)

    kit_rules = load_kit_rules(KIT_RULES_FILE)
    df = apply_kit_rules(df, kit_rules)

    df["is_fulfillment"] = df[CANAL_COL].apply(_infer_is_fulfillment_from_channel)

    if PROD_COL not in df.columns:
        df[PROD_COL] = "SEM_PRODUTO"
    df[PROD_COL] = df[PROD_COL].astype(str).str.strip()

    desc_col = coalesce_first_existing_column(df, ["Descricao", "Descrição"], None)
    marca_col = coalesce_first_existing_column(df, ["Marca"], None)

    if desc_col is None:
        df["Descricao"] = "SEM_DESCRICAO"
    else:
        df[desc_col] = df[desc_col].astype(str).str.strip()

    if marca_col is None:
        df["Marca"] = "SEM_MARCA"
    else:
        df[marca_col] = df[marca_col].astype(str).str.strip()

    if "Qtde" in df.columns:
        df["Qtde"] = pd.to_numeric(df["Qtde"], errors="coerce")
    elif "Quantidade" in df.columns:
        df["Qtde"] = pd.to_numeric(df["Quantidade"], errors="coerce")
    else:
        df["Qtde"] = np.nan
    df["Qtde"] = df["Qtde"].fillna(0)

    if ESTOQUE_COL not in df.columns:
        df[ESTOQUE_COL] = np.nan
    df[ESTOQUE_COL] = df[ESTOQUE_COL].apply(parse_br_number)
    df[ESTOQUE_COL] = pd.to_numeric(df[ESTOQUE_COL], errors="coerce")

    total_col = "Total" if "Total" in df.columns else None
    preco_col = None
    for c in ["Preço", "Preco", "Valor Unitário", "Valor_Unitario", "ValorUnitario"]:
        if c in df.columns:
            preco_col = c
            break

    if total_col:
        df["Total_num"] = df[total_col].apply(parse_br_number)
    else:
        df["Total_num"] = np.nan

    if preco_col:
        df["Preco_num"] = df[preco_col].apply(parse_br_number)
    else:
        df["Preco_num"] = np.nan

    df["receita"] = df["Total_num"]
    mask_missing_total = df["receita"].isna()
    if mask_missing_total.any():
        df.loc[mask_missing_total, "receita"] = df.loc[mask_missing_total, "Preco_num"] * df.loc[mask_missing_total, "Qtde"]
    df["receita"] = df["receita"].fillna(0.0)

    data_min = df[DATE_COL].min()
    data_max = df[DATE_COL].max()
    checks = {
        "linhas": int(len(df)),
        "pedidos_unicos": int(df[KEY_COL].nunique()),
        "data_min": str(data_min.date()) if pd.notna(data_min) else None,
        "data_max": str(data_max.date()) if pd.notna(data_max) else None,
        "datas_invalidas_pct": float(df[DATE_COL].isna().mean()),
        "receita_total": float(df["receita"].sum()),
        "ean_source_col": ean_source_col,
        "sku_source_col": sku_source_col,
        "eans_validos": int((df["EAN"].astype(str).str.len() > 0).sum()),
        "skus_validos": int((df["SKU"].astype(str).str.len() > 0).sum()),
        "coluna_estoque_local_presente": bool(ESTOQUE_COL in df.columns),
        "estoque_local_na_pct": float(df[ESTOQUE_COL].isna().mean()) if ESTOQUE_COL in df.columns else 1.0,
        "kits_configurados": int(len(kit_rules)),
        "linhas_kit": int(df["is_kit"].sum()) if "is_kit" in df.columns else 0,
        "ingest_info": ingest_info,
    }

    daily = (
        df.groupby(DATE_COL)
          .agg(
              pedidos=(KEY_COL, "nunique"),
              itens=("Qtde", "sum"),
              receita=("receita", "sum")
          )
          .reset_index()
          .sort_values(DATE_COL)
    )

    daily = daily.set_index(DATE_COL).asfreq("D").fillna(0).reset_index()
    daily.to_csv(OUT_SAIDA_DAILY, index=False, sep=";", encoding="utf-8-sig")

    daily["mes"] = daily[DATE_COL].dt.to_period("M").astype(str)
    monthly = (
        daily.groupby("mes")
            .agg(
                pedidos=("pedidos", "sum"),
                itens=("itens", "sum"),
                receita=("receita", "sum")
            )
            .reset_index()
            .sort_values("mes")
    )
    monthly.to_csv(OUT_SAIDA_MONTHLY, index=False, sep=";", encoding="utf-8-sig")

    por_canal = (
        df.groupby(CANAL_COL)
          .agg(
              receita=("receita", "sum"),
              pedidos=(KEY_COL, "nunique"),
              itens=("Qtde", "sum")
          )
          .reset_index()
          .sort_values("receita", ascending=False)
    )
    por_canal.to_csv(OUT_SAIDA_POR_CANAL, index=False, sep=";", encoding="utf-8-sig")

    por_sku = (
        df.groupby("EAN")
          .agg(
              receita=("receita", "sum"),
              itens=("Qtde", "sum"),
              pedidos=(KEY_COL, "nunique")
          )
          .reset_index()
          .sort_values("receita", ascending=False)
    )

    abc = por_sku.copy()
    total_rec = float(abc["receita"].sum()) if len(abc) else 0.0
    abc["pct"] = (abc["receita"] / total_rec) if total_rec else 0.0
    abc["pct_acum"] = abc["pct"].cumsum()
    abc["classe_abc"] = np.select(
        [abc["pct_acum"] <= 0.80, abc["pct_acum"] <= 0.95],
        ["A", "B"],
        default="C"
    )
    abc.to_csv(OUT_SAIDA_ABC, index=False, sep=";", encoding="utf-8-sig")
    skus_A = set(abc.loc[abc["classe_abc"] == "A", "EAN"].tolist())

    receita_total = float(df["receita"].sum())
    pedidos_unicos = int(df[KEY_COL].nunique())
    itens_total = float(df["Qtde"].sum())
    ticket_medio = float(df.groupby(KEY_COL)["receita"].sum().mean()) if pedidos_unicos else 0.0

    kpis = {
        "receita_total": receita_total,
        "pedidos_unicos": pedidos_unicos,
        "itens_total": itens_total,
        "ticket_medio": ticket_medio,
    }

    ts_total = daily.set_index(DATE_COL)["receita"]
    ts_total = ensure_daily_index(ts_total)

    fc_total = pd.DataFrame(columns=["data", "receita_prevista"])
    if len(ts_total) >= MIN_DIAS_PARA_FORECAST and int((ts_total != 0).sum()) >= MIN_DIAS_NAO_ZERO:
        try:
            model = ExponentialSmoothing(ts_total, trend="add", seasonal=None).fit(optimized=True)
            fc = model.forecast(HORIZON_DIAS)
            idx = pd.date_range(ts_total.index.max() + pd.Timedelta(days=1), periods=HORIZON_DIAS, freq="D")
            fc_total = pd.DataFrame({"data": idx, "receita_prevista": np.maximum(fc.values, 0)})
        except Exception:
            pass
    fc_total.to_csv(OUT_FORECAST_TOTAL, index=False, sep=";", encoding="utf-8-sig")

    dc = (
        df.groupby([DATE_COL, CANAL_COL])
          .agg(receita=("receita", "sum"))
          .reset_index()
          .sort_values(DATE_COL)
    )

    fc_canais = []
    for canal_nome, g in dc.groupby(CANAL_COL):
        ts = g.set_index(DATE_COL)["receita"]
        ts = ensure_daily_index(ts)
        if len(ts) < MIN_DIAS_PARA_FORECAST or int((ts != 0).sum()) < MIN_DIAS_NAO_ZERO:
            continue
        try:
            model = ExponentialSmoothing(ts, trend="add", seasonal=None).fit(optimized=True)
            fc = model.forecast(HORIZON_DIAS)
            idx = pd.date_range(ts.index.max() + pd.Timedelta(days=1), periods=HORIZON_DIAS, freq="D")
            tmp = pd.DataFrame({"data": idx, "receita_prevista": np.maximum(fc.values, 0)})
            tmp[CANAL_COL] = canal_nome
            fc_canais.append(tmp)
        except Exception:
            continue

    fc_canal = pd.concat(fc_canais, ignore_index=True) if fc_canais else pd.DataFrame(columns=["data", "receita_prevista", CANAL_COL])
    fc_canal.to_csv(OUT_FORECAST_CANAL, index=False, sep=";", encoding="utf-8-sig")

    ds = (
        df[df["EAN"].isin(skus_A)]
          .groupby([DATE_COL, "EAN"])
          .agg(receita=("receita", "sum"))
          .reset_index()
          .sort_values(DATE_COL)
    )

    fc_skuA_list = []
    for prod, g in ds.groupby("EAN"):
        ts = g.set_index(DATE_COL)["receita"]
        ts = ensure_daily_index(ts)
        if len(ts) < MIN_DIAS_PARA_FORECAST or int((ts != 0).sum()) < MIN_DIAS_NAO_ZERO:
            continue
        try:
            model = ExponentialSmoothing(ts, trend="add", seasonal=None).fit(optimized=True)
            fc = model.forecast(HORIZON_DIAS)
            idx = pd.date_range(ts.index.max() + pd.Timedelta(days=1), periods=HORIZON_DIAS, freq="D")
            tmp = pd.DataFrame({"data": idx, "receita_prevista": np.maximum(fc.values, 0)})
            tmp["EAN"] = prod
            fc_skuA_list.append(tmp)
        except Exception:
            continue

    fc_skuA = pd.concat(fc_skuA_list, ignore_index=True) if fc_skuA_list else pd.DataFrame(columns=["data", "receita_prevista", "EAN"])
    fc_skuA.to_csv(OUT_FORECAST_SKU_A, index=False, sep=";", encoding="utf-8-sig")

    canal_daily = (
        df.groupby([DATE_COL, CANAL_COL])
          .agg(receita=("receita", "sum"))
          .reset_index()
    )
    alertas_canal = alertas_7d_por_grupo(canal_daily, CANAL_COL, "receita", "CANAL")
    alertas_canal.to_csv(OUT_ALERTAS_CANAL, index=False, sep=";", encoding="utf-8-sig")

    sku_daily_A = (
        df[df["EAN"].isin(skus_A)]
          .groupby([DATE_COL, "EAN"])
          .agg(receita=("receita", "sum"))
          .reset_index()
    )
    alertas_skuA = alertas_7d_por_grupo(sku_daily_A, "EAN", "receita", "SKU A")
    alertas_skuA.to_csv(OUT_ALERTAS_SKU_A, index=False, sep=";", encoding="utf-8-sig")

    alertas_all = pd.concat([alertas_canal, alertas_skuA], ignore_index=True) if (len(alertas_canal) or len(alertas_skuA)) else pd.DataFrame()
    alertas_all.to_csv(OUT_ALERTAS_ALL, index=False, sep=";", encoding="utf-8-sig")
    atualizar_tracking_alertas(alertas_all)

    repl_full, cand_full, resumo_full = compute_full_35d(
        df,
        receita_col="receita",
        ean_col="EAN",
        sku_col="SKU"
    )
    repl_full.to_csv(OUT_FULL_REPOSICAO_35D, index=False, encoding="utf-8-sig", sep=";")
    cand_full.to_csv(OUT_FULL_CANDIDATOS_35D, index=False, encoding="utf-8-sig", sep=";")
    audit_full = gerar_auditoria_full(
        df,
        repl_full,
        receita_col="receita",
        ean_col="EAN"
    )
    audit_full.to_csv(OUT_FULL_AUDITORIA_35D, index=False, encoding="utf-8-sig", sep=";")
    print("[OK] Auditoria FULL gerada.")

    repos_geral, repos_geral_accel, resumo_geral = compute_geral_reposicao(
        df,
        receita_col="receita",
        ean_col="EAN",
        sku_col="SKU"
    )
    repos_geral.to_csv(OUT_REPOSICAO_GERAL, index=False, encoding="utf-8-sig", sep=";")
    repos_geral_accel.to_csv(OUT_REPOSICAO_GERAL_ACCEL, index=False, encoding="utf-8-sig", sep=";")

    resumo = {
        "checks": checks,
        "kpis": kpis,
        "top_canais": por_canal.head(5).to_dict(orient="records"),
        "top_skus": por_sku.head(10).to_dict(orient="records"),
        "abc_resumo": abc["classe_abc"].value_counts().to_dict(),
        "qtd_skus_A": int((abc["classe_abc"] == "A").sum()),
        "data_ref": str(pd.to_datetime(df[DATE_COL].max()).date()) if pd.notna(df[DATE_COL].max()) else None,
        "forecast": {
            "total_disponivel": bool(len(fc_total)),
            "por_canal_disponivel": bool(len(fc_canal)),
            "skuA_disponivel": bool(len(fc_skuA)),
            "min_dias_para_forecast": MIN_DIAS_PARA_FORECAST,
            "dias_historico": int((daily[DATE_COL].max() - daily[DATE_COL].min()).days) if len(daily) else 0
        },
        "full_35d": resumo_full,
        "reposicao_geral": resumo_geral,
        "artefatos_full": {
            "reposicao_csv": OUT_FULL_REPOSICAO_35D,
            "candidatos_csv": OUT_FULL_CANDIDATOS_35D,
            "auditoria_csv": OUT_FULL_AUDITORIA_35D,
        },
        "artefatos_reposicao_geral": {
            "reposicao_csv": OUT_REPOSICAO_GERAL,
            "acelerando_csv": OUT_REPOSICAO_GERAL_ACCEL,
        }
    }
    with open(OUT_RESUMO_JSON, "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)

    lines = []
    lines.append("# Relatório — Copiloto de Vendas (V3.3 + FULL 35d + Reposição Geral)\n")
    lines.append(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
    lines.append(f"Período da base (histórico completo): {checks['data_min']} a {checks['data_max']}\n")

    lines.append("## KPIs\n")
    lines.append(f"- Receita total: {money_br(kpis['receita_total'])}")
    lines.append(f"- Pedidos únicos: {kpis['pedidos_unicos']}")
    lines.append(f"- Itens totais: {int(kpis['itens_total'])}")
    lines.append(f"- Ticket médio: {money_br(kpis['ticket_medio'])}\n")

    lines.append("## Forecast (30 dias)\n")
    if len(fc_total):
        total_30 = float(fc_total["receita_prevista"].sum())
        lines.append(f"- Receita prevista total (30d): {money_br(total_30)}\n")
    else:
        lines.append(f"- Forecast total indisponível (precisa >= {MIN_DIAS_PARA_FORECAST} dias e dados suficientes).\n")

    lines.append("## FULL (Mercado Livre) — Horizonte 35 dias\n")
    lines.append(f"- Canal FULL (referência): `{FULL_CANAL_EXATO}`")
    lines.append(f"- Base de cálculo de vendas do FULL: somente Mercado Livre")
    lines.append(f"- Data de referência: {resumo_full.get('data_ref')}")
    lines.append(f"- EANs com vendas em MELI (90d): {resumo_full.get('eans_meli_90d')}")
    lines.append(f"- EANs com vendas em FULL (90d): {resumo_full.get('eans_full_90d')}")
    lines.append(f"- Candidatos brutos ao FULL: {resumo_full.get('candidatos_brutos')}")
    lines.append(f"- Candidatos filtrados ao FULL: {resumo_full.get('candidatos_filtrados')}")
    lines.append(f"- Arquivo reposição: `{OUT_FULL_REPOSICAO_35D}`")
    lines.append(f"- Arquivo candidatos: `{OUT_FULL_CANDIDATOS_35D}`")
    lines.append(f"- Arquivo auditoria: `{OUT_FULL_AUDITORIA_35D}`\n")

    if len(repl_full):
        top = repl_full.head(15).copy()
        lines.append("### Top reposição FULL (prévia)\n")
        for _, r in top.iterrows():
            lines.append(
                f"- {r['EAN']} | {r['Descricao']} | ABC {r['classe_abc']} | "
                f"u30={float(r['unidades_30d']):.1f} | "
                f"forecast35={float(r['forecast_unid_35d']):.1f} | "
                f"alvo={float(r['estoque_alvo_full_35d']):.1f} | "
                f"envio={int(r['recomendacao_envio_full'])}"
            )
        lines.append("")
    else:
        lines.append("- Nenhuma reposição FULL calculada (sem EANs com vendas no FULL no recorte de 90d).\n")

    if len(cand_full):
        topc = cand_full.head(15).copy()
        lines.append("### Top candidatos FULL (prévia)\n")
        for _, r in topc.iterrows():
            lines.append(
                f"- {r['EAN']} | {r['Descricao']} | ABC {r['classe_abc']} | "
                f"u30={float(r['unidades_30d']):.1f} | "
                f"score={float(r['score_full']):.3f} | "
                f"forecast35={float(r['forecast_unid_35d']):.1f} | "
                f"qtd_segura={int(r['qtd_segura_envio_full'])}"
            )
        lines.append("")
    else:
        lines.append("- Nenhum candidato FULL calculado (critérios mínimos não atendidos).\n")

    lines.append("## Reposição Geral — Estoque Interno\n")
    lines.append(f"- Data de referência: {resumo_geral.get('data_ref')}")
    lines.append(f"- EANs analisados: {resumo_geral.get('eans_analisados')}")
    lines.append(f"- Acelerando venda: {resumo_geral.get('acelerando')}")
    lines.append(f"- Críticos (90d): {resumo_geral.get('criticos_90d')}")
    lines.append(f"- Urgentes (90d): {resumo_geral.get('urgentes_90d')}")
    lines.append(f"- Atenção (90d): {resumo_geral.get('atencao_90d')}")
    lines.append(f"- Sem giro (90d): {resumo_geral.get('sem_giro_90d')}")
    lines.append(f"- Arquivo reposição geral: `{OUT_REPOSICAO_GERAL}`")
    lines.append(f"- Arquivo acelerando venda: `{OUT_REPOSICAO_GERAL_ACCEL}`\n")

    if len(repos_geral):
        topg = repos_geral.head(20).copy()
        lines.append("### Top urgências de reposição geral (prévia)\n")
        for _, r in topg.iterrows():
            cob90 = r["cobertura_90d"]
            cob90_txt = f"{float(cob90):.1f}d" if pd.notna(cob90) else "n/a"
            lines.append(
                f"- {r['EAN']} | {r['Descricao']} | estoque={float(r['estoque_atual']):.1f} | "
                f"cob90={cob90_txt} | "
                f"status={r['status_cobertura_90d']} | "
                f"repor90={int(r['reposicao_sugerida_90d'])} | "
                f"trend={r['trend_status']}"
            )
        lines.append("")
    else:
        lines.append("- Nenhuma linha calculada para reposição geral.\n")

    if len(repos_geral_accel):
        topa = repos_geral_accel.head(20).copy()
        lines.append("### Produtos acelerando venda (prévia)\n")
        for _, r in topa.iterrows():
            growth = r["crescimento_ult30_vs_prev30_pct"]
            growth_txt = f"{float(growth):.2f}%" if pd.notna(growth) else "n/a"
            cob90 = r["cobertura_90d"]
            cob90_txt = f"{float(cob90):.1f}d" if pd.notna(cob90) else "n/a"
            lines.append(
                f"- {r['EAN']} | {r['Descricao']} | trend={r['trend_status']} | "
                f"crescimento={growth_txt} | cob90={cob90_txt} | "
                f"repor90={int(r['reposicao_sugerida_90d'])}"
            )
        lines.append("")
    else:
        lines.append("- Nenhum produto classificado como acelerando venda.\n")

    lines.append("## Alertas\n")
    if len(alertas_all) == 0:
        lines.append("- Nenhum alerta disparado pelos thresholds atuais.\n")
    else:
        show = alertas_all.sort_values(["severidade", "variacao_pct"]).head(20)
        for _, a in show.iterrows():
            if CANAL_COL in a and pd.notna(a.get(CANAL_COL, np.nan)):
                lines.append(f"- [{a['severidade']}] CANAL {a[CANAL_COL]}: {a['variacao_pct']}% (7d vs prev7d)")
            else:
                lines.append(f"- [{a['severidade']}] SKU A {a['EAN']}: {a['variacao_pct']}% (7d vs prev7d)")
        lines.append("")

    with open(OUT_RELATORIO_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    export_dash_tables_to_db({
        "dash_base_vendas": df,
        "dash_saida_daily": daily,
        "dash_saida_monthly": monthly,
        "dash_saida_por_canal": por_canal,
        "dash_saida_abc": abc,
        "dash_alertas": alertas_all,
        "dash_full_reposicao_35d": repl_full,
        "dash_full_candidatos_35d": cand_full,
        "dash_full_auditoria_35d": audit_full,
        "dash_reposicao_geral": repos_geral,
        "dash_reposicao_geral_accel": repos_geral_accel,
    })

    print("[OK] V3.3 finalizada (HISTÓRICO COMPLETO + FULL 35d + Reposição Geral por EAN com Estoque Local)!")
    print("Arquivos gerados:")
    print(f"- {OUT_SAIDA_DAILY} / {OUT_SAIDA_MONTHLY} / {OUT_SAIDA_POR_CANAL} / {OUT_SAIDA_ABC}")
    print(f"- {OUT_FORECAST_TOTAL} / {OUT_FORECAST_CANAL} / {OUT_FORECAST_SKU_A}")
    print(f"- {OUT_ALERTAS_CANAL} / {OUT_ALERTAS_SKU_A} / {OUT_ALERTAS_ALL} / {OUT_ALERTAS_TRACKING}")
    print(f"- {OUT_FULL_REPOSICAO_35D} / {OUT_FULL_CANDIDATOS_35D} / {OUT_FULL_AUDITORIA_35D}")
    print(f"- {OUT_REPOSICAO_GERAL} / {OUT_REPOSICAO_GERAL_ACCEL}")
    print(f"- {OUT_RESUMO_JSON} / {OUT_RELATORIO_MD}")
    print(f"Período (df): {checks['data_min']} -> {checks['data_max']} | linhas: {checks['linhas']} | pedidos únicos: {checks['pedidos_unicos']}")
    print(f"Coluna usada como EAN base: {checks['ean_source_col']}")
    print(f"Coluna usada como SKU base: {checks['sku_source_col']}")


if __name__ == "__main__":
    main()