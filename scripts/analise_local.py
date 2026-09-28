"""Valida localmente os resultados do MVP com os CSVs oficiais da ANP."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
RESULTS_DIR = ROOT / "resultados"
EVIDENCE_DIR = ROOT / "evidencias"

USECOLS = [
    "Regiao - Sigla",
    "Estado - Sigla",
    "Municipio",
    "CNPJ da Revenda",
    "Produto",
    "Data da Coleta",
    "Valor de Venda",
    "Valor de Compra",
    "Unidade de Medida",
    "Bandeira",
]


def strip_accents(value: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(char)
    )


def station_hash(value: object) -> str | None:
    if pd.isna(value):
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def load_data() -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for path in sorted(RAW_DIR.rglob("*.csv")):
        for chunk in pd.read_csv(
            path,
            sep=";",
            encoding="utf-8-sig",
            usecols=USECOLS,
            dtype=str,
            chunksize=200_000,
            low_memory=False,
        ):
            parts.append(chunk)
    if not parts:
        raise FileNotFoundError("Nenhum CSV da ANP encontrado em data/raw")
    return pd.concat(parts, ignore_index=True)


def clean_data(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.rename(
        columns={
            "Regiao - Sigla": "regiao",
            "Estado - Sigla": "uf",
            "Municipio": "municipio",
            "CNPJ da Revenda": "cnpj_revenda",
            "Produto": "produto",
            "Data da Coleta": "data_coleta",
            "Valor de Venda": "valor_venda",
            "Valor de Compra": "valor_compra",
            "Unidade de Medida": "unidade_medida",
            "Bandeira": "bandeira",
        }
    )
    for col in ["regiao", "uf", "municipio", "produto", "unidade_medida", "bandeira"]:
        df[col] = df[col].str.strip().str.upper()
    df["produto"] = df["produto"].map(lambda x: strip_accents(x) if isinstance(x, str) else x)
    df["data_coleta"] = pd.to_datetime(df["data_coleta"], format="%d/%m/%Y", errors="coerce")
    for col in ["valor_venda", "valor_compra"]:
        df[col] = pd.to_numeric(df[col].str.replace(",", ".", regex=False), errors="coerce")
    df["posto_id"] = df["cnpj_revenda"].map(station_hash)
    df = df.drop(columns=["cnpj_revenda"])
    df = df[df["produto"].isin(["GASOLINA", "ETANOL"])].copy()
    df["ano"] = df["data_coleta"].dt.year
    df["mes"] = df["data_coleta"].dt.to_period("M").astype(str)
    return df


def quality_report(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    domains = {
        "regiao": "N, NE, CO, SE, S",
        "uf": "27 UFs brasileiras",
        "municipio": "texto não vazio",
        "produto": "GASOLINA ou ETANOL",
        "data_coleta": "2024-01-01 a 2025-12-31",
        "valor_venda": "> 0 e < 20 R$/litro",
        "valor_compra": "> 0 e < 20 R$/litro; ausência permitida",
        "unidade_medida": "R$ / litro",
        "bandeira": "texto não vazio; ausência permitida",
        "posto_id": "hash SHA-256 truncado de 16 caracteres",
        "ano": "2024 ou 2025",
        "mes": "AAAA-MM",
    }
    for col in df.columns:
        series = df[col]
        rows.append(
            {
                "atributo": col,
                "tipo": str(series.dtype),
                "linhas": len(df),
                "nulos": int(series.isna().sum()),
                "percentual_nulos": round(series.isna().mean() * 100, 4),
                "distintos": int(series.nunique(dropna=True)),
                "minimo": str(series.min()) if series.notna().any() else "",
                "maximo": str(series.max()) if series.notna().any() else "",
                "dominio_esperado": domains.get(col, ""),
            }
        )
    return pd.DataFrame(rows)


def analyze(df: pd.DataFrame) -> dict[str, object]:
    valid = df[
        df["data_coleta"].notna()
        & df["valor_venda"].between(0.5, 20)
        & df["uf"].str.fullmatch(r"[A-Z]{2}", na=False)
    ].drop_duplicates(
        subset=["posto_id", "produto", "data_coleta", "valor_venda"]
    )

    state_2025 = (
        valid[valid["ano"] == 2025]
        .groupby(["uf", "produto"], as_index=False)
        .agg(preco_medio=("valor_venda", "mean"), mediana=("valor_venda", "median"),
             desvio_padrao=("valor_venda", "std"), observacoes=("valor_venda", "size"))
    )
    state_2025[["preco_medio", "mediana", "desvio_padrao"]] = state_2025[
        ["preco_medio", "mediana", "desvio_padrao"]
    ].round(3)
    state_2025.to_csv(RESULTS_DIR / "precos_estaduais_2025.csv", index=False)

    monthly = (
        valid.groupby(["mes", "produto"], as_index=False)
        .agg(preco_medio=("valor_venda", "mean"), observacoes=("valor_venda", "size"))
    )
    monthly["preco_medio"] = monthly["preco_medio"].round(3)
    monthly.to_csv(RESULTS_DIR / "evolucao_mensal_2024_2025.csv", index=False)

    pivot = (
        valid.groupby(["uf", "mes", "produto"], as_index=False)["valor_venda"]
        .mean()
        .pivot(index=["uf", "mes"], columns="produto", values="valor_venda")
        .reset_index()
    )
    pivot = pivot.dropna(subset=["GASOLINA", "ETANOL"])
    pivot["razao_etanol_gasolina"] = pivot["ETANOL"] / pivot["GASOLINA"]
    competitiveness = (
        pivot.assign(competitivo=lambda x: x["razao_etanol_gasolina"] <= 0.70)
        .groupby("uf", as_index=False)
        .agg(
            meses_analisados=("mes", "size"),
            meses_etanol_competitivo=("competitivo", "sum"),
            razao_media=("razao_etanol_gasolina", "mean"),
        )
    )
    competitiveness["percentual_meses_competitivo"] = (
        competitiveness["meses_etanol_competitivo"] / competitiveness["meses_analisados"] * 100
    ).round(1)
    competitiveness["razao_media"] = competitiveness["razao_media"].round(3)
    competitiveness.sort_values(
        ["percentual_meses_competitivo", "razao_media"], ascending=[False, True]
    ).to_csv(RESULTS_DIR / "competitividade_etanol.csv", index=False)

    dispersion = (
        valid.groupby(["uf", "produto"], as_index=False)
        .agg(media=("valor_venda", "mean"), desvio=("valor_venda", "std"),
             amplitude=("valor_venda", lambda x: x.quantile(0.95) - x.quantile(0.05)))
    )
    dispersion["coeficiente_variacao"] = dispersion["desvio"] / dispersion["media"]
    dispersion = dispersion.round(3).sort_values("coeficiente_variacao", ascending=False)
    dispersion.to_csv(RESULTS_DIR / "dispersao_estadual.csv", index=False)

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(11, 5.5))
    colors = {"GASOLINA": "#1f4e79", "ETANOL": "#2a9d8f"}
    for product, group in monthly.groupby("produto"):
        ax.plot(group["mes"], group["preco_medio"], marker="o", label=product, color=colors.get(product))
    ax.set(title="Evolução mensal do preço médio de combustíveis (2024-2025)", xlabel="Mês", ylabel="R$/litro")
    ax.tick_params(axis="x", rotation=60)
    ax.legend(title="Produto")
    fig.tight_layout()
    fig.savefig(EVIDENCE_DIR / "evolucao_mensal.png", dpi=180)
    plt.close(fig)

    gas = state_2025[state_2025["produto"] == "GASOLINA"].sort_values("preco_medio")
    fig, ax = plt.subplots(figsize=(11, 7))
    ax.barh(gas["uf"], gas["preco_medio"], color="#1f4e79")
    ax.set(title="Preço médio da gasolina por UF em 2025", xlabel="R$/litro", ylabel="UF")
    fig.tight_layout()
    fig.savefig(EVIDENCE_DIR / "gasolina_por_uf_2025.png", dpi=180)
    plt.close(fig)

    duplicate_count = int(df.duplicated(subset=["posto_id", "produto", "data_coleta", "valor_venda"]).sum())
    out_of_domain = int((~df["valor_venda"].between(0.5, 20) & df["valor_venda"].notna()).sum())
    top_gas = gas.nlargest(3, "preco_medio")[["uf", "preco_medio"]].to_dict("records")
    low_gas = gas.nsmallest(3, "preco_medio")[["uf", "preco_medio"]].to_dict("records")
    top_comp = competitiveness.sort_values(
        ["percentual_meses_competitivo", "razao_media"], ascending=[False, True]
    ).head(5)[["uf", "percentual_meses_competitivo", "razao_media"]].to_dict("records")
    top_disp = dispersion.head(5)[["uf", "produto", "coeficiente_variacao"]].to_dict("records")

    summary = {
        "raw_rows_selected_products": int(len(df)),
        "valid_rows": int(len(valid)),
        "duplicate_rows_removed": duplicate_count,
        "out_of_domain_sale_prices": out_of_domain,
        "date_min": str(valid["data_coleta"].min().date()),
        "date_max": str(valid["data_coleta"].max().date()),
        "states": int(valid["uf"].nunique()),
        "municipalities": int(valid["municipio"].nunique()),
        "top_gasoline_2025": top_gas,
        "lowest_gasoline_2025": low_gas,
        "best_ethanol_competitiveness": top_comp,
        "highest_dispersion": top_disp,
    }
    (RESULTS_DIR / "resumo.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    raw = load_data()
    clean = clean_data(raw)
    quality_report(clean).to_csv(EVIDENCE_DIR / "qualidade_por_atributo.csv", index=False)
    summary = analyze(clean)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
