"""Validação local e geração das evidências do MVP CISA KEV + NVD."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RESULTS = ROOT / "resultados"
EVIDENCE = ROOT / "evidencias"
RESULTS.mkdir(exist_ok=True)
EVIDENCE.mkdir(exist_ok=True)


def select_cvss(metrics: dict) -> tuple[float | None, str | None, str | None]:
    """Seleciona a versão CVSS mais recente disponível no registro NVD."""
    for key, version in (
        ("cvssMetricV40", "4.0"),
        ("cvssMetricV31", "3.1"),
        ("cvssMetricV30", "3.0"),
        ("cvssMetricV2", "2.0"),
    ):
        entries = metrics.get(key) or []
        if not entries:
            continue
        primary = next((item for item in entries if item.get("type") == "Primary"), entries[0])
        cvss = primary.get("cvssData", {})
        score = cvss.get("baseScore")
        severity = cvss.get("baseSeverity") or primary.get("baseSeverity")
        return score, severity, version
    return None, None, None


def nvd_weaknesses(cve: dict) -> list[str]:
    values: list[str] = []
    for item in cve.get("weaknesses", []):
        for description in item.get("description", []):
            value = description.get("value")
            if value and value.startswith("CWE-") and value not in values:
                values.append(value)
    return values


def main() -> None:
    cisa = json.loads((RAW / "cisa_kev.json").read_text(encoding="utf-8"))
    nvd = json.loads((RAW / "nvd_kev.json").read_text(encoding="utf-8"))

    cisa_rows = []
    for item in cisa["vulnerabilities"]:
        cisa_rows.append(
            {
                "cve_id": item.get("cveID"),
                "fornecedor": item.get("vendorProject"),
                "produto": item.get("product"),
                "nome_vulnerabilidade": item.get("vulnerabilityName"),
                "data_adicao": item.get("dateAdded"),
                "descricao_cisa": item.get("shortDescription"),
                "acao_requerida": item.get("requiredAction"),
                "data_limite": item.get("dueDate"),
                "uso_ransomware": item.get("knownRansomwareCampaignUse"),
                "triagem_forense": item.get("forensicTriage"),
                "notas": item.get("notes"),
                "cwes_cisa": item.get("cwes") or [],
            }
        )

    nvd_rows = []
    for wrapper in nvd["vulnerabilities"]:
        cve = wrapper["cve"]
        score, severity, version = select_cvss(cve.get("metrics", {}))
        nvd_rows.append(
            {
                "cve_id": cve.get("id"),
                "data_publicacao": cve.get("published"),
                "ultima_modificacao": cve.get("lastModified"),
                "status_nvd": cve.get("vulnStatus"),
                "cvss_score": score,
                "cvss_severidade": severity,
                "cvss_versao": version,
                "cwes_nvd": nvd_weaknesses(cve),
            }
        )

    cisa_df = pd.DataFrame(cisa_rows)
    nvd_df = pd.DataFrame(nvd_rows)
    merged = cisa_df.merge(nvd_df, on="cve_id", how="outer", indicator=True)

    merged["data_adicao"] = pd.to_datetime(merged["data_adicao"], errors="coerce")
    merged["data_limite"] = pd.to_datetime(merged["data_limite"], errors="coerce")
    merged["data_publicacao"] = pd.to_datetime(merged["data_publicacao"], errors="coerce", utc=True).dt.tz_localize(None)
    merged["ultima_modificacao"] = pd.to_datetime(merged["ultima_modificacao"], errors="coerce", utc=True).dt.tz_localize(None)
    merged["cwes"] = merged.apply(
        lambda row: row["cwes_nvd"] if isinstance(row["cwes_nvd"], list) and row["cwes_nvd"] else row["cwes_cisa"],
        axis=1,
    )
    merged["cwes_texto"] = merged["cwes"].apply(lambda value: "|".join(value) if isinstance(value, list) else "")
    merged["dias_publicacao_ate_kev"] = (merged["data_adicao"] - merged["data_publicacao"]).dt.days
    merged["janela_correcao_dias"] = (merged["data_limite"] - merged["data_adicao"]).dt.days
    merged["ano_adicao"] = merged["data_adicao"].dt.year.astype("Int64")
    merged["mes_adicao"] = merged["data_adicao"].dt.to_period("M").astype(str)
    merged["ano_cve"] = pd.to_numeric(merged["cve_id"].str.extract(r"CVE-(\d{4})-")[0], errors="coerce").astype("Int64")
    merged["ransomware_confirmado"] = merged["uso_ransomware"].str.upper().eq("KNOWN")
    merged["prioridade"] = pd.cut(
        merged["cvss_score"], bins=[-0.01, 3.9, 6.9, 8.9, 10], labels=["BAIXA", "MEDIA", "ALTA", "CRITICA"]
    ).astype("string")

    export_cols = [
        "cve_id", "fornecedor", "produto", "nome_vulnerabilidade", "data_publicacao", "data_adicao",
        "data_limite", "dias_publicacao_ate_kev", "janela_correcao_dias", "cvss_score", "cvss_severidade",
        "cvss_versao", "prioridade", "uso_ransomware", "ransomware_confirmado", "triagem_forense",
        "cwes_texto", "status_nvd", "descricao_cisa", "acao_requerida", "notas",
    ]
    merged[export_cols].to_csv(RESULTS / "vulnerabilidades_enriquecidas.csv", index=False, date_format="%Y-%m-%d")

    fornecedores = (
        merged.groupby("fornecedor", dropna=False)
        .agg(
            vulnerabilidades=("cve_id", "count"), produtos=("produto", "nunique"),
            cvss_medio=("cvss_score", "mean"), ransomware_confirmado=("ransomware_confirmado", "sum"),
            mediana_dias_ate_kev=("dias_publicacao_ate_kev", "median"),
        )
        .reset_index().sort_values(["vulnerabilidades", "fornecedor"], ascending=[False, True])
    )
    fornecedores["cvss_medio"] = fornecedores["cvss_medio"].round(2)
    fornecedores.to_csv(RESULTS / "fornecedores.csv", index=False)

    produtos = (
        merged.groupby(["fornecedor", "produto"], dropna=False)
        .agg(vulnerabilidades=("cve_id", "count"), ransomware_confirmado=("ransomware_confirmado", "sum"))
        .reset_index().sort_values(["vulnerabilidades", "fornecedor", "produto"], ascending=[False, True, True])
    )
    produtos.to_csv(RESULTS / "produtos.csv", index=False)

    temporal = (
        merged.groupby("mes_adicao")
        .agg(vulnerabilidades_adicionadas=("cve_id", "count"), ransomware_confirmado=("ransomware_confirmado", "sum"))
        .reset_index().sort_values("mes_adicao")
    )
    temporal.to_csv(RESULTS / "evolucao_temporal.csv", index=False)

    cwes = merged[["cve_id", "fornecedor", "ransomware_confirmado", "cwes"]].explode("cwes")
    cwes = cwes[cwes["cwes"].notna() & cwes["cwes"].ne("")]
    cwe_summary = (
        cwes.groupby("cwes")
        .agg(vulnerabilidades=("cve_id", "nunique"), ransomware_confirmado=("ransomware_confirmado", "sum"))
        .reset_index().sort_values(["vulnerabilidades", "cwes"], ascending=[False, True])
    )
    cwe_summary.to_csv(RESULTS / "fraquezas_cwe.csv", index=False)

    merged[merged["ransomware_confirmado"]].sort_values(
        ["cvss_score", "data_adicao"], ascending=[False, False]
    )[export_cols].to_csv(RESULTS / "vulnerabilidades_ransomware.csv", index=False, date_format="%Y-%m-%d")

    quality_rows = []
    quality_cols = [
        "cve_id", "fornecedor", "produto", "data_publicacao", "data_adicao", "data_limite", "cvss_score",
        "cvss_severidade", "cwes_texto", "uso_ransomware", "triagem_forense", "status_nvd",
        "dias_publicacao_ate_kev", "janela_correcao_dias",
    ]
    for col in quality_cols:
        series = merged[col]
        non_null = series.dropna()
        quality_rows.append({
            "atributo": col, "tipo": str(series.dtype), "linhas": len(series),
            "nulos": int(series.isna().sum()), "percentual_nulos": round(series.isna().mean() * 100, 2),
            "distintos": int(non_null.astype(str).nunique()),
            "minimo": str(non_null.min()) if len(non_null) else None,
            "maximo": str(non_null.max()) if len(non_null) else None,
        })
    pd.DataFrame(quality_rows).to_csv(EVIDENCE / "qualidade_por_atributo.csv", index=False)

    delay_non_negative = merged.loc[merged["dias_publicacao_ate_kev"] >= 0, "dias_publicacao_ate_kev"]
    summary = {
        "catalogo_cisa": cisa.get("catalogVersion"), "data_catalogo": cisa.get("dateReleased"),
        "registros_cisa": len(cisa_df), "registros_nvd": len(nvd_df), "registros_integrados": len(merged),
        "correspondencias_ambas_fontes": int((merged["_merge"] == "both").sum()),
        "cves_duplicados_cisa": int(cisa_df["cve_id"].duplicated().sum()),
        "cves_duplicados_nvd": int(nvd_df["cve_id"].duplicated().sum()),
        "fornecedores": int(merged["fornecedor"].nunique()),
        "produtos": int(merged[["fornecedor", "produto"]].drop_duplicates().shape[0]),
        "cves_ransomware_confirmado": int(merged["ransomware_confirmado"].sum()),
        "percentual_ransomware_confirmado": round(merged["ransomware_confirmado"].mean() * 100, 2),
        "cvss_ausente": int(merged["cvss_score"].isna().sum()), "cwe_ausente": int(merged["cwes_texto"].eq("").sum()),
        "atrasos_negativos": int((merged["dias_publicacao_ate_kev"] < 0).sum()),
        "mediana_dias_publicacao_ate_kev": float(delay_non_negative.median()),
        "media_dias_publicacao_ate_kev": round(float(delay_non_negative.mean()), 1),
        "mediana_janela_correcao_dias": float(merged["janela_correcao_dias"].median()),
        "cvss_medio": round(float(merged["cvss_score"].mean()), 2),
        "inicio_adicoes": merged["data_adicao"].min().strftime("%Y-%m-%d"),
        "fim_adicoes": merged["data_adicao"].max().strftime("%Y-%m-%d"),
    }
    (RESULTS / "resumo.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    plt.style.use("seaborn-v0_8-whitegrid")
    top = fornecedores.head(15).sort_values("vulnerabilidades")
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(top["fornecedor"], top["vulnerabilidades"], color="#2457a7")
    ax.set_title("Fornecedores com mais vulnerabilidades exploradas no CISA KEV")
    ax.set_xlabel("Quantidade de CVEs"); ax.set_ylabel("")
    for idx, value in enumerate(top["vulnerabilidades"]): ax.text(value + 2, idx, str(value), va="center", fontsize=9)
    fig.tight_layout(); fig.savefig(EVIDENCE / "top_fornecedores.png", dpi=180); plt.close(fig)

    annual = merged.groupby("ano_adicao").size().reset_index(name="vulnerabilidades")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar(annual["ano_adicao"].astype(str), annual["vulnerabilidades"], color="#c2413b")
    ax.set_title("Vulnerabilidades adicionadas ao catálogo KEV por ano")
    ax.set_xlabel("Ano de inclusão no catálogo"); ax.set_ylabel("Quantidade de CVEs")
    for idx, value in enumerate(annual["vulnerabilidades"]): ax.text(idx, value + max(annual["vulnerabilidades"]) * 0.015, str(value), ha="center", fontsize=9)
    fig.tight_layout(); fig.savefig(EVIDENCE / "evolucao_anual_kev.png", dpi=180); plt.close(fig)

    sev = merged.assign(
        severidade=merged["cvss_severidade"].fillna("SEM CVSS"),
        grupo_ransomware=merged["ransomware_confirmado"].map({True: "Ransomware conhecido", False: "Sem confirmação"}),
    )
    order = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "SEM CVSS"]
    pivot = sev.pivot_table(index="severidade", columns="grupo_ransomware", values="cve_id", aggfunc="count", fill_value=0).reindex(order).fillna(0)
    fig, ax = plt.subplots(figsize=(10, 5.5))
    pivot.plot(kind="bar", stacked=True, color=["#2457a7", "#f59e0b"], ax=ax)
    ax.set_title("Severidade CVSS e uso conhecido em campanhas de ransomware")
    ax.set_xlabel("Severidade"); ax.set_ylabel("Quantidade de CVEs"); ax.tick_params(axis="x", rotation=0); ax.legend(title="")
    fig.tight_layout(); fig.savefig(EVIDENCE / "severidade_ransomware.png", dpi=180); plt.close(fig)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("\nTop 10 fornecedores:\n", fornecedores.head(10).to_string(index=False))
    print("\nTop 10 CWEs:\n", cwe_summary.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
