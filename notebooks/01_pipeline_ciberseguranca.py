# Databricks notebook source
# MAGIC %md
# MAGIC # MVP — Priorização de vulnerabilidades exploradas
# MAGIC **Autor:** Bruno Bryto Borges — **Matrícula:** 231013304
# MAGIC
# MAGIC Pipeline em nuvem que integra o catálogo **Known Exploited Vulnerabilities (KEV)** da CISA com o **National Vulnerability Database (NVD)**, produzindo camadas Bronze, Silver e Gold em Delta Lake.

# COMMAND ----------

from pathlib import Path
import json
import urllib.request

from pyspark.sql import functions as F

CISA_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
NVD_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0?hasKev&resultsPerPage=2000"
TABLE_PREFIX = "tarefa_4"

catalog = spark.sql("SELECT current_catalog()").first()[0]
schema = spark.sql("SELECT current_schema()").first()[0]

# Remove somente as tabelas conhecidas da versao anterior do projeto (tema ANP).
# As tabelas compartilhadas de dimensao/qualidade serao recriadas abaixo.
legacy_tables = [
    "bronze_precos",
    "silver_precos",
    "dim_localidade",
    "dim_produto",
    "dim_tempo",
    "dim_posto",
    "fato_precos",
    "qualidade",
    "precos_uf_2025",
    "evolucao_mensal",
    "competitividade_etanol",
    "dispersao",
]
for legacy_name in legacy_tables:
    spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{TABLE_PREFIX}_{legacy_name}`")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{TABLE_PREFIX}_files`")
raw_dir = Path(f"/Volumes/{catalog}/{schema}/{TABLE_PREFIX}_files/ciberseguranca")
raw_dir.mkdir(parents=True, exist_ok=True)
print(f"Destino persistente: {catalog}.{schema}.{TABLE_PREFIX}_*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Coleta das fontes oficiais
# MAGIC Os arquivos são obtidos diretamente da CISA e do NVD. A URL, o horário de ingestão e os metadados da fonte são preservados para rastreabilidade.

# COMMAND ----------

def download_json(url: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 tarefa-4-unb"})
    with urllib.request.urlopen(request, timeout=180) as response:
        destination.write_bytes(response.read())

cisa_path = raw_dir / "cisa_kev.json"
nvd_path = raw_dir / "nvd_kev.json"
download_json(CISA_URL, cisa_path)
download_json(NVD_URL, nvd_path)
print("CISA bytes:", cisa_path.stat().st_size)
print("NVD bytes:", nvd_path.stat().st_size)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Camada Bronze

# COMMAND ----------

cisa_root = spark.read.option("multiline", True).json(str(cisa_path))
nvd_root = spark.read.option("multiline", True).json(str(nvd_path))

cisa_meta = cisa_root.select("title", "catalogVersion", "dateReleased", "count").first()
nvd_meta = nvd_root.select("totalResults", "timestamp", "version").first()

bronze_cisa = (
    cisa_root.select(F.explode("vulnerabilities").alias("v"))
    .select("v.*")
    .withColumn("url_origem", F.lit(CISA_URL))
    .withColumn("versao_catalogo", F.lit(cisa_meta["catalogVersion"]))
    .withColumn("data_catalogo", F.lit(cisa_meta["dateReleased"]))
    .withColumn("ingerido_em", F.current_timestamp())
)

bronze_nvd = (
    nvd_root.select(F.explode("vulnerabilities").alias("wrapper"))
    .select("wrapper.cve.*")
    .withColumn("url_origem", F.lit(NVD_URL))
    .withColumn("ingerido_em", F.current_timestamp())
)

bronze_cisa.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TABLE_PREFIX}_bronze_cisa_kev")
bronze_nvd.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TABLE_PREFIX}_bronze_nvd_kev")
print("Bronze CISA:", bronze_cisa.count(), "| Bronze NVD:", bronze_nvd.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Integração, padronização e camada Silver
# MAGIC A chave de integração é o identificador CVE. São extraídos CVSS, severidade, CWE, uso em ransomware e datas para produzir indicadores de priorização.

# COMMAND ----------

cisa_silver = bronze_cisa.select(
    F.upper(F.trim("cveID")).alias("cve_id"),
    F.trim("vendorProject").alias("fornecedor"),
    F.trim("product").alias("produto"),
    F.trim("vulnerabilityName").alias("nome_vulnerabilidade"),
    F.to_date("dateAdded").alias("data_adicao"),
    F.to_date("dueDate").alias("data_limite"),
    F.trim("shortDescription").alias("descricao_cisa"),
    F.trim("requiredAction").alias("acao_requerida"),
    F.trim("knownRansomwareCampaignUse").alias("uso_ransomware"),
    F.trim("forensicTriage").alias("triagem_forense"),
    "notes",
    F.col("cwes").alias("cwes_cisa"),
    "versao_catalogo",
)

nvd_silver = bronze_nvd.select(
    F.upper(F.trim("id")).alias("cve_id"),
    F.to_timestamp("published").alias("data_publicacao"),
    F.to_timestamp("lastModified").alias("ultima_modificacao"),
    F.trim("vulnStatus").alias("status_nvd"),
    F.coalesce(
        F.expr("element_at(filter(metrics.cvssMetricV40, x -> x.type = 'Primary'), 1).cvssData.baseScore"),
        F.col("metrics.cvssMetricV40")[0]["cvssData"]["baseScore"],
        F.expr("element_at(filter(metrics.cvssMetricV31, x -> x.type = 'Primary'), 1).cvssData.baseScore"),
        F.col("metrics.cvssMetricV31")[0]["cvssData"]["baseScore"],
        F.expr("element_at(filter(metrics.cvssMetricV30, x -> x.type = 'Primary'), 1).cvssData.baseScore"),
        F.col("metrics.cvssMetricV30")[0]["cvssData"]["baseScore"],
        F.expr("element_at(filter(metrics.cvssMetricV2, x -> x.type = 'Primary'), 1).cvssData.baseScore"),
        F.col("metrics.cvssMetricV2")[0]["cvssData"]["baseScore"],
    ).cast("double").alias("cvss_score"),
    F.coalesce(
        F.expr("element_at(filter(metrics.cvssMetricV40, x -> x.type = 'Primary'), 1).cvssData.baseSeverity"),
        F.col("metrics.cvssMetricV40")[0]["cvssData"]["baseSeverity"],
        F.expr("element_at(filter(metrics.cvssMetricV31, x -> x.type = 'Primary'), 1).cvssData.baseSeverity"),
        F.col("metrics.cvssMetricV31")[0]["cvssData"]["baseSeverity"],
        F.expr("element_at(filter(metrics.cvssMetricV30, x -> x.type = 'Primary'), 1).cvssData.baseSeverity"),
        F.col("metrics.cvssMetricV30")[0]["cvssData"]["baseSeverity"],
        F.expr("element_at(filter(metrics.cvssMetricV2, x -> x.type = 'Primary'), 1).baseSeverity"),
        F.col("metrics.cvssMetricV2")[0]["baseSeverity"],
    ).alias("cvss_severidade"),
    F.expr("filter(flatten(transform(weaknesses, x -> transform(x.description, y -> y.value))), z -> z like 'CWE-%')").alias("cwes_nvd"),
)

silver = (
    cisa_silver.join(nvd_silver, "cve_id", "full")
    .withColumn(
        "cwes",
        F.when(F.size("cwes_nvd") > 0, F.array_distinct("cwes_nvd"))
        .otherwise(
            F.split(
                F.regexp_replace(F.coalesce(F.col("cwes_cisa").cast("string"), F.lit("")), r"[\[\]'\"]", ""),
                r"\s*,\s*",
            )
        ),
    )
    .withColumn("ransomware_confirmado", F.upper("uso_ransomware") == F.lit("KNOWN"))
    .withColumn("dias_publicacao_ate_kev", F.datediff("data_adicao", F.to_date("data_publicacao")))
    .withColumn("janela_correcao_dias", F.datediff("data_limite", "data_adicao"))
    .withColumn("ano_adicao", F.year("data_adicao"))
    .withColumn("mes_adicao", F.date_format("data_adicao", "yyyy-MM"))
    .withColumn(
        "prioridade_cvss",
        F.when(F.col("cvss_score") >= 9, "CRITICA")
        .when(F.col("cvss_score") >= 7, "ALTA")
        .when(F.col("cvss_score") >= 4, "MEDIA")
        .when(F.col("cvss_score").isNotNull(), "BAIXA"),
    )
    .withColumn(
        "registro_valido",
        F.col("cve_id").rlike(r"^CVE-\d{4}-\d{4,}$")
        & F.col("data_adicao").isNotNull()
        & F.col("fornecedor").isNotNull()
        & F.col("produto").isNotNull(),
    )
    .dropDuplicates(["cve_id"])
)

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TABLE_PREFIX}_silver_vulnerabilidades")
print("Silver integrada:", silver.count(), "| válidas:", silver.filter("registro_valido").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Modelo estrela e camada Gold

# COMMAND ----------

valid = spark.table(f"{TABLE_PREFIX}_silver_vulnerabilidades").filter("registro_valido")

dim_fornecedor = valid.select("fornecedor").distinct().withColumn("fornecedor_id", F.sha2("fornecedor", 256))
dim_produto = valid.select("fornecedor", "produto").distinct().withColumn("produto_id", F.sha2(F.concat_ws("|", "fornecedor", "produto"), 256))
dim_tempo = valid.select(F.col("data_adicao").alias("data")).distinct().withColumn("ano", F.year("data")).withColumn("mes", F.month("data")).withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
dim_cwe = valid.select(F.explode_outer("cwes").alias("cwe_id")).filter("cwe_id is not null").distinct()

fato = (
    valid.join(dim_fornecedor, "fornecedor")
    .join(dim_produto, ["fornecedor", "produto"])
    .select(
        "cve_id", "fornecedor_id", "produto_id", "data_adicao", "data_publicacao", "data_limite",
        "cvss_score", "cvss_severidade", "prioridade_cvss", "ransomware_confirmado",
        "dias_publicacao_ate_kev", "janela_correcao_dias", "status_nvd", "triagem_forense",
    )
)

ponte_cve_cwe = valid.select("cve_id", F.explode_outer("cwes").alias("cwe_id")).filter("cwe_id is not null").dropDuplicates()

for name, frame in {
    "dim_fornecedor": dim_fornecedor,
    "dim_produto": dim_produto,
    "dim_tempo": dim_tempo,
    "dim_cwe": dim_cwe,
    "fato_vulnerabilidades": fato,
    "ponte_cve_cwe": ponte_cve_cwe,
}.items():
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TABLE_PREFIX}_{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Qualidade por atributo

# COMMAND ----------

quality_rows = []
for field in valid.schema.fields:
    column = field.name
    stats = valid.agg(
        F.count("*").alias("linhas"),
        F.sum(F.col(column).isNull().cast("int")).alias("nulos"),
        F.countDistinct(F.col(column).cast("string")).alias("distintos"),
        F.min(F.col(column).cast("string")).alias("minimo"),
        F.max(F.col(column).cast("string")).alias("maximo"),
    ).withColumn("atributo", F.lit(column)).withColumn("tipo", F.lit(field.dataType.simpleString()))
    quality_rows.append(stats)

quality = quality_rows[0]
for row in quality_rows[1:]:
    quality = quality.unionByName(row)
quality = quality.withColumn("percentual_nulos", F.round(F.col("nulos") / F.col("linhas") * 100, 2))
quality.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_qualidade")
display(quality.select("atributo", "tipo", "linhas", "nulos", "percentual_nulos", "distintos", "minimo", "maximo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Respostas às perguntas de negócio

# COMMAND ----------

fornecedores = (
    valid.groupBy("fornecedor")
    .agg(
        F.countDistinct("cve_id").alias("vulnerabilidades"),
        F.countDistinct("produto").alias("produtos"),
        F.round(F.avg("cvss_score"), 2).alias("cvss_medio"),
        F.sum(F.col("ransomware_confirmado").cast("int")).alias("ransomware_confirmado"),
        F.expr("percentile_approx(dias_publicacao_ate_kev, 0.5)").alias("mediana_dias_ate_kev"),
    )
)
fornecedores.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_fornecedores")
display(fornecedores.orderBy(F.desc("vulnerabilidades")))

# COMMAND ----------

temporal = valid.groupBy("ano_adicao", "mes_adicao").agg(
    F.countDistinct("cve_id").alias("vulnerabilidades_adicionadas"),
    F.sum(F.col("ransomware_confirmado").cast("int")).alias("ransomware_confirmado"),
).orderBy("mes_adicao")
temporal.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_evolucao_temporal")
display(temporal)

# COMMAND ----------

cwes = (
    valid.select("cve_id", "ransomware_confirmado", F.explode_outer("cwes").alias("cwe_id"))
    .filter("cwe_id is not null")
    .groupBy("cwe_id")
    .agg(
        F.countDistinct("cve_id").alias("vulnerabilidades"),
        F.sum(F.col("ransomware_confirmado").cast("int")).alias("ransomware_confirmado"),
    )
)
cwes.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_fraquezas_cwe")
display(cwes.orderBy(F.desc("vulnerabilidades")))

# COMMAND ----------

ransomware = valid.filter("ransomware_confirmado").select(
    "cve_id", "fornecedor", "produto", "cvss_score", "cvss_severidade", "data_adicao", "data_limite", "cwes"
)
ransomware.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_ransomware")
display(ransomware.orderBy(F.desc("cvss_score"), F.desc("data_adicao")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Evidência de persistência
# MAGIC Todas as tabelas abaixo são relidas do catálogo após a gravação.

# COMMAND ----------

tables = [
    "bronze_cisa_kev", "bronze_nvd_kev", "silver_vulnerabilidades", "dim_fornecedor", "dim_produto",
    "dim_tempo", "dim_cwe", "fato_vulnerabilidades", "ponte_cve_cwe", "qualidade", "fornecedores",
    "evolucao_temporal", "fraquezas_cwe", "ransomware",
]
persistencia = [(f"{catalog}.{schema}.{TABLE_PREFIX}_{name}", spark.table(f"{TABLE_PREFIX}_{name}").count()) for name in tables]
display(spark.createDataFrame(persistencia, ["tabela_delta", "linhas_persistidas"]))
