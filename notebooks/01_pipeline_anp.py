# Databricks notebook source
# MAGIC %md
# MAGIC # MVP - Pipeline de preços de combustíveis da ANP
# MAGIC **Autor:** Bruno Bryto Borges - Matrícula 231013304
# MAGIC
# MAGIC Este notebook implementa um pipeline reprodutível em nuvem nas camadas Bronze, Silver e Gold.

# COMMAND ----------

from pathlib import Path
import hashlib
import os
import urllib.request
import zipfile

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

URLS = {
    "2024-01": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-2024-01.zip",
    "2024-02": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-2024-02.zip",
    "2025-01": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-2025-01.zip",
    "2025-02": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-2025-02.zip",
}

TABLE_PREFIX = "tarefa_4"
catalog = spark.sql("SELECT current_catalog()").first()[0]
schema = spark.sql("SELECT current_schema()").first()[0]
print(f"Destino persistente: {catalog}.{schema}.{TABLE_PREFIX}_*")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{TABLE_PREFIX}_files`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Coleta e camada Bronze
# MAGIC Os arquivos são baixados da fonte oficial, extraídos no armazenamento temporário do cluster e lidos com esquema explícito. A tabela Bronze mantém todos os atributos originais e a URL de origem.

# COMMAND ----------

tmp_dir = Path(f"/Volumes/{catalog}/{schema}/{TABLE_PREFIX}_files/raw")
tmp_dir.mkdir(parents=True, exist_ok=True)
csv_paths = []
for period, url in URLS.items():
    zip_path = tmp_dir / f"{period}.zip"
    urllib.request.urlretrieve(url, zip_path)
    period_dir = tmp_dir / period
    period_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(period_dir)
    csv_path = next(period_dir.glob("*.csv"))
    csv_paths.append((period, url, str(csv_path)))
    print(period, csv_path.name, csv_path.stat().st_size)

# COMMAND ----------

schema_raw = T.StructType([
    T.StructField("Regiao - Sigla", T.StringType()),
    T.StructField("Estado - Sigla", T.StringType()),
    T.StructField("Municipio", T.StringType()),
    T.StructField("Revenda", T.StringType()),
    T.StructField("CNPJ da Revenda", T.StringType()),
    T.StructField("Nome da Rua", T.StringType()),
    T.StructField("Numero Rua", T.StringType()),
    T.StructField("Complemento", T.StringType()),
    T.StructField("Bairro", T.StringType()),
    T.StructField("Cep", T.StringType()),
    T.StructField("Produto", T.StringType()),
    T.StructField("Data da Coleta", T.StringType()),
    T.StructField("Valor de Venda", T.StringType()),
    T.StructField("Valor de Compra", T.StringType()),
    T.StructField("Unidade de Medida", T.StringType()),
    T.StructField("Bandeira", T.StringType()),
])

bronze_parts = []
for period, url, path in csv_paths:
    part = (
        spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8")
        .schema(schema_raw).csv(path)
        .withColumn("periodo_arquivo", F.lit(period))
        .withColumn("url_origem", F.lit(url))
        .withColumn("ingerido_em", F.current_timestamp())
    )
    bronze_parts.append(part)

bronze = bronze_parts[0]
for part in bronze_parts[1:]:
    bronze = bronze.unionByName(part)

(bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{TABLE_PREFIX}_bronze_precos"))
print("Bronze:", spark.table(f"{TABLE_PREFIX}_bronze_precos").count(), "linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformação e camada Silver
# MAGIC Regras: padronização de texto; conversão de datas e números; seleção de gasolina e etanol; pseudonimização do CNPJ; validação de domínio; remoção de duplicatas exatas da medição.

# COMMAND ----------

silver_base = (
    spark.table(f"{TABLE_PREFIX}_bronze_precos")
    .select(
        F.upper(F.trim(F.col("Regiao - Sigla"))).alias("regiao"),
        F.upper(F.trim(F.col("Estado - Sigla"))).alias("uf"),
        F.upper(F.trim(F.col("Municipio"))).alias("municipio"),
        F.sha2(F.col("CNPJ da Revenda"), 256).alias("posto_id"),
        F.upper(F.trim(F.col("Produto"))).alias("produto"),
        F.to_date("Data da Coleta", "dd/MM/yyyy").alias("data_coleta"),
        F.regexp_replace("Valor de Venda", ",", ".").cast("double").alias("valor_venda"),
        F.regexp_replace("Valor de Compra", ",", ".").cast("double").alias("valor_compra"),
        F.upper(F.trim(F.col("Unidade de Medida"))).alias("unidade_medida"),
        F.upper(F.trim(F.col("Bandeira"))).alias("bandeira"),
        "periodo_arquivo", "url_origem", "ingerido_em",
    )
    .withColumn("produto", F.translate("produto", "ÁÀÂÃÉÊÍÓÔÕÚÇ", "AAAAEEIOOOUC"))
    .filter(F.col("produto").isin("GASOLINA", "ETANOL"))
    .withColumn("ano", F.year("data_coleta"))
    .withColumn("mes", F.date_format("data_coleta", "yyyy-MM"))
    .withColumn(
        "registro_valido",
        F.col("data_coleta").isNotNull()
        & F.col("valor_venda").between(0.5, 20.0)
        & F.col("uf").rlike("^[A-Z]{2}$"),
    )
)

dedup_window = Window.partitionBy("posto_id", "produto", "data_coleta", "valor_venda").orderBy(F.col("ingerido_em").desc())
silver = (
    silver_base.withColumn("ordem_duplicata", F.row_number().over(dedup_window))
    .filter("ordem_duplicata = 1")
    .drop("ordem_duplicata")
)
(silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{TABLE_PREFIX}_silver_precos"))
print("Silver:", spark.table(f"{TABLE_PREFIX}_silver_precos").count(), "linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Modelo dimensional e camada Gold
# MAGIC Modelo estrela: `fato_precos` contém as medições; `dim_localidade`, `dim_produto`, `dim_tempo` e `dim_posto` descrevem os eixos analíticos.

# COMMAND ----------

valid = spark.table(f"{TABLE_PREFIX}_silver_precos").filter("registro_valido")

dim_localidade = valid.select("regiao", "uf", "municipio").distinct().withColumn(
    "localidade_id", F.sha2(F.concat_ws("|", "regiao", "uf", "municipio"), 256)
)
dim_produto = valid.select("produto", "unidade_medida").distinct().withColumn(
    "produto_id", F.sha2(F.concat_ws("|", "produto", "unidade_medida"), 256)
)
dim_tempo = valid.select("data_coleta").distinct().withColumn("ano", F.year("data_coleta")).withColumn(
    "mes", F.month("data_coleta")
).withColumn("ano_mes", F.date_format("data_coleta", "yyyy-MM"))
dim_posto = valid.groupBy("posto_id").agg(F.max("bandeira").alias("bandeira"))

fato = (
    valid.join(dim_localidade, ["regiao", "uf", "municipio"])
    .join(dim_produto, ["produto", "unidade_medida"])
    .select("posto_id", "localidade_id", "produto_id", "data_coleta", "valor_venda", "valor_compra", "periodo_arquivo")
)

for name, frame in {
    "dim_localidade": dim_localidade,
    "dim_produto": dim_produto,
    "dim_tempo": dim_tempo,
    "dim_posto": dim_posto,
    "fato_precos": fato,
}.items():
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TABLE_PREFIX}_{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Qualidade dos dados

# COMMAND ----------

quality = []
for field in silver.schema.fields:
    col = field.name
    row = silver.agg(
        F.count("*").alias("linhas"),
        F.sum(F.col(col).isNull().cast("int")).alias("nulos"),
        F.countDistinct(col).alias("distintos"),
        F.min(F.col(col).cast("string")).alias("minimo"),
        F.max(F.col(col).cast("string")).alias("maximo"),
    ).withColumn("atributo", F.lit(col)).withColumn("tipo", F.lit(field.dataType.simpleString()))
    quality.append(row)

quality_df = quality[0]
for row in quality[1:]:
    quality_df = quality_df.unionByName(row)

quality_df.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_qualidade")
display(quality_df.select("atributo", "tipo", "linhas", "nulos", "distintos", "minimo", "maximo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Respostas às perguntas de negócio

# COMMAND ----------

precos_uf_2025 = (
    valid.filter("ano = 2025")
    .groupBy("uf", "produto")
    .agg(F.round(F.avg("valor_venda"), 3).alias("preco_medio"), F.count("*").alias("observacoes"))
)
precos_uf_2025.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_precos_uf_2025")
display(precos_uf_2025.orderBy(F.desc("preco_medio")))

# COMMAND ----------

mensal = (
    valid.groupBy("mes", "produto")
    .agg(F.round(F.avg("valor_venda"), 3).alias("preco_medio"), F.count("*").alias("observacoes"))
    .orderBy("mes", "produto")
)
mensal.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_evolucao_mensal")
display(mensal)

# COMMAND ----------

media_estado_mes = valid.groupBy("uf", "mes", "produto").agg(F.avg("valor_venda").alias("preco_medio"))
razao = (
    media_estado_mes.groupBy("uf", "mes").pivot("produto", ["GASOLINA", "ETANOL"]).agg(F.first("preco_medio"))
    .dropna(subset=["GASOLINA", "ETANOL"])
    .withColumn("razao_etanol_gasolina", F.col("ETANOL") / F.col("GASOLINA"))
    .withColumn("etanol_competitivo", F.col("razao_etanol_gasolina") <= 0.70)
)
competitividade = (
    razao.groupBy("uf")
    .agg(
        F.count("*").alias("meses_analisados"),
        F.sum(F.col("etanol_competitivo").cast("int")).alias("meses_etanol_competitivo"),
        F.round(F.avg("razao_etanol_gasolina"), 3).alias("razao_media"),
    )
    .withColumn("percentual_meses_competitivo", F.round(F.col("meses_etanol_competitivo") / F.col("meses_analisados") * 100, 1))
)
competitividade.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_competitividade_etanol")
display(competitividade.orderBy(F.desc("percentual_meses_competitivo"), "razao_media"))

# COMMAND ----------

dispersao = (
    valid.groupBy("uf", "produto")
    .agg(
        F.round(F.avg("valor_venda"), 3).alias("media"),
        F.round(F.stddev("valor_venda"), 3).alias("desvio"),
        F.round(F.expr("percentile_approx(valor_venda, 0.95) - percentile_approx(valor_venda, 0.05)"), 3).alias("amplitude_p90"),
    )
    .withColumn("coeficiente_variacao", F.round(F.col("desvio") / F.col("media"), 3))
)
dispersao.write.format("delta").mode("overwrite").saveAsTable(f"{TABLE_PREFIX}_dispersao")
display(dispersao.orderBy(F.desc("coeficiente_variacao")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evidência de persistência
# MAGIC A célula abaixo relê as tabelas Delta do catálogo após a escrita. O resultado comprova que os dados permanecem armazenados na nuvem, independentemente das variáveis em memória do notebook.

# COMMAND ----------

tables = [
    "bronze_precos", "silver_precos", "dim_localidade", "dim_produto", "dim_tempo",
    "dim_posto", "fato_precos", "qualidade", "precos_uf_2025", "evolucao_mensal",
    "competitividade_etanol", "dispersao",
]
persistencia = [(f"{catalog}.{schema}.{TABLE_PREFIX}_{name}", spark.table(f"{TABLE_PREFIX}_{name}").count()) for name in tables]
display(spark.createDataFrame(persistencia, ["tabela_delta", "linhas_persistidas"]))
