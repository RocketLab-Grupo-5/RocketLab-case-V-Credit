# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # csv_exports
# MAGIC Exporta as tabelas da camada **gold** como CSVs prontos para o Power BI.
# MAGIC
# MAGIC - Um CSV por tabela (fatos e dimensões separadas), com o mesmo nome da tabela
# MAGIC - Datas em formato ISO, UTF-8 com BOM (acentos funcionam no Power BI/Excel)
# MAGIC - Tabelas pequenas via pandas (arquivo único e limpo); tabelas grandes via Spark
# MAGIC - Ao final: validação de linhas e um .zip opcional para baixar tudo de uma vez

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração
# MAGIC Ajuste os nomes abaixo para o seu ambiente.

# COMMAND ----------

catalog = "vcredit_medallion"        # no Free Edition o catálogo padrão é "workspace"
schema_gold = "gold"         # schema onde estão as tabelas da camada gold
volume = "exports"           # volume que recebe os CSVs (é criado se não existir)
subpasta = "powerbi"         # pasta dentro do volume

# Lista vazia = exporta todas as tabelas do schema gold
tabelas_incluir = []
tabelas_ignorar = []

# Acima deste número de linhas, a exportação usa Spark em vez de pandas
LIMITE_LINHAS_PANDAS = 500_000

pasta_saida = f"/Volumes/{catalog}/{schema_gold}/{volume}/{subpasta}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cria o volume e a pasta de saída

# COMMAND ----------

import os
import shutil

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema_gold}.{volume}")
os.makedirs(pasta_saida, exist_ok=True)

print(f"CSVs serão salvos em: {pasta_saida}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lista as tabelas a exportar

# COMMAND ----------

if tabelas_incluir:
    tabelas = list(tabelas_incluir)
else:
    linhas_show = spark.sql(f"SHOW TABLES IN {catalog}.{schema_gold}").collect()
    tabelas = sorted(r.tableName for r in linhas_show if not r.isTemporary)

tabelas = [t for t in tabelas if t not in tabelas_ignorar]

print(f"{len(tabelas)} tabela(s) para exportar:")
for t in tabelas:
    print(f"  - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funções de exportação

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType, ShortType, IntegerType, LongType,
    DateType, TimestampType, TimestampNTZType, DecimalType,
)

TIPOS_INTEIROS = (ByteType, ShortType, IntegerType, LongType)
BOM_UTF8 = b"\xef\xbb\xbf"


def preparar_para_csv(df):
    """Converte datas/timestamps em texto ISO e decimais em double, para o Power BI ler sem ambiguidade."""
    for campo in df.schema.fields:
        coluna = F.col(f"`{campo.name}`")
        if isinstance(campo.dataType, DecimalType):
            # Evita valores como "0E-10" no CSV e o tipo "object" no pandas
            df = df.withColumn(campo.name, coluna.cast("double"))
        elif isinstance(campo.dataType, DateType):
            df = df.withColumn(campo.name, F.date_format(coluna, "yyyy-MM-dd"))
        elif isinstance(campo.dataType, (TimestampType, TimestampNTZType)):
            df = df.withColumn(campo.name, F.date_format(coluna, "yyyy-MM-dd HH:mm:ss"))
    return df


def exportar_com_pandas(df, destino):
    """Para tabelas pequenas: gera um único arquivo, já com o nome final."""
    pdf = df.toPandas()

    # Inteiros com nulos viram float no pandas (123 -> 123.0), o que quebra chaves no Power BI.
    # O tipo "Int64" (com I maiúsculo) aceita nulos e mantém os valores como inteiros.
    for campo in df.schema.fields:
        if isinstance(campo.dataType, TIPOS_INTEIROS):
            pdf[campo.name] = pdf[campo.name].astype("Int64")

    # utf-8-sig grava o BOM, que faz o Power BI reconhecer os acentos automaticamente
    pdf.to_csv(destino, index=False, encoding="utf-8-sig")


def exportar_com_spark(df, destino):
    """Para tabelas grandes: o Spark grava uma pasta com um part-*.csv, que depois vira o arquivo final."""
    pasta_tmp = destino.replace(".csv", "_tmp")

    (
        df.coalesce(1)                   # uma partição = um único arquivo de saída
        .write.mode("overwrite")
        .option("header", True)
        .option("escape", '"')           # aspas internas viram "" (padrão do Power BI/Excel)
        .option("encoding", "UTF-8")
        .csv(pasta_tmp)
    )

    parte = next(
        f for f in os.listdir(pasta_tmp)
        if f.startswith("part-") and f.endswith(".csv")
    )

    # Copia o part-*.csv para o nome final, adicionando o BOM no início
    with open(os.path.join(pasta_tmp, parte), "rb") as origem, open(destino, "wb") as saida:
        saida.write(BOM_UTF8)
        shutil.copyfileobj(origem, saida)

    shutil.rmtree(pasta_tmp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exporta todas as tabelas

# COMMAND ----------

import time
import pandas as pd

resultados = []

for tabela in tabelas:
    nome_completo = f"{catalog}.{schema_gold}.{tabela}"
    destino = os.path.join(pasta_saida, f"{tabela}.csv")
    inicio = time.time()

    try:
        df = preparar_para_csv(spark.table(nome_completo))
        linhas = df.count()

        if linhas <= LIMITE_LINHAS_PANDAS:
            exportar_com_pandas(df, destino)
            metodo = "pandas"
        else:
            exportar_com_spark(df, destino)
            metodo = "spark"

        resultados.append({
            "tabela": tabela,
            "linhas": linhas,
            "colunas": len(df.columns),
            "metodo": metodo,
            "tamanho_kb": round(os.path.getsize(destino) / 1024, 1),
            "segundos": round(time.time() - inicio, 1),
            "status": "ok",
        })
        print(f"[ok]   {tabela}: {linhas:,} linhas ({metodo})")

    except Exception as e:
        resultados.append({"tabela": tabela, "status": f"ERRO: {str(e)[:300]}"})
        print(f"[erro] {tabela}: {str(e)[:300]}")

resumo = pd.DataFrame(resultados)
display(resumo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação: o CSV tem o mesmo número de linhas da tabela?

# COMMAND ----------

divergencias = []

for r in resultados:
    if r["status"] != "ok":
        continue

    caminho = os.path.join(pasta_saida, f"{r['tabela']}.csv")
    linhas_csv = (
        spark.read
        .option("header", True)
        .option("multiLine", True)       # textos com quebra de linha não contam como linhas extras
        .option("escape", '"')
        .csv(caminho)
        .count()
    )

    if linhas_csv != r["linhas"]:
        divergencias.append((r["tabela"], r["linhas"], linhas_csv))

if divergencias:
    for tabela, esperado, obtido in divergencias:
        print(f"[divergência] {tabela}: tabela={esperado:,} | csv={obtido:,}")
else:
    print("Todos os CSVs batem com as tabelas de origem.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. (Opcional) Zip com todos os CSVs, para baixar de uma vez só
# MAGIC O zip é montado no disco local (`/tmp`) e depois copiado para o volume,
# MAGIC porque volumes não aceitam a escrita com saltos que o formato zip exige.

# COMMAND ----------

caminho_zip_local = shutil.make_archive("/tmp/gold_powerbi", "zip", pasta_saida)
destino_zip = f"/Volumes/{catalog}/{schema_gold}/{volume}/gold_powerbi.zip"
shutil.copyfile(caminho_zip_local, destino_zip)

print(f"Zip pronto para download: {destino_zip}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Como baixar
# MAGIC 1. Abra o **catalog Explorer** → catálogo `workspace` → schema `gold` → **volumes** → `exports`
# MAGIC 2. Clique em `gold_powerbi.zip` (ou entre em `powerbi/` para arquivos individuais) → **Download**
# MAGIC 3. Descompacte numa pasta fixa e aponte o Power BI para ela (de preferência via parâmetro de caminho)
# MAGIC
# MAGIC Lembrete no Power Query: tipar colunas numéricas e de data com **"Usando localidade → Inglês (EUA)"**,
# MAGIC porque o CSV usa ponto como separador decimal.