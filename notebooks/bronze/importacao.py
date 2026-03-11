# Databricks notebook source
# MAGIC %run "/Workspace/Users/rafaelnogueirasms@gmail.com/Criando_medalhao/Analise_pesada/config/config"

# COMMAND ----------

#importa dados
df_raw = spark.read.csv(f"{caminho_raw}",header= True,encoding='latin1',sep=";")

# deixa as colunas em minúsculas
for c in df_raw.columns:
  df_raw = df_raw.withColumnRenamed(c,c.lower())

# retira espaços no nome das colunas
df_raw = df_raw.toDF(*[col.replace(" ", "_")\
for col in df_raw.columns])

#insere coluna com data de ingestão
df_raw=df_raw.withColumn("ingestion_data", current_date())   

#limpa duplicatas
df_raw_limpo = df_raw.dropDuplicates()

#converte em tabela temporária
df_raw_limpo.createOrReplaceTempView("df_raw_limpo")

#salva a tabela final
if not spark.catalog.tableExists("workspace.bronze.cisp_raw"):
    df_raw_limpo.write.format("delta")\
                .mode("overwrite")\
                .saveAsTable("workspace.bronze.cisp_raw")
else:
    spark.sql("""insert into workspace.bronze.cisp_raw select * from df_raw_limpo where not exists (select * from workspace.bronze.cisp_raw)""")