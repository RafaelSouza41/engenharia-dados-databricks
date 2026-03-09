# Databricks notebook source
# MAGIC %run "/Workspace/Users/rafaelnogueirasms@gmail.com/Criando_medalhao/Analise_pesada/config/config"

# COMMAND ----------

df_gold = spark.read.table("workspace.silver.cisp")

col_roubo = [c for c in df_gold.columns if "roubo" in c.lower()]
col_furto = [c for c in df_gold.columns if "furto" in c.lower()]
col_drogas = [c for c in df_gold.columns if "drogas" in c.lower()]

c_padrao = ["mes","ano","mes_ano","regiao","munic","registro_ocorrencias"]
c_data = ["ingestion_Data"]

df_roubo = df_gold.select(*(c_padrao + col_roubo + c_data))
df_furto = df_gold.select(*(c_padrao + col_furto + c_data))
df_drogas = df_gold.select(*(c_padrao + col_drogas + c_data))

# ROUBO
if not spark.catalog.tableExists("workspace.gold.roubo"):
    df_roubo.write \
        .format("delta") \
        .partitionBy("ano") \
        .mode("append") \
        .saveAsTable("workspace.gold.roubo")
else:
    df_roubo.createOrReplaceTempView("df_novos")
    spark.sql("""insert into workspace.gold.roubo select * from df_novos where not exists (select * from workspace.gold.roubo )""")


# FURTO
if not spark.catalog.tableExists("workspace.gold.furto"):
    df_furto.write \
        .format("delta") \
        .partitionBy("ano") \
        .mode("append") \
        .saveAsTable("workspace.gold.furto")
else:
    df_furto.createOrReplaceTempView("df_novos")
    spark.sql("""insert into workspace.gold.furto select * from df_novos where not exists (select * from workspace.gold.furto )""")


# drogas
if not spark.catalog.tableExists("workspace.gold.drogas"):
    df_drogas.write \
        .format("delta") \
        .partitionBy("ano") \
        .mode("append") \
        .saveAsTable("workspace.gold.drogas")
else:
    df_drogas.createOrReplaceTempView("df_droga")
    spark.sql("""insert into workspace.gold.drogas select * from df_droga where not exists (select * from workspace.gold.furto )""")

#Particionando os dados por ano e separando entre furto e roubo
#df_roubo.write \
#.format("delta") \
#.partitionBy("ano") \
#.save("/Volumes/workspace/default/3-curated/Violencia/Roubo")

#df_furto.write \
#.format("delta") \
#.partitionBy("ano") \
#.save("/Volumes/workspace/default/3-curated/Violencia/Furto")

#df_roubo.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.cisp_roubo")
#df_furto.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.cisp_furto")