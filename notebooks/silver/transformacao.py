# Databricks notebook source
# MAGIC %run "/Workspace/Users/rafaelnogueirasms@gmail.com/Criando_medalhao/Analise_pesada/config/config"

# COMMAND ----------

df_silver= spark.read.table("workspace.bronze.cisp_raw")

df_silver = df_silver.filter(to_date(col("ingestion_data")) == current_date())

# marca os registros invalidados
df_silver = df_silver.withColumn("invalido",
            when(col("cisp").rlike("^[0-9]+$"), None)
            .otherwise("x"))

# elimina as regiões preenchidas com números
df_silver = df_silver.withColumn("regiao",
            when(col("cisp").rlike("^[0-9]+$"), col("regiao"))
            .otherwise(None))            

# elimina as regiões nulas
df_silver = df_silver.filter(col("regiao").isNotNull())

#substituição de dados incorrtos
df_silver= df_silver.replace({"Grande NiterÃÂÃÂÃÂÃÂ³i":"Grande Niterói" \
                ,"Grande NiterÃÂ³i":"Grande Niterói"\
                ,"Grande NiterÃ³i":"Grande Niterói"}, subset=["regiao"])

#conversão das colunas
df_silver = df_silver.withColumn("mes",col("mes").cast("int"))
df_silver = df_silver.withColumn("ano",col("ano").cast("int"))
df_silver = df_silver.withColumn("hom_doloso",col("hom_doloso").cast("int"))
df_silver = df_silver.withColumn("lesao_corp_morte",col("lesao_corp_morte").cast("int"))
df_silver = df_silver.withColumn("latrocinio",col("latrocinio").cast("int"))
df_silver = df_silver.withColumn("roubo_transeunte",col("roubo_transeunte").cast("int"))
df_silver = df_silver.withColumn("roubo_celular",col("roubo_celular").cast("int"))
df_silver = df_silver.withColumn("roubo_em_coletivo",col("roubo_em_coletivo").cast("int"))
df_silver = df_silver.withColumn("roubo_rua",col("roubo_rua").cast("int"))
df_silver = df_silver.withColumn("roubo_veiculo",col("roubo_veiculo").cast("int"))
df_silver = df_silver.withColumn("roubo_carga",col("roubo_carga").cast("int"))
df_silver = df_silver.withColumn("roubo_comercio",col("roubo_comercio").cast("int"))
df_silver = df_silver.withColumn("roubo_comercio",col("roubo_comercio").cast("int"))
df_silver = df_silver.withColumn("roubo_residencia",col("roubo_residencia").cast("int"))
df_silver = df_silver.withColumn("roubo_banco",col("roubo_banco").cast("int"))
df_silver = df_silver.withColumn("roubo_cx_eletronico",col("roubo_cx_eletronico").cast("int"))
df_silver = df_silver.withColumn("roubo_conducao_saque",col("roubo_conducao_saque").cast("int"))
df_silver = df_silver.withColumn("roubo_apos_saque",col("roubo_apos_saque").cast("int"))
df_silver = df_silver.withColumn("roubo_bicicleta",col("roubo_bicicleta").cast("int"))
df_silver = df_silver.withColumn("outros_roubos",col("outros_roubos").cast("int"))
df_silver = df_silver.withColumn("outros_roubos",col("outros_roubos").cast("int"))
df_silver = df_silver.withColumn("outros_roubos",col("outros_roubos").cast("int"))
df_silver = df_silver.withColumn("furto_transeunte",col("furto_transeunte").cast("int"))
df_silver = df_silver.withColumn("furto_coletivo",col("furto_coletivo").cast("int"))
df_silver = df_silver.withColumn("furto_celular",col("furto_celular").cast("int"))
df_silver = df_silver.withColumn("furto_bicicleta",col("furto_bicicleta").cast("int"))
df_silver = df_silver.withColumn("outros_furtos",col("outros_furtos").cast("int"))
df_silver = df_silver.withColumn("total_furtos",col("total_furtos").cast("int"))
df_silver = df_silver.withColumn("sequestro",col("sequestro").cast("int"))
df_silver = df_silver.withColumn("sequestro_relampago",col("sequestro_relampago").cast("int"))
df_silver = df_silver.withColumn("estelionato",col("estelionato").cast("int"))
df_silver = df_silver.withColumn("apreensao_drogas",col("apreensao_drogas").cast("int"))
df_silver = df_silver.withColumn("posse_drogas",col("posse_drogas").cast("int"))
df_silver = df_silver.withColumn("trafico_drogas",col("trafico_drogas").cast("int"))
df_silver = df_silver.withColumn("apreensao_drogas_sem_autor",col("apreensao_drogas_sem_autor").cast("int"))
df_silver = df_silver.withColumn("recuperacao_veiculos",col("recuperacao_veiculos").cast("int"))
df_silver = df_silver.withColumn("apf",col("apf").cast("int"))
df_silver = df_silver.withColumn("aaapai",col("aaapai").cast("int"))
df_silver = df_silver.withColumn("cmp",col("cmp").cast("int"))
df_silver = df_silver.withColumn("cmba",col("cmba").cast("int"))
df_silver = df_silver.withColumn("ameaca",col("ameaca").cast("int"))
df_silver = df_silver.withColumn("pessoas_desaparecidas",col("pessoas_desaparecidas").cast("int"))
df_silver = df_silver.withColumn("encontro_cadaver",col("encontro_cadaver").cast("int"))
df_silver = df_silver.withColumn("encontro_ossada",col("encontro_ossada").cast("int"))  
df_silver = df_silver.withColumn("pol_militares_mortos_serv",col("pol_militares_mortos_serv").cast("int"))  
df_silver = df_silver.withColumn("pol_civis_mortos_serv",col("pol_civis_mortos_serv").cast("int"))  
df_silver = df_silver.withColumn("fase",col("fase").cast("int"))  


#usa apenas os invalidados
df_erro = df_silver.where (col("invalido")=="x")

#removenddo "
df_erro = df_erro.withColumn(
    "cisp",
    regexp_replace("cisp", '"', '')
)

#removendo #
df_erro = df_erro.withColumn(
    "mes",
    regexp_replace("mes", '"', '')
)

# colocando a informação na coluna regiao
df_erro = df_erro.withColumn("regiao", df_erro["mes"])


 #separa os dados na coluna consolidada
df_erro = df_erro.withColumn(
    "col_split",
    split("cisp", ";")
)


df_corrigido = df_erro\
    .withColumn("cisp", col("col_split").getItem(0)) \
    .withColumn("mes", col("col_split").getItem(1)) \
    .withColumn("ano", col("col_split").getItem(2)) \
    .withColumn("mes_ano", col("col_split").getItem(3)) \
    .withColumn("aisp", col("col_split").getItem(4)) \
    .withColumn("risp", col("col_split").getItem(5)) \
    .withColumn("munic", col("col_split").getItem(6))    


#corrigo falha na coluna mês
df_corrigido= df_corrigido.withColumn("mes",substring(col("mes_ano").cast("string"), -2, 2))

#remove colunas
df_corrigido= df_corrigido.drop("invalido","col_split")
df_silver = df_silver.filter(col("invalido").isNull())
df_silver= df_silver.drop("invalido")

#une os datagrames
df_silver = df_silver.union(df_corrigido)

#salva a tabela final
if not spark.catalog.tableExists("workspace.silver.cisp"):
    df_silver.write.format("delta")\
                .mode("overwrite")\
                .saveAsTable("workspace.silver.cisp")
else:
    #converte em tabela temporária
    df_silver.createOrReplaceTempView("df_silver")
    spark.sql("""insert into workspace.silver.cisp select * from df_silver where not exists (select * from workspace.silver.cisp)""")
    