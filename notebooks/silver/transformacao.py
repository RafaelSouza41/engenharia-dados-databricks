# Databricks notebook source
# MAGIC %run "/Workspace/Users/rafaelnogueirasms@gmail.com/Criando_medalhao/Analise_pesada/config/config"

# COMMAND ----------

def leitura_bronze():

    df_bronze= spark.read.table(f"{table_bronze}")
    df_bronze = df_bronze.filter(to_date(col("ingestion_data")) == current_date())
    return df_bronze


def trata_colunas(df_bronze):

    #conversão das colunas
    cols_int = ["mes","ano","hom_doloso","latrocinio","roubo_transeunte","roubo_celular"]

    for c in cols_int:
        df_silver = df_bronze.withColumn(c, col(c).cast("int"))
    
    return df_silver


def dados_incorretos(df_silver):

    # elimina as regiões preenchidas com números
    df_silver = df_silver.withColumn("regiao",
                when(col("regiao").rlike("^[0-9]+$"), col("regiao"))
                .otherwise(None))            

    # elimina as regiões nulas
    df_silver = df_silver.filter(col("regiao").isNotNull())

    #substituição de dados incorrtos
    df_silver= df_silver.replace({"Grande NiterÃÂÃÂÃÂÃÂ³i":"Grande Niterói" \
                    ,"Grande NiterÃÂ³i":"Grande Niterói"\
                    ,"Grande NiterÃ³i":"Grande Niterói"}, subset=["regiao"])

    #corrigo falha na coluna mês
    df_corrigido= df_silver.withColumn("mes",substring(col("mes_ano").cast("string"), -2, 2))

    # marca os registros invalidados
    df_corrigido = df_corrigido.withColumn("invalido", when(col("cisp").rlike("^[0-9]+$"), None).otherwise("x"))

    return df_corrigido


def define_dados_invalidos(df_corrigido):

    #usa apenas os invalidados
    df_erro = df_corrigido.where (col("invalido")=="x")

    return df_erro


def limpeza_dados(df_erro):

    #removenddo "
    df_erro = df_erro.withColumn("cisp", regexp_replace("cisp", '"', ''))

    #removendo #
    df_erro = df_erro.withColumn(
        "mes",
        regexp_replace("mes", '"', '')
    )

    # colocando a informação na coluna regiao
    df_erro = df_erro.withColumn("regiao", df_erro["mes"])

    return df_erro


def dados_aglutinados(df_erro):

    #separa os dados na coluna consolidada
    df_erro = df_erro.withColumn("col_split",split("cisp", ";"))

    df_erro = df_erro\
        .withColumn("cisp", col("col_split").getItem(0)) \
        .withColumn("mes", col("col_split").getItem(1)) \
        .withColumn("ano", col("col_split").getItem(2)) \
        .withColumn("mes_ano", col("col_split").getItem(3)) \
        .withColumn("aisp", col("col_split").getItem(4)) \
        .withColumn("risp", col("col_split").getItem(5)) \
        .withColumn("munic", col("col_split").getItem(6))    

    #remove colunas
    df_erro= df_erro.drop("invalido","col_split")

    return df_erro


def removendo_duplicidades(df_silver, df_corrigido):

    #pega os dados válidos
    df_corrigido = df_corrigido.filter(col("invalido").isNull())
    df_corrigido= df_corrigido.drop("invalido")

    #une os dataframes
    df_silver = df_silver.union(df_corrigido)

    #criando um ID
    df_silver = df_silver.withColumn("id", concat_ws("_", col("cisp"), \
                                     col("mes"), col("ano"), col("aisp"), \
                                     col("risp"),col("registro_ocorrencias")))

    #cria uma particão por id ordenado
    window = Window.partitionBy("id").orderBy(col("ingestion_data").desc())

    df_silver = df_silver.withColumn("rn", row_number().over(window)) \
                        .filter(col("rn") == 1) \
                        .drop("rn")

    return df_silver

df_bronze = leitura_bronze()
df_silver = trata_colunas(df_bronze)
df_corrigido = dados_incorretos(df_silver)
df_erro = define_dados_invalidos(df_corrigido)
df_erro = limpeza_dados(df_erro)
df_silver = dados_aglutinados(df_erro)
df_silver = removendo_duplicidades(df_silver, df_corrigido)


total_processado = df_silver.count()
data_inicio = datetime.now()


if total_processado == 0:
    raise Exception("Pipeline gerou dataset vazio")
else:
    try:
        if not spark.catalog.tableExists("workspace.silver.cisp"):
            df_silver.write.format("delta")\
                        .mode("overwrite")\
                        .saveAsTable("workspace.silver.cisp")
        else:
            #converte em tabela temporária
            df_silver.createOrReplaceTempView("df_final")
            spark.sql("""
                        MERGE INTO workspace.silver.cisp as d
                        USING df_final as o
                        ON d.id = o.id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """)
                
        total_tabela = spark.read.table("workspace.silver.cisp").count()
        
        pipeline = "pipeline_silver"
        camada = "silver"
        tabela = "carga"
        status="sucesso"
        e=""
    except Exception as e:
        print(f"Erro no pipeline {table}: {e}")    
    
    atualiza_carga(pipeline, camada, tabela,
                   total_processado, total_tabela,
                   data_inicio, status,str(e))
    