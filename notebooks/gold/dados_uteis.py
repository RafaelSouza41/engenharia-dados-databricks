# Databricks notebook source
# MAGIC %run "/Workspace/Users/rafaelnogueirasms@gmail.com/Criando_medalhao/Analise_pesada/config/config"

# COMMAND ----------

# DBTITLE 1,Cell 2
def abre_silver():
    df_gold = spark.read.table(f"{table_silver}")
    return df_gold


def cria_dfs(c_padrao, c_data,df, coluna):

    v_col = [c for c in df.columns if f"{coluna}" in c.lower()]
    df = df.select(*(c_padrao + v_col + c_data))
    return df

def salvamento(df, table,caminho_gold=f"{caminho_gold}"):
    
    pipeline = "pipeline_gold"
    camada = "gold"

    data_inicio = datetime.now()

    try:
        total_processado = df.count()

        if total_processado >0:
            
            v_tabela = f"{caminho_gold}.{table}"
            
            if not spark.catalog.tableExists(v_tabela):
                df.write \
                .format("delta") \
                .partitionBy("ano") \
                .mode("append") \
                .saveAsTable(v_tabela)

            else:

                df.createOrReplaceTempView("df_novos")
                spark.sql(f"""
                            MERGE INTO {v_tabela} as d
                            USING df_novos as o
                            ON d.id = o.id
                            WHEN MATCHED THEN UPDATE SET *
                            WHEN NOT MATCHED THEN INSERT *
                            """)

            # contabilizando total de registros carregados em silver
            total_tabela = spark.read.table(f"{caminho_gold}" + "." + f"{table}").count()
            e=""
            atualiza_carga(pipeline, camada, table,
                            total_processado, total_tabela,
                            data_inicio, "sucesso", str(e) )

                                                        
        else:
            raise Exception("Pipeline gerou dataset vazio")
                
    except Exception as e:

        atualiza_carga( pipeline, camada, table,
                        0, 0, data_inicio, "erro", 
                        str(e))
        raise


#================================================
# padrão de colunas
#================================================
c_padrao = ["id","mes","ano","mes_ano","regiao","munic","registro_ocorrencias"]
c_data = ["ingestion_Data"]

#abertura da tabela
df_gold = abre_silver()

#================================================
# definição dos dataframes
#================================================

#df_roubo= cria_dfs(c_padrao, c_data,df_gold, coluna="roubo")
#df_furto= cria_dfs(c_padrao, c_data,df_gold, coluna="furto")
#df_drogas= cria_dfs(c_padrao, c_data,df_gold, coluna="drogas")

#================================================
# atualizando dados finais
#================================================

tipos = ["roubo","furto","drogas"]
for crime in tipos:
    df_temp = cria_dfs(c_padrao, c_data, df_gold, crime)
    try:
        salvamento(df_temp, table=crime)
    except Exception as e:
        print(f"Erro no pipeline {table}: {e}")    