# Databricks notebook source
import os
from datetime import datetime
from pyspark.sql.functions import col, current_date, when, regexp_replace,split, \
                                  right,substring, to_date,concat_ws, row_number, \
                                  current_timestamp, date_format, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import pandas as pd

caminho_raw ="/Volumes/workspace/default/imported-files/BaseDPEvolucaoMensalCisp.csv"
table_bronze = "workspace.bronze.cisp_raw"
table_silver = "workspace.silver.cisp"
caminho_gold="workspace.gold"
caminho_carga = "workspace.controle.carga"

def atualiza_carga(pipeline,camada, tabela, total_processado,
        total_tabela, data_inicio, status, mensagem=None,
        caminho_carga=caminho_carga):

    data_fim = datetime.now()
    tempo_execucao = (data_fim - data_inicio).total_seconds()

    df_log = spark.createDataFrame([(pipeline, camada, tabela, total_processado,
                                    total_tabela, data_inicio, data_fim, tempo_execucao,
                                    status, mensagem)],
                                    ["pipeline", "camada", "tabela", "total_processado",
                                    "total_tabela", "data_inicio", "data_fim", "tempo_execucao",
                                    "status", "mensagem"])

    if not spark.catalog.tableExists(f"{caminho_carga}"):

        df_log.write.format("delta")\
                    .mode("overwrite")\
                    .saveAsTable(f"{caminho_carga}")
    else:

        df_log.write.format("delta")\
                    .mode("append")\
                    .saveAsTable(f"{caminho_carga}")
