# Databricks notebook source
pipeline = {
    "bronze": "/Workspace/Users/rafaelnogueirasms@gmail.com/Criando_medalhao/Analise_pesada/notebooks/bonze/importacao",
    "silver": "/Workspace/Users/rafaelnogueirasms@gmail.com/Criando_medalhao/Analise_pesada/notebooks/silver/transformacao",
    "gold": "/Workspace/Users/rafaelnogueirasms@gmail.com/Criando_medalhao/Analise_pesada/notebooks/gold/dados_uteis"
}

for etapa, notebook in pipeline.items():

    print(f"Iniciando etapa {etapa}")

    try:
        resultado = dbutils.notebook.run(notebook, 0)
        print(f"{etapa} concluído: {resultado}")

    except Exception as erro:
        print(f"Falha na etapa {etapa}")
        raise erro