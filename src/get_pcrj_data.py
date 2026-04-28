import os

from google.cloud import bigquery
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


project = os.getenv("PROJECT_NAME")
client = bigquery.Client(project=project)
tables = [
    "bairro", 
    "area_planejamento",
    "regiao_administrativa",
    "subprefeitura"
]

# Perform queries and save into CSVs.
for table in tables:
    QUERY = (
        f'SELECT * FROM `datario.dados_mestres.{table}`')

    query_job = client.query(QUERY)
    df = query_job.to_dataframe()
    df.to_csv(f"data/{table}.csv", index=False)


QUERY = """
    SELECT * 
    FROM `datario.adm_central_atendimento_1746.chamado` 
    WHERE data_particao >= '2023-01-01' AND data_particao <= '2024-12-31'
"""

query_job = client.query(QUERY)
df = query_job.to_dataframe()
df.to_csv(f"data/chamados_1746.csv", index=False)
