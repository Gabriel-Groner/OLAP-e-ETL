from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging
from sqlalchemy import text

log = logging.getLogger(__name__)

def extrair_e_carregar():
    log.info("Iniciando a extração da Dimensão Produto (AdventureWorksDW2022)...")

    # 1) EXTRAÇÃO SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_source')

    sql = """
    SELECT
        p.ProductKey AS id_produto,
        p.EnglishProductName AS nome,
        p.Color AS cor,
        psc.EnglishProductSubcategoryName AS categoria
    FROM dbo.DimProduct AS p
    LEFT JOIN dbo.DimProductSubcategory AS psc
        ON p.ProductSubcategoryKey = psc.ProductSubcategoryKey
    WHERE
        p.FinishedGoodsFlag = 1;
    """

    try:
        df = mssql_hook.get_pandas_df(sql)
        log.info(f"Extração realizada com sucesso: {len(df)} registros.")
    except Exception as e:
        log.error(f"Erro durante extração SQL Server: {e}")
        raise

    # 2) CARGA no Postgres
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()

    try:
        # Limpa fato e dimensão antes de recarregar (opcional, mas seguro pra estudo)
        with engine.begin() as conn:
            # Se fato_vendas ainda estiver vazia, isso não causa problema
            #conn.execute(text("TRUNCATE TABLE public.fato_vendas RESTART IDENTITY;"))
            #conn.execute(text("TRUNCATE TABLE public.dim_produto RESTART IDENTITY;"))
            conn.execute(text("""
            TRUNCATE TABLE public.fato_vendas, public.dim_produto
            RESTART IDENTITY CASCADE;"""))


        # Reinsere os dados sem tentar dropar a tabela
        df.to_sql(
            'dim_produto',
            engine,
            schema='public',
            if_exists='append',
            index=False
        )
        log.info("Carga finalizada com sucesso na tabela public.dim_produto.")
    except Exception as e:
        log.error(f"Erro na carga do PostgreSQL: {e}")
        raise


with DAG(
    'etl_dimensao_produto',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'dw', 'produto']
) as dag:

    tarefa_mover_dados = PythonOperator(
        task_id='mover_dados',
        python_callable=extrair_e_carregar
    )
