from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
import logging

log = logging.getLogger(__name__)


def carregar_dim_cliente():
    """
    ETL da Dimensão Cliente:
    - Extrai de DimCustomer + DimGeography (SQL Server)
    - Trunca dim_cliente (e, por FK, fato_vendas) no Postgres
    - Carrega os dados em public.dim_cliente
    """
    log.info("Iniciando carga da Dimensão Cliente...")

    mssql_hook = MsSqlHook(mssql_conn_id="sql_server_source")

    sql_clientes = """
    SELECT
        c.CustomerKey              AS id_cliente,
        c.FirstName                AS primeiro_nome,
        c.LastName                 AS ultimo_nome,
        c.Gender                   AS genero,
        c.EmailAddress             AS email,
        g.City                     AS cidade,
        g.EnglishCountryRegionName AS pais
    FROM dbo.DimCustomer AS c
    LEFT JOIN dbo.DimGeography AS g
      ON c.GeographyKey = g.GeographyKey;
    """

    try:
        df = mssql_hook.get_pandas_df(sql_clientes)
        log.info(f"DimCliente - registros extraídos: {len(df)}")
    except Exception as e:
        log.error(f"Erro na extração da Dimensão Cliente: {e}")
        raise

    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = pg_hook.get_sqlalchemy_engine()

    try:
        with engine.begin() as conn:
            # Trunca a dimensão cliente e todas as tabelas que dependem dela (fato_vendas)
            conn.execute(
                text(
                    "TRUNCATE TABLE public.dim_cliente RESTART IDENTITY CASCADE;"
                )
            )

        df.to_sql(
            "dim_cliente",
            engine,
            schema="public",
            if_exists="append",
            index=False,
        )

        log.info("Carga da Dimensão Cliente concluída com sucesso.")
    except Exception as e:
        log.error(f"Erro na carga da Dimensão Cliente: {e}")
        raise


def carregar_fato_vendas():
    """
    ETL da Fato Vendas:
    - Extrai de FactInternetSales + DimDate (SQL Server)
    - Trunca fato_vendas no Postgres
    - Carrega os dados em public.fato_vendas
    """
    log.info("Iniciando carga da Fato Vendas...")

    mssql_hook = MsSqlHook(mssql_conn_id="sql_server_source")

    sql_fato = """
    SELECT
        CAST(f.SalesOrderNumber + '-' + CAST(f.SalesOrderLineNumber AS VARCHAR(10)) AS VARCHAR(50)) AS id_venda,
        f.ProductKey              AS id_produto,
        f.CustomerKey             AS id_cliente,
        d.FullDateAlternateKey    AS data_venda,
        f.OrderQuantity           AS quantidade,
        f.UnitPrice               AS valor_unitario,
        f.SalesAmount             AS valor_total
    FROM dbo.FactInternetSales AS f
    JOIN dbo.DimDate AS d
      ON f.OrderDateKey = d.DateKey;
    """

    try:
        df = mssql_hook.get_pandas_df(sql_fato)
        log.info(f"FatoVendas - registros extraídos: {len(df)}")
    except Exception as e:
        log.error(f"Erro na extração da Fato Vendas: {e}")
        raise

    pg_hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = pg_hook.get_sqlalchemy_engine()

    try:
        with engine.begin() as conn:
            # Aqui só precisamos truncar a fato (ela referencia as dimensões, não o contrário)
            conn.execute(text("TRUNCATE TABLE public.fato_vendas;"))

        df.to_sql(
            "fato_vendas",
            engine,
            schema="public",
            if_exists="append",
            index=False,
        )

        log.info("Carga da Fato Vendas concluída com sucesso.")
    except Exception as e:
        log.error(f"Erro na carga da Fato Vendas: {e}")
        raise


# Definição da DAG
with DAG(
    "etl_clientes_fato_vendas",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # execução manual
    catchup=False,
    tags=["etl", "dw", "cliente", "fato_vendas"],
) as dag:

    tarefa_dim_cliente = PythonOperator(
        task_id="carregar_dim_cliente",
        python_callable=carregar_dim_cliente,
    )

    tarefa_fato_vendas = PythonOperator(
        task_id="carregar_fato_vendas",
        python_callable=carregar_fato_vendas,
    )

    # Ordem: primeiro clientes, depois fato (por causa das FKs)
    tarefa_dim_cliente >> tarefa_fato_vendas
