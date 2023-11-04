from airflow.models import Variable, Connection
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

import pandas as pd
from itertools import chain
import io


def create_database_on_postgresql(database_name, postgres_conn_id):

    # Connect to the database
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()

    print(f"\nCreating database {database_name} in PostgreSQL")

    cursor = conn.cursor()

    # Cria o schema
    # -------------
    try:
        # https://stackoverflow.com/questions/74899785/psycopg2-errors-activesqltransaction-create-database-cannot-run-inside-a-transa
        conn.autocommit = True
        query = f"""
            CREATE DATABASE {database_name};
        """

        cursor.execute(query)
        cursor.close()
        conn.close()

        print(f"\nDatabase {database_name} created in PostgreSQL")

    except Exception as e:

        print(e)
        print(f"\nDatabase {database_name} already exists in PostgreSQL")

def create_schema_on_postgresql(schema_name, postgres_conn_id):

    # Connect to the database
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()

    print(f"\nCreating schema {schema_name} in PostgreSQL")

    cursor = conn.cursor()

    # Cria o schema
    # -------------
    cursor.execute(
        f"""
            CREATE SCHEMA IF NOT EXISTS {schema_name};
        """
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\nSchema {schema_name} created in PostgreSQL")

def create_user_on_postgresql(user, password, postgres_conn_id, database):

    # Connect to the database
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()

    print(f"\nCreating user {user} in PostgreSQL")

    cursor = conn.cursor()

    # Cria o schema
    # -------------
    try:
        cursor.execute(
            f"""
                CREATE USER {user} WITH PASSWORD '{password}';
            """
        )

        cursor.execute(
            f"""
                GRANT ALL PRIVILEGES ON DATABASE {database} TO {user};
            """
        )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"\nUser {user} created in PostgreSQL")

    except Exception as e:
        print(e)


def create_table_from_dataframe_postgresql(
        df,
        schema_name,
        table_name,
        postgres_conn_id,
        replace_table=False
):
    """
    Cria uma tabela no PostgreSQL a partir de um Dataframe

    Args:
        df (
        pandas.core.frame.DataFrame
        ): Dataframe a ser carregado no PostgreSQL
        schema_name (str): Nome do schema
        table_name (str): Nome da tabela
        postgres_conn_id (str): Id da conexão do banco no AIRFLOW
    """

    # Connect to the database
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()

    print(f"\nCreating table {schema_name}.{table_name} in PostgreSQL")

    # Selecionando as colunas e seus tipos
    # ------------------------------------
    columns, types = zip(
        *[(col, df[col].dtype) for col in df.columns]
    )

    # Traduz os tipos do pandas para os tipos do postgresql
    types_mapping = {
        "int64": "INT",
        "float64": "FLOAT",
        "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
    }
    types = [types_mapping[str(t)] for t in types]

    cursor = conn.cursor()

    # Cria o schema
    # -------------
    cursor.execute(
        f"""
            CREATE SCHEMA IF NOT EXISTS {schema_name}
        """
    )

    if replace_table:
        # Apaga a tabela
        # -------------
        cursor.execute(
            f"""
                DROP TABLE IF EXISTS {schema_name}.{table_name}
            """
        )

    # Cria a tabela
    # -------------
    cursor.execute(
        f"""
            CREATE TABLE IF NOT EXISTS 
            {schema_name}.{table_name} 
            ({', '.join(
                [
                    f'"{str(col)}" {col_type}' 
                    for col_type, col in zip(types, columns)
                ]
            )})
        """
    )

    conn.commit()
    conn.close()

def populate_table_from_dataframe_postgresql(
    df,
    schema_name,
    table_name,
    postgres_conn_id,
    drop_previous_records=False
):
    """Popula uma tabela no PostgreSQL a partir de um Dataframe

    Args:
        df (pd): Dataframe a ser carregado no PostgreSQL
        schema_name (str): Nome do schema
        table_name (str): Nome da tabela
        postgres_conn_id (str): Id da conexão do banco no AIRFLOW
        drop_previous_records (bool, optional): Se True, apaga todos os registros da tabela antes de incluir os novos. Defaults to False.
    """    
    # Connect to the database
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()
    cursor = conn.cursor()

    if drop_previous_records:
        print(f"\nDropping previous records from {schema_name}.{table_name} in PostgreSQL")
        cursor.execute(
            f"""
                DELETE FROM {schema_name}.{table_name}
            """
        )
        conn.commit()

    print(f"\nInserting data into {schema_name}.{table_name} in PostgreSQL")
    conn.commit()
    # commit the changes to the database
    # Insert data into table using copy expert
    buffer = io.StringIO()
    
    df.to_csv(buffer, sep=';', header=False, index=False, encoding='utf-8')

    buffer.seek(0)
    cursor.copy_expert(
        f"""
            COPY {schema_name}.{table_name}
            FROM STDIN
            WITH
                (
                    FORMAT CSV,
                    DELIMITER E';',
                    HEADER FALSE
                )
        """,
        buffer
    )
    
    conn.commit()
    conn.close()


def load_dataframe_and_transform_to_star_schema_postgresql(
        dataframe_path,
        colunas_fato = ["col1", "col2"],
        tabela_fato_nome = "tabela_fato",
        mapping_colunas_dimensao = {'dim1':["col3", "col4"]},
        postgres_conn_id='DMBIAUDI',
        table_schema="public"
):
    """Realiza a carga de um dataframe para o postgresql e transforma em um modelo estrela no PostgreSQL

    Args:
        dataframe_path (str): Path do dataframe a ser carregado. Deve ser um arquivo csv.
        colunas_fato (list, optional): Lista de colunas que serão incluídas na tabela fato. Defaults to ["col1", "col2"].
        tabela_fato_nome (str, optional): Nome da tabela fato. Defaults to "tabela_fato".
        mapping_colunas_dimensao (dict, optional): Dicionário especificando como serão construídas as tabelas dimensão -> Nome: [colunas]. Defaults to {'dim1':["col3", "col4"]}.
        postgres_conn_id (str, optional): Id da conexão do banco no AIRFLOW. Defaults to 'DMBIAUDI'.
        table_schema (str, optional): Schema da tabela. Defaults to "public".
    """
    
    # Load dataframe
    # ----------------
    dataframe = pd.read_csv(dataframe_path)
    print( dataframe.info() )

    # Seleciona apenas as colunas que serão utilizadas
    # ------------------------------------------------
    colunas_fato_e_dimensoes = colunas_fato + list(chain.from_iterable(mapping_colunas_dimensao.values()))

    # Check if all columns are in the dataframe
    # -----------------------------------------
    for col in colunas_fato_e_dimensoes:
        if col not in dataframe.columns:
            raise ValueError(f"Column {col} not in dataframe")

    dataframe = dataframe[colunas_fato_e_dimensoes]

    # Cria um dataframe para cada dimensão
    # ------------------------------------
    dimensao = {}
    for dim, cols in mapping_colunas_dimensao.items():
        dim_dataframe = dataframe[cols].drop_duplicates()
        sk_name = f"sk_{dim.replace('DIM_', '')}"
        dim_dataframe[sk_name] = dim_dataframe.index

        dimensao[dim] = dim_dataframe

    # Substitui as colunas de dimensão pelo respectivo SK na tabela fato
    # ------------------------------------------------------------------
    for dim, dim_dataframe in dimensao.items():
        # join the dimension dataframe to the original dataframe
        dataframe = dataframe.merge(
            dim_dataframe, 
            on=mapping_colunas_dimensao[dim], 
            how="left"
        )

        # drop the original columns
        dataframe = dataframe.drop(columns=mapping_colunas_dimensao[dim])
        print( dim_dataframe.head() )

    print(dataframe.head())
    print(dataframe.columns)

    
    # Create the dimentions and fact table
    for dim, dim_dataframe in dimensao.items():
        # lowercase the column names
        dim_dataframe.columns = [col.lower() for col in dim_dataframe.columns]

        create_table_from_dataframe_postgresql(
            dim_dataframe,
            table_schema,
            dim.upper(),
            postgres_conn_id,
            replace_table=True
        )

        populate_table_from_dataframe_postgresql(
            dim_dataframe,
            table_schema,
            dim.upper(),
            postgres_conn_id,
            drop_previous_records=True
        )

    # lowercase the column names
    dataframe.columns = [col.lower() for col in dataframe.columns]
    create_table_from_dataframe_postgresql(
        dataframe,
        table_schema,
        tabela_fato_nome,
        postgres_conn_id,
        replace_table=True
    )

    populate_table_from_dataframe_postgresql(
        dataframe,
        table_schema,
        tabela_fato_nome,
        postgres_conn_id,
        drop_previous_records=True
    )


def write_hora_etl_to_postgresql(
    table_schema,
    table_name,
    postgres_conn_id,
):
    # UTC-3 (recife)
    horario = datetime.now() - timedelta(hours=3) 
    df_horario = pd.DataFrame( {"hora_etl":[horario]} )
    
    df_horario.to_sql(
        table_name,
        con=PostgresHook(postgres_conn_id=postgres_conn_id).get_sqlalchemy_engine(),
        schema=table_schema,
        if_exists="replace",
        index=False,
    )
