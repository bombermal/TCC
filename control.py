import os
import time
import json
import typer
import shutil
import docker
import socket
import requests
import subprocess
import pandas as pd
from string import Template
from psycopg2 import connect
from base64 import b64encode
from datetime import datetime
from sqlalchemy import create_engine
from typing_extensions import Annotated

# get the machine name
OS_TYPE = os.name
if OS_TYPE == 'nt':  # for Windows
    HOSTNAME = socket.gethostname()
    MACHINE_IP = socket.gethostbyname(HOSTNAME)
else:  # for Unix-based systems
    HOSTNAME = os.uname()[1]
    MACHINE_IP = os.popen('hostname -I').read()
    
ABS_PATH = "Ivan/OneDrive/Projetos/Códigos ( Profissional )/Material criado/TCC/"
if (HOSTNAME == "residenciabi-04") | (HOSTNAME == "storage"):
    ABS_PATH = "opt/bi/TCC/"

print("OS_TYPE:", OS_TYPE)
print("HOSTNAME:", HOSTNAME)
print("MACHINE_IP:", MACHINE_IP)

default_conn_params_source = {
    "drivername": "postgresql",
    "username": "postgres",
    "password": "postgres",
    "host": "192.168.1.30", #"192.168.1.30"
    "port": "5432",
    "database": "tpc",
    "schema": "tpc"
}
default_conn_params_target = {
    "drivername": "postgresql",
    "username": "postgres",
    "password": "postgres",
    "host": "192.168.1.31", #"192.168.1.31"
    "port": "5432",
    "database": "tpc",
    "schema": "tpc"
}
GET_TABLES_NAMES_QUERY = """SELECT
	    relname AS table_name
	FROM 
	    pg_stat_user_tables
	WHERE 
	    schemaname = 'tpc'"""

app = typer.Typer(help="Control test flow")

@app.command(help="Start monitoring services.")
def start_stop_monitoring(
    condition: Annotated[str, "Can be 'start', 'stop' or 'restart'."] = "start",
    abs_path: Annotated[str, "Absolute path to monitoring folder."] = ABS_PATH
    ):
    """
    Starts, stops or restarts monitoring services using Docker Compose.

    Args:
        condition (str): Can be 'start', 'stop' or 'restart'. Default is 'start'.
        abs_path (str): Absolute path to project folder. Default is ABS_PATH.

    Returns:
        None
    """
    # Up monitoring services
    monitoring_path = abs_path + "Monitoring_VM/Prometheus-Grafana/"
    # Home or Work
    home_prefix = "/mnt/d/" if (HOSTNAME != "residenciabi-04") else "/"
    
    print("monit:", monitoring_path)
    print("prefix:", home_prefix)
    
    file_path = "D:/"+ monitoring_path if OS_TYPE == "nt" else home_prefix + monitoring_path
    env_path = file_path + ".env"
    compose_path = file_path + "compose.yaml"  
    
    if condition == "start":
        print("env path:", env_path)  
        if HOSTNAME == "residenciabi-04":
            env_string = "ENV=tre"
            # Overwrite the file
            with open(env_path, 'w') as file:
                file.write(env_string)
        else:
            env_string = "ENV=home"
            # Overwrite the file
            with open(env_path, 'w') as file:
                file.write(env_string)
        
        if OS_TYPE == "nt":
            # Run docker compose up command on windows    
            command = ["docker-compose", "-f", compose_path, "up", "-d"]
            print("Compose path:", compose_path)  
            try:
                subprocess.run(command, check=True, shell=True)
            except subprocess.CalledProcessError as e:
                print("An error occurred:", e)
        else:
            # Run docker compose up command on linux
            command = ["docker-compose", "-f", compose_path, "up", "-d"]    
            print("Compose path:", compose_path)  
            try:
                subprocess.run(command, check=True, shell=False)
            except subprocess.CalledProcessError as e:
                print("An error occurred:", e)
    elif condition == "stop":
        if OS_TYPE == "nt":
            # Run docker compose up command on windows    
            command = ["docker-compose", "-f", compose_path, "down"]
            print("Compose path:", compose_path)  
            try:
                subprocess.run(command, check=True, shell=True)
            except subprocess.CalledProcessError as e:
                print("An error occurred:", e)
        else:
            # Run docker compose up command on linux
            command = ["docker-compose", "-f", compose_path, "down"]
            print("Compose path:", compose_path)  
            try:
                subprocess.run(command, check=True, shell=False)
            except subprocess.CalledProcessError as e:
                print("An error occurred:", e)
    else:
        start_stop_monitoring(condition='stop')
        start_stop_monitoring(condition='start')
    
@app.command(help="Populate source DB.")    
def populate_db():
    """
    Populates the source database with synthetic data.


    Returns:
        None
    """
    home_prefix = '/'

    compose_path = "D:/"+ ABS_PATH + f"Load_base_DB/" + "compose.yaml" if OS_TYPE == "nt" else home_prefix + ABS_PATH + f"Load_base_DB/" + "compose.yaml"
    
    # Run python load_db.py container
    if OS_TYPE == "nt":
        # Run docker compose up command on windows    
        command = ["docker-compose", "-f", compose_path, "up", "-d", "--build"]
        print("Compose path:", compose_path)  
        try:
            subprocess.run(command, check=True, shell=True)
        except subprocess.CalledProcessError as e:
            print("An error occurred:", e)
    else:
        # Run docker compose up command on linux
        command = ["docker-compose", "-f", compose_path, "up", "-d", "--build"]
        print("Compose path:", compose_path)  
        try:
            subprocess.run(command, check=True, shell=False)
        except subprocess.CalledProcessError as e:
            print("An error occurred:", e)
            
    # Wait 3 seconds
    time.sleep(3)
    # Wait for docker container stop running
    # create a Docker client
    client = docker.from_env()
    # check if a container is running
    container_name = 'populate_source'
    container = client.containers.get(container_name)
    # wait for the container to stop running
    count = 1
    while container.status == 'running':
        time.sleep(1)
        # Clear terminal
        print("\033c")
        print(f'{container_name.capitalize()} is {container.status}{count*".":<3}')
        count = (count + 1)% 10
        container.reload()
    
    print(f"{container_name.capitalize()} finished.")

@app.command(help="Check Airbyte API status")
def check_airbyte(
    host: Annotated[str, "Airbyte host."] = "192.168.1.33",
    port: Annotated[int, "Airbyte port."] = 8006,
    user: Annotated[str, "Airbyte user."] = "admin",
    password: Annotated[str, "Airbyte password."] = "12345"
    ):
    """
    Checks the health of an Airbyte instance using the provided credentials.

    Args:
        host (str): Airbyte host.
        port (int): Airbyte port.
        user (str): Airbyte user.
        password (str): Airbyte password.

    Returns:
        None
    """
    url = f'http://{host}:{port}/health'
    
    # Request
    response = requests.get(url, auth=(user, password))
    print(f'Airbyte check:\n{response.text}\nCode:{response.status_code}')
 
@app.command(help="Start Airbyte sync.")
def sync_airbyte(
    host: Annotated[str, "Host address"] = "192.168.1.33",
    port: Annotated[int, "Host Port" ] = 8006,
    user: Annotated[str, "User"] = "admin",
    password: Annotated[str, "Password"] = "12345"
    ):
    """
    This function starts an Airbyte sync. It resets and syncs data from an Airbyte connection.

    Args:
        host (str): Airbyte host.
        port (int): Airbyte port.
        user (str): Airbyte user.
        password (str): Airbyte password.
        return_dict (bool): If True, returns a dictionary with throughput and other info.

    Returns:
        If return_dict is True, returns a dictionary with the following keys:
            - jobId
            - jobType
            - startTime
            - bytesSynced
            - rowsSynced
            - TimeDelta
            - Throughput
    """
    root_url = f'http://{host}:{port}/'
    
    # Get ID
    url = root_url +'v1/connections'
    # Request
    response = requests.get(url, auth=(user, password))
    print(f'Response code:{response.status_code}\n')

    conn_id = response.json()['data'][0]['connectionId']
    print(f"Airbyte connection ID: {conn_id}")
    
    url = root_url + 'v1/jobs'

    load = 'reset'
    payload = {
        "jobType": load,
        "connectionId": conn_id
        }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
        }
    
    # Reset job
    print("Resetting Airbyte Job...")
    response = requests.post(url, json=payload, headers=headers, auth=(user, password))
    # Wait for reset finish
    condition = True
    while condition:
        time.sleep(5)
        # Request
        response = requests.get(url, auth=(user, password))
        jobs_list = response.json()['data']
        condition = jobs_list[-1]['status'] != 'succeeded'
        
    # Sync job
    payload['jobType'] = 'sync'
    print("Sync Airbyte Job...")
    response = requests.post(url, json=payload, headers=headers, auth=(user, password))
    
    condition = True
    while condition:
        time.sleep(5)
        # Request
        response = requests.get(url, auth=(user, password))
        jobs_list = response.json()['data']
        condition = jobs_list[-1]['status'] != 'succeeded'
        
    print("Airbyte sync finished.")
      
    # Calculate througput
    jobs_df = pd.DataFrame(jobs_list)
    jobs_df.startTime = pd.to_datetime(jobs_df.startTime, format='ISO8601')
    jobs_df.lastUpdatedAt = pd.to_datetime(jobs_df.lastUpdatedAt, format='ISO8601')
    jobs_df['TimeDelta'] = jobs_df.lastUpdatedAt - jobs_df.startTime
    jobs_df.TimeDelta = jobs_df.TimeDelta.dt.seconds
    jobs_df['Throughput'] = round(jobs_df.rowsSynced / jobs_df.TimeDelta, 2)

    idx = jobs_df.index[-1]
    columns = ["jobId", "jobType", "startTime", "bytesSynced", "rowsSynced", "TimeDelta", "Throughput"]
    
    return jobs_df.loc[idx,columns].to_dict()

@app.command(help="Start Airflow sync.")
def sync_airflow(
    host: Annotated[str, "Host address"] = "192.168.1.32",
    port: Annotated[int, "Host Port" ] = 12464,
    user: Annotated[str, "User"] = "airflow",
    password: Annotated[str, "Password"] = "airflow",
    dag_id: Annotated[str, "DAG ID."] = "dag_tpc"
    ):
    """
    This function starts an Airflow sync by sending a POST request to the Airflow API.
    It waits for the sync to finish and returns a dictionary with the benchmark_id, startTime, endTime and TimeDelta.
    
    Args:
        host (str): Host address. Default is "192.168.1.32".
        port (int): Host Port. Default is 12464.
        user (str): User. Default is "airflow".
        password (str): Password. Default is "airflow".
        dag_id (str): DAG ID. Default is "dag_tpc".
    
    Returns:
        dict: A dictionary with the benchmark_id, startTime, endTime and TimeDelta.
    """
    
    root_url = f"http://{host}:{port}/api/v1/dags/{dag_id}/dagRuns"
    dag_run_id = f'manual_{datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")}'
    
    payload = {
            "dag_run_id": dag_run_id
            }
    
    print("Starting Airflow sync...")
    response = requests.post(root_url, auth=(user, password), json=payload)
    # verificando a resposta
    if response.status_code == 200:
        print("Post enviado.")
    else:
        print(f"Erro ao criar tarefa: {response.content}")
        
    condition = True
    while condition:
        time.sleep(5)
        response = requests.get(root_url+"?order_by=-execution_date&limit=2", auth=(user, password))
        response_status = response.json()["dag_runs"][0]['state']
        if response_status == 'success':
            condition = False
        elif response_status == 'failed':
            break

    print("Airflow sync finished.")
    response_tasks = response.json()["dag_runs"]
    for ii in response_tasks:
        if ii["dag_run_id"] ==  dag_run_id:
            start = ii["start_date"]
            end = ii["end_date"]
            timedelta = datetime.fromisoformat(end) - datetime.fromisoformat(start)
            break
    
    resp_dict = {"benchmark_id": dag_run_id, "startTime": start, "endTime": end,
                 "TimeDelta": round(timedelta.total_seconds(), 2)}
    
    return resp_dict

@app.command(help="Create and store syntehtic data.")
def create_data(    
                sf: Annotated[int, "Scale fator for ammount of data created."] = 3,
                output: Annotated[str, "Output folder name."] = "Synthetic_data",
                move: Annotated[bool, "If True, move the data created in the directory."] = True
                ):
    
    data_create_path = ABS_PATH + "Data_create/"
    
    # Home or Work
    home_prefix = "/"
    
    data_create_path = "D:/"+ data_create_path if OS_TYPE == "nt" else home_prefix + data_create_path
    
    compose_path = data_create_path + "compose.yaml"
    env_path = data_create_path + ".env"
    
    # Prepare .env file
    env_string = f'QTY={sf}\nOUT="{output}"'
    # Overwrite the file
    with open(env_path, 'w') as file:
        file.write(env_string)
    
    # Run docker compose up command
    if OS_TYPE == "nt":
        # Run docker compose up command on windows    
        command = ["docker-compose", "-f", compose_path, "up", "-d", "--build"]
        print("Compose path:", compose_path)  
        try:
            subprocess.run(command, check=True, shell=True)
        except subprocess.CalledProcessError as e:
            print("An error occurred:", e)
    else:
        # Run docker compose up command on linux
        command = ["docker-compose", "-f", compose_path, "up", "-d", "--build"]
        print("Compose path:", compose_path)  
        try:
            subprocess.run(command, check=True, shell=False)
        except subprocess.CalledProcessError as e:
            print("An error occurred:", e)
            
    # Wait 3 seconds
    time.sleep(3)
    # Wait for docker container stop running
    # create a Docker client
    client = docker.from_env()
    # check if a container is running
    container_name = 'data_create'
    container = client.containers.get(container_name)
    # wait for the container to stop running
    count = 1
    while container.status == 'running':
        time.sleep(1)
        # Clear terminal
        print("\033c")
        print(f'{container_name.capitalize()} is {container.status}{count*".":<3}')
        count = (count + 1)% 10
        container.reload()
    
    print(f"{container_name.capitalize()} finished.")  
    if move:
        # Move created data to the correct folder
        old_path = data_create_path + f'Tools/{output}/'
        new_path = "D:/"+ ABS_PATH + f"Load_base_DB/" if OS_TYPE == "nt" else home_prefix + ABS_PATH + f"Load_base_DB"
        # Move directory
        print("Old path:", old_path)
        print("New path:", new_path)
        try:
            try:
                shutil.rmtree(f'{new_path}/{output}')
            except Exception as e:
                print(e)
                pass
            shutil.move(old_path, new_path )
        except Exception as e:
            print(f"Error while moving directory {output}. Old -> New Path\n{e}")

@app.command(help="Create static data in a choosen directory")
def static_data(
    test_range: Annotated[int, "Number of tests to run."] = 1,
    test_start: Annotated[int, "Indicate the starting scale for the test"] = 3
    ):
    sf_list = [test_start]
    for _ in range(test_range):
        sf_list.append(round(sf_list[-1] * 1.2))
    sf_list.pop(-1)
    print(f"\nBenchmark range: {sf_list}\n")
    
    for sf in sf_list:
        create_data(sf=sf, output=f"sf{sf}", move=False)
        print(f"Create data for sf = {sf}")

@app.command(help="Create connection object.")
def create_connection(
                    params: Annotated[str, "Connection parameters."] = default_conn_params_target
                    ):
    """
    Create a connection object to a PostgreSQL database.

    Args:
        params (str): Connection parameters.

    Returns:
        tuple: A tuple containing the connection object and cursor object.
    """
    conn = connect(database=params['database'], user=params['username'],
                        password=params['password'], host=params['host'], port=params['port'])
    cur = conn.cursor()
    cur.execute(f'SET search_path TO {params["schema"]}')
    return conn, cur

@app.command(help="Truncate table.")
def truncate_table(
        cur:  Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        title: Annotated[str, "Title of the table."],
        ):
    """
    Truncates a table in the database.

    Args:
        cur (Cursor): Cursor object.
        conn (Connection): Connection object.
        title (str): Title of the table.
    """
    cur.execute(f"TRUNCATE TABLE {title.lower()}")
    conn.commit()
        
@app.command(help="Start benchmark.")
def benchmark(
    test_range: Annotated[int, "Number of tests to run."] = 1,
    test_start: Annotated[int, "Indicate the starting scale for the test"] = 3,
    output: Annotated[str, "Output folder name."] = "Result_json",
    framework: Annotated[str, "Framework to use. Can be 'airbyte' or 'airflow'."] = "airbyte",
    use_static: Annotated[bool, "If True use static data or create for each run"] = True
    ):
    """
    Runs a benchmark test for a given range of scale factors, populating a database, syncing data using either Airbyte or Airflow, and saving the results in JSON files.

    Args:
        test_range (int): Number of tests to run.
        output (str): Output folder name.
        framework (str): Framework to use. Can be 'airbyte' or 'airflow'.

    Returns:
        None
    """
    
    sf_list = [test_start]
    for _ in range(test_range):
        sf_list.append(round(sf_list[-1] * 1.2))
    sf_list.pop(-1)
    print(f"\nBenchmark range: {sf_list}\n")
    
    for tst in sf_list:
        if use_static:
            root_path = '/opt/tpc-data'
            source_host = ""#--host 10.16.45.131
            env_string = f"HOST_FLAG='{source_host}'\nINPUT_PATH='--source-path {root_path}/sf{tst}'"
            env_path = '/opt/bi/TCC/Load_base_DB/.env'
            # Overwrite the file
            with open(env_path, 'w') as file:
                file.write(env_string)
            # Createa unique ID
            id_num = b64encode(datetime.now().strftime("%Y%m%d%H%M%S").encode('utf-8')).decode('utf-8')
            # Populate DB
            s_time = time.time()
            populate_db()
            e_time = time.time()
            operation = 'populate_db'
        
            conn_string = Template("${drivername}://${username}:${password}@${host}:${port}/${database}").safe_substitute(default_conn_params_source)
            engine = create_engine(conn_string, echo=False)
            # print(conn_string)
            query = GET_TABLES_NAMES_QUERY

            df_aux = pd.read_sql(query, engine)
            df_aux.sort_values(by="table_name", inplace=True)
            tables_names = df_aux.table_name.to_list()
            df_aux["n_rows"] = 0
            df_aux["total_size_bytes"] = 0
            for tbl in tables_names:
                # Count rows query
                count_query = f"SELECT COUNT(*) FROM tpc.{tbl}"
                size_query = f"SELECT pg_relation_size('tpc.{tbl}')"
                count_resp = pd.read_sql(count_query, engine).values[0][0]
                size_resp = pd.read_sql(size_query, engine).values[0][0]
                df_aux.loc[df_aux.table_name == tbl, "n_rows"] = count_resp
                df_aux.loc[df_aux.table_name == tbl, "total_size_bytes"] = size_resp
            total_size_bytes = df_aux.total_size_bytes.to_list()
            rows_count = df_aux.n_rows.to_list()
            
            dct_keys = ['benchmark_id', 'operation', 'start_time', 'end_time', 'sf', 'tables_names', 'rows_count', 'total_size_bytes']
            
            dct_values = [id_num, operation, s_time, e_time, tst, tables_names, rows_count, total_size_bytes]
            aux = {key: value for key, value in zip(dct_keys, dct_values)}
                
            # save json file as result.json append as newline
            with open(f'{output}/{framework}_populate_source.txt', 'a') as file:
                json.dump(aux, file)
                file.write('\n')

            if framework == 'airbyte':            
                # Reset Airbyte
                result_dict = sync_airbyte()
                # Timestamp to string
                result_dict['startTime'] = result_dict['startTime'].strftime("%Y-%m-%d %H:%M:%S")
                result_dict['operation'] = 'sync_airbyte'
                result_dict["benchmark_id"] = id_num
            else:
                result_dict = sync_airflow()
                result_dict['operation'] = 'sync_airflow'
                result_dict["benchmark_id"] = id_num    
            
            with open(f'{output}/{framework}_status.txt', 'a') as file:
                json.dump(result_dict, file)
                file.write('\n')
            
            conn_string = Template("${drivername}://${username}:${password}@${host}:${port}/${database}").safe_substitute(default_conn_params_target)
            engine = create_engine(conn_string, echo=False)
            
            df_aux = pd.read_sql(query, engine)
            df_aux.sort_values(by="table_name", inplace=True)
            target_tables_names = df_aux.table_name.to_list()
            df_aux["n_rows"] = 0
            df_aux["total_size_bytes"] = 0
            for tbl in target_tables_names:
                # Count rows query
                count_query = f"SELECT COUNT(*) FROM tpc.{tbl}"
                size_query = f"SELECT pg_relation_size('tpc.{tbl}')"
                count_resp = pd.read_sql(count_query, engine).values[0][0]
                size_resp = pd.read_sql(size_query, engine).values[0][0]
                df_aux.loc[df_aux.table_name == tbl, "n_rows"] = count_resp
                df_aux.loc[df_aux.table_name == tbl, "total_size_bytes"] = size_resp
            total_size_bytes = df_aux.total_size_bytes.to_list()
            rows_count = df_aux.n_rows.to_list()
            
            dct_keys = ['benchmark_id', 'sf', 'tables_names', 'rows_count', 'total_size_bytes']
            dct_values = [id_num, tst, target_tables_names, rows_count, total_size_bytes]
            aux = {key: value for key, value in zip(dct_keys, dct_values)}
            
            # save json file as result.json append as newline
            with open(f'{output}/{framework}_populate_target.txt', 'a') as file:
                json.dump(aux, file)
                file.write('\n')
                
            # Truncate all tables
            conn, cur = create_connection(params=default_conn_params_target)
            for tbl in target_tables_names:
                truncate_table(cur=cur, conn=conn, title=tbl)
                
            conn, cur = create_connection(params=default_conn_params_source)
            for tbl in tables_names:
                truncate_table(cur=cur, conn=conn, title=tbl)

        
if __name__ == "__main__":
    app()