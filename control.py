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
if HOSTNAME == "residenciabi-04":
    ABS_PATH = "opt/bi/TCC/"

print("OS_TYPE:", OS_TYPE)
print("HOSTNAME:", HOSTNAME)
print("MACHINE_IP:", MACHINE_IP)

default_conn_params_source = {
    "drivername": "postgresql",
    "username": "postgres",
    "password": "postgres",
    "host": "192.168.1.30",
    "port": "5432",
    "database": "tpc"
}
default_conn_params_target = {
    "drivername": "postgresql",
    "username": "postgres",
    "password": "postgres",
    "host": "192.168.1.31",
    "port": "5432",
    "database": "tpc"
}

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
def populate_db(
    sf: Annotated[int, "Scale fator for ammount of data created."] = 3,
    output: Annotated[str, "Output folder name."] = "Synthetic_data"
    ):
    """
    Populates the source database with synthetic data.

    Args:
        sf (int): Scale factor for amount of data created.
        output (str): Output folder name.

    Returns:
        None
    """
    data_create_path = ABS_PATH + "Data_create/"
    # Home or Work
    home_prefix = "/mnt/d/" if (HOSTNAME != "residenciabi-04") else "/"
    
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
    host: str = "192.168.1.33",
    port: int = 8006,
    user: str = "admin",
    password: str = "12345",
    return_dict: bool = False
    ):
    """
    Force Reset and Syncs data from an Airbyte connection.

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
      
    if return_dict:
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

@app.command(help="Start benchmark.")
def benchmark(
    test_range: Annotated[int, "Number of tests to run."] = 1,
    output: Annotated[str, "Output folder name."] = "Result_json"
    ):
    
    dct_keys = ['benchmark_id', 'operation', 'start_time', 'end_time', 'sf', 'tables_names', 'rows_count', 'total_size_bytes']
    
    sf_list = [3]
    for _ in range(test_range):
        sf_list.append(round(sf_list[-1] * 1.5))
    sf_list.pop(-1)
    print(f"\nBenchmark range: {sf_list}\n")
    for id_num, tst in enumerate(sf_list):
        # Populate DB
        s_time = time.time()
        populate_db(sf=tst)
        e_time = time.time()
        operation = 'populate_db'
        
        conn_string = Template("${drivername}://${username}:${password}@${host}:${port}/${database}").safe_substitute(default_conn_params_source)
        engine = create_engine(conn_string, echo=False)
        
        query = """SELECT
            relname AS table_name,
            n_live_tup AS row_count,
            pg_total_relation_size(relid) AS total_size_bytes
        FROM 
            pg_stat_user_tables
        WHERE 
            schemaname = 'tpc'"""

        df_aux = pd.read_sql(query, engine)
        tables_names = df_aux.table_name.to_list()
        rows_count = df_aux.row_count.to_list()
        total_size_bytes = df_aux.total_size_bytes.to_list()
        
        dct_values = [id_num, operation, s_time, e_time, tst, tables_names, rows_count, total_size_bytes]
        aux = {key: value for key, value in zip(dct_keys, dct_values)}
        
        # save json file as result.json append as newline
        with open(f'{output}/populate_source.txt', 'a') as file:
            json.dump(aux, file)
            file.write('\n')
            
        # Reset Airbyte
        result_dict = sync_airbyte(return_dict=True)
        # Timestamp to string
        result_dict['startTime'] = result_dict['startTime'].strftime("%Y-%m-%d %H:%M:%S")
        result_dict['operation'] = 'sync_airbyte'
        result_dict["benchmark_id"] = id_num
        with open(f'{output}/airbyte_status.txt', 'a') as file:
            json.dump(result_dict, file)
            file.write('\n')
        
        conn_string = Template("${drivername}://${username}:${password}@${host}:${port}/${database}").safe_substitute(default_conn_params_target)
        engine = create_engine(conn_string, echo=False)
        
        df_aux = pd.read_sql(query, engine)
        tables_names = df_aux.table_name.to_list()
        rows_count = df_aux.row_count.to_list()
        total_size_bytes = df_aux.total_size_bytes.to_list()
        
        operation = 'sync_airbyte'
        dct_values = [id_num, operation, s_time, e_time, tst, tables_names, rows_count, total_size_bytes]
        aux = {key: value for key, value in zip(dct_keys, dct_values)}
        
        # save json file as result.json append as newline
        with open(f'{output}/populate_target.txt', 'a') as file:
            json.dump(aux, file)
            file.write('\n')
        
if __name__ == "__main__":
    app()