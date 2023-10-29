import os
import time
import typer
import shutil
import docker
import socket
import subprocess
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

print("OS_TYPE:", OS_TYPE)
print("HOSTNAME:", HOSTNAME)
print("MACHINE_IP:", MACHINE_IP)

app = typer.Typer(help="Control test flow")

@app.command(help="Start monitoring services.")
def start_stop_monitoring(
    condition: Annotated[str, "Can be 'start', 'stop' or 'restart'."] = "start",
    abs_path: Annotated[str, "Absolute path to monitoring folder."] = ABS_PATH
    ):
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
    new_path = "D:/"+ ABS_PATH + f"Load_base_DB/" if OS_TYPE == "nt" else home_prefix + ABS_PATH + f"Load_base_DB/"
    # Move directory
    print("Old path:", old_path)
    print("New path:", new_path)
    try:
        shutil.rmtree(f'{new_path}/{output}')
        shutil.move(old_path, new_path )
    except:
        print(f"Error while moving directory {output}. Old -> New Path")

    compose_path = "D:/"+ ABS_PATH + f"Load_base_DB/" + "compose.yaml" if OS_TYPE == "nt" else home_prefix + ABS_PATH + f"Load_base_DB/" + "compose.yaml"
    # Run python load_db.py container
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
    
if __name__ == "__main__":
    app()