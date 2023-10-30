import json
import glob
import os

PATHS_DAS_VARIAVEIS = ["./dags/*/variables.json", "./dags/*/fixed_variables.json"]
PATH_FINAL_DAS_VARIAVEIS = "./dags/configs/variables.json"

PATH_DAS_CONEXOES = ["./dags/*/connections.json", "./dags/*/fixed_connections.json"]
PATH_FINAL_DAS_CONEXOES = "./dags/configs/connections.json"

def deletar_arquivo(path):
    if os.path.exists(path):
        os.remove(path)
        print(f"File '{path}' deleted successfully.")
    else:
        print(f"File '{path}' does not exist.")

def criar_o_path_caso_nao_exista():
    os.makedirs(os.path.dirname("./dags/configs/"), exist_ok=True)

def juntar_variaveis_em_um_unico_json():
    variaveis = {}
    for path in PATHS_DAS_VARIAVEIS:
        for path_arquivo in glob.glob(path):
            with open(path_arquivo) as arquivo:
                variaveis.update(json.load(arquivo))
    with open(PATH_FINAL_DAS_VARIAVEIS, "w") as arquivo:
        json.dump(variaveis, arquivo, indent=4)

def juntar_conexoes_em_um_unico_json():
    conexoes = {}
    for path in PATH_DAS_CONEXOES:
        for path_arquivo in glob.glob(path):
            with open(path_arquivo) as arquivo:
                conexoes.update(json.load(arquivo))
    with open(PATH_FINAL_DAS_CONEXOES, "w") as arquivo:
        json.dump(conexoes, arquivo, indent=4)

if __name__ == "__main__":
    criar_o_path_caso_nao_exista()
    deletar_arquivo(PATH_FINAL_DAS_VARIAVEIS)
    deletar_arquivo(PATH_FINAL_DAS_CONEXOES)
    juntar_variaveis_em_um_unico_json()
    juntar_conexoes_em_um_unico_json()
