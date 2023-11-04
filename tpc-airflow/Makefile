MYDIR=/home/myaccount/dev

## @ remover imagens desnecessárias
.PHONY: removeNone
removeNone: ## remove todas as images com tag <none>
#docker rmi  $(docker images | grep none | awk '{print $3;}')
	if [ ! -d "./plugins" ]; then docker images | grep none | awk '{ print $3; }' | xargs docker rmi --force

.PHONY: autorizarPastas
autorizarPastas: ## autoriza as pastas para o usuario airflow
	if [ ! -d "./extract" ]; then mkdir ./extract; fi
	if [ ! -d "./logs" ]; then mkdir ./logs; fi
	if [ ! -d "./plugins" ]; then mkdir ./plugins; fi
	if [ ! -d "./dags" ]; then mkdir ./dags; fi
	if [ ! -d "./volumes" ]; then mkdir ./volumes; fi
	
	sudo chmod -R 777 ./extract
	sudo chmod -R 777 ./logs
	sudo chmod -R 777 ./plugins
	sudo chmod -R 777 ./volumes
	sudo chmod -R 777 ./dags
	
## @ criacao das estruturas para extract 
.PHONY: esqueleto
esqueleto: ## cria todas as pastas necessárias para extração dos arquivos
	if [ ! -d "./extract" ]; then mkdir -m777 ./extract; fi
	if [ ! -d "./logs" ]; then mkdir -m777 ./logs; fi
	if [ ! -d "./plugins" ]; then mkdir -m777 ./plugins; fi


## @ inicializacao
.PHONY: iniciar
iniciar: ## inicia o airflow
	docker-compose up -d --build

## @ importacao
.PHONY: importar
importar: ## importa as variaveis e conexoes do airflow

# python3 juntar_variaveis_em_um_unico_json.py
	docker-compose exec airflow-worker airflow variables import ./dags/configs/variables.json
	docker-compose exec airflow-worker airflow connections import ./dags/configs/connections.json --overwrite

## @ exportacao
.PHONY: exportar
exportar: autorizarPastas ## exporta as variaveis e conexoes do airflow
	docker-compose exec airflow-worker airflow variables export ./dags/variables.json
	docker-compose exec airflow-worker airflow connections export ./dags/connections.json
	

## @ ativa e executa todas as dags do $nome_projeto
.PHONY: rodar $(nome_projeto)
rodar $(nome_projeto):
	docker-compose -T exec airflow-scheduler for i in $(grep -oP '(?<="dag_id": ")[^"]*' <(airflow dags list -o json -S $AIRFLOW_HOME/dags/$(nome_projeto))); do    airflow dags unpause $i; done
	docker-compose -T exec airflow-scheduler for i in $(grep -oP '(?<="dag_id": ")[^"]*' <(airflow dags list -o json -S $AIRFLOW_HOME/dags/$(nome_projeto))); do    airflow dags trigger $i; done

## @ download jars
.PHONY: jars
download_and_configure_spark_jars: ## baixa os jars necessarios para o spark
	wget https://download.oracle.com/otn-pub/otn_software/jdbc/1918/ojdbc8.jar
	wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar

	mkdir -p ./dockerfiles/jupyter/jars
	mkdir -p ./dockerfiles/spark/jars
	mkdir -p ./dockerfiles/airflow/jars

	cp ./*.jar ./dockerfiles/jupyter/jars
	cp ./*.jar ./dockerfiles/spark/jars
	cp ./*.jar ./dockerfiles/airflow/jars

	rm ./*.jar

## @ pipeline completo da inicializacao
.PHONY: pipeline
pipeline: ## pipeline completo para implantação do airflow
	make download_and_configure_spark_jars	
	make esqueleto
	make autorizarPastas
	make iniciar
	make importar
#make removeNone
#make importar
#make exportar

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
