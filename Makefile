# ===========================================================================
# SismoWatch — atajos de desarrollo
# ---------------------------------------------------------------------------
# Cada objetivo es un atajo de un comando de Docker/Compose o de pytest.
# No contiene lógica de negocio: esa vive en Airflow, Spark o la API.
# ===========================================================================
SHELL := /bin/bash
COMPOSE ?= docker compose
MC_IMAGE ?= bitnamilegacy/minio-client:2025.7.21-debian-12-r3

-include .env
export

.PHONY: help build up down restart ps logs psql psql-api minio-ls airflow-shell smoke

help: ## Muestra los objetivos disponibles
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Construye la imagen de Airflow (JDK + Spark + JAR)
	$(COMPOSE) build

up: ## Levanta el entorno en segundo plano
	$(COMPOSE) up -d

down: ## Detiene y elimina los contenedores (conserva volúmenes)
	$(COMPOSE) down

restart: ## Reinicia los servicios
	$(COMPOSE) restart

ps: ## Estado y salud de los servicios
	$(COMPOSE) ps

logs: ## Logs en vivo (Ctrl+C para salir)
	$(COMPOSE) logs -f --tail=100

psql: ## Consola psql como superusuario
	$(COMPOSE) exec postgres psql -U "$(POSTGRES_SUPERUSER)" -d "$(POSTGRES_DB)"

psql-api: ## Consola psql con el rol de la API (para comprobar permisos)
	$(COMPOSE) exec -e PGPASSWORD="$(SISMO_API_PASSWORD)" postgres \
		psql -h 127.0.0.1 -U "$(SISMO_API_USER)" -d "$(POSTGRES_DB)"

minio-ls: ## Lista los buckets de MinIO (sin publicar puertos)
	docker run --rm --network sismowatch-net \
		-e "MC_HOST_sismowatch=http://$(MINIO_ROOT_USER):$(MINIO_ROOT_PASSWORD)@minio:9000" \
		$(MC_IMAGE) mc ls sismowatch

airflow-shell: ## Shell dentro del contenedor del scheduler
	$(COMPOSE) exec airflow-scheduler bash

smoke: ## Smoke test Spark -> MinIO -> PostgreSQL (limpia solo lo suyo)
	$(COMPOSE) exec -T airflow-scheduler spark-submit \
		--master 'local[*]' --driver-memory 1g \
		/opt/airflow/spark/jobs/_smoke.py
