# ===========================================================================
# SismoWatch — atajos de desarrollo
# ---------------------------------------------------------------------------
# Cada objetivo es un atajo de un comando de Docker/Compose o de pytest.
# No contiene lógica de negocio: esa vive en Airflow, Spark o la API.
# ===========================================================================
SHELL := /bin/bash

# Detección automática del socket de Docker Rootless si DOCKER_HOST no está definido
ifeq ($(DOCKER_HOST),)
  USER_DOCKER_SOCK := /run/user/$(shell id -u)/docker.sock
  ifneq ($(wildcard $(USER_DOCKER_SOCK)),)
    export DOCKER_HOST := unix://$(USER_DOCKER_SOCK)
  endif
endif

COMPOSE ?= docker compose
MC_IMAGE ?= bitnamilegacy/minio-client:2025.7.21-debian-12-r3

-include .env
export

.PHONY: help setup build up down restart ps logs psql psql-api minio-ls airflow-shell smoke

help: ## Muestra los objetivos disponibles
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup: ## Prepara el entorno inicial creando .env si no existe
	@if [ ! -f .env ]; then \
		echo "Creando .env a partir de .env.example..."; \
		cp .env.example .env; \
		echo "✓ Archivo .env creado con éxito."; \
	else \
		echo "✓ .env ya existe, conservando configuración actual."; \
	fi

build: ## Construye las imágenes Docker del proyecto
	$(COMPOSE) build

up: ## Levanta el entorno en segundo plano
	$(COMPOSE) up -d --build

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

test: ## Ejecuta las pruebas automatizadas con pytest en el contenedor
	$(COMPOSE) exec -T airflow-scheduler pytest /opt/airflow/tests -v

transform: ## Ejecuta el job de transformación Bronze a Silver con Spark
	$(COMPOSE) exec -T airflow-scheduler spark-submit \
		--master 'local[*]' --driver-memory 1g \
		/opt/airflow/spark/jobs/transform_silver.py

gold: ## Ejecuta el job de publicación a Gold y detección de anomalías
	$(COMPOSE) exec -T airflow-scheduler python3 \
		/opt/airflow/spark/jobs/publish_gold.py

backfill: ## Ejecuta el backfill histórico en Airflow (ej: make backfill DAYS=30)
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger backfill_history \
		--conf '{"days": $(or $(DAYS),30)}'

report: ## Genera el reporte diario con IA o fallback determinista
	$(COMPOSE) exec -T airflow-scheduler python3 \
		/opt/airflow/spark/jobs/generate_daily_report.py

report-fallback: ## Genera el reporte forzando el modo fallback determinista
	$(COMPOSE) exec -T airflow-scheduler python3 \
		/opt/airflow/spark/jobs/generate_daily_report.py --force-fallback

reports-check: ## Consulta el reporte generado, indicadores y auditoria de IA
	$(COMPOSE) exec postgres psql -U postgres -d sismowatch -c \
		"SELECT report_id, reference_date, provider, model, status, latency_ms, substring(summary_md from 1 for 180) AS preview FROM curated.ai_reports ORDER BY generated_at DESC LIMIT 1;"
	$(COMPOSE) exec postgres psql -U postgres -d sismowatch -c \
		"SELECT indicators FROM curated.ai_reports ORDER BY generated_at DESC LIMIT 1;"
	$(COMPOSE) exec postgres psql -U postgres -d sismowatch -c \
		"SELECT run_id, operation, provider, model, status, latency_ms, created_at FROM curated.ai_runs ORDER BY created_at DESC LIMIT 3;"

api-up: ## Construye y arranca el servicio API en background
	$(COMPOSE) up -d --build api

api-logs: ## Muestra los logs en tiempo real del contenedor API
	$(COMPOSE) logs -f api

test-api: ## Ejecuta la suite de pruebas unitarias y de integración de la API
	$(COMPOSE) exec -T api pytest /app/tests -v

test-all: test test-api ## Ejecuta todas las pruebas (pipeline + API)

users-list: ## Lista los usuarios registrados en app.users y sus roles
	$(COMPOSE) exec -T api python3 -m app.manage_users --list

user-reset: ## Restablece la contraseña de un usuario (ej: make user-reset EMAIL=admin@sismowatch.local PASSWORD=nuevo_pass)
	$(COMPOSE) exec -T api python3 -m app.manage_users --reset-password --email "$(EMAIL)" --password "$(PASSWORD)"

rbac-check: ## Prueba en vivo los controles RBAC con curl (401, 403 viewer, 200 admin)
	@echo "=== 1. Prueba 401 Unauthorized (sin token) ==="
	@curl -i -s http://localhost:8000/api/v1/quarantine | head -n 8
	@echo "\n=== 2. Prueba 403 Forbidden (con token viewer) ==="
	@VIEWER_TOKEN=$$(curl -s -X POST http://localhost:8000/api/v1/auth/login -H "Content-Type: application/json" -d '{"email":"viewer@sismowatch.local","password":"$(VIEWER_PASSWORD)"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4); \
	curl -i -s -H "Authorization: Bearer $$VIEWER_TOKEN" http://localhost:8000/api/v1/quarantine | head -n 8
	@echo "\n=== 3. Prueba 200 OK (con token admin) ==="
	@ADMIN_TOKEN=$$(curl -s -X POST http://localhost:8000/api/v1/auth/login -H "Content-Type: application/json" -d '{"email":"admin@sismowatch.local","password":"$(ADMIN_PASSWORD)"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4); \
	curl -i -s -H "Authorization: Bearer $$ADMIN_TOKEN" http://localhost:8000/api/v1/quarantine | head -n 8

minio-check: ## Lista los objetos en Bronze y particiones en Silver
	@echo "=== MinIO Bronze (primeros 5 objetos) ==="
	@docker run --rm --network sismowatch-net \
		-e "MC_HOST_sismowatch=http://$(MINIO_ROOT_USER):$(MINIO_ROOT_PASSWORD)@minio:9000" \
		$(MC_IMAGE) mc ls -r sismowatch/sismowatch-bronze/ | head -n 5
	@echo "\n=== MinIO Silver (particiones de terremotos) ==="
	@docker run --rm --network sismowatch-net \
		-e "MC_HOST_sismowatch=http://$(MINIO_ROOT_USER):$(MINIO_ROOT_PASSWORD)@minio:9000" \
		$(MC_IMAGE) mc ls sismowatch/sismowatch-silver/earthquakes/

api-check: ## Verifica en vivo los endpoints de la API (KPIs, Export CSV/GeoJSON, Agente IA)
	@echo "=== 1. KPIs sísmicos ==="
	@TOKEN=$$(curl -s -X POST http://localhost:8000/api/v1/auth/login -H "Content-Type: application/json" -d '{"email":"viewer@sismowatch.local","password":"$(VIEWER_PASSWORD)"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4); \
	curl -s -H "Authorization: Bearer $$TOKEN" http://localhost:8000/api/v1/earthquakes/kpis; echo ""; \
	echo "\n=== 2. Exportación CSV (cabeceras HTTP) ==="; \
	curl -s -D - -o /dev/null -H "Authorization: Bearer $$TOKEN" "http://localhost:8000/api/v1/earthquakes/export?format=csv&limit=5" | grep -E -i "content-type|content-disposition"; \
	echo "\n=== 3. Exportación GeoJSON (primeros 120 caracteres) ==="; \
	curl -s -H "Authorization: Bearer $$TOKEN" "http://localhost:8000/api/v1/earthquakes/export?format=geojson&limit=2" | cut -c 1-120; echo ""; \
	echo "\n=== 4. Agente Analítico de IA ==="; \
	curl -s -X POST http://localhost:8000/api/v1/agent/query -H "Authorization: Bearer $$TOKEN" -H "Content-Type: application/json" -d '{"question":"¿Cuáles son los sismos más fuertes registrados?"}'

web-up: ## Construye y arranca el frontend Vue 3 en el puerto 3000
	$(COMPOSE) up -d --build web

web-logs: ## Muestra los logs en tiempo real del contenedor web
	$(COMPOSE) logs -f web

viewer-check: ## Prueba los permisos del rol viewer (200 en lectura, 403 en cuarentena y backfill)
	@echo "=== 1. Obteniendo token de Viewer ==="
	@VIEWER_TOKEN=$$(curl -s -X POST http://localhost:8000/api/v1/auth/login -H "Content-Type: application/json" -d '{"email":"viewer@sismowatch.local","password":"$(VIEWER_PASSWORD)"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4); \
	echo "\n=== 2. Consulta autorizada a KPIs (GET /earthquakes/kpis) ==="; \
	curl -s -H "Authorization: Bearer $$VIEWER_TOKEN" http://localhost:8000/api/v1/earthquakes/kpis; echo ""; \
	echo "\n=== 3. Intento de acceso a Cuarentena (debe dar 403) ==="; \
	curl -i -s -H "Authorization: Bearer $$VIEWER_TOKEN" http://localhost:8000/api/v1/quarantine | head -n 8; \
	echo "\n=== 4. Intento de disparo de Backfill (debe dar 403) ==="; \
	curl -i -s -H "Authorization: Bearer $$VIEWER_TOKEN" -H "Content-Type: application/json" -d '{"days":7}' http://localhost:8000/api/v1/backfill/trigger | head -n 8

governance-up: ## Levanta OpenMetadata en segundo plano (perfil governance: MySQL, OpenSearch, Server)
	$(COMPOSE) --profile governance up -d

governance-down: ## Detiene los servicios de OpenMetadata (perfil governance)
	$(COMPOSE) --profile governance down

governance-ps: ## Estado de salud de los servicios de gobernanza
	$(COMPOSE) --profile governance ps

governance-catalog: ## Exporta y verifica de forma determinista el catalogo y linaje Medallion
	python3 scripts/governance/register_catalog.py --verify --export-spec scripts/governance/catalog_spec.json

governance-live: ## Registra el catalogo y aristas de linaje en vivo en OpenMetadata
	python3 scripts/governance/register_catalog.py --live --om-url http://localhost:8585

security-scan: ## Audita archivos rastreados e historial de commits asegurando cero secretos
	python3 scripts/governance/security_scan.py

test-governance: ## Ejecuta pruebas de catalogo, linaje y escaneo de seguridad en Airflow
	$(COMPOSE) exec -T airflow-scheduler pytest /opt/airflow/tests/test_governance.py /opt/airflow/tests/test_security_scan.py -v






