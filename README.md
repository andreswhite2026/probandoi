# SismoWatch

Plataforma de ingeniería de datos sísmicos en tiempo casi real construida sobre **Apache Airflow,
Apache Spark, arquitectura Medallion, MinIO, PostgreSQL, OpenMetadata, FastAPI, React y un modelo de
lenguaje local (Ollama)**.

Fuente de datos: [USGS Earthquake Catalog](https://earthquake.usgs.gov/fdsnws/event/1/) (pública, sin
API key).

> **Estado actual: Fase 0 completada.** El repositorio contiene el esqueleto de documentación y la
> configuración base. Todavía **no** existe `docker-compose.yml`: se construye en la Fase 1.
> El progreso real está en [`BITACORA.md`](BITACORA.md) y las decisiones en [`PROYECTO.md`](PROYECTO.md).

---

## Qué resuelve

Convertir un feed público que cambia cada minuto (sin historial, sin validación, sin resumen) en un
producto de datos confiable:

1. **Ingesta** automática cada 5 minutos + backfill de 30 días, idempotente por `id`.
2. **Procesamiento** con Spark en capas Bronze → Silver → Gold.
3. **Calidad** bloqueante con cuarentena y métricas por ejecución.
4. **IA** para reporte diario y detección de anomalías, con cifras calculadas por código.
5. **Plataforma** web con 3 vistas, roles `viewer`/`admin` y actualización en vivo.

## Documentación

| Documento | Para qué sirve |
|---|---|
| [`PROYECTO.md`](PROYECTO.md) | Fuente única de verdad: alcance, decisiones, estado |
| [`BITACORA.md`](BITACORA.md) | Qué se hizo realmente y qué se verificó |
| [`docs/ARQUITECTURA.md`](docs/ARQUITECTURA.md) | Componentes, almacenamiento, modelo de datos, trade-offs |
| [`docs/GUIA_ESTUDIO.md`](docs/GUIA_ESTUDIO.md) | Estudio progresivo por módulos |
| [`docs/GLOSARIO.md`](docs/GLOSARIO.md) | Glosario acumulativo con ejemplos del proyecto |
| [`PRESENTACION_PROMPT.md`](PRESENTACION_PROMPT.md) | Guion de sustentación y preguntas probables |
| [`docs/decisiones/`](docs/decisiones/) | ADRs: por qué cada decisión |

## Requisitos previos

- Docker Engine + Docker Compose v2
- 8 GB de RAM libres recomendados (OpenMetadata es el servicio más pesado)
- Puertos libres: 3000, 5433, 8000, 8082, 9002, 9003 (y 8585/9201/3307 solo con `profile governance`)

## Puesta en marcha

Se documentará aquí el comando exacto cuando exista el compose (Fase 1):

```bash
cp .env.example .env      # completar los valores CAMBIAR
docker compose up -d
```

## Pruebas

Se documentará el comando (`pytest`) y su alcance real a partir de la Fase 3.

## Solución de problemas

Se irá completando con errores reales encontrados y su solución (ver `BITACORA.md`).
