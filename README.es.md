# Retail CDC Lakehouse

Un lakehouse con **Apache Iceberg, CDC, historial de revisiones y recuperación ante fallos**.
El objetivo es demostrar cómo mantener consistentes varias tablas analíticas aunque
el proceso se interrumpa entre escrituras.

[README técnico en inglés](README.md) · [Guía completa](docs/GUIA_COMPLETA.md) ·
[Arquitectura](docs/ARCHITECTURE.md) · [Evidencia](docs/VALIDATION.md)

## El siguiente nivel del portafolio

| Proyecto anterior | Competencia adicional de este proyecto |
| --- | --- |
| Procesamiento batch de movilidad | Tablas abiertas Iceberg y snapshots históricos |
| Series macroeconómicas e historial | Historial por revisión con eventos tardíos y borrados |
| Pagos y entrega repetida en Kafka | Publicación consistente entre tablas con commits separados |
| Pruebas de recuperación | Salida abrupta de un proceso y lectores que conservan resultados anteriores |

La demo usa pedidos ficticios deterministas. No representa ventas reales, datos de
clientes ni métricas de rendimiento de producción. Kafka e Iceberg sí se ejecutan
realmente en las verificaciones descritas en `docs/VALIDATION.md`.

## Ejecutar

En Linux, macOS o WSL2 con Python 3.12 y uv:

```bash
uv sync --frozen --extra dev
uv run --frozen retail --root data/demo demo --count 1000
uv run --frozen retail --root data/demo report --generation 1
make lint test
```

Resultado esperado: 2.467 eventos, 800 pedidos vigentes, 2.267 revisiones históricas,
cuatro grupos analíticos, cero diferencias de conciliación y cero huecos finales de
revisión. Hay 200 eliminaciones. Los importes PEN y USD se presentan por separado.

La demo falla intencionalmente después de escribir la tabla vigente y luego vuelve
a procesar el lote. El lector sigue consultando la publicación anterior hasta que
las cuatro tablas estén validadas y se publique la nueva generación.

## Qué estudiar

1. Contratos, transporte y semántica: [DATA_CONTRACT](docs/DATA_CONTRACT.md).
2. Snapshots y publicación atómica: [ARCHITECTURE](docs/ARCHITECTURE.md).
3. Construcción y ejecución por fases: [GUIA_COMPLETA](docs/GUIA_COMPLETA.md).
4. Diagnóstico, recuperación y copias: [RUNBOOK](docs/RUNBOOK.md).
5. Decisiones defendibles: [ADRs](docs/adr/001-publication-protocol.md).
6. Presentación a reclutadores: [PORTFOLIO](docs/PORTFOLIO.md).
7. Evolución a mayor volumen: [SCALING](docs/SCALING.md).

## Límites explícitos

La implementación usa un escritor, almacenamiento POSIX, catálogo SQLite y reescritura
completa de tablas; conserva hasta 100.000 eventos de transporte. No incluye cómputo
distribuido, alta disponibilidad ni un despliegue cloud. La arquitectura modular y
el plan de escalado explican cómo evolucionar esos componentes sin atribuirles una
capacidad que aún no se ha implementado.

El historial está ordenado por `revision`; `updated_at` se conserva como atributo de
auditoría. Una eliminación lógica conserva Bronze e historial. Un adaptador acepta un
subconjunto de eventos Debezium sin esquema; la integración PostgreSQL → Debezium
completa queda diferenciada del camino Kafka → Iceberg que sí se prueba.

Para publicarlo, utiliza un repositorio nuevo llamado `retail-cdc-lakehouse`; sigue la
última fase de la guía. Los archivos generados, dependencias y datos locales se excluyen
del paquete fuente mediante Git.
