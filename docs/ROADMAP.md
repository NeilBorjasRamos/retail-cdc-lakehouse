# Roadmap

## Implementado en v0.1.0

- Ingesta JSONL y Kafka real con contrato estricto y bloqueo ante errores.
- Iceberg changes/current/history/gold con snapshots fijados por publicación.
- Repetición idempotente, conflictos explícitos, borrados y revisiones tardías.
- Evolución nullable de region y diagnóstico de huecos de revisión.
- Recuperación de fallos entre tablas, salida abrupta del proceso y lectores sin bloqueo.
- Demo, pruebas, evidencia agregada, wheel, contenedor y workflow CI.

## Siguiente iteración

1. Provisionar PostgreSQL + Debezium y probar corte inicial, restart y comparación con fuente.
2. Automatizar backup/restore a la misma ruta y reconstrucción portátil desde publicaciones/Bronze.
3. Métricas de fase y coste de lectura/escritura; medir antes de cambiar el plan físico.
4. Política de retención con protección explícita de snapshots referenciados por publicaciones.
5. Pruebas de compatibilidad de dependencias y actualización de hashes/digests de contenedores.

## Iteración distribuida

REST catalog, object storage, coordinación transaccional con fencing, procesamiento
incremental distribuido y observabilidad de servicio. Cada punto necesita código,
despliegue y pruebas de aceptación. Ver SCALING; no forma parte del alcance actual.
