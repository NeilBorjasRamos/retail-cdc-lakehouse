# Evolución a mayor volumen

El formato Iceberg y los límites modulares permiten evolucionar el diseño. La versión
actual sigue siendo local y acotada; los componentes de la derecha no están implementados.

| Área | Referencia ejecutable | Siguiente implementación y condición de aceptación |
| --- | --- | --- |
| Cómputo | Python/Arrow, ledger en memoria y full overwrite | Spark/Flink incremental; mismo resultado ante reordenación, duplicados y caídas |
| Catálogo | SQLCatalog SQLite | REST catalog con auth y backend HA; probar conflictos optimistas reales |
| Archivos | Filesystem POSIX local | S3/ADLS con IAM y cifrado; no trasladar filelock/fsync como si fueran garantías de object storage |
| Publicación | SQLite y bloqueo de un host | Servicio transaccional con CAS/leases/fencing; probar expiración de líder y lectores concurrentes |
| Ingesta | Partición explícita, ejecución finita | Coordinador de particiones y checkpoints; probar rebalance sin pérdida y sin publicar cursores prematuros |
| Escrituras | Reemplazo de tablas completas | Merge incremental y particiones por patrón de consulta; medir archivos afectados y conflictos |
| Historial | Revisión por entidad | Definir event time, correcciones retroactivas y bitemporalidad si el negocio la necesita |
| Orquestación | CLI y GitHub CI | Jobs desplegados con límites, retries, alertas y control de solapamiento |
| Observabilidad | JSON de publicación y auditoría local | Métricas de lag, duración, bytes, candidatos huérfanos y alertas sobre gaps sostenidos |
| Fuente | Generador y cliente Kafka real | PostgreSQL/Debezium provisionados y conciliación independiente contra fuente |
| Retención | Conservar snapshots | GC por alcanzabilidad de publicaciones, retención histórica y pruebas de restauración |

## Orden propuesto de implementación

Primero instrumentar duración de cada fase y memoria máxima sobre una carga controlada.
Conservar tests de contrato y de publicación como suite de aceptación. Después sustituir
catálogo y almacenamiento, manteniendo un solo escritor hasta probar fencing distribuido.
No habilitar varios escritores copiando simplemente el código a varios contenedores.

Mover la publicación a un backend transaccional antes de particionar el cómputo. Introducir
una identidad de ejecución y una época de líder que el servicio rechace si perdió el lease.
El CAS debe proteger tanto la generación visible como los cursores. Reejecutar todas las
caídas entre fases, incluida pérdida de conectividad tras confirmar y antes de recibir ACK.

Solo después reemplazar la materialización completa por cambios incrementales y diseñar
un plan para late revisions: hay que recalcular el intervalo afectado y mantener current
sin retroceso. Comparar cada implementación con el modelo puro en datasets manejables.
Revisar partición por día/bucket según consultas y volumen; una tabla pequeña no obtiene
valor por crear muchas particiones vacías.

## Medidas que aún faltan

No existen cifras de throughput, latencia p95/p99, coste cloud ni número máximo de
usuarios. Los límites de 100000 eventos y 32 MiB son límites defensivos del programa,
no capacidad certificada. Antes de fijar un SLO se necesita un entorno definido,
workload documentado, prueba sostenida y análisis de recuperación y costes.
