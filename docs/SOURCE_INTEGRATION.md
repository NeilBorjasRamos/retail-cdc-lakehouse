# Integración PostgreSQL → Debezium: diseño pendiente de prueba

El camino probado de esta versión empieza con JSONL o mensajes Kafka producidos por
el generador. Este documento define la integración de una tabla operacional real.
No existe un despliegue Debezium/PostgreSQL incluido ni una ejecución de ese conector
validada. Evitar confundir envelope compatible con extracción real demostrada.

## Modelo fuente requerido

Una base llamada retail con public.orders, clave primaria order_id, importes BIGINT
en unidades menores, IDs de cliente pseudónimos, revision BIGINT y updated_at TIMESTAMPTZ.
El esquema debe coincidir con DATA_CONTRACT. No añadir nombres, correos o direcciones.
La fuente debe impedir reutilización de order_id después de una eliminación, incluso
si la tabla ya no conserva la fila: usar un registro de identidades o soft delete de
origen con la traducción correspondiente en un adaptador probado.

En cada update, una regla transaccional debe asignar revision=OLD.revision+1 y conservar
order_id; no confiar en que cada cliente actualice revision correctamente. Un delete
produce la revisión lógica before.revision+1 en el adaptador. updated_at de ese delete
es la última actualización de before, no el instante del borrado.

Habilitar `wal_level=logical` y dimensionar replication slots/WAL según retención y lag.
Establecer REPLICA IDENTITY FULL en orders para que before incluya todas las columnas.
Aplicar privilegios mínimos de replicación, SELECT y publicación según la versión de
PostgreSQL y el procedimiento de Debezium utilizado. Mantener credenciales fuera del repo.

## Propiedades del conector a configurar

| Propiedad | Valor o requisito |
| --- | --- |
| connector.class | io.debezium.connector.postgresql.PostgresConnector |
| plugin.name | pgoutput |
| database.dbname | retail |
| table.include.list | public.orders |
| topic.prefix | retail, dando retail.public.orders |
| snapshot.mode | initial para un corte nuevo verificado |
| slot.name / publication.name | Nombres exclusivos y gestionados para este conector |
| key.converter | org.apache.kafka.connect.json.JsonConverter |
| key.converter.schemas.enable | false |
| value.converter | org.apache.kafka.connect.json.JsonConverter |
| value.converter.schemas.enable | false |
| Credenciales, hostname, TLS | Inyectados en despliegue, no valores versionados |

Mantener los envelopes nativos: no usar un SMT que elimine before/source/op. TIMESTAMPTZ
debe llegar como cadena con zona; probarlo con la versión de conector elegida. El adaptador
acepta metadatos adicionales y no interpreta tipos enteros de timestamp como fechas.
No enviar heartbeats ni topics de DDL al topic de pedidos.

## Plan de validación obligatorio antes de declarar integración completa

1. Insertar un conjunto conocido de pedidos en PostgreSQL; iniciar snapshot y comparar
   el número de filas, IDs y sumas por moneda contra current publicado.
2. Ejecutar updates transaccionales con revisión automática; verificar before y after.
3. Borrar pedidos y comprobar evento d, before completo y tombstone posterior.
4. Detener conector, escribir cambios, reiniciarlo y confirmar que no pierde eventos.
5. Interrumpir el escritor Iceberg entre commits y verificar cursor/publicación anterior.
6. Añadir region nullable y comprobar snapshot antiguo y nuevas versiones.
7. Simular WAL retenido insuficiente, slot perdido y topic sin offsets requeridos;
   el sistema debe bloquear y exigir nuevo corte verificado.
8. Comparar estado final contra una consulta independiente de la base fuente en un
   punto de corte conocido. La conciliación interna del lakehouse no sustituye este paso.

Para preservar historia se necesita un log íntegro desde el corte. Un topic compactado
puede permitir reconstruir estado reciente pero haber eliminado revisiones intermedias.
No aumentar particiones ni cambiar estrategia de claves dentro de una raíz existente.

Referencia primaria: [Debezium PostgreSQL 3.3](https://debezium.io/documentation/reference/3.3/connectors/postgresql.html).
La configuración exacta debe revisarse para la versión desplegada; no incluye aquí
una promesa de compatibilidad para todas las versiones del conector.
