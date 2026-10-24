# Contrato de datos

## Transporte JSONL

Cada línea es un objeto con exactamente `topic`, `partition`, `offset`, `key` y `value`.
`topic` usa caracteres ASCII de tema Kafka, máximo 200; `partition` es entero 0..10000;
`offset` es entero no negativo de 64 bits; booleanos no cuentan como enteros.
`key` es exactamente `{"order_id":"O00000001"}`. Una clave permanece en el mismo
tema y partición durante toda la vida de una raíz.

`value=null` es un tombstone Kafka: se registra el transporte pero no crea otra
eliminación lógica. La eliminación es el evento `op=d` que lo precede en una fuente
Debezium normal. Un tombstone aislado no borra el pedido vigente.

La identidad de transporte es `(topic, partition, offset)`. Su hash se calcula sobre
el JSON canónico del registro, no sobre los bytes originales de Kafka. Variaciones
de espacios no causan conflictos. Cambiar el contenido de una coordenada ya procesada
bloquea el lote. Nunca se reutiliza una raíz para un topic recreado con offsets reiniciados.

## Envelope de origen

Se exige un objeto sin wrapper `schema/payload`, con `before`, `after`, `op` y `source`.
`source.db=retail` y `source.table=orders`. Los metadatos adicionales Debezium, como
`ts_ms`, `transaction`, `schema` dentro de source o LSN, pueden conservarse en el raw.
No se ordena por `source.txId`, timestamps o LSN. La fuente debe conservar revisiones
por entidad y claves estables.

| Operación | Requisitos | Efecto |
| --- | --- | --- |
| `r` / `c` | `before=null`, after completo | Revisar/agregar estado de la revisión indicada |
| `u` | before y after completos, misma clave, revisión after mayor | Agregar revisión y recalcular estado vigente |
| `d` | before completo, `after=null` | Marca de eliminación en `before.revision+1` |
| Kafka tombstone | value null y clave válida | Solo ledger de transporte |

Las revisiones no se reinician. Reinsertar una clave después de su eliminación se
rechaza, incluso con un número superior: usar otra identidad de pedido. Truncates,
DDL, transacciones multi-tabla, heartbeats y cambios de clave no están soportados.
Filtrar esos topics antes de enviarlos al motor; nunca ignorar un evento desconocido
que aparezca en el topic de pedidos.

## Fila de negocio

| Campo | Tipo / validación |
| --- | --- |
| `order_id` | `O` seguido de 8 dígitos ASCII; igual a la clave Kafka |
| `customer_id` | `C` seguido de 6 dígitos; IDs ficticios en la demo |
| `revision` | Entero 1..1000000000 |
| `amount_minor` | Entero 1..10000000000; unidades menores, sin flotantes |
| `currency` | PEN o USD; nunca se suman entre monedas |
| `status` | created, paid, shipped o cancelled |
| `updated_at` | ISO 8601 con zona, año 2000..2099; normalización UTC |
| `region` | Opcional y nullable; LIM, ARE o CUS |

Las columnas desconocidas fallan. Añadir `region` es la única evolución física
implementada. Se añade como nullable a current/history usando un nuevo field ID de
Iceberg; las filas antiguas reciben null. No se implementan renombres, drop, cambios
de tipo ni ampliaciones de enumeraciones de forma automática.

La identidad lógica es `(order_id, revision)`. Un duplicado con el mismo estado se
colapsa aunque llegue en otro offset o mediante `r` y `c`. El mismo par con distinto
estado es un conflicto, no un último-write-wins silencioso. La ausencia de region y
region=null se consideran el mismo estado.

## Tablas de producto

| Tabla | Grano | Contenido |
| --- | --- | --- |
| changes | Coordenada de transporte | topic, partition, offset, raw_hash, raw JSON canónico |
| current | order_id vigente | Última revisión no eliminada |
| history | order_id + revision | Versiones únicas, deleted y valid_to_revision |
| gold | Fecha UTC + moneda + estado | orders y amount_minor de current |

El historial usa intervalos de revisión, no intervalos de tiempo de negocio. Los
huecos entre revisiones conocidas se exponen mediante `revision_gaps`. Una revisión
tardía puede cambiar esos intervalos sin cambiar current. Las publicaciones antiguas
mantienen el historial que se conocía en aquella generación.

## Límites y errores

El motor limita cada lote a 32 MiB, cada línea a 64 KiB y el ledger retenido a 100000
registros de transporte. El generador admite 1..10000 pedidos; el lector Kafka ingiere
1..10000 mensajes por invocación. Se rechazan JSON con claves repetidas, números no
finitos, UTF-8 inválido y profundidad excesiva.

Cualquier error bloquea el lote entero y conserva los offsets publicados anteriores.
No existe una ruta automática que descarte eventos ni una DLQ que permita saltarse
cambios de negocio. Los bytes de lotes aceptados por el motor se conservan en Bronze
antes de validarlos. Un mensaje Kafka que ni siquiera se pueda decodificar como JSON
falla antes de formar un lote: permanece en el broker, sin archivo Bronze nuevo.
