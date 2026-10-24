# Arquitectura y garantías

## Problema

Cada commit Iceberg es atómico para una tabla. Una generación del producto requiere
cuatro tablas relacionadas: `changes`, `current`, `history` y `gold`. Una caída entre
commits deja snapshots físicamente válidos que no forman un producto consistente.

El punto de visibilidad es una fila de `publications`, no el último snapshot Iceberg.
La fila contiene generación, SHA del lote, versión del modelo, evolución de esquema,
cuatro snapshot IDs, conteos y offsets siguientes por partición. Una transacción SQLite
publica todo ese control simultáneamente. El CAS comprueba que la generación anterior
siga siendo la esperada; un bloqueo POSIX impide escritores simultáneos por raíz.

## Secuencia de escritura

1. Adquirir `writer.lock` sin espera; otro escritor falla explícitamente.
2. Leer hasta 32 MiB y conservar el lote en Bronze mediante archivo temporal, fsync,
   rename y fsync del directorio Bronze. El nombre final es el SHA-256 de los bytes.
3. Registrar el intento. Intentos `started` de un proceso anterior pasan a `abandoned`
   una vez adquirido el bloqueo exclusivo.
4. Capturar la publicación anterior; cargar `changes` desde SU snapshot.
5. Validar el nuevo lote. Identidad de transporte: tema, partición y offset.
6. Detectar conflictos de coordenadas, versiones y cambios de partición de una clave.
7. Materializar las cuatro tablas de forma determinista con el conjunto aceptado.
8. Añadir `region` nullable si aparece por primera vez; escribir snapshots candidatos.
9. Leer nuevamente los cuatro snapshots persistidos y compararlos con el candidato.
10. En una transacción SQLite, comprobar generación y publicar manifest + offsets.

Los commits candidatos pueden quedar huérfanos respecto a las publicaciones si falla
el paso 8 o 9. No se intenta un rollback distribuido. La siguiente ejecución reconstruye
desde la publicación anterior y sobrescribe candidatos; los snapshots antiguos se
conservan para consultas históricas y diagnóstico.

## Lectura y consistencia

`report()` abre el catálogo existente sin inicializar tablas ni adquirir el bloqueo
del escritor. Captura una publicación y lee cada tabla con `scan(snapshot_id=...)`.
Una nueva publicación durante la consulta no altera los IDs capturados. El test
`test_readers_do_not_wait_for_writer_lock` verifica esta separación.

La proyección de una tabla Iceberg usa el esquema compatible actual: después de añadir
`region`, un snapshot anterior puede mostrar esa columna como null. La garantía es
consistencia de datos de la generación, no congelación del esquema de presentación.

Una tabla todavía vacía puede carecer de snapshot. En ese caso el manifiesto guarda
null y el lector construye una tabla Arrow vacía con el esquema correspondiente.

## Semántica del historial

Cada pedido tiene revisiones enteras crecientes en la fuente. Se conserva una fila
por `(order_id, revision)` con intervalo `[revision, valid_to_revision)`. La última
versión tiene límite null. Una eliminación usa `before.revision + 1` y `deleted=true`.
`current` contiene la revisión máxima de cada clave siempre que no sea una eliminación.

Una versión 2 recibida después de la 3 rellena el historial sin hacer retroceder el
estado vigente. Se toleran huecos y se contabilizan mediante `revision_gaps`; una
conciliación correcta no demuestra que todas las revisiones de la fuente hayan llegado.
Un snapshot inicial puede empezar en una revisión superior a 1. El historial comienza
en el corte de extracción; no se inventan versiones anteriores.

`updated_at` se normaliza a UTC y permanece como cadena ISO de auditoría. No determina
orden de aplicación. Gold agrupa por fecha UTC de la actualización de la versión vigente,
moneda y estado. Un importe de un pedido creado no se presenta como ingreso reconocido.

## Kafka

El adaptador asigna una partición explícitamente. Captura el high watermark para limitar
la ejecución; solicita desde el siguiente offset publicado y desactiva autocommit y
almacenamiento automático de offsets. No utiliza `subscribe`, rebalances ni offsets
de un grupo como punto de recuperación. El bloqueo de la raíz cubre lectura e ingesta.

Se admiten huecos de offsets porque no todas las fuentes Kafka son densas. Eso no
autoriza a reconstruir historial completo desde un topic ya compactado: se necesita
el log íntegro desde el corte o una copia Bronze verificada. La comparación del low
watermark detecta una pérdida por retención en el inicio requerido; no prueba por sí
sola que un topic nunca haya sido compactado o recreado.

## Persistencia y confianza

| Componente | Responsabilidad | Limitación |
| --- | --- | --- |
| `bronze/` | Bytes de lotes, incluso lotes inválidos del motor | Kafka se serializa a JSONL; no son bytes originales del broker |
| `catalog.sqlite` + warehouse | Metadatos y datos Iceberg | Catálogo local, archivos con rutas absolutas |
| `control.sqlite` | Publicaciones, offsets y auditoría de intentos | Un host, transacciones locales |
| `writer.lock` | Exclusión entre procesos cooperantes | No protege de escritores externos que ignoren el protocolo |
| `report()` | Lectura consistente y conciliación interna | Reconstruye en memoria; no consulta la fuente original |

El entorno se considera controlado: no se admiten ediciones manuales del warehouse,
escrituras por otros motores ni eliminación de snapshots referenciados. fsync de Bronze
y las transacciones de catálogo no equivalen a una certificación contra cortes de
energía de todos los sistemas de archivos. Las pruebas de proceso cubren una caída
de software, no pérdida física del disco.

## Referencias primarias

- [PyIceberg API: escrituras, esquemas y snapshots](https://py.iceberg.apache.org/api/).
- [Catálogo SQL PyIceberg](https://py.iceberg.apache.org/configuration/#sql-catalog).
- [Debezium PostgreSQL](https://debezium.io/documentation/reference/3.3/connectors/postgresql.html).
- [Cliente Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html).
- [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html).

Las decisiones de publicación son propias de este proyecto; no son una transacción
multi-tabla ofrecida por el catálogo Iceberg.
