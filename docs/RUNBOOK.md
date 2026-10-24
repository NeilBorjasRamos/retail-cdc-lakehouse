# Operación y recuperación

## Estado saludable

Ejecuta `uv run --frozen retail --root data/manual report`. Salida 0 y reconciled=true
indican que las cuatro tablas publicadas corresponden al ledger de esa generación.
Revisa también revision_gaps: una publicación consistente puede tener revisiones
intermedias todavía ausentes. Esto no compara contra una base fuente real.

El CLI devuelve 0 en éxito, 1 en fallo operativo/contrato y 2 si report detecta diferencias.
Los mensajes de ContractError son causas estáticas, sin interpolar payloads. Otros
errores se reducen a tipo y mensaje genérico para evitar credenciales en consola.
El uso directo de la API Python permite depurar en un entorno privado.

## Fallo durante una ingesta

No borres el catálogo ni ajustes offsets a mano. Consulta report: debe seguir visible
la última publicación completa. Reejecuta exactamente el mismo lote con ingest, o la
misma invocación Kafka. El motor carga el snapshot publicado del ledger y reconstruye.
Si hubo salida abrupta, el bloqueo se libera al morir el proceso y el intento started
se clasificará como abandoned al adquirirlo el siguiente escritor.

Una salida abrupta puede dejar temporales Bronze `.tmp` o snapshots candidatos no
publicados. Los temporales no se usan como lotes. No se expiran snapshots automáticamente.
El intento fallido se conserva; la publicación anterior no se modifica.

## Tabla de diagnóstico

| Síntoma | Interpretación | Acción |
| --- | --- | --- |
| Timeout de filelock | Otro escritor de esa raíz sigue activo | Verificar proceso y esperar su final; no borrar el archivo lock para eludirlo |
| JSON inválido / fila no soportada | Contrato roto | Corregir productor y planificar reconstrucción; no saltar automáticamente el offset |
| Coordenada con contenido distinto | Reutilización de topic/offset o edición del lote | Comparar evidencia Bronze y origen; usar una raíz nueva si se recreó el topic |
| Misma revisión con distinto estado | Revisión de origen reutilizada | Corregir la fuente; no seleccionar arbitrariamente un ganador |
| Unseen offset behind checkpoint | Export desordenado o incompleto | Revisar orden por partición y rango del export; conservar contenido original |
| Retention gap / topic reset | El cursor requerido ya no está disponible | Recuperar log completo desde copia o reconstruir en otra raíz desde corte verificado |
| Unknown generation | No se publicó esa generación | Consultar las existentes; no inventar snapshots a partir de latest |
| Modelo incompatible | Se cambió MODEL_VERSION | Usar release compatible para lectura y migrar/reconstruir en una raíz nueva |
| Candidate read-back mismatch | Archivos escritos no corresponden al modelo | Detener promoción; investigar almacenamiento y código antes de reintentar |
| reconciled=false | Corrupción o escritura ajena al protocolo | Detener ingesta, conservar copia e investigar; no publicar para ocultar la diferencia |
| Demo requiere raíz vacía | Ya existe evidencia local | Elegir otro --root |

Si un evento Kafka contiene JSON válido pero un contrato incompatible, su lote queda
en Bronze y los offsets no avanzan. Si ni siquiera se puede decodificar, el broker es
la copia disponible: retener/exportar ese mensaje antes de la limpieza del topic.
Corregir el productor con otro evento no elimina automáticamente el mensaje bloqueante.
La resolución requiere un nuevo corte/reconstrucción verificados o desarrollar y probar
un procedimiento explícito de corrección; no existe un comando oculto skip-offset.

## Auditoría local

`control.sqlite` contiene publications y attempts. Los estados de intento son started,
failed, abandoned, published y replayed. Las publicaciones incluyen el SHA del lote que
las generó, snapshot IDs, conteos, versión de modelo y offsets siguientes. Ejemplo de
inspección de solo lectura con Python estándar:

```python
import sqlite3
from pathlib import Path
path = Path("data/manual/control.sqlite").resolve()
with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
    print(db.execute("SELECT id,status,generation FROM attempts ORDER BY id").fetchall())
    print(db.execute("SELECT generation FROM publications ORDER BY generation").fetchall())
```

No registrar payloads operacionales ni cadenas de conexión en tickets. La demo es
ficticia; una adaptación real exige accesos y retención adecuados para Bronze/history.

## Copia y restauración

El catálogo y los metadatos Iceberg contienen rutas absolutas. Una copia portátil a
otra ruta o a otro host NO se obtiene moviendo los sqlite y los Parquet sin más.

Para copia consistente local: detener todos los escritores, mantener la raíz estable,
tomar el bloqueo de la raíz con una herramienta cooperante y copiar el directorio
completo, incluidos catalog.sqlite, control.sqlite y cualquier archivo WAL/SHM presente,
warehouse y Bronze. Evitar copias de archivos SQLite mientras haya transacciones
activas; para un servicio continuo se necesita una estrategia de snapshot/backup propia.

La restauración de esa copia requiere la misma ruta absoluta y permisos, y debe
probarse en aislamiento con report y consultas de generaciones históricas. No restaurar
encima de una raíz activa. El backup/restore completo todavía no tiene una prueba
automatizada en esta versión; no se atribuye un RPO/RTO garantizado.

Para portabilidad, reconstruir una raíz nueva desde los lotes Bronze verificados:
recuperar el orden de lotes publicados de la copia de publications, leer el input_sha
y localizar sus JSONL. Aplicarlos secuencialmente con ingest y reconciliar. No ordenar
archivos Bronze alfabéticamente: los hashes no indican orden. Los lotes de intentos
fallidos requieren investigación antes de incluirse. Verificar cobertura de origen;
si faltan lotes, detener reconstrucción.

## Retención y eliminación

Un borrado CDC es lógico; Bronze, history y snapshots anteriores conservan el pedido.
No representa una eliminación física de datos personales. No hay información personal
real en los fixtures. Para una fuente real, definir políticas de erasure y acceso.

No ejecutar expire_snapshots ni remove_orphan_files contra este warehouse sin cruzar
todas las publicaciones retenidas con los snapshots y archivos alcanzables. Un fichero
no referenciado por el snapshot latest puede ser necesario para generación 1.
Esta versión conserva toda la historia; la política de GC queda pendiente en ROADMAP.

## Kafka y contenedor

Compose expone Kafka solo en 127.0.0.1:29092. Para detenerlo conservando datos usa
`docker compose stop kafka`. `docker compose down` elimina los contenedores pero
conserva volúmenes; añadir `-v` destruye el entorno y no debe usarse sobre datos útiles.
CI sí lo usa porque sus volúmenes son efímeros.

El app container usa un volumen gestionado y UID 10001. El adaptador host usa localhost;
desde un contenedor debe usarse `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` y la misma red Compose.
El smoke documentado se ejecuta en el host, no dentro de la imagen de aplicación.
