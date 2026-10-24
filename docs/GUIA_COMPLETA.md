# Construcción y ejecución paso a paso

Esta guía desarrolla el cuarto proyecto del portafolio: consistencia entre tablas
Iceberg bajo CDC y fallos. El camino principal usa datos ficticios. Sigue las fases
en orden; la integración de una base PostgreSQL real es una ampliación separada.

## Fase 1 — Entender el problema de negocio

Un pedido puede pasar de creado a pagado y enviado, o desaparecer del sistema
operacional. Los analistas necesitan estado vigente, historial y totales que correspondan
al mismo conjunto de cambios. La pregunta principal es qué ve el lector si se cae
el escritor después de actualizar current pero antes de actualizar gold.

Entregable: explicar el punto de publicación y las invariantes de las cuatro tablas.
Lee `ARCHITECTURE.md` y ADR 001. Criterio de éxito: distinguir commit Iceberg individual
de publicación multi-tabla del producto.

## Fase 2 — Preparar el entorno

Requisitos: Python 3.12, uv y Git, en Linux/macOS o WSL2. Docker Compose solo se necesita
en la fase Kafka o para ejecutar la imagen. Comprueba `python --version`, `uv --version`
y `git --version`. Desde la raíz del proyecto:

```bash
uv sync --frozen --extra dev
uv run --frozen retail --help
```

`uv.lock` fija dependencias transitivas y hashes; no se instala desde requirements
sin versión. Criterio de éxito: ver los comandos generate, ingest, report, demo y kafka.

## Fase 3 — Ejecutar una demo completa

```bash
uv run --frozen retail --root data/demo demo --count 1000
```

La raíz debe estar vacía. Si existe una demo previa usa `data/demo-02`, sin borrar datos.
El comando crea tres lotes, publica el primero, inyecta un error después de current,
comprueba que siga visible la primera generación, reintenta y publica los restantes.
Repite el último lote y verifica la publicación histórica. El resultado agregado queda
en `data/demo/validation.json`.

Criterio de éxito: reconciled=true; changes=2467, current=800, history=2267, gold=4;
revision_gaps=0; replay_added=0. No uses esta ejecución como medición de latencia.

## Fase 4 — Inspeccionar el contrato

Lee `examples/batch-01.jsonl` y `src/retail/contract.py`. Identifica clave de Kafka,
offset y revisión de negocio: no representan lo mismo. Consulta `DATA_CONTRACT.md`.
Los importes se expresan como enteros, los timestamps llevan zona y los IDs son
ficticios. El adaptador exige imágenes completas de before en updates/deletes.

Ejercicio: en una copia del ejemplo sustituye PEN por una moneda no soportada. Ingiere
esa copia en una raíz nueva. Debe fallar con código de salida 1 y no crear publicación.
No edites un lote de producción para saltarte un conflicto de coordenadas.

## Fase 5 — Generar una carga reproducible

```bash
uv run --frozen retail generate --output data/input --count 1000
```

El primer lote contiene 1000 snapshots iniciales; el segundo, 1200 transportes de
updates, deletes y tombstones; el tercero, 267 revisiones lógicas tardías. Cada quinto
pedido se elimina. Algunos pedidos llegan con revisión 3 antes de recibir la 2.

Los offsets siempre respetan el orden de transporte de cada partición. La llegada
tardía es una propiedad lógica del escenario, no una afirmación de que Kafka reordene
mensajes dentro de una partición. El generador no sobrescribe un archivo existente.

## Fase 6 — Publicar Bronze y la generación inicial

```bash
uv run --frozen retail --root data/manual ingest data/input/batch-01.jsonl
uv run --frozen retail --root data/manual report
```

Revisa el subdirectorio Bronze: el nombre de cada lote es el SHA-256 de sus bytes.
Se escribe mediante temporal, fsync y rename antes de transformar. El catálogo almacena
rutas de metadatos Iceberg y los archivos Parquet se escriben bajo warehouse.

Criterio de éxito: generación 1, 1000 filas current y 1000 history; dos monedas separadas.
El contenido de data está ignorado por Git. No subir el warehouse al repositorio.

## Fase 7 — Aplicar CDC y evolución de esquema

```bash
uv run --frozen retail --root data/manual ingest data/input/batch-02.jsonl
uv run --frozen retail --root data/manual report
```

Se elimina current para 200 claves. Los tombstones no crean revisiones adicionales.
Los eventos con region provocan una operación Iceberg add_column nullable, sin cambiar
los field IDs anteriores. El ledger conserva eventos de todas las operaciones.

Criterio de éxito: 800 current; 267 huecos de revisión. reconciled=true significa
consistencia interna con los eventos recibidos, no ausencia de eventos pendientes.

## Fase 8 — Completar historial con eventos tardíos

```bash
uv run --frozen retail --root data/manual ingest data/input/batch-03.jsonl
uv run --frozen retail --root data/manual report
```

El modelo ordena por revisión, reconstruye intervalos y conserva la versión 3 como
vigente donde corresponde. Criterio de éxito: 2267 filas history, cero huecos y 800
current. Los totales finales son PEN 3800000 y USD 3800000 unidades menores, por separado.

## Fase 9 — Probar repetición y consultas históricas

```bash
uv run --frozen retail --root data/manual ingest data/input/batch-03.jsonl
uv run --frozen retail --root data/manual report --generation 1
```

La repetición debe devolver added=0 y no crear otra generación. La consulta histórica
debe seguir mostrando 1000 current, aunque la generación final tenga 800. El manifiesto
fija snapshots; no se calculan esos resultados leyendo los últimos estados.

## Fase 10 — Revisar las pruebas de fallo

```bash
uv run --frozen pytest tests/test_engine.py tests/test_process_recovery.py tests/test_quality.py -q
```

Los tests inyectan fallos después de cada tabla y después de la validación. Un test
separado inicia un proceso, escribe snapshots y termina con os._exit(77), sin cleanup
Python. El proceso padre verifica la publicación previa, el desbloqueo por el SO y la
recuperación. Otro corrompe el candidato gold y comprueba que no se promocione.

Esto demuestra recuperación de un proceso, no resistencia comprobada a un corte
eléctrico, pérdida de disco o caída de un catálogo remoto.

## Fase 11 — Ingerir desde Kafka real

```bash
docker compose up -d --wait kafka
uv run --frozen python scripts/kafka_smoke.py
```

El script crea un topic temporal con nombre único, produce los cambios, ingiere en
micro-lotes de 400 y compara contra resultados conocidos. Luego publica un evento
con contrato inválido y verifica que los offsets no avancen. Guarda evidencia en
data/kafka-smoke/validation.json. Para repetir usa otro SMOKE_ROOT:

```bash
SMOKE_ROOT=data/kafka-smoke-02 uv run --frozen python scripts/kafka_smoke.py
docker compose stop kafka
```

El broker conserva los topics. `stop` no destruye volúmenes. El entorno Compose es
un broker local sin TLS ni alta disponibilidad. No se debe exponer a redes públicas.
Para una fuente existente, usa `retail --root data/source kafka --topic ... --partition 0`.

## Fase 12 — Entender el camino de una fuente real

Lee `SOURCE_INTEGRATION.md`. Especifica tabla orders con clave estable, revisión
monótona, timestamptz e importes enteros; replica identity FULL para before. Configura
Debezium con JSON sin schemas y una extracción inicial completa.

Esa fase es un diseño de integración, no una ejecución ya validada en este proyecto.
No presentes la prueba del productor sintético como una prueba del conector PostgreSQL.
Antes de usarlo, añade una prueba que compare la tabla fuente real con current y que
verifique snapshot inicial, slot, retención WAL, updates, deletes y reinicio del conector.

## Fase 13 — Calidad, empaquetado y contenedor

```bash
make lint test
uv build
docker compose build app
docker compose run --rm app --root /app/data/container-demo demo --count 10
```

El contenedor se ejecuta como UID 10001 y guarda su estado en un volumen. No montar
un directorio de otro UID sin preparar permisos. La guía documenta estos comandos;
consulta VALIDATION para saber qué se ejecutó localmente y qué espera CI.

## Fase 14 — Preparar GitHub sin archivos generados

```bash
git init -b main
git add .
uv run --frozen python scripts/check_repo.py
git diff --cached --stat
git status --short
```

Si Git ya está inicializado, omite init. El checker examina los archivos del índice,
no borra nada y falla si detecta datos generados, archivos privados, tamaños superiores
a 2 MiB o ciertos patrones de secretos. Es una ayuda limitada: revisa también el diff.
`git archive` crea el paquete a partir del commit, por lo que no incorpora data, venv,
logs, coberturas ni cachés locales. Conserva README, pruebas y evidencia agregada.

## Fase 15 — Publicar y demostrar

Crea un repositorio vacío `retail-cdc-lakehouse` en GitHub. Desde el proyecto:

```bash
git add .
git commit -m "feat: add recoverable Iceberg CDC lakehouse"
git remote add origin https://github.com/NeilBorjasRamos/retail-cdc-lakehouse.git
git push -u origin main
```

Omite remote add si ya existe y verifica antes su destino con `git remote -v`.
El workflow ejecuta lint, higiene, tests, demo, Kafka real, wheel en entorno limpio e
imagen Docker. Solo añade una insignia verde y un enlace de CI cuando haya terminado
una ejecución real satisfactoria. Si falla, corrige la causa y conserva el enlace de
la ejecución que valida el commit final.

Descripción sugerida: “Recoverable CDC lakehouse with Apache Iceberg, revision history,
atomic snapshot publication and Kafka failure drills.” Topics sugeridos: data-engineering,
apache-iceberg, change-data-capture, kafka, python, duckdb y portfolio.

Usa `PORTFOLIO.md` para una demo de cinco minutos. Explica también límites, decisiones
y ampliaciones pendientes; la evidencia y el criterio técnico hacen defendible el proyecto.
