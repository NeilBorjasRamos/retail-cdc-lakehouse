# Validación ejecutada

Fecha: 24 de octubre de 2026. Python 3.12.14, Linux x86_64. Todos los pedidos son ficticios.
No se descargaron ni publicaron datos de clientes o empresas reales.

## Evidencia local

| Verificación | Resultado |
| --- | --- |
| Tests pytest | 56 aprobados, sin omisiones |
| Cobertura de código fuente | 97.08 %, umbral requerido 85 % |
| Ruff lint y formato | Aprobados |
| Demo Iceberg de 1000 pedidos | 2467 transportes; 800 current; 2267 history; 4 gold |
| Reconciliación final | Cero diferencias en las cuatro tablas |
| Revisiones intermedias pendientes | Cero al terminar los tres lotes |
| Fallos entre commits | Probar después de cada tabla y antes de publicar; cursor anterior preservado |
| Salida abrupta | Subprocess os._exit(77), sin cleanup Python, con recuperación posterior |
| Lectura durante bloqueo de escritor | Publicación previa consultable sin esperar el lock |
| Evolución de esquema | region añadida nullable en Iceberg; consulta histórica conservada |
| Kafka real | Apache Kafka 3.9.1 KRaft nativo con Java 17, 2467 mensajes válidos en 7 publicaciones |
| Registro inválido en Kafka | Bloqueado; generación y siguiente offset conservados en 2467 |
| Wheel y sdist | Construidos; wheel instalado y demo ejecutada en entorno limpio |

La evidencia agregada se guarda en [local-demo.json](../evidence/local-demo.json),
[kafka-smoke.json](../evidence/kafka-smoke.json) y [checks.json](../evidence/checks.json).
Los snapshot IDs y el nombre único del topic varían en cada ejecución; los conteos y
totales conocidos son deterministas. Los archivos de warehouse no se incluyen.

## Totales esperados independientes

El generador asigna amount_minor=1000+17*i para i=1..1000, alterna PEN/USD y elimina
los múltiplos de 5. Quedan 400 pedidos por moneda, con suma 3800000 unidades menores
en cada una. Los tests y el smoke comparan estos valores constantes; no obtienen
el valor esperado consultando el mismo gold que intentan verificar.

Además de esas constantes, report reconstruye el modelo desde el ledger publicado y
lo compara con las cuatro tablas fijadas. Esta es conciliación interna: la certificación
de una fuente PostgreSQL real requiere la comparación externa descrita en SOURCE_INTEGRATION.

## Lo que no se ejecutó o no se afirma

Docker no está disponible en el entorno local de construcción: el archivo Compose y
la imagen están incluidos, pero no se presenta una ejecución Docker local satisfactoria.
El broker usado para la prueba fue una distribución Kafka nativa. El workflow de GitHub
incluye Compose, build y ejecución de la imagen; su éxito debe confirmarse cuando el
proyecto se publique y se ejecute Actions. No hay enlace de CI remoto verificado aún.

Tampoco se probó PostgreSQL → Debezium, REST catalog, almacenamiento cloud, múltiples
escritores distribuidos, backup/restore completo, pérdida eléctrica, rendimiento sostenido
o una política de GC. La historia es por revisión, no bitemporal; la demo no demuestra
exactly-once extremo a extremo.

La suite emite advertencias de deprecación de PyIceberg con versiones transitivas de
pyparsing/Pydantic fijadas por uv.lock. No son fallos de las pruebas. Actualizaciones
de dependencias deben repetir la suite y los drills, sin ocultar nuevas incompatibilidades.

## Reproducir

```bash
uv sync --frozen --extra dev
make lint test
uv run --frozen retail --root data/validation-new demo --count 1000
docker compose up -d --wait kafka
SMOKE_ROOT=data/kafka-validation-new uv run --frozen python scripts/kafka_smoke.py
uv build
```

En un checkout Git, ejecutar también `make hygiene`. Al extraer un ZIP, inicializar Git
y añadir el código revisado al índice como indica la fase 14 antes de usar ese checker.
No sustituir los archivos de evidencia del repositorio por logs con datos privados.
