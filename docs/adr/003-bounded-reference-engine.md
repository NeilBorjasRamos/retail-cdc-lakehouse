# ADR 003 — Motor acotado antes de distribuir cómputo

Estado: aceptada para v0.1.0.

Contexto: una demo con muchos servicios puede parecer avanzada sin demostrar recuperación.
Se necesita un modelo ejecutable y revisable que sirva como referencia para una futura
implementación distribuida.

Decisión: usar un escritor cooperante, tablas Iceberg reales, SQLite local, Python/Arrow,
full overwrite y límites explícitos. Separar contrato, transformación pura, persistencia,
publicación, transporte y serving. Añadir Kafka real como integración verificable, sin
atribuirle al generador una extracción de PostgreSQL.

Consecuencias: la suite reproduce fallos con pocos recursos y compara resultados exactos.
El coste crece con el ledger retenido y no constituye un servicio de alto volumen. El
límite de 100000 eventos evita crecimiento inadvertido; alcanzarlo exige migración o
nuevo corte con cobertura verificada. No elimina historia automáticamente.

Alternativa futura: SCALING detalla REST catalog, object storage, coordinación con
fencing y cómputo incremental. No se habilitan varios escritores con el bloqueo local.
