# Presentación a un reclutador técnico

## Problema y demostración en cinco minutos

Explica que las tablas están relacionadas pero sus commits no son una sola transacción.
Muestra el flujo de publicación y la diferencia entre último snapshot y última generación
validada. Ejecuta demo con una raíz nueva y presenta los resultados agregados.

Después consulta generación 1: conserva 1000 pedidos aunque la generación final tenga
800. Señala el test que termina un proceso entre commits y la prueba de Kafka real.
Muestra cómo un evento fuera de contrato bloquea el cursor. Cierra con dos límites
concretos: full overwrite en memoria y coordinación de un único host.

## Archivos para la revisión

| Pregunta de entrevista | Dónde demostrarla |
| --- | --- |
| ¿Qué ocurre entre dos commits? | engine.py, storage.py y ADR 001 |
| ¿Puede repetirse una entrega? | tests/test_engine.py y claves de transporte/revisión |
| ¿Qué pasa con un evento tardío? | model.py y test_schema_evolution_and_late_revision |
| ¿Se probó una caída real de proceso? | tests/test_process_recovery.py, os._exit(77) |
| ¿Cómo sabes que gold es correcto? | Candidato leído del disco, report y totales conocidos en tests |
| ¿Es realmente distribuido? | SCALING y límite explícito de un escritor local |
| ¿Son datos reales de comercio? | No; generator.py produce IDs y pedidos ficticios |
| ¿Está probado Debezium contra PostgreSQL? | No; SOURCE_INTEGRATION delimita ese trabajo pendiente |

## Descripción sugerida para el portafolio

“Motor CDC de referencia con Apache Iceberg y Kafka, historial de revisiones, evolución
de esquema y publicación consistente por snapshots. Incluye pruebas de recuperación
entre commits, salida abrupta de proceso, conflictos de repetición y conciliación.”

Al explicar tu participación, usa afirmaciones que puedas demostrar y modifica el
proyecto con tus propias decisiones. No atribuirse operación de clientes reales,
millones de transacciones por segundo ni experiencia productiva a partir de esta demo.
Conocer las limitaciones forma parte de la competencia que el proyecto busca mostrar.

## Ejercicios para apropiarte del proyecto

Implementa una consulta por revisión de un pedido, añade una prueba de reconstrucción
desde Bronze y después incorpora una fuente PostgreSQL real. Justifica cómo cambian
las garantías antes de introducir un catálogo remoto o varios escritores. Registra
cada decisión en un ADR y acompaña cada nueva garantía con un escenario de fallo.
