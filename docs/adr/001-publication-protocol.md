# ADR 001 — Publicación por manifest de snapshots

Estado: aceptada para la implementación local.

Contexto: cuatro commits Iceberg independientes no forman una transacción multi-tabla.
Un rollback manual después de cada fallo introduce nuevas ventanas de fallo y no
resuelve lectores que ya hayan observado tablas intermedias.

Decisión: almacenar una publicación inmutable con los cuatro snapshot IDs y cursores.
Su inserción transaccional y compare-and-swap constituye el punto de visibilidad. Los
lectores capturan una publicación; los escritores no alteran filas publicadas.

Consecuencias: los candidatos incompletos pueden quedarse en el catálogo sin hacerse
visibles mediante la API. Leer latest directamente rompe el protocolo. Se necesita
retener todos los snapshots alcanzables desde publicaciones históricas. El servicio
de publicación actual es SQLite de un host, no coordinación distribuida.

Validación: fallos después de cada tabla y después de validación, candidato gold
incorrecto y salida abrupta de subprocess; la publicación y sus offsets no avanzan.
