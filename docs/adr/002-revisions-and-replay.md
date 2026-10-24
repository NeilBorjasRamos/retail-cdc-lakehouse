# ADR 002 — Revisión lógica y dos identidades de repetición

Estado: aceptada.

Contexto: un offset identifica entrega, no versión de negocio. El mismo estado puede
recibirse dos veces en diferentes offsets; una corrección tardía puede tener revisión
menor que la ya vigente. Un timestamp puede estar atrasado o corregirse retroactivamente.

Decisión: deduplicar el transporte por topic/partition/offset y contenido canónico;
deduplicar estado por order_id/revision y contenido de negocio normalizado. Ordenar
historial por revision y diagnosticar huecos sin inventar versiones. Bloquear conflictos
y cambios de partición de una clave. Una revisión borrada prohíbe reutilización de ID.

Consecuencias: se requiere una revisión monótona en origen; no es un adaptador universal
para cualquier tabla Debezium. No hay semántica bitemporal ni resolución last-write-wins
de conflictos. El orden de updated_at no sustituye a revision. Se conserva el transporte
para auditoría incluso cuando no crea otro estado lógico.

Validación: duplicados semánticos, conflictos de versión/coordenada, versión 3 antes de
2, tombstones y prohibición de reinserción de identidad eliminada.
