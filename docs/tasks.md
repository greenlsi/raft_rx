# Tareas

## Documentación

- [x] Redactar requisitos del sistema.
- [x] Redactar diseño por capas y mapeo sobre `rxnet`.
- [x] Definir el alcance de la primera versión productiva.

## Estructura del proyecto

- [x] Crear árbol de código Python.
- [x] Crear árbol de código C.
- [x] Añadir README principal y comandos de build/test.

## Núcleo Raft compartido conceptualmente

- [x] Definir tipos de mensaje y entradas de log.
- [x] Definir semántica de términos, votos y commit.
- [x] Implementar reglas de validación de log.
- [x] Implementar recuperación desde almacenamiento persistente.

## Python

- [x] Crear paquete `raft_rx`.
- [x] Implementar reloj determinista para tests.
- [x] Implementar transporte en memoria.
- [x] Implementar persistencia por ficheros.
- [x] Implementar `TelemetrySink` no-op y JSONL.
- [x] Implementar máquina de estados clave-valor persistente.
- [x] Implementar nodo Raft sobre `rxnet.fsm`.
- [x] Implementar clúster de simulación.
- [x] Crear ejemplo ejecutable de base de datos distribuida.
- [x] Crear shell externa para inspección del clúster.
- [x] Añadir tests de elección, replicación y recuperación.

## C

- [x] Crear librería `raft`.
- [x] Implementar tipos públicos, límites y códigos de error.
- [x] Implementar transporte en memoria.
- [x] Implementar persistencia de referencia en ficheros.
- [ ] Implementar telemetría opcional a JSON Lines.
- [x] Implementar máquina de estados clave-valor persistente.
- [x] Implementar nodo Raft sobre `rxnet/fsm.h`.
- [x] Crear ejemplo de clúster de 3 nodos.
- [x] Añadir tests de elección, replicación y reinicio.

## Verificación

- [x] Ejecutar tests Python.
- [x] Compilar C.
- [x] Ejecutar tests C.
- [x] Verificar el ejemplo clave-valor en ambos lenguajes.
- [x] Actualizar la documentación con el estado final del entregable.
