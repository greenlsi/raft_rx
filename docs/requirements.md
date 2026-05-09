# Requisitos

## Objetivo

Construir una librería Raft sobre `../rxnet` en C y en Python, orientada a:

- sistemas empotrados distribuidos con restricciones de dependencias,
- aplicaciones de terminal en macOS y Linux,
- integración opcional con herramientas externas de observabilidad,
- ejecución determinista basada en ticks y máquinas de estados.

El resultado debe incluir un ejemplo funcional de base de datos clave-valor distribuida con persistencia.

## Alcance funcional

La librería debe implementar el núcleo del algoritmo Raft:

- elección de líder,
- heartbeats y mantenimiento de liderazgo,
- replicación de log,
- confirmación por mayoría,
- aplicación ordenada de entradas a una máquina de estados,
- persistencia de `current_term`, `voted_for` y del log,
- recuperación tras reinicio desde almacenamiento persistente,
- redirección o rechazo explícito de peticiones de cliente si el nodo no es líder.

## Requisitos de arquitectura

### R1. Modelo reactivo sobre `rxnet`

Cada nodo Raft debe modelarse como una máquina de estados `rxnet` con, al menos, estos roles:

- `FOLLOWER`
- `CANDIDATE`
- `LEADER`

Las transiciones entre roles deben estar gobernadas por eventos latcheados en el tick actual:

- expiración de timeout de elección,
- recepción de heartbeat o `AppendEntries`,
- recepción de votos,
- descubrimiento de un término superior.

### R2. Separación por capas

La solución debe separar claramente:

- núcleo del protocolo Raft,
- transporte,
- persistencia,
- máquina de estados de aplicación,
- observabilidad.

El núcleo no debe depender de una implementación concreta de red, shell o UI.

### R3. Compatibilidad C y Python

Debe existir una implementación en C y otra en Python con semántica alineada en:

- tipos de mensaje,
- reglas de transición de término y voto,
- reglas de validación de log,
- semántica de commit,
- interfaz de transporte,
- interfaz de persistencia,
- API de integración con una máquina de estados.

No es obligatorio compartir binarios ni ABI, pero sí el modelo conceptual y el comportamiento observable.

## Requisitos de transporte

### R4. Transporte abstracto

La librería debe definir una interfaz abstracta de transporte con capacidad para:

- enviar mensajes Raft a un peer,
- recibir todos los mensajes pendientes para un nodo en un tick,
- inyectar peticiones de cliente,
- operar sin asignación dinámica en los caminos críticos del runtime C cuando sea posible.

### R5. Transporte de referencia

Debe incluirse al menos un transporte de referencia determinista y portable:

- transporte en memoria para simulación, tests y ejemplo local.

Además, las demos de proceso independiente deben demostrar que el núcleo no
depende del bus en memoria:

- C: transporte TCP enchufable con tabla de peers persistente.
- Python: transporte HTTP en la demo externa, implementado con librería estándar.

Los transportes de host deben poder añadirse sin modificar el núcleo Raft.

## Requisitos de persistencia

### R6. Estado persistente mínimo

Cada nodo debe persistir de forma duradera:

- `current_term`,
- `voted_for`,
- el log Raft,
- el estado aplicado de la máquina de estados del ejemplo clave-valor.

### R7. Escritura segura

La persistencia en host debe usar escrituras atómicas o cuasi-atómicas razonables:

- fichero temporal + `rename`/`replace`,
- formato legible y auditable,
- recuperación robusta ante reinicios limpios.

Para objetivos empotrados, la API debe permitir sustituir esta persistencia por otra adaptada a flash, NVRAM o almacenamiento específico.

## Requisitos de la máquina de estados de aplicación

### R8. API de máquina de estados

Debe existir una interfaz de aplicación con estas capacidades:

- validar o aceptar comandos serializados,
- aplicar entradas committed en orden,
- consultar estado visible para cliente,
- cargar y guardar estado persistente.

### R9. Ejemplo clave-valor

Debe entregarse una máquina de estados de ejemplo con operaciones:

- `set(key, value)`
- `delete(key)`
- `get(key)`

`get` puede resolverse localmente sobre el estado aplicado. `set` y `delete` deben entrar en el log del líder.

## Requisitos de observabilidad

### R10. Observabilidad opcional y externa

La observabilidad debe ser totalmente opcional. Si no se activa, el núcleo Raft no debe depender de shell, TUI o librerías visuales.

### R11. Eventos estructurados

Cuando se active, cada nodo debe poder emitir eventos estructurados externos, al menos para:

- cambio de rol,
- cambio de término,
- voto emitido o recibido,
- recepción y envío de RPCs,
- append y commit de entradas,
- aplicación de comandos a la máquina de estados,
- errores de persistencia o transporte.

### R12. Shell externa

Debe incluirse una shell externa básica, cómoda y extensible para Python. Su función mínima será:

- visualizar nodos, roles y términos,
- ver longitud de log y `commit_index`,
- mostrar eventos recientes,
- consultar el estado clave-valor del clúster de ejemplo.

Las demos de proceso independiente pueden incorporar una CLI local como FSM del
mismo runtime siempre que el núcleo se mantenga separado de esa UI.

## Requisitos no funcionales

### R13. Portabilidad

El código C debe compilar al menos en:

- macOS,
- Linux.

La arquitectura debe mantenerse apta para portar a plataformas empotradas.

### R14. Dependencias

- C: sin dependencias externas obligatorias aparte de `rxnet` y la libc estándar.
- Python: solo librería estándar más `rxnet` local.
- La shell externa no debe introducir dependencias en el runtime embebido.

### R15. Determinismo y testabilidad

La implementación debe poder ejecutarse de forma determinista en tests:

- reloj controlable,
- transporte en memoria,
- inyección explícita de timeouts,
- verificación del estado de clúster y del log.

### R16. Calidad de producción

El entregable debe incluir:

- documentación de requisitos, diseño y tareas,
- tests automatizados en C y Python,
- ejemplos ejecutables,
- manejo explícito de errores,
- API pública clara.

## Criterios de aceptación

Se considera completado cuando:

1. `docs/requirements.md`, `docs/design.md` y `docs/tasks.md` describen de forma coherente el sistema.
2. Existe una librería Python usable y testeada.
3. Existe una librería C usable y compilable.
4. Un clúster de 3 nodos en memoria elige líder y replica operaciones clave-valor.
5. El estado persiste y puede recuperarse tras reinicio.
6. La observabilidad puede activarse o desactivarse sin afectar al núcleo.
7. Existen demos independientes en C y Python que arrancan nodos en procesos
   separados y permiten reconfiguración de membresía.
