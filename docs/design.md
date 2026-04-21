# Diseño

## Resumen

La solución se organiza en cinco capas:

1. `core`: reglas del protocolo Raft y modelado del nodo como FSM `rxnet`.
2. `transport`: entrega de mensajes y peticiones de cliente.
3. `storage`: persistencia de metadatos Raft, log y estado aplicado.
4. `state_machine`: lógica de aplicación; en este proyecto, un almacén clave-valor.
5. `telemetry`: emisión opcional de eventos estructurados para una shell externa.

Las implementaciones C y Python comparten la misma semántica, aunque no el mismo layout interno.

## Modelo de nodo

Cada nodo mantiene:

- `node_id`
- `peers`
- `current_term`
- `voted_for`
- `log`
- `commit_index`
- `last_applied`
- `role`
- `leader_id`
- `election_deadline_ms`
- `heartbeat_deadline_ms`
- `next_index[peer]`
- `match_index[peer]`

La FSM `rxnet` modela el rol y los eventos principales de Raft:

- `FOLLOWER`
- `CANDIDATE`
- `LEADER`

El resto del estado vive en la estructura de usuario del nodo y se actualiza en callbacks de fase.

La tabla principal de transición es:

- `FOLLOWER -- timeout_expired --> CANDIDATE / become_candidate`
- `FOLLOWER -- has_append_entries --> FOLLOWER / handle_append_entries`
- `FOLLOWER -- has_vote_request --> FOLLOWER / handle_vote_request`
- `FOLLOWER -- has_vote --> FOLLOWER / ignore_vote`
- `CANDIDATE -- timeout_expired --> FOLLOWER / back_to_follower_due_to_timeout`
- `CANDIDATE -- received_majority_votes --> LEADER / become_leader`
- `CANDIDATE -- has_append_entries --> FOLLOWER / handle_append_entries`
- `CANDIDATE -- has_vote_request --> CANDIDATE / handle_vote_request`
- `CANDIDATE -- has_vote --> CANDIDATE / handle_vote`
- `LEADER -- has_append_entries --> FOLLOWER / handle_append_entries`
- `LEADER -- has_vote_request --> LEADER / ignore_vote_request`
- `LEADER -- has_vote --> LEADER / ignore_vote`
- `LEADER -- time_for_heartbeat --> LEADER / send_heartbeat`

Las colas de eventos se drenan en `latch_inputs` y los guards de la FSM consultan si hay eventos pendientes de cada tipo.

## Mapeo sobre fases `rxnet`

### Latch inputs

En `latch_inputs` el nodo:

- lee la hora actual desde un reloj abstracto,
- drena mensajes entrantes del transporte,
- clasifica RPCs y respuestas en colas pendientes,
- detecta si hay timeout de elección o de heartbeat,
- acumula comandos de cliente pendientes,
- calcula flags de transición para la FSM.

En esta fase también se preparan buffers de salida y se marcan escrituras persistentes pendientes.

### Evaluate

`rxnet` evalúa la primera transición habilitada según el orden de la tabla anterior.
Esto fuerza una semántica clara basada en eventos Raft, no en callbacks laterales.

### Commit

La FSM publica el nuevo rol y ejecuta una acción diferida asociada a la transición:

- `become_candidate`: incrementa término, vota por sí mismo y envía `RequestVote`,
- `become_leader`: inicializa índices de replicación y envía heartbeat inmediato,
- `back_to_follower_due_to_timeout`: abandona la candidatura actual y rearma timeout,
- `handle_append_entries`: procesa el RPC del líder,
- `handle_vote_request`: decide concesión de voto,
- `handle_vote`: acumula votos de una elección activa,
- `send_heartbeat`: envía `AppendEntries` vacío,
- `ignore_vote` y `ignore_vote_request`: consumen eventos no relevantes para ese rol.

### Deferred actions

Las acciones diferidas se usan solo para cambios de rol y envíos iniciales asociados a la transición. Esto mantiene la separación entre decisión de estado y efectos laterales.

### Dump outputs

En `dump_outputs` el nodo:

- persiste metadatos y log si están sucios,
- envía mensajes pendientes,
- avanza la replicación del líder,
- aplica entradas committed a la máquina de estados,
- emite telemetría opcional.

## Tipos de mensaje

Se definen cuatro tipos básicos:

- `REQUEST_VOTE`
- `REQUEST_VOTE_RESPONSE`
- `APPEND_ENTRIES`
- `APPEND_ENTRIES_RESPONSE`

Y un tipo auxiliar interno para clientes:

- `CLIENT_COMMAND`

Cada mensaje contiene:

- `term`
- `source`
- `target`
- campos específicos del RPC

`APPEND_ENTRIES` transporta cero o más entradas. Cero entradas equivale a heartbeat.

## Log y commit

Cada entrada de log contiene:

- `term`
- `index`
- `command`

El líder:

- acepta comandos de cliente,
- los añade al log local,
- replica mediante `AppendEntries`,
- actualiza `commit_index` cuando la entrada está replicada por mayoría en el término actual.

Los followers:

- validan `prev_log_index` y `prev_log_term`,
- corrigen conflictos truncando el sufijo incompatible,
- añaden las nuevas entradas válidas,
- actualizan `commit_index` según `leader_commit`.

## Persistencia

Se usan dos abstracciones:

- `RaftStorage`: estado persistente del protocolo.
- `StateMachineStorage`: persistencia del estado aplicado de la aplicación.

En host, la persistencia de referencia se implementa con ficheros por nodo:

- `meta.json` o `meta.txt`
- `log.jsonl` o `log.txt`
- `kv.json` o `kv.txt`

Las escrituras de metadatos y de la máquina de estados se hacen mediante fichero temporal y reemplazo atómico. El log puede reescribirse entero en esta primera versión para simplificar robustez y trazabilidad.

## Transporte

La interfaz de transporte ofrece:

- `send(message)`
- `recv_for(node_id) -> mensajes`
- `submit_client_command(node_id, command)`
- `recv_client_commands(node_id)`

La implementación de referencia es un bus en memoria, determinista y sin hilos, útil para:

- tests,
- simulación,
- ejemplo de base de datos distribuida en un solo proceso.

La API deja espacio para transportes futuros sobre UDP, sockets Unix o colas RTOS.

## Reloj

El tiempo no se lee directamente del sistema dentro del core. Se abstrae mediante un reloj:

- Python: `Clock.now_ms()`
- C: callback o campo actualizado por el integrador

Esto permite tests deterministas y despliegues empotrados con timers propios.

## Observabilidad

La observabilidad se implementa con un `TelemetrySink` opcional. Si es `NULL` o `NoOp`, no existe coste de dependencia externa.

Los eventos se emiten como estructuras ligeras y pueden serializarse como JSON Lines para ser consumidos por una shell externa.

La shell de referencia:

- es un proceso independiente,
- lee eventos desde ficheros JSONL,
- presenta una vista agregada del clúster,
- no se enlaza con la aplicación C ni forma parte del runtime embebido.

## API pública propuesta

### Python

- `raft_rx.application`
- `raft_rx.clock`
- `raft_rx.messages`
- `raft_rx.storage`
- `raft_rx.transport`
- `raft_rx.telemetry`
- `raft_rx.node`
- `raft_rx.cluster`

### C

- `include/raft/raft.h`
- `include/raft/kv.h`
- `include/raft/telemetry.h`
- `include/raft/memory_transport.h`
- `include/raft/file_storage.h`

## Diferencias deliberadas entre C y Python

Python prioriza:

- ergonomía,
- simulación,
- shell externa,
- inspección fácil del estado.

C prioriza:

- API explícita,
- portabilidad a entornos restringidos,
- buffers y capacidades fijas razonables,
- ausencia de dependencias adicionales.

La semántica de protocolo debe mantenerse alineada, aunque la representación interna no sea idéntica.
