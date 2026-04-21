# raft_rx

Implementación de Raft sobre `rxnet` en C y Python, con:

- núcleo del consenso modelado como FSM `rxnet`,
- persistencia,
- transporte abstracto,
- telemetría opcional,
- ejemplo de base de datos clave-valor distribuida.

La primera implementación de referencia usa un transporte en memoria determinista para tests y ejemplos locales. La observabilidad se expone como eventos JSONL consumibles por una shell externa.

## Comandos

Tests de todo el proyecto:

```bash
make test
```

Ejemplo Python:

```bash
PYTHONPATH=python/src:../rxnet/python python3 python/examples/kv_cluster.py
```

Shell interactiva Python genérica:

```bash
PYTHONPATH=python/src:../rxnet/python python3 python/tools/raftsh.py
```

Comandos principales de la shell genérica:

- `status`: ver estado del clúster
- `leader`: ver líder actual
- `tick [N] [MS]`: avanzar la simulación
- `autotick [MS]`: configure periodic shell-driven ticking in milliseconds; default is `300`, and `0` disables it. Internally the shell advances the cluster in smaller stable tick quanta.
- `stop NODE`, `start NODE`, `restart NODE`: inyectar fallos y recuperación
- `events [NODE ...]`: ver telemetría reciente

Shell del ejemplo KV:

```bash
PYTHONPATH=python/src:../rxnet/python python3 python/examples/kv_shell.py
```

Comandos adicionales del ejemplo KV:

- `set KEY VALUE`
- `get KEY [NODE]`
- `delete KEY`

`add_node` no está implementado todavía porque requiere reconfiguración de membresía Raft.

Ejemplo C:

```bash
make -C c build/raft_kv_cluster
./c/build/raft_kv_cluster
```

Artefacto de librería C:

```bash
make -C c build/libraft_rx.a
```
