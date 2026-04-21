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

Ejemplo C:

```bash
make -C c build/raft_kv_cluster
./c/build/raft_kv_cluster
```

Artefacto de librería C:

```bash
make -C c build/libraft_rx.a
```
