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
uv run --project python python/examples/kv_cluster.py
```

Ejemplo Python de reconfiguración con una sola traza `rxnet` continua:

```bash
uv run --project python python/examples/kv_reconfigure_trace.py
```

Genera:

- `python/var/python-reconfigure-trace/trace.bin`
- `python/var/python-reconfigure-trace/trace.html`

Shell interactiva Python genérica:

```bash
uv run --project python python/tools/raftsh.py
```

Comandos principales de la shell genérica:

- `status`: ver estado del clúster
- `leader`: ver líder actual
- `tick [N] [MS]`: avanzar la simulación
- `autotick [MS]`: configurar ticking periódico dirigido por la shell en milisegundos; el valor por defecto es `300` y `0` lo desactiva. Internamente la shell avanza el clúster en quanta menores y estables.
- `stop NODE`, `start NODE`, `restart NODE`: inyectar fallos y recuperación
- `members [NODE]`: ver la configuración estable o joint del clúster
- `addnode NODE`, `rmnode NODE`: pedir una reconfiguración Raft por joint consensus
- `log NODE`: inspeccionar el log local y ver qué entradas están `COMMITTED` o `UNCOMMITTED`
- `maxlog [ENTRIES]`: consultar o cambiar, por consenso, el umbral de compactación
- `events [NODE ...]`: ver telemetría reciente

Shell del ejemplo KV:

```bash
uv run --project python python/examples/kv_shell.py
```

Comandos adicionales del ejemplo KV:

- `set KEY VALUE`
- `get KEY [NODE]`
- `delete KEY`

La shell estándar es genérica. `set`, `get` y `delete` se registran desde el ejemplo KV y no forman parte del núcleo de la shell.

Demo Python independiente (aplicación separada que consume `raft-rx`):

```bash
uv run --project python/examples/demo_app raft-rx-demo --bind 127.0.0.1:7400
uv run --project python/examples/demo_app raft-rx-demo --join 127.0.0.1:7400 --bind 127.0.0.1:7402
```

Tests de la demo independiente:

```bash
uv run --project python/examples/demo_app --extra dev pytest -q
```

Empaquetado de la demo independiente:

```bash
cd python/examples/demo_app
uv build
```

Ejemplo C:

```bash
make -C c build/raft_kv_cluster
./c/build/raft_kv_cluster
```

Guía de usuario C (integración, ciclo de vida, referencia completa):

- [`c/docs/user-guide.md`](c/docs/user-guide.md)

Demo app C independiente (aplicación separada que consume `raft_rx`):

```bash
make -C c/examples/demo_app
./c/examples/demo_app/build/raft_kv_demo_app
```

La API C ya soporta reconfiguración consensuada con `raft_node_request_membership_change(...)`, persistencia de configuración estable/joint y quorum doble durante `joint consensus`.

Ejemplo C trazado con `rxnet` + export de una sola `trace.bin` continua:

```bash
make -C c build/raft_kv_cluster_trace
./c/build/raft_kv_cluster_trace
```

Genera:

- `c/var/c-trace-example/trace.bin`

Para abrir la traza:

```bash
python3 -m rxnet.tools.trace c/var/c-trace-example/trace.bin --report report.html --open
```

Artefacto de librería C:

```bash
make -C c build/libraft_rx.a
```

Variante trazada:

```bash
make -C c build/libraft_rx_trace.a
```
