# raft-rx C demo app

Cada nodo Raft corre como un proceso independiente y se comunica con los demás
por TCP/IP usando una capa de transporte enchufable (`raft_transport_t`).

La CLI de cada nodo es una `rx_fsm_machine` registrada en el mismo runtime
cooperativo que el nodo Raft.  El `main()` solo crea máquinas y llama a
`rx_coop_exec_run()` — el hilo principal es el único hilo de cómputo; el único
hilo de fondo es el listener TCP.

## Build

```bash
make -C c/examples/demo_app
# resultado: c/examples/demo_app/build/raft_node
```

## Cluster inicial (3 nodos)

Abre tres terminales:

```bash
# Terminal 1
./build/raft_node --id n1 --port 5001 \
  --member n1 --member n2 --member n3 \
  --peer n2=127.0.0.1:5002 --peer n3=127.0.0.1:5003

# Terminal 2
./build/raft_node --id n2 --port 5002 \
  --member n1 --member n2 --member n3 \
  --peer n1=127.0.0.1:5001 --peer n3=127.0.0.1:5003

# Terminal 3
./build/raft_node --id n3 --port 5003 \
  --member n1 --member n2 --member n3 \
  --peer n1=127.0.0.1:5001 --peer n2=127.0.0.1:5002
```

## Añadir un nodo (`--join HOST:PORT`)

El nodo que quiere unirse solo necesita la dirección de **un** nodo existente.
No hace falta `-—peer` ni `--member` en el nuevo nodo:

```bash
# Terminal 4
./build/raft_node --id n4 --port 5004 --join 127.0.0.1:5001
```

**Qué ocurre internamente:**

1. n4 envía un *join-request* (con su `id`, `host`, `port`) a n1.
2. n1 recibe la solicitud y la **inunda** a todos sus peers (n2, n3) con el
   flag `forwarded=1` para que no la reenvíen de nuevo.
3. Cada nodo añade n4 a su lista de peers locales (para poder enviarle mensajes
   cuando sea necesario).
4. El nodo que en ese momento sea **líder** aplica el cambio de membresía vía
   Raft.  Si ningún nodo es aún líder, la solicitud queda pendiente hasta que
   se elija uno.
5. El líder envía `AppendEntries` a n4, que pasa a participar en el consenso.

Los nodos existentes **no necesitan reiniciarse** para añadir un nuevo miembro.

## Eliminar un nodo

En el nodo líder:

```bash
n1> leave          # el nodo actual abandona el cluster (solo si es líder)
n1> rmnode n4      # expulsa n4 del cluster (solo si es líder)
```

Si este nodo no es el líder, usa `rmnode` en el que sí lo sea:

```bash
n2> rmnode n4
```

## Comandos CLI

| Comando          | Descripción                                  |
|------------------|----------------------------------------------|
| `set KEY VALUE`  | Envía set al líder (solo en líder)           |
| `get KEY`        | Lee KEY del estado KV local                  |
| `delete KEY`     | Envía delete al líder (solo en líder)        |
| `status`         | Estado del nodo local                        |
| `leader`         | Quién es el líder actual                     |
| `stop`           | Simula caída del nodo local                  |
| `start`          | Simula reinicio del nodo local               |
| `members`        | Configuración de membresía actual            |
| `log`            | Entradas del log Raft                        |
| `addnode NODE`   | Añade NODE al cluster (solo líder)           |
| `rmnode NODE`    | Elimina NODE del cluster (solo líder)        |
| `leave`          | Este nodo abandona el cluster (solo líder)   |
| `help`           | Ayuda                                        |
| `quit`           | Salir                                        |

## Relanzar un nodo

Una vez que el nodo ha arrancado al menos una vez, todo su estado persiste en
disco (`var/raft/<id>/`).  Solo necesita el `--id`:

```bash
./build/raft_node --id n1
```

Puerto, host, peers y membresía se recuperan automáticamente de `self.txt` y
`peers.txt`.

## Opciones del ejecutable

| Opción                  | Primera vez | Reinicio | Descripción                                      |
|-------------------------|:-----------:|:--------:|--------------------------------------------------|
| `--id NAME`             | Requerido   | Requerido| Identificador del nodo                           |
| `--port PORT`           | Requerido   | Opcional | Puerto TCP de escucha (persiste en `self.txt`)   |
| `--host HOST`           | Opcional    | Opcional | IP propia anunciada (defecto: 127.0.0.1)         |
| `--data DIR`            | Opcional    | Opcional | Directorio de datos (defecto: `var/raft`)        |
| `--member ID`           | Requerido   | Omitir   | Miembro inicial del cluster (repetible)          |
| `--peer ID=HOST:PORT`   | Requerido   | Omitir   | Dirección de un peer (repetible; persiste)       |
| `--join HOST:PORT`      | Para unirse | —        | Unirse al cluster vía ese introductor            |

## Limpieza

```bash
make -C c/examples/demo_app clean
```
