from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: raftsh.py NODE_EVENT.jsonl [...]", file=sys.stderr)
        return 1

    latest = {}
    recent = defaultdict(list)

    for arg in argv[1:]:
        path = Path(arg)
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            node_id = event["node_id"]
            latest[node_id] = event
            recent[node_id].append(event)
            recent[node_id] = recent[node_id][-5:]

    for node_id in sorted(latest):
        event = latest[node_id]
        print(
            f"{node_id:>4}  role={event['role']:<9} term={event['term']:<3} "
            f"event={event['event']}"
        )
        for item in recent[node_id]:
            print(f"      t={item['time_ms']:<5} {item['event']:<14} {item['payload']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
