# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: MIT

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class TelemetrySink:
    def emit(self, event: dict[str, Any]) -> None:
        raise NotImplementedError


class NullTelemetrySink(TelemetrySink):
    def emit(self, event: dict[str, Any]) -> None:
        del event


class JsonlTelemetrySink(TelemetrySink):
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
