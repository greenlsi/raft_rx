# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-only

from __future__ import annotations

import time
from dataclasses import dataclass


class Clock:
    def now_ms(self) -> int:
        raise NotImplementedError


@dataclass(slots=True)
class ManualClock(Clock):
    value_ms: int = 0

    def now_ms(self) -> int:
        return self.value_ms

    def advance(self, delta_ms: int) -> None:
        self.value_ms += delta_ms


class SystemClock(Clock):
    def now_ms(self) -> int:
        return int(time.monotonic() * 1000)
