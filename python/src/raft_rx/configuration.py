from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ClusterConfiguration:
    old_members: tuple[str, ...]
    new_members: tuple[str, ...] | None = None
    index: int = 0

    def is_joint(self) -> bool:
        return self.new_members is not None

    def stable_members(self) -> tuple[str, ...]:
        return self.old_members

    def all_members(self) -> tuple[str, ...]:
        if self.new_members is None:
            return self.old_members
        members = list(self.old_members)
        for member in self.new_members:
            if member not in members:
                members.append(member)
        return tuple(members)

    def mode(self) -> str:
        return "joint" if self.is_joint() else "stable"

    def contains(self, node_id: str) -> bool:
        return node_id in self.all_members()

    @staticmethod
    def normalize_members(members: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        normalized: list[str] = []
        for member in members:
            if member and member not in normalized:
                normalized.append(member)
        return tuple(normalized)

    @classmethod
    def stable(cls, members: list[str] | tuple[str, ...], index: int = 0) -> "ClusterConfiguration":
        return cls(old_members=cls.normalize_members(members), new_members=None, index=index)

    @classmethod
    def joint(
        cls,
        old_members: list[str] | tuple[str, ...],
        new_members: list[str] | tuple[str, ...],
        index: int,
    ) -> "ClusterConfiguration":
        return cls(
            old_members=cls.normalize_members(old_members),
            new_members=cls.normalize_members(new_members),
            index=index,
        )
