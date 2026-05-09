#!/usr/bin/env python3
# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-only

"""
fsm_draw.py

Generate state-machine diagrams from Python source code.

Designed for rxnet-style FSMs where transitions are declared as:\n\n    Transition(from_state, to_state, guard, action, label)\n\nor with keywords:\n\n    Transition(\n        from_state=WAITING_PLAYERS,\n        to_state=CONFIGURING_GAME,\n        guard=enough_players,\n        action=configure,\n        label="players_ready",\n    )\n\nIt also supports the older educational 4-tuple style:\n\n    (from_state, guard, to_state, action)

The script exports DOT files and, if Graphviz is installed, renders SVG/PNG/PDF.

Usage:

    python fsm_draw.py path/to/rxnet --out diagrams --format svg
    python fsm_draw.py path/to/file.py --out diagrams --format png
    python fsm_draw.py path/to/rxnet --only raft --format svg

Requirements:

    pip install graphviz

System dependency for rendering PNG/SVG/PDF:

    macOS: brew install graphviz
    Ubuntu/Debian: sudo apt install graphviz

If Graphviz system binaries are not installed, the script still writes .dot files.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class TransitionEdge:
    source: str
    target: str
    guard: str
    action: str
    name: str
    file: Path
    line: int


@dataclass
class Machine:
    name: str
    file: Path
    transitions: list[TransitionEdge] = field(default_factory=list)

    @property
    def safe_name(self) -> str:
        value = re.sub(r"[^A-Za-z0-9_]+", "_", self.name).strip("_")
        return value or "fsm"


def expr_to_label(node: ast.AST | None) -> str:
    """Convert an AST expression into a compact human-readable label."""
    if node is None:
        return "None"

    if isinstance(node, ast.Constant):
        return str(node.value) if isinstance(node.value, str) else repr(node.value)

    if isinstance(node, ast.Name):
        return node.id

    if isinstance(node, ast.Attribute):
        base = expr_to_label(node.value)
        return f"{base}.{node.attr}" if base else node.attr

    if isinstance(node, ast.Call):
        func = expr_to_label(node.func)
        return f"{func}()"

    if isinstance(node, ast.Lambda):
        return "lambda"

    try:
        return ast.unparse(node)
    except Exception:
        return node.__class__.__name__


def looks_like_state(label: str) -> bool:
    if not label:
        return False
    if label.isdigit():
        return False
    if len(label) > 100:
        return False
    return True


def extract_transition_from_call(node: ast.Call, file: Path) -> Optional[TransitionEdge]:
    """Extract Transition(from_state, to_state, guard, action, label)."""
    callee = expr_to_label(node.func)
    if callee.split(".")[-1] != "Transition":
        return None

    # Positional form: Transition(from_state, to_state, guard, action, label)
    source_node: ast.AST | None = None
    target_node: ast.AST | None = None
    guard_node: ast.AST | None = None
    action_node: ast.AST | None = None
    name_node: ast.AST | None = None

    if len(node.args) >= 5:
        source_node, target_node, guard_node, action_node, name_node = node.args[:5]
    else:
        # Keyword form matching the actual rxnet Transition class.
        #
        # class Transition:
        #     from_state: int
        #     to_state: int
        #     guard: Guard | None = None
        #     action: Action | None = None\n        #     label: str | None = None\n        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}\n        source_node = kwargs.get("from_state")\n        target_node = kwargs.get("to_state")\n        guard_node = kwargs.get("guard")\n        action_node = kwargs.get("action")\n        name_node = kwargs.get("label")

    if source_node is None or target_node is None:
        return None

    source = expr_to_label(source_node)
    target = expr_to_label(target_node)
    guard = expr_to_label(guard_node)
    action = expr_to_label(action_node)
    name = expr_to_label(name_node)

    if not (looks_like_state(source) and looks_like_state(target)):
        return None

    return TransitionEdge(
        source=source,
        target=target,
        guard=guard,
        action=action,
        name=name,
        file=file,
        line=getattr(node, "lineno", 0),
    )


def extract_transition_from_tuple(node: ast.Tuple | ast.List, file: Path) -> Optional[TransitionEdge]:
    """Fallback: extract educational tuple (origen, guarda, destino, accion)."""
    if len(node.elts) != 4:
        return None

    source = expr_to_label(node.elts[0])
    guard = expr_to_label(node.elts[1])
    target = expr_to_label(node.elts[2])
    action = expr_to_label(node.elts[3])

    if not (looks_like_state(source) and looks_like_state(target)):
        return None

    return TransitionEdge(
        source=source,
        target=target,
        guard=guard,
        action=action,
        name="",
        file=file,
        line=getattr(node, "lineno", 0),
    )


def list_contains_transitions(node: ast.AST, file: Path) -> list[TransitionEdge]:
    """Return transitions found directly inside a list/tuple literal."""
    if not isinstance(node, (ast.List, ast.Tuple)):
        return []

    transitions: list[TransitionEdge] = []
    for elt in node.elts:
        if isinstance(elt, ast.Call):
            transition = extract_transition_from_call(elt, file)
            if transition:
                transitions.append(transition)
        elif isinstance(elt, (ast.Tuple, ast.List)):
            transition = extract_transition_from_tuple(elt, file)
            if transition:
                transitions.append(transition)
    return transitions


def assignment_name(target: ast.AST) -> Optional[str]:
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    if isinstance(target, ast.Subscript):
        return expr_to_label(target)
    return None


def attach_parents(tree: ast.AST) -> None:
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            setattr(child, "_parent", parent)


def enclosing_class_name(node: ast.AST) -> Optional[str]:
    parent = getattr(node, "_parent", None)
    while parent is not None:
        if isinstance(parent, ast.ClassDef):
            return parent.name
        parent = getattr(parent, "_parent", None)
    return None


def infer_machine_name(var_name: Optional[str], class_name: Optional[str], file: Path) -> str:
    if class_name and var_name:
        return f"{class_name}.{var_name}"
    if class_name:
        return class_name
    if var_name:
        return var_name
    return file.stem


def extract_machines_from_file(file: Path) -> list[Machine]:
    try:
        source = file.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        source = file.read_text(encoding="latin-1")

    try:
        tree = ast.parse(source, filename=str(file))
    except SyntaxError as exc:
        print(f"[WARN] Skipping {file}: syntax error at line {exc.lineno}", file=sys.stderr)
        return []

    attach_parents(tree)
    machines: list[Machine] = []

    for node in ast.walk(tree):
        # transitions = [Transition(...), ...]
        # self.transitions = [Transition(...), ...]
        if isinstance(node, ast.Assign):
            transitions = list_contains_transitions(node.value, file)
            if not transitions:
                continue

            for target in node.targets:
                var_name = assignment_name(target)
                class_name = enclosing_class_name(node)
                machine_name = infer_machine_name(var_name, class_name, file)
                machines.append(Machine(machine_name, file, transitions.copy()))

        # transitions: list[...] = [Transition(...), ...]
        elif isinstance(node, ast.AnnAssign):
            transitions = list_contains_transitions(node.value, file) if node.value else []
            if not transitions:
                continue
            var_name = assignment_name(node.target)
            class_name = enclosing_class_name(node)
            machine_name = infer_machine_name(var_name, class_name, file)
            machines.append(Machine(machine_name, file, transitions.copy()))

        # FSM(name="raft", transitions=[Transition(...), ...])
        # FSM([...])
        elif isinstance(node, ast.Call):
            transitions: list[TransitionEdge] = []
            machine_name: Optional[str] = None

            for kw in node.keywords:
                if kw.arg in {"transitions", "transition_table", "table"}:
                    transitions = list_contains_transitions(kw.value, file)
                elif kw.arg in {"name", "machine_name"} and isinstance(kw.value, ast.Constant):
                    machine_name = str(kw.value.value)

            if not transitions and node.args:
                callee = expr_to_label(node.func).lower()
                if "fsm" in callee or "machine" in callee:
                    transitions = list_contains_transitions(node.args[0], file)

            if transitions:
                class_name = enclosing_class_name(node)
                inferred = machine_name or infer_machine_name(None, class_name, file)
                machines.append(Machine(inferred, file, transitions.copy()))

    return machines


def find_python_files(path: Path) -> list[Path]:
    if path.is_file() and path.suffix == ".py":
        return [path]
    if path.is_dir():
        ignored_dirs = {".git", ".venv", "venv", "__pycache__", ".mypy_cache", ".pytest_cache"}
        files: list[Path] = []
        for file in path.rglob("*.py"):
            if any(part in ignored_dirs for part in file.parts):
                continue
            files.append(file)
        return sorted(files)
    return []


def merge_machines(machines: Iterable[Machine]) -> list[Machine]:
    merged: dict[tuple[str, Path], Machine] = {}
    for machine in machines:
        key = (machine.name, machine.file)
        if key not in merged:
            merged[key] = Machine(machine.name, machine.file, [])
        merged[key].transitions.extend(machine.transitions)
    return list(merged.values())


def dot_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def edge_label(t: TransitionEdge, show_actions: bool = True, show_lines: bool = False) -> str:
    parts = []

    if t.name and t.name not in {"None", ""}:
        parts.append(str(t.name))

    if t.guard and t.guard not in {"None", "True", "always", "lambda", ""}:
        parts.append(f"[{t.guard}]")

    if show_actions and t.action and t.action not in {"None", "pass", "lambda", ""}:
        parts.append(f"/ {t.action}")

    if show_lines:
        parts.append(f"L{t.line}")

    return "\n".join(parts) if parts else " "


def machine_to_dot(machine: Machine, show_actions: bool = True, show_lines: bool = False) -> str:
    graph_id = re.sub(r"[^A-Za-z0-9_]", "_", machine.safe_name)
    lines = [
        f'digraph "{dot_escape(graph_id)}" {{',
        '  rankdir=LR;',
        '  graph [fontname="Helvetica", labelloc="t", labeljust="l"];',
        '  node [shape=ellipse, fontname="Helvetica", fontsize=11];',
        '  edge [fontname="Helvetica", fontsize=10];',
        f'  label="{dot_escape(machine.name)}\\n{dot_escape(str(machine.file))}";',
        "",
    ]

    states = sorted({t.source for t in machine.transitions} | {t.target for t in machine.transitions})
    state_ids = {state: hashlib.sha1(state.encode("utf-8")).hexdigest()[:10] for state in states}

    for state in states:
        lines.append(f'  "{state_ids[state]}" [label="{dot_escape(state)}"];')

    lines.append("")

    for t in machine.transitions:
        label = edge_label(t, show_actions=show_actions, show_lines=show_lines)
        lines.append(
            f'  "{state_ids[t.source]}" -> "{state_ids[t.target]}" '
            f'[label="{dot_escape(label)}"];'
        )

    lines.append("}")
    return "\n".join(lines) + "\n"


def render_with_graphviz(dot_file: Path, fmt: str) -> Optional[Path]:
    if fmt == "dot":
        return dot_file

    out_file = dot_file.with_suffix(f".{fmt}")
    try:
        subprocess.run(
            ["dot", f"-T{fmt}", str(dot_file), "-o", str(out_file)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return out_file
    except FileNotFoundError:
        print("[WARN] Graphviz 'dot' command not found. Wrote DOT only.", file=sys.stderr)
        return None
    except subprocess.CalledProcessError as exc:
        print(f"[WARN] Graphviz failed for {dot_file}: {exc.stderr}", file=sys.stderr)
        return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Draw FSM diagrams from rxnet Python Transition(from_state, to_state, guard, action, label) tables."
    )
    parser.add_argument("path", type=Path, help="Python file or project directory")
    parser.add_argument("--out", type=Path, default=Path("fsm_diagrams"), help="Output directory")
    parser.add_argument(
        "--format",
        choices=["dot", "svg", "png", "pdf"],
        default="svg",
        help="Output format. DOT is always generated.",
    )
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="Only export machines whose name or file path contains this substring.",
    )
    parser.add_argument(
        "--hide-actions",
        action="store_true",
        help="Do not include action names in edge labels.",
    )
    parser.add_argument(
        "--show-lines",
        action="store_true",
        help="Include source line numbers in edge labels.",
    )
    parser.add_argument(
        "--combine",
        action="store_true",
        help="Combine all extracted transitions into one diagram.",
    )
    args = parser.parse_args()

    files = find_python_files(args.path)
    if not files:
        print(f"No Python files found in {args.path}", file=sys.stderr)
        return 1

    all_machines: list[Machine] = []
    for file in files:
        all_machines.extend(extract_machines_from_file(file))

    machines = merge_machines(all_machines)

    if args.only:
        needle = args.only.lower()
        machines = [
            m for m in machines
            if needle in m.name.lower() or needle in str(m.file).lower()
        ]

    if not machines:
        print("No FSM transition tables found.", file=sys.stderr)
        print("Expected Transition(from_state, to_state, guard, action, label).", file=sys.stderr)
        return 2

    args.out.mkdir(parents=True, exist_ok=True)

    if args.combine:
        combined = Machine("combined_fsm", args.path, [])
        for machine in machines:
            combined.transitions.extend(machine.transitions)
        machines = [combined]

    generated: list[Path] = []
    for idx, machine in enumerate(machines, start=1):
        name = f"{idx:02d}_{machine.safe_name}"
        dot_file = args.out / f"{name}.dot"
        dot_file.write_text(
            machine_to_dot(
                machine,
                show_actions=not args.hide_actions,
                show_lines=args.show_lines,
            ),
            encoding="utf-8",
        )
        generated.append(dot_file)

        rendered = render_with_graphviz(dot_file, args.format)
        if rendered:
            generated.append(rendered)

    print("Generated:")
    for path in generated:
        print(f"  {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
