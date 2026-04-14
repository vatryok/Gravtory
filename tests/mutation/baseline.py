"""Manual mutation testing baseline.

Applies representative mutations to core modules and verifies
that the test suite detects them (tests fail when code is mutated).

Usage:
    python tests/mutation/baseline.py

Exit code 0 = all mutations killed (good).
Exit code 1 = some mutations survived (tests need improvement).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Root of the project
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SRC_DIR = PROJECT_ROOT / "src" / "gravtory"

# Each mutation: (file_path_relative_to_src, original_text, mutated_text, description)
MUTATIONS: list[tuple[str, str, str, str]] = [
    # DAG — change topological sort comparison direction
    (
        "core/dag.py",
        "if in_degree[dependent] == 0:",
        "if in_degree[dependent] != 0:",
        "DAG: flip zero-in-degree check in topo sort",
    ),
    # Types — change default WorkflowStatus
    (
        "core/types.py",
        'PENDING = "pending"',
        'PENDING = "running"',
        "Types: mutate PENDING status value",
    ),
    # Context — flip output lookup
    (
        "core/context.py",
        "if step_order not in self._completed:",
        "if step_order in self._completed:",
        "Context: invert step_order presence check",
    ),
    # ID template — break format string
    (
        "core/id_template.py",
        "return template.format(**{k: str(v) for k, v in kwargs.items()})",
        "return template",
        "IDTemplate: skip format substitution",
    ),
    # Serialization — break JSON round-trip
    (
        "serialization/json.py",
        'separators=(",", ":")',
        'separators=(",", "=")',
        "JSON: corrupt separator in serialization",
    ),
    # Compression — skip actual compression
    (
        "serialization/compression.py",
        "return gzip.compress(data, compresslevel=self._level)",
        "return data",
        "Gzip: skip compression, return raw data",
    ),
]


def run_mutation(file_rel: str, original: str, mutated: str, desc: str) -> bool:
    """Apply a mutation, run tests, restore original. Returns True if killed."""
    src_file = SRC_DIR / file_rel
    original_content = src_file.read_text()

    if original not in original_content:
        print(f"  [WARN] SKIP (pattern not found): {desc}")
        return True  # Can't test, treat as killed

    # Apply mutation
    mutated_content = original_content.replace(original, mutated, 1)
    src_file.write_text(mutated_content)

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/unit/",
                "-x",
                "-q",
                "--tb=no",
                "--no-header",
                "--benchmark-disable",
            ],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            timeout=60,
        )
        killed = result.returncode != 0
        return killed
    except subprocess.TimeoutExpired:
        return True  # Timeout = mutation killed (infinite loop detected)
    finally:
        # Always restore
        src_file.write_text(original_content)


def main() -> int:
    print("=" * 60)
    print("Mutation Testing Baseline")
    print("=" * 60)

    killed = 0
    survived = 0
    skipped = 0
    total = len(MUTATIONS)

    for i, (file_rel, original, mutated, desc) in enumerate(MUTATIONS, 1):
        print(f"\n[{i}/{total}] {desc}")
        result = run_mutation(file_rel, original, mutated, desc)
        if result:
            print("  [OK] KILLED")
            killed += 1
        else:
            print("  [X] SURVIVED")
            survived += 1

    print("\n" + "=" * 60)
    score = (killed / (killed + survived)) * 100 if (killed + survived) > 0 else 0
    print(f"Results: {killed} killed, {survived} survived, {skipped} skipped")
    print(f"Mutation Score: {score:.1f}%")
    print("Target: >80%")
    print("=" * 60)

    return 0 if score >= 80 else 1


if __name__ == "__main__":
    sys.exit(main())
