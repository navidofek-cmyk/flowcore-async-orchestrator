from __future__ import annotations

from pathlib import Path


def test_readme_covers_phase_04_operational_surface() -> None:
    readme = Path(__file__).resolve().parent.parent / "README.md"
    content = readme.read_text(encoding="utf-8")

    required_snippets = [
        "## Execution Modes",
        "## Observability",
        "## Operational CLI",
        "## Docker Quick Start",
        "## Guarantees And Limits",
        "flowcore run --executor local",
        "flowcore run --executor distributed",
        "flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore",
        "flowcore dashboard --redis-url redis://localhost:6379/0 --queue-name flowcore",
        "flowcore seed-demo --redis-url redis://localhost:6379/0 --queue-name flowcore",
        "flowcore metrics",
        "flowcore status",
        "flowcore dlq",
        "flowcore replay-dlq",
        "at-least-once",
        "idempotency is practical deduplication",
    ]

    for snippet in required_snippets:
        assert snippet in content
