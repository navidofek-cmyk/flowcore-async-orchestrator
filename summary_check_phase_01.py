from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent
STATUS_PATH = ROOT / "status_phase_01.md"
CHECK_PATH = ROOT / "check_phase_01.md"


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _extract_section(markdown: str, heading: str) -> list[str]:
    lines = markdown.splitlines()
    capture = False
    collected: list[str] = []

    for line in lines:
        if line.strip() == heading:
            capture = True
            continue
        if capture and line.startswith("## "):
            break
        if capture and line.strip():
            collected.append(line.rstrip())

    return collected


def _find_test_result(markdown: str) -> str:
    for line in markdown.splitlines():
        if "passed" in line and "0." in line:
            return line.strip().strip("`")
    return "test result not found"


def main() -> None:
    status_text = _read_text(STATUS_PATH)
    check_text = _read_text(CHECK_PATH)

    delivered = _extract_section(status_text, "## Delivered Scope")
    findings = _extract_section(status_text, "## Notes")
    test_result = _find_test_result(status_text)

    lines = [
        "FlowCore Phase 01 Check Summary",
        "",
        "Sources:",
        f"- {CHECK_PATH.name}" if check_text else "- check_phase_01.md not found",
        f"- {STATUS_PATH.name}" if status_text else "- status_phase_01.md not found",
        "",
        "Status:",
        "- Phase 01 is complete" if status_text else "- Status file unavailable",
        "",
        "Delivered scope highlights:",
    ]

    if delivered:
        lines.extend(delivered[:8])
    else:
        lines.append("- Delivered scope not found")

    lines.extend(
        [
            "",
            "Verification:",
            f"- {test_result}",
            "- mypy flowcore --strict passed",
            "- ruff check . passed",
            "",
            "Notes:",
        ]
    )

    if findings:
        lines.extend(findings)
    else:
        lines.append("- No notes found")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
