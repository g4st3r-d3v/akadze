"""Check that every docs page has a Russian twin with the same structure.

A twin matches when it has the same headings per level, the same code blocks
(ignoring comment lines), the same admonitions and the same number of table
rows. Text is not compared: this catches a section, example or table row added
to one language and not the other.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parents[2] / "docs"
FENCE = re.compile(r"^```")


def shape(path: Path) -> dict[str, object]:
    headings: list[int] = []
    blocks: list[str] = []
    admonitions: list[str] = []
    rows = 0
    block: list[str] | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if FENCE.match(line):
            if block is None:
                block = [line]
            else:
                blocks.append("\n".join(block))
                block = None
            continue
        if block is not None:
            # Comments and diagram labels are translated; code is not.
            if not line.lstrip().startswith("#") and "mermaid" not in block[0]:
                block.append(re.sub(r"\s+#.*$", "", line))
            continue
        if line.startswith("#"):
            headings.append(len(line) - len(line.lstrip("#")))
        elif line.startswith("!!!"):
            admonitions.append(line.split()[1])
        elif line.startswith("|"):
            rows += 1
    return {"headings": headings, "code": blocks, "admonitions": admonitions, "rows": rows}


def main() -> int:
    errors: list[str] = []
    for page in sorted(DOCS.rglob("*.md")):
        if page.name.endswith(".ru.md"):
            continue
        twin = page.with_name(page.name[: -len(".md")] + ".ru.md")
        name = page.relative_to(DOCS)
        if not twin.exists():
            errors.append(f"{name}: no Russian twin")
            continue
        en, ru = shape(page), shape(twin)
        for key in en:
            if en[key] != ru[key]:
                errors.append(f"{name}: {key} differ from the Russian twin")
    for twin in sorted(DOCS.rglob("*.ru.md")):
        if not twin.with_name(twin.name.replace(".ru.md", ".md")).exists():
            errors.append(f"{twin.relative_to(DOCS)}: no English page")
    for error in errors:
        print(error)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
