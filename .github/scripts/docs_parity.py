"""Check that every docs page has a Russian twin with the same structure.

A twin matches when it has the same headings per level, the same code blocks
(comment lines and trailing ``#`` comments ignored, diagram bodies ignored),
the same admonitions, the same mkdocstrings directives and the same number of
table rows. Prose is not compared: this catches a section, example, API entry
or table row added to one language and not the other. Code is compared, so
keep string literals in examples untranslated.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parents[2] / "docs"
FENCE = re.compile(r"^\s*(`{3,}|~{3,})(.*)$")
ADMONITION = re.compile(r"^\s*(!!!|\?\?\?\+?)\s*(\S+)?")
UNTRANSLATED = ("mermaid",)

Shape = dict[str, list[str]]


def shape(path: Path) -> Shape:
    found: Shape = {"heading": [], "code": [], "admonition": [], "directive": [], "table": []}
    fence: str | None = None
    language = ""
    block: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        opened = FENCE.match(line)
        if fence is None and opened:
            fence, language, block = opened.group(1), opened.group(2).strip(), []
            continue
        if fence is not None:
            closed = FENCE.match(line)
            if closed and closed.group(1)[0] == fence[0] and len(closed.group(1)) >= len(fence):
                body = [] if language.startswith(UNTRANSLATED) else block
                found["code"].append("\n".join([language, *body]))
                fence = None
            elif not line.lstrip().startswith("#"):
                # Comments are translated; code is not.
                block.append(re.sub(r"\s+#.*$", "", line).strip())
            continue
        stripped = line.lstrip()
        admonition = ADMONITION.match(line)
        if line.startswith("#"):
            found["heading"].append(line.split(" ", 1)[0])
        elif admonition:
            found["admonition"].append(f"{admonition.group(1)} {admonition.group(2) or ''}")
        elif stripped.startswith(":::"):
            found["directive"].append(stripped.split()[1] if len(stripped.split()) > 1 else "")
        elif stripped.startswith("|"):
            found["table"].append("row")
    return found


def first_difference(en: list[str], ru: list[str]) -> str:
    for index, (left, right) in enumerate(zip(en, ru, strict=False)):
        if left != right:
            return f"#{index + 1}: EN {left!r} / RU {right!r}"
    return f"EN has {len(en)}, RU has {len(ru)}"


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
                errors.append(f"{name}: {key} differs, {first_difference(en[key], ru[key])}")
    for twin in sorted(DOCS.rglob("*.ru.md")):
        if not twin.with_name(twin.name.replace(".ru.md", ".md")).exists():
            errors.append(f"{twin.relative_to(DOCS)}: no English page")
    for error in errors:
        print(error)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
