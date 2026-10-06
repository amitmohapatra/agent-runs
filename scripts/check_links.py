"""Fail on a broken relative link in this repo's Markdown.

Every ``[text](target)`` whose target is not a URL must name a file or directory that exists,
and a ``#anchor`` must match a heading (GitHub's slug rules) or an ``<a id=...>`` in that file.
Standard library only, so CI and ``make links`` run it without installing anything.

    python scripts/check_links.py [root]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

SKIP_DIRS = {".git", ".venv", "node_modules", ".claude", "vendor", "build", "dist", ".pytest_cache"}
LINK = re.compile(r"(?<!!)\[[^\]]*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
IMAGE = re.compile(r"!\[[^\]]*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
HTML_ANCHOR = re.compile(r"<a\s+(?:id|name)=\"([^\"]+)\"", re.IGNORECASE)
FENCE = re.compile(r"^\s*(```|~~~)")


def _prose(text: str) -> list[str]:
    """The lines outside fenced code blocks, with inline code spans kept for headings."""
    out, fenced = [], False
    for line in text.splitlines():
        if FENCE.match(line):
            fenced = not fenced
            out.append("")
            continue
        out.append("" if fenced else line)
    return out


def slug(heading: str) -> str:
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading)  # a link keeps its text
    text = re.sub(r"<[^>]+>", "", text).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def anchors(path: Path, cache: dict[Path, set[str]]) -> set[str]:
    if path not in cache:
        seen: dict[str, int] = {}
        found: set[str] = set()
        for line in _prose(path.read_text(encoding="utf-8")):
            found.update(HTML_ANCHOR.findall(line))
            m = HEADING.match(line)
            if not m:
                continue
            base = slug(m.group(2))
            n = seen.get(base, 0)
            seen[base] = n + 1
            found.add(base if n == 0 else f"{base}-{n}")
        cache[path] = found
    return cache[path]


def markdown_files(root: Path) -> list[Path]:
    return sorted(
        p
        for p in root.rglob("*.md")
        if not any(part in SKIP_DIRS for part in p.relative_to(root).parts)
    )


def check(root: Path) -> list[str]:
    errors: list[str] = []
    cache: dict[Path, set[str]] = {}
    for md in markdown_files(root):
        for lineno, raw in enumerate(_prose(md.read_text(encoding="utf-8")), 1):
            line = re.sub(r"`[^`]*`", "", raw)  # a link inside inline code is not a link
            for target in LINK.findall(line) + IMAGE.findall(line):
                if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.IGNORECASE):
                    continue  # http:, https:, mailto: ...
                file_part, _, anchor = target.partition("#")
                dest = md if not file_part else (md.parent / file_part).resolve()
                where = f"{md.relative_to(root)}:{lineno}: {target}"
                if not dest.exists():
                    errors.append(f"{where} (no such file)")
                elif anchor and dest.suffix == ".md" and anchor not in anchors(dest, cache):
                    errors.append(f"{where} (no such anchor)")
    return errors


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    files = markdown_files(root)
    errors = check(root)
    for error in errors:
        print(error)
    print(f"{len(files)} Markdown files checked, {len(errors)} broken links")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
