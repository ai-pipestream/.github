#!/usr/bin/env python3
"""
Generate Docsify sidebar for the docs/ directory.

Rules:
- Recursively scan docs/ for .md files
- Ignore docs/README.md and docs/_sidebar.md
- Group links by first-level subdirectory under docs/
  - Top-level .md files (except README and _sidebar) appear as top-level bullets
  - For each immediate subdirectory of docs/, add a bold heading and list all
    .md files found anywhere under that directory (recursively)
- Link titles are derived from the filename (without extension):
  - Replace '-' and '_' with spaces
  - Title Case each word
- Links should be absolute from docs root as Docsify expects, e.g. /design/architecture
- Sort directories alphabetically and files within each group alphabetically by title

Idempotent: running multiple times yields the same output for the same inputs.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Tuple

DOCS_DIR = Path(__file__).resolve().parents[1] / "docs"
SIDEBAR_FILE = DOCS_DIR / "_sidebar.md"


def to_title(name: str) -> str:
    """Convert a filename (without extension) to Title Case per rules."""
    # Normalize hyphens/underscores to spaces
    normalized = name.replace("-", " ").replace("_", " ")
    # Title Case words
    return " ".join(word.capitalize() for word in normalized.split())


def collect_markdown_files(base: Path) -> List[Path]:
    """Collect all .md files under docs/ except the ignored ones."""
    files: List[Path] = []
    for path in base.rglob("*.md"):
        # Ignore the sidebar itself and the root README.md only
        if path.name == "_sidebar.md":
            continue
        if path == base / "README.md":
            continue
        files.append(path)
    return files


def group_files(files: List[Path], base: Path) -> Tuple[List[Tuple[str, str]], Dict[str, List[Tuple[str, str]]]]:
    """
    Group files by first-level subdirectory under docs/.

    Returns:
      top_level: list of tuples (title, link)
      by_dir: dict mapping section name (Title Case dir) to list of tuples (title, link)
    """
    top_level: List[Tuple[str, str]] = []
    by_dir: Dict[str, List[Tuple[str, str]]] = {}

    for file_path in files:
        rel = file_path.relative_to(base)
        # Build the Docsify-style absolute link (no .md extension)
        link = "/" + str(rel.with_suffix("")).replace(os.sep, "/")
        title = to_title(file_path.stem)

        # Determine if this is top-level or within a subdirectory
        if len(rel.parts) == 1:
            top_level.append((title, link))
        else:
            section_raw = rel.parts[0]  # first-level directory under docs/
            section_name = to_title(section_raw)
            by_dir.setdefault(section_name, []).append((title, link))

    # Sort top-level files by title
    top_level.sort(key=lambda x: x[0].lower())
    # Sort directories and files within
    sorted_by_dir: Dict[str, List[Tuple[str, str]]] = {}
    for section in sorted(by_dir.keys(), key=lambda s: s.lower()):
        items = sorted(by_dir[section], key=lambda x: x[0].lower())
        sorted_by_dir[section] = items

    return top_level, sorted_by_dir


def render_sidebar(top_level: List[Tuple[str, str]], by_dir: Dict[str, List[Tuple[str, str]]]) -> str:
    lines: List[str] = []
    # Always include Home at the top
    lines.append("- [Home](/)")

    # Top-level files
    for title, link in top_level:
        lines.append(f"- [{title}]({link})")

    # Blank line between sections if there are any directories
    if by_dir:
        lines.append("")

    # Sections
    first = True
    for section, items in by_dir.items():
        if not first:
            lines.append("")  # blank line between sections
        first = False
        lines.append(f"- **{section}**")
        for title, link in items:
            lines.append(f"  - [{title}]({link})")

    lines.append("")  # trailing newline
    return "\n".join(lines)


def main() -> None:
    if not DOCS_DIR.exists():
        raise SystemExit(f"docs directory not found at {DOCS_DIR}")

    files = collect_markdown_files(DOCS_DIR)
    top_level, by_dir = group_files(files, DOCS_DIR)
    content = render_sidebar(top_level, by_dir)

    SIDEBAR_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = SIDEBAR_FILE.read_text(encoding="utf-8") if SIDEBAR_FILE.exists() else None

    if existing == content:
        # Idempotent: no rewrite needed
        return

    SIDEBAR_FILE.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
