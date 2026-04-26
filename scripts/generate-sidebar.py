#!/usr/bin/env python3
"""
Generate Docsify sidebar for the docs/ directory.

Features:
- Recursive scanning: Creates a nested sidebar structure matching the filesystem.
- Configurable ordering: Uses docs/sidebar-config.json to control top-level section order.
- Smart defaults: 
  - "Home" is always first.
  - "README.md" in a directory is treated as the index/overview for that section.
  - Unconfigured directories are sorted alphabetically after configured ones.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Union

DOCS_DIR = Path(__file__).resolve().parents[1] / "docs"
SIDEBAR_FILE = DOCS_DIR / "_sidebar.md"
CONFIG_FILE = DOCS_DIR / "sidebar-config.json"


def load_config() -> Dict[str, List[str]]:
    """Load sidebar configuration if present."""
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            print(f"Warning: Could not parse {CONFIG_FILE}. Using default sorting.")
    return {"section_order": [], "forced_top_files": []}


def to_title(name: str) -> str:
    """Convert a filename/dirname to Title Case."""
    # Normalize hyphens/underscores to spaces
    normalized = name.replace("-", " ").replace("_", " ")
    # Title Case words
    return " ".join(word.capitalize() for word in normalized.split())


def get_link_path(file_path: Path, base_dir: Path) -> str:
    """Generate relative Docsify link path (e.g., folder/file)."""
    rel_path = file_path.relative_to(base_dir)
    # Remove suffix for clean URLs if desired, or keep .md. Docsify usually handles no extension well.
    # Using no extension for cleaner URLs.
    # Note: Removing leading slash to ensure compatibility with subpath hosting (GitHub Pages)
    return str(rel_path.with_suffix("")).replace(os.sep, "/")


def scan_directory(current_dir: Path, base_dir: Path, config: Dict) -> List[str]:
    """
    Recursively scan a directory and return lines for the sidebar.
    """
    lines: List[str] = []
    indent = "  " * (len(current_dir.relative_to(base_dir).parts) if current_dir != base_dir else 0)

    # 1. Collect items
    files: List[Path] = []
    subdirs: List[Path] = []

    for item in current_dir.iterdir():
        if item.name.startswith(".") or item.name == "_sidebar.md" or item.name == "index.html" or item.name == "style.css" or item.name == "sidebar-config.json":
            continue
        
        if item.is_file() and item.suffix == ".md":
            files.append(item)
        elif item.is_dir():
            subdirs.append(item)

    # 2. Sort Files
    # README.md comes first as the "Overview" or section link
    files.sort(key=lambda f: (f.name != "README.md", f.name.lower()))

    # 3. Generate File Links
    for f in files:
        title = to_title(f.stem)
        link = get_link_path(f, base_dir)
        
        # Special case: If it's the root README, we handle it separately in main() as "Home"
        if f == base_dir / "README.md":
            continue
            
        # If it's a README inside a subdir, name it "Overview" or the Dir Name
        if f.name == "README.md":
            title = "Overview"
        
        lines.append(f"{indent}- [{title}]({link})")

    # 4. Sort Subdirectories
    # Use config order for current level if applicable
    section_order = config.get("section_order", [])
    
    def dir_sort_key(d: Path) -> tuple:
        name = d.name
        try:
            # If explicitly ordered, use its index
            return (0, section_order.index(name))
        except ValueError:
            # Otherwise sort alphabetically at the end
            return (1, name.lower())

    # Only apply section_order to the root docs directory
    if current_dir == base_dir:
        subdirs.sort(key=dir_sort_key)
    else:
        subdirs.sort(key=lambda d: d.name.lower())

    # 5. Recurse into Subdirectories
    for d in subdirs:
        title = to_title(d.name)
        
        # Check if there's a README in the subdir to link the folder title to
        readme_path = d / "README.md"
        if readme_path.exists():
             link = get_link_path(readme_path, base_dir)
             lines.append(f"{indent}- **[{title}]({link})**")
        else:
             lines.append(f"{indent}- **{title}**")
             
        lines.extend(scan_directory(d, base_dir, config))

    return lines


def main() -> None:
    if not DOCS_DIR.exists():
        raise SystemExit(f"docs directory not found at {DOCS_DIR}")

    config = load_config()
    
    # Start content
    lines = []
    lines.append("- [← Back to Home](../)")
    lines.append("- [Docs Overview](README)")
    
    # Recursively scan
    # We process the root DOCS_DIR. 
    # Note: scan_directory handles the indentation logic.
    root_content = scan_directory(DOCS_DIR, DOCS_DIR, config)
    lines.extend(root_content)

    content = "\n".join(lines) + "\n"

    SIDEBAR_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = SIDEBAR_FILE.read_text(encoding="utf-8") if SIDEBAR_FILE.exists() else None

    if existing == content:
        print("Sidebar is up to date.")
        return

    SIDEBAR_FILE.write_text(content, encoding="utf-8")
    print(f"Sidebar generated at {SIDEBAR_FILE}")


if __name__ == "__main__":
    main()
