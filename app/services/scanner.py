from pathlib import Path


def scan_folder(folder: str) -> list[str]:
    """Return sorted list of all .pdf paths found recursively under *folder*."""
    path = Path(folder)
    if not path.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")
    if not path.is_dir():
        raise FileNotFoundError(f"Not a directory: {folder}")
    return sorted(str(p) for p in path.rglob("*.pdf"))
