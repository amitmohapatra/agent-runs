"""Fail when a built wheel or sdist holds anything but the package's source.

    python scripts/check_dist.py DIST_DIR PACKAGE [PACKAGE ...]

``DIST_DIR`` holds the wheel and the sdist of one distribution (``uv build --out-dir DIST_DIR``).
``PACKAGE`` is a top-level directory as the wheel installs it (``trellis``); the sdist holds it
under ``src/``. Allowed besides: the wheel's ``*.dist-info/``, and the sdist's ``PKG-INFO``,
``pyproject.toml``, ``.gitignore``, README, CHANGELOG and LICENSE. Tests, examples, benchmarks,
docs, data and compiled files are findings, so a ``pip install`` of main installs only the source.
"""

from __future__ import annotations

import sys
import tarfile
import zipfile
from pathlib import Path

#: hatchling always adds .gitignore to an sdist (it reads it to decide what to leave out)
TOP = ("PKG-INFO", "pyproject.toml", "README.md", "CHANGELOG.md", "NOTICE", ".gitignore")


def _compiled(name: str) -> bool:
    return "__pycache__/" in name or name.endswith((".pyc", ".pyo"))


def wheel_strays(path: Path, packages: list[str]) -> list[str]:
    with zipfile.ZipFile(path) as wheel:
        names = [n for n in wheel.namelist() if not n.endswith("/")]
    return [
        n
        for n in names
        if _compiled(n)
        or not (
            n.split("/", 1)[0].endswith(".dist-info")
            or any(n.startswith(f"{p}/") for p in packages)
        )
    ]


def sdist_strays(path: Path, packages: list[str]) -> list[str]:
    with tarfile.open(path) as sdist:
        names = [m.name.split("/", 1)[-1] for m in sdist.getmembers() if m.isfile()]
    return [
        n
        for n in names
        if _compiled(n)
        or not (
            n in TOP or n.startswith("LICENSE") or any(n.startswith(f"src/{p}/") for p in packages)
        )
    ]


def main(argv: list[str]) -> int:
    if not argv[1:]:
        sys.stdout.write(__doc__ or "")
        return 2
    dist, packages = Path(argv[0]), argv[1:]
    wheels, sdists = sorted(dist.glob("*.whl")), sorted(dist.glob("*.tar.gz"))
    if not wheels or not sdists:
        sys.stdout.write(f"{dist}: expected a wheel and an sdist, found {sorted(dist.iterdir())}\n")
        return 1
    found = [f"{w.name}: {n}" for w in wheels for n in wheel_strays(w, packages)]
    found += [f"{s.name}: {n}" for s in sdists for n in sdist_strays(s, packages)]
    found.append(f"{dist}: {len(found)} file(s) outside {', '.join(packages)} and the metadata")
    sys.stdout.write("\n".join(found) + "\n")
    return 1 if len(found) > 1 else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
