"""Check the Git index; never delete source or user data."""

import re
import subprocess
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")
    if not any(paths):
        raise SystemExit("No indexed files; initialize Git and git add the reviewed source first")
    failures = []
    patterns = [
        re.compile(p)
        for p in (
            rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
            rb"gh[pousr]_[A-Za-z0-9]{30,}",
            rb"AKIA[0-9A-Z]{16}",
        )
    ]
    for name in filter(None, paths):
        path = Path(name)
        if any(
            p in {"data", ".venv", "__pycache__", "dist", "build", ".pytest_cache", ".ruff_cache"}
            for p in path.parts
        ) or (path.name.startswith(".env") and path.name != ".env.example"):
            failures.append(name + ": runtime or private file")
        content = (root / name).read_bytes()
        if len(content) > 2 * 1024 * 1024:
            failures.append(name + ": exceeds 2 MiB")
        if path.suffix in {".sqlite", ".parquet", ".avro", ".log", ".pyc"}:
            failures.append(name + ": generated artifact")
        if any(pattern.search(content) for pattern in patterns):
            failures.append(name + ": possible secret")
    if failures:
        raise SystemExit("\n".join(failures))
    print(f"Repository hygiene passed: {len(list(filter(None, paths)))} tracked files")


if __name__ == "__main__":
    main()
