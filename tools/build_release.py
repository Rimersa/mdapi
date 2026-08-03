from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCUMENTS = (
    "README.md",
    "QUICKSTART.md",
    "USER_GUIDE.md",
    "DEPLOYMENT.md",
    "SERVER_DATA_FORMAT.md",
    "DATA_CONTRACT.md",
    "BENCHMARK.md",
    "RELEASE_NOTES.md",
)
PACKAGE_DIRECTORIES = ("deploy", "examples", "scripts")
ADMIN_TOOLS = (
    "migrate_catalog_v2.py",
    "summarize_catalog.py",
    "prewarm.py",
)


def project_version() -> str:
    raw = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    matched = re.search(r'^version\s*=\s*"([^"]+)"$', raw, re.MULTILINE)
    if matched is None:
        raise RuntimeError("pyproject.toml 中没有找到项目版本")
    return matched.group(1)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def copy_release_sources(stage: Path) -> None:
    for name in DOCUMENTS:
        shutil.copy2(PROJECT_ROOT / name, stage / name)
    for name in ("install-server.sh", "install-client.sh"):
        target = stage / name
        shutil.copy2(PROJECT_ROOT / name, target)
        target.chmod(0o755)
    for name in PACKAGE_DIRECTORIES:
        shutil.copytree(
            PROJECT_ROOT / name,
            stage / name,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
    tools = stage / "tools"
    tools.mkdir()
    for name in ADMIN_TOOLS:
        shutil.copy2(PROJECT_ROOT / "tools" / name, tools / name)


def write_checksums(stage: Path) -> None:
    files = sorted(
        path
        for path in stage.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    )
    lines = [f"{sha256(path)}  {path.relative_to(stage)}" for path in files]
    (stage / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="构建单一、可审计的正式发行包")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "dist",
    )
    args = parser.parse_args()
    version = project_version()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    release_name = f"market-data-api-{version}"
    stage = output / release_name
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir()
    copy_release_sources(stage)

    gateway = stage / "bin" / "mdapi-gateway.pyz"
    subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "tools" / "build_gateway_pyz.py"),
            str(gateway),
        ],
        check=True,
    )
    wheel_dir = stage / "wheels"
    wheel_dir.mkdir()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-build-isolation",
            "--no-deps",
            "--wheel-dir",
            str(wheel_dir),
            str(PROJECT_ROOT),
        ],
        check=True,
    )
    expected_wheel = wheel_dir / f"market_data_api-{version}-py3-none-any.whl"
    if not expected_wheel.is_file():
        raise RuntimeError(f"没有生成预期wheel: {expected_wheel}")

    write_checksums(stage)
    archive = output / f"{release_name}-easy-install.tar.gz"
    temporary = archive.with_suffix(archive.suffix + ".partial")
    temporary.unlink(missing_ok=True)
    with tarfile.open(temporary, "w:gz") as handle:
        handle.add(stage, arcname=release_name)
    temporary.replace(archive)
    checksum_file = archive.with_suffix(archive.suffix + ".sha256")
    checksum_file.write_text(
        f"{sha256(archive)}  {archive.name}\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "version": version,
                "directory": str(stage),
                "archive": str(archive),
                "archive_bytes": archive.stat().st_size,
                "archive_sha256": sha256(archive),
                "wheel": str(expected_wheel),
                "gateway": str(gateway),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
