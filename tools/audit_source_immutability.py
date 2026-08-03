from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GATEWAY_ARCHIVE = PROJECT_ROOT / "bin" / "mdapi-gateway.pyz"
if GATEWAY_ARCHIVE.is_file():
    sys.path.insert(0, str(GATEWAY_ARCHIVE))
else:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

DEFAULT_SOURCE_ROOT = Path("/data/market_data_lake/lake/curated")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="比较派生Manifest记录与当前原始文件size/mtime"
    )
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    args = parser.parse_args()
    source_root = args.source_root.resolve(strict=True)
    derived_root = args.derived_root.resolve(strict=True)
    manifests = sorted((derived_root / "objects").rglob("manifest.json"))
    mismatches = []
    checked = 0
    for manifest_path in manifests:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for item in manifest["source_files"]:
            path = (source_root / item["relative_path"]).resolve(strict=True)
            if not path.is_relative_to(source_root):
                raise RuntimeError(f"源路径越界: {path}")
            stat = path.stat()
            checked += 1
            if (
                stat.st_size != int(item["size"])
                or stat.st_mtime_ns != int(item["mtime_ns"])
            ):
                mismatches.append(
                    {
                        "path": str(path),
                        "manifest_size": int(item["size"]),
                        "current_size": stat.st_size,
                        "manifest_mtime_ns": int(item["mtime_ns"]),
                        "current_mtime_ns": stat.st_mtime_ns,
                    }
                )
    output = {
        "manifests": len(manifests),
        "source_files_checked": checked,
        "mismatches": mismatches,
    }
    print(json.dumps(output, ensure_ascii=False))
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
