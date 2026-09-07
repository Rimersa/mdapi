"""Preserve operator settings while upgrading the gateway configuration."""

import sys
from pathlib import Path


def main():
    output, previous, data_root, token_file, metadata_index = sys.argv[1:]
    values = {
        "MDAPI_GATEWAY_HOST": "10.10.10.87",
        "MDAPI_GATEWAY_PORT": "18787",
        "MDAPI_MAX_STREAMS": "2",
        "MDAPI_QUEUE_TIMEOUT": "300",
        "MDAPI_METADATA_INDEX": metadata_index,
    }
    old = Path(previous)
    if old.exists():
        for line in old.read_text().splitlines():
            key, separator, value = line.partition("=")
            if separator and key in values:
                values[key] = value.strip()
    values["MDAPI_DATA_ROOT"] = data_root
    values["MDAPI_TOKEN_FILE"] = token_file
    index_path = Path(values["MDAPI_METADATA_INDEX"]).expanduser().resolve()
    if index_path.is_relative_to(Path(data_root).resolve()):
        raise ValueError("元数据缓存不能位于行情数据根目录内")
    if any("\n" in value or "\r" in value for value in values.values()):
        raise ValueError("配置值不能包含换行符")
    Path(output).write_text(
        "".join(f"{key}={value}\n" for key, value in values.items())
    )


if __name__ == "__main__":
    main()
