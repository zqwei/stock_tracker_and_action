from __future__ import annotations

import os
import importlib.metadata as metadata


def _version_tuple(raw: str) -> tuple[int, ...]:
    parts: list[int] = []
    for token in raw.split("."):
        digits = "".join(ch for ch in token if ch.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def test_streamlit_runtime_contract() -> None:
    streamlit_raw = metadata.version("streamlit")
    protobuf_raw = metadata.version("protobuf")

    streamlit_version = _version_tuple(streamlit_raw)
    protobuf_version = _version_tuple(protobuf_raw)

    if streamlit_version < (1, 31) and protobuf_version >= (4,):
        assert os.environ.get("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION") == "python", (
            "Incompatible streamlit/protobuf combo requires "
            "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python"
        )

    # A direct import guards against streamlit/protobuf runtime mismatch.
    import streamlit as st  # noqa: F401
