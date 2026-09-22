"""YAML rendering shared by every emitted bundle file."""

from __future__ import annotations

from typing import Any

import yaml


class _Dumper(yaml.SafeDumper):
    """Keeps nested bundle YAML readable by indenting sequences under their key."""

    def increase_indent(self, flow: bool = False, indentless: bool = False):
        return super().increase_indent(flow, False)


def dump_yaml(document: dict[str, Any]) -> str:
    return yaml.dump(document, Dumper=_Dumper, sort_keys=False, width=100, allow_unicode=True)
