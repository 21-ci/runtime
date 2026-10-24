"""MkDocs hook: prepare the OpenAPI document for the API explorer page.

Runs before every build (`mkdocs build` / `serve`). It reads openapi/openapi.json, the document `make openapi`
generates from the code, and writes a copy to docs/assets/openapi.json (git-ignored):

- spec-internal references (acceptance ids such as API-13, contract ids, docs/NN pointers) are removed from
  summaries and descriptions, because the public site does not have those documents;
- a local server and a Bearer security scheme are added when the spec has none, so "Try it" works against a node.

Nothing else is generated: the reference pages are the guides themselves (docs/api.md, docs/cli.md, ...).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_ID = r"(?:[A-Z]{1,4}-[A-Z]?\d+[a-z]?)"


def clean_text(text: str) -> str:
    """Remove spec-internal references from OpenAPI prose."""
    text = re.sub(rf"\s*\((?:contracts\s+)?{_ID}(?:,\s*{_ID})*\)", "", text)  # "(API-13)", "(contracts C-9)"
    text = re.sub(rf",\s*{_ID}\)", ")", text)  # "(scope `x`, API-11)"
    text = re.sub(r"\s*\((?:see\s+)?docs/\d\d[^)]*\)", "", text)  # "(see docs/07 ...)"
    return re.sub(r"\s+([.;:,])", r"\1", text)


def clean_tree(node):
    if isinstance(node, dict):
        return {
            k: (clean_text(v) if k in ("summary", "description") and isinstance(v, str) else clean_tree(v))
            for k, v in node.items()
        }
    if isinstance(node, list):
        return [clean_tree(v) for v in node]
    return node


def build_spec(source: Path) -> dict:
    spec = clean_tree(json.loads(source.read_text(encoding="utf-8")))
    spec.setdefault("servers", [{"url": "http://127.0.0.1:8081", "description": "Local node (control listener)"}])
    schemes = spec.setdefault("components", {}).setdefault("securitySchemes", {})
    if not schemes:
        schemes["bearer"] = {
            "type": "http",
            "scheme": "bearer",
            "description": "Root token (`RT_ROOT_TOKEN`) or a tenant key `rtk_<key_id>_<secret>`. "
            "`x-rt-key: <key>` works too.",
        }
        spec.setdefault("security", [{"bearer": []}])
    return spec


def on_pre_build(config, **kwargs):
    root = Path(config["config_file_path"]).resolve().parent
    target = Path(config["docs_dir"]) / "assets" / "openapi.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    spec = build_spec(root / "openapi" / "openapi.json")
    target.write_text(json.dumps(spec, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return config
