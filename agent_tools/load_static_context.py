"""
load_static_context.py

Lazy-loading context generator for the Invoker framework.

Produces a SKELETON context.json optimized for LLM consumption:
- File tree (max depth 2) instead of full treemap dump
- Skills METADATA ONLY — skill index is loaded separately via _index.yaml
- File REFERENCES (path + exists + size) instead of inline content
- Schema SUMMARIES (top-level keys only) instead of full schema bodies
- Function/class SIGNATURES extracted via AST from Python files

Target: context.json MUST stay under 1,000 lines (governance mandate).
Agents that need full file content MUST use grep/jq to target specific data
at runtime instead of reading the entire context.json.

Gitignore compliance:
- The initial context FULLY respects .gitignore — no ignored file content is
  included.  Treemap and dependencies_report are gitignored and excluded.
- On-demand loading of ignored files (treemap, dependencies_report) is handled
  by context_loader.py, NOT by this module.
"""

import os
import re
import ast
import yaml
import json
from typing import Dict, Any, List, Optional

# Paths relativos dentro del proyecto
SKILLS_PATH = os.path.join("agent", "skills", "skills_registry.yaml")
AGENT_RULES_PATH = os.path.join("agent", "rules", "agent_rules.md")
SCHEMAS_PATH = os.path.join("agent", "agent_protocol", "schemas")

# Gitignored analysis files — NOT loaded in static context.
# Available on-demand via context_loader.load_on_demand().
DEPENDENCIES_REPORT_PATH = os.path.join("agent", "analysis", "dependencies_report.md")
TREEMAP_PATH = os.path.join("agent", "analysis", "treemap.md")

# Agent definition paths
AGENT_SENIOR_CONTEXT = os.path.join("agent", "agent_senior", "context", "context.md")
AGENT_SENIOR_WORKFLOW = os.path.join("agent", "agent_senior", "workflow", "workflow.md")
AGENT_EXECUTOR_CONTEXT = os.path.join("agent", "agent_executor", "context", "context.md")
AGENT_EXECUTOR_WORKFLOW = os.path.join("agent", "agent_executor", "workflow", "workflow.md")
AGENT_INSPECTOR_CONTEXT = os.path.join("agent", "agent_inspector", "context", "context.md")
AGENT_INSPECTOR_WORKFLOW = os.path.join("agent", "agent_inspector", "workflow", "workflow.md")


# ---------------------------------------------------------------------------
# Config-driven exclusions (read from agent_framework_config.yaml)
# ---------------------------------------------------------------------------
# Defaults are used when the config file is missing (e.g. first run).

_DEFAULT_EXCLUDED_FROM_SIGNATURES = {"agent/skills"}
_DEFAULT_EXCLUDED_DIRS_FROM_TREE = {
    "agent/skills",
    "agent/agent_outputs",
    "agent/agent_logs",
}


def _load_framework_config() -> Dict[str, Any]:
    """Load agent_framework_config.yaml from the project root. Returns {} on failure."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root, "agent_framework_config.yaml")
    if not os.path.exists(config_path):
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _get_excluded_from_signatures() -> set:
    cfg = _load_framework_config().get("static_context", {})
    entries = cfg.get("excluded_from_signatures", None)
    if isinstance(entries, list) and entries:
        return set(entries)
    return set(_DEFAULT_EXCLUDED_FROM_SIGNATURES)


def _get_excluded_dirs_from_tree() -> set:
    cfg = _load_framework_config().get("static_context", {})
    entries = cfg.get("excluded_dirs_from_tree", None)
    if isinstance(entries, list) and entries:
        return set(entries)
    return set(_DEFAULT_EXCLUDED_DIRS_FROM_TREE)


# Resolved at import time.
EXCLUDED_FROM_SIGNATURES = _get_excluded_from_signatures()
EXCLUDED_DIRS_FROM_TREE = _get_excluded_dirs_from_tree()


# ---------------------------------------------------------------------------
# Lazy-loading helpers
# ---------------------------------------------------------------------------

def _load_gitignore_spec(root: str):
    """Load .gitignore patterns from project root. Returns a pathspec or None."""
    try:
        import pathspec
        gitignore_path = os.path.join(root, ".gitignore")
        if os.path.exists(gitignore_path):
            with open(gitignore_path, "r", encoding="utf-8") as f:
                patterns = [p for p in f.read().splitlines() if p.strip() and not p.startswith("#")]
            return pathspec.PathSpec.from_lines("gitwildmatch", patterns)
    except ImportError:
        pass
    return None


def load_yaml_file(path: str) -> Any:
    """Carga un archivo YAML."""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _file_metadata(path: str) -> Dict[str, Any]:
    """Return lightweight metadata for a file instead of its full content."""
    if not os.path.exists(path):
        return {"path": path, "exists": False}
    size = os.path.getsize(path)
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    line_count = len(lines)
    # Extract markdown section headers (## / ### headings)
    headers = [
        ln.strip() for ln in lines
        if ln.strip().startswith("#")
    ]
    return {
        "path": path,
        "exists": True,
        "size_bytes": size,
        "line_count": line_count,
        "sections": headers[:10],  # cap to avoid bloat
    }


def _file_ref(path: str) -> Dict[str, Any]:
    """Minimal file reference — path + exists only. Used for agent definitions."""
    return {"path": path, "exists": os.path.exists(path)}


def _compact_skills_index(full_registry: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract a compact index: name, category, purpose only."""
    skills = full_registry.get("skills", [])
    if not skills:
        return []
    return [
        {
            "name": s.get("name", ""),
            "category": s.get("category", ""),
            "purpose": s.get("purpose", ""),
        }
        for s in skills
    ]


def _schema_summary(path: str) -> Dict[str, Any]:
    """Load a schema and return only its top-level keys."""
    if not os.path.exists(path):
        return {}
    if path.endswith((".yaml", ".yml")):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    elif path.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        return {}
    if isinstance(data, dict):
        return {"top_level_keys": list(data.keys())}
    return {"type": type(data).__name__}


def _generate_file_tree(root: str, max_depth: int = 2, spec=None) -> List[str]:
    """Generate a file tree respecting .gitignore, capped at max_depth.

    Also skips directories listed in EXCLUDED_DIRS_FROM_TREE to keep the
    static context compact (skills folders, agent outputs, logs).
    """
    ignored_dirs = {".git", "__pycache__", "node_modules", ".venv", "venv"}
    tree: List[str] = []

    def _walk(directory: str, prefix: str, depth: int) -> None:
        if depth > max_depth:
            return
        try:
            items = sorted(os.listdir(directory))
        except PermissionError:
            return

        visible = []
        for name in items:
            if name in ignored_dirs:
                continue
            full = os.path.join(directory, name)
            rel = os.path.relpath(full, root).replace(os.sep, "/")

            # Skip directories explicitly excluded from the tree
            if os.path.isdir(full) and rel in EXCLUDED_DIRS_FROM_TREE:
                continue

            if spec:
                if spec.match_file(rel):
                    continue
                if os.path.isdir(full) and spec.match_file(rel + "/"):
                    continue
            visible.append(name)

        for idx, name in enumerate(visible):
            full = os.path.join(directory, name)
            is_last = idx == len(visible) - 1
            connector = "└── " if is_last else "├── "
            if os.path.isdir(full):
                tree.append(f"{prefix}{connector}{name}/")
                ext = "    " if is_last else "│   "
                _walk(full, prefix + ext, depth + 1)
            else:
                tree.append(f"{prefix}{connector}{name}")

    _walk(root, "", 0)
    return tree


def _extract_python_signatures(
    root: str, max_depth: int = 3, spec=None
) -> Dict[str, List[str]]:
    """Walk Python files up to max_depth and extract class/function signatures via AST.

    Respects .gitignore (via spec) and skips directories listed in
    EXCLUDED_FROM_SIGNATURES to prevent agent implementation folders
    from inflating the output.
    """
    signatures: Dict[str, List[str]] = {}
    ignored_dirs = {".git", "__pycache__", "node_modules", ".venv", "venv"}

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in ignored_dirs]
        rel_dir = os.path.relpath(dirpath, root).replace(os.sep, "/")
        depth = 0 if rel_dir == "." else rel_dir.count("/") + 1
        if depth > max_depth:
            dirnames.clear()
            continue

        # Skip directories excluded from signatures (e.g. agent/skills)
        if any(rel_dir == ex or rel_dir.startswith(ex + "/") for ex in EXCLUDED_FROM_SIGNATURES):
            dirnames.clear()
            continue

        # Filter directories via .gitignore
        if spec:
            dirnames[:] = [
                d for d in dirnames
                if not spec.match_file(
                    os.path.relpath(os.path.join(dirpath, d), root).replace(os.sep, "/") + "/"
                )
            ]

        for fname in filenames:
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(dirpath, fname)
            rel_path = os.path.relpath(fpath, root).replace(os.sep, "/")
            # Skip files matched by .gitignore
            if spec and spec.match_file(rel_path):
                continue
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    source = f.read()
                tree = ast.parse(source, filename=rel_path)
            except (SyntaxError, UnicodeDecodeError):
                continue

            sigs: List[str] = []
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.ClassDef):
                    sigs.append(f"class {node.name}")
                elif isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                    args = [a.arg for a in node.args.args]
                    sigs.append(f"def {node.name}({', '.join(args)})")
            if sigs:
                signatures[rel_path] = sigs
    return signatures


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def load_static_context() -> Dict[str, Any]:
    """Load a SKELETON static context (lazy-loading mode)."""
    context: Dict[str, Any] = {}

    # 1. Skills registry — metadata only (skill index loaded separately via _index.yaml)
    if os.path.exists(SKILLS_PATH):
        full_registry = load_yaml_file(SKILLS_PATH)
        metadata = full_registry.get("metadata", {})
        context["skills_registry"] = {
            "metadata": metadata,
            "_note": "Skill index is loaded separately via _index.yaml. Use grep/jq for full registry.",
            "_index_path": "agent/skills/_index.yaml",
            "_registry_path": "agent/skills/skills_registry.yaml",
            "_trigger_engine_path": "agent/skills/_trigger_engine.yaml",
        }
    else:
        context["skills_registry"] = {}
        print(f"[WARNING] Skills registry not found at {SKILLS_PATH}")

    # 2. Agent rules — metadata reference only (governance content, always tracked)
    context["agent_rules"] = _file_metadata(AGENT_RULES_PATH)

    # 3. Gitignored files — path-only references.
    #    These files are NOT included in the initial context.
    #    Use context_loader.load_on_demand("treemap") or
    #    context_loader.load_on_demand("dependencies_report") to fetch them.
    context["on_demand_files"] = {
        "treemap": {
            "path": TREEMAP_PATH,
            "loader": "context_loader.load_on_demand('treemap')",
            "_note": "Gitignored. Load on demand for structural analysis.",
        },
        "dependencies_report": {
            "path": DEPENDENCIES_REPORT_PATH,
            "loader": "context_loader.load_on_demand('dependencies_report')",
            "_note": "Gitignored. Load on demand for dependency analysis.",
        },
    }

    # Shared .gitignore spec for file tree and signatures
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gitignore_spec = _load_gitignore_spec(project_root)

    # 4. File tree (max depth 2) — folders only, no individual skill files
    context["file_tree"] = {
        "max_depth": 2,
        "tree": _generate_file_tree(project_root, max_depth=2, spec=gitignore_spec),
    }

    # 5. Python signatures — class/function index (.gitignore respected, agent/skills excluded)
    context["python_signatures"] = _extract_python_signatures(
        project_root, max_depth=3, spec=gitignore_spec
    )

    # 6. Schemas — summaries only
    context["schemas"] = {}
    if os.path.exists(SCHEMAS_PATH):
        for fname in os.listdir(SCHEMAS_PATH):
            fpath = os.path.join(SCHEMAS_PATH, fname)
            context["schemas"][fname] = _schema_summary(fpath)

    # 7. Agent definitions — minimal path references only
    context["agent_definitions"] = {
        "senior": {
            "context": _file_ref(AGENT_SENIOR_CONTEXT),
            "workflow": _file_ref(AGENT_SENIOR_WORKFLOW),
        },
        "executor": {
            "context": _file_ref(AGENT_EXECUTOR_CONTEXT),
            "workflow": _file_ref(AGENT_EXECUTOR_WORKFLOW),
        },
        "inspector": {
            "context": _file_ref(AGENT_INSPECTOR_CONTEXT),
            "workflow": _file_ref(AGENT_INSPECTOR_WORKFLOW),
        },
    }

    return context


def save_static_context_as_json(
    context: Dict[str, Any],
    output_path: str = "agent/agent_outputs/context.json",
) -> None:
    """Save static context to a JSON file for agent consumption."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(context, f, indent=2)

    size = os.path.getsize(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)
    print(f"Static context saved: {output_path}")
    print(f"  Size: {size:,} bytes ({size / 1024:.1f} KB)")
    print(f"  Lines: {line_count:,}")


# Ejecución de prueba
if __name__ == "__main__":
    static_context = load_static_context()
    save_static_context_as_json(static_context)
    skills = static_context.get("skills_registry", {})
    metadata = skills.get("metadata", {})
    print(f"\nSkeleton context generated:")
    print(f"  Skills registry version: {metadata.get('version', 'unknown')}")
    print(f"  Skills total: {metadata.get('total_skills', 0)} (index loaded separately via _index.yaml)")
    print(f"  Python files with signatures: {len(static_context.get('python_signatures', {}))}")
    print(f"  Schemas summarized: {list(static_context.get('schemas', {}).keys())}")
    print(f"  File tree entries: {len(static_context.get('file_tree', {}).get('tree', []))}")
