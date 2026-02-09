"""
load_static_context.py

Carga los archivos de contexto estáticos del framework:
- skills_registry.yaml
- agent_rules.md
- agent/dependencies_report.md
- agent/treemap.md
- Schemas de agent_protocol (opcional)

Devuelve un diccionario centralizado listo para consumo por agentes.
"""

import os
import yaml
import json
from typing import Dict, Any, Optional

# Paths relativos dentro del proyecto
SKILLS_PATH = os.path.join("agent", "skills", "skills_registry.yaml")
AGENT_RULES_PATH = os.path.join("agent", "rules", "agent_rules.md")
DEPENDENCIES_REPORT_PATH = os.path.join("agent", "analysis", "dependencies_report.md")
TREEMAP_PATH = os.path.join("agent", "analysis", "treemap.md")
SCHEMAS_PATH = os.path.join("agent", "agent_protocol", "schemas")

# New modular context paths
AGENT_SENIOR_CONTEXT = os.path.join("agent", "agent_senior", "context", "context.md")
AGENT_SENIOR_WORKFLOW = os.path.join("agent", "agent_senior", "workflow", "workflow.md")
AGENT_EXECUTOR_CONTEXT = os.path.join("agent", "agent_executor", "context", "context.md")
AGENT_EXECUTOR_WORKFLOW = os.path.join("agent", "agent_executor", "workflow", "workflow.md")
AGENT_INSPECTOR_CONTEXT = os.path.join("agent", "agent_inspector", "context", "context.md")
AGENT_INSPECTOR_WORKFLOW = os.path.join("agent", "agent_inspector", "workflow", "workflow.md")


def load_yaml_file(path: str) -> Any:
    """Carga un archivo YAML."""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_json_file(path: str) -> Any:
    """Carga un archivo JSON."""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_md_file(path: str) -> str:
    """Carga un archivo Markdown."""
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_static_context() -> Dict[str, Any]:
    """Carga todo el contexto estático del framework."""
    context: Dict[str, Any] = {}

    # 1. Skills registry
    if os.path.exists(SKILLS_PATH):
        context["skills_registry"] = load_yaml_file(SKILLS_PATH)
    else:
        context["skills_registry"] = {}
        print(f"[WARNING] Skills registry not found at {SKILLS_PATH}")

    # 2. Agent rules
    if os.path.exists(AGENT_RULES_PATH):
        context["agent_rules"] = load_md_file(AGENT_RULES_PATH)
    else:
        context["agent_rules"] = ""
        print(f"[WARNING] Agent rules file not found at {AGENT_RULES_PATH}")

    # 3. Dependencies report
    if os.path.exists(DEPENDENCIES_REPORT_PATH):
        context["dependencies_report"] = load_md_file(DEPENDENCIES_REPORT_PATH)
    else:
        context["dependencies_report"] = ""
        print(f"[WARNING] Dependencies report not found at {DEPENDENCIES_REPORT_PATH}")

    # 4. Treemap
    if os.path.exists(TREEMAP_PATH):
        context["treemap"] = load_md_file(TREEMAP_PATH)
    else:
        context["treemap"] = ""
        print(f"[WARNING] Treemap file not found at {TREEMAP_PATH}")

    # 5. Schemas (optional)
    context["schemas"] = {}
    if os.path.exists(SCHEMAS_PATH):
        for fname in os.listdir(SCHEMAS_PATH):
            fpath = os.path.join(SCHEMAS_PATH, fname)
            if fname.endswith(".yaml") or fname.endswith(".yml"):
                context["schemas"][fname] = load_yaml_file(fpath)
            elif fname.endswith(".json"):
                context["schemas"][fname] = load_json_file(fpath)

    # 6. Agent Definitions
    context["agent_definitions"] = {
        "senior": {
            "context": load_md_file(AGENT_SENIOR_CONTEXT),
            "workflow": load_md_file(AGENT_SENIOR_WORKFLOW)
        },
        "executor": {
            "context": load_md_file(AGENT_EXECUTOR_CONTEXT),
            "workflow": load_md_file(AGENT_EXECUTOR_WORKFLOW)
        },
        "inspector": {
            "context": load_md_file(AGENT_INSPECTOR_CONTEXT),
            "workflow": load_md_file(AGENT_INSPECTOR_WORKFLOW)
        }
    }

    return context


def save_static_context_as_json(context: Dict[str, Any], output_path: str = "agent/agent_outputs/context.json") -> None:
    """Save static context to a JSON file for agent consumption."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(context, f, indent=2)
    print(f"Static context saved as JSON: {output_path}")


# Ejecución de prueba
if __name__ == "__main__":
    static_context = load_static_context()
    save_static_context_as_json(static_context)
    print("Static context loaded successfully:")
    print(f"Skills loaded: {len(static_context.get('skills_registry', {}))}")
    print(f"Agent rules length: {len(static_context.get('agent_rules', ''))} characters")
    print(f"Dependencies report length: {len(static_context.get('dependencies_report', ''))} characters")
    print(f"Treemap length: {len(static_context.get('treemap', ''))} characters")
    print(f"Schemas loaded: {list(static_context.get('schemas', {}).keys())}")
