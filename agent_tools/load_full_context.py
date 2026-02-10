"""
load_full_context.py

Carga contexto estático + dinámico y genera un JSON consolidado
para que los agentes LLM lo consuman de manera eficiente.

On-demand files (treemap, dependencies_report) are NOT included by default.
Use context_loader.enrich_context() to attach them when needed.
"""

import os
import json
import yaml
from typing import Dict, Any, List, Optional
try:
    from .load_static_context import load_static_context
    from .context_loader import enrich_context
except (ImportError, ValueError):
    from load_static_context import load_static_context
    from context_loader import enrich_context



def load_yaml_file(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_json_file(path):
    with open(path, "r") as f:
        return json.load(f)


def load_full_context(
    task_plan_path,
    system_config_path,
    summary_path,
    on_demand: Optional[List[str]] = None,
):
    """Load static + dynamic context, optionally enriched with on-demand files.

    Args:
        on_demand: Optional list of on-demand file names to attach.
                   E.g. ["treemap", "dependencies_report"].
                   These are gitignored files loaded temporarily for analysis.
    """
    # 1. Cargar contexto estático
    context = load_static_context()

    # 2. Cargar contexto dinámico
    dynamic_context: Dict[str, Any] = {}

    # Task plan
    if os.path.exists(task_plan_path):
        dynamic_context["task_plan"] = load_json_file(task_plan_path)
    else:
        raise FileNotFoundError(f"Task plan not found: {task_plan_path}")

    # System config
    if os.path.exists(system_config_path):
        dynamic_context["system_config"] = load_yaml_file(system_config_path)
    else:
        raise FileNotFoundError(f"System config not found: {system_config_path}")

    # Summary
    if os.path.exists(summary_path):
        dynamic_context["summary"] = load_yaml_file(summary_path)
    else:
        raise FileNotFoundError(f"Summary file not found: {summary_path}")

    # 3. Combinar contexto
    full_context = {**context, **dynamic_context}

    # 4. On-demand enrichment (gitignored files loaded temporarily)
    if on_demand:
        full_context = enrich_context(full_context, on_demand)

    return full_context


def save_context_as_json(full_context, output_path="agent/agent_outputs/context.json"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(full_context, f, indent=2)
    print(f"Full context saved as JSON: {output_path}")


# Ejecución de prueba
if __name__ == "__main__":
    TASK_PLAN = "agent/agent_outputs/plans/task_plan.json"
    SYSTEM_CONFIG = "config/system_config.yaml"
    SUMMARY = "agent/agent_outputs/summary/summary.yaml"

    full_context = load_full_context(TASK_PLAN, SYSTEM_CONFIG, SUMMARY)
    save_context_as_json(full_context)