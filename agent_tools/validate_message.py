#!/usr/bin/env python3
"""
Inter-agent Message Validator

Validates inter-agent messages against defined JSON schemas.

Usage:
    python validate_message.py <message_file> [--schema <schema_file>]
    python validate_message.py <message_file> --type <envelope|plan|report|config>

Examples:
    python validate_message.py task_envelope.json --type envelope
    python validate_message.py task_plan.json --schema agent/agent_protocol/schemas/task_plan.schema.json

Returns:
    Exit code 0 if valid, 1 if invalid
    Outputs validation report to stdout
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import jsonschema
    from jsonschema import Draft202012Validator, ValidationError

    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False


# Schema type mapping
SCHEMA_TYPES = {
    "envelope": "task_envelope.schema.json",
    "plan": "task_plan.schema.json",
    "report": "execution_report.schema.json",
    "config": "system_config.schema.yaml",
}


def get_project_root() -> Path:
    """Get the project root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "agent").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parent.parent


def load_json_file(file_path: Path) -> dict[str, Any]:
    """Load and parse a JSON file."""
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


def load_yaml_file(file_path: Path) -> dict[str, Any]:
    """Load and parse a YAML file."""
    if not HAS_YAML:
        raise ImportError("PyYAML is required for YAML files. Install with: pip install pyyaml")
    with open(file_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_file(file_path: Path) -> dict[str, Any]:
    """Load a JSON or YAML file based on extension."""
    suffix = file_path.suffix.lower()
    if suffix in (".json",):
        return load_json_file(file_path)
    elif suffix in (".yaml", ".yml"):
        return load_yaml_file(file_path)
    else:
        # Try JSON first, then YAML
        try:
            return load_json_file(file_path)
        except json.JSONDecodeError:
            return load_yaml_file(file_path)


def get_schema_path(schema_type: str) -> Path:
    """Get the path to a built-in schema."""
    if schema_type not in SCHEMA_TYPES:
        raise ValueError(f"Unknown schema type: {schema_type}. Valid types: {list(SCHEMA_TYPES.keys())}")

    project_root = get_project_root()
    schema_file = SCHEMA_TYPES[schema_type]
    return project_root / "agent" / "agent_protocol" / "schemas" / schema_file


def calculate_checksum(payload: dict[str, Any]) -> str:
    """Calculate SHA-256 checksum of payload."""
    payload_str = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload_str.encode()).hexdigest()


def validate_checksum(message: dict[str, Any]) -> tuple[bool, str]:
    """Validate the checksum in a message envelope."""
    if "validation" not in message or "checksum" not in message.get("validation", {}):
        return True, "No checksum to validate"

    if "payload" not in message:
        return False, "Message has checksum but no payload"

    expected = message["validation"]["checksum"]
    actual = calculate_checksum(message["payload"])

    if expected == actual:
        return True, "Checksum valid"
    else:
        return False, f"Checksum mismatch: expected {expected}, got {actual}"


def validate_references(message: dict[str, Any]) -> list[str]:
    """Validate internal references in a task plan."""
    warnings = []

    if "action_plan" not in message:
        return warnings

    # Collect all action IDs
    action_ids = {action.get("action_id") for action in message.get("action_plan", [])}

    # Check dependencies reference valid actions
    for action in message.get("action_plan", []):
        for dep_id in action.get("depends_on", []):
            if dep_id not in action_ids:
                warnings.append(f"Action {action.get('action_id')} depends on unknown action: {dep_id}")

    # Check subtask references
    for subtask in message.get("task_decomposition", []):
        for action_id in subtask.get("actions", []):
            if action_id not in action_ids:
                warnings.append(f"Subtask {subtask.get('subtask_id')} references unknown action: {action_id}")

    return warnings


def check_dependency_cycles(message: dict[str, Any]) -> list[str]:
    """Check for circular dependencies in action plan."""
    errors = []

    if "action_plan" not in message:
        return errors

    # Build dependency graph
    graph: dict[str, list[str]] = {}
    for action in message.get("action_plan", []):
        action_id = action.get("action_id")
        graph[action_id] = action.get("depends_on", [])

    # DFS cycle detection
    visited: set[str] = set()
    rec_stack: set[str] = set()

    def has_cycle(node: str) -> bool:
        visited.add(node)
        rec_stack.add(node)

        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                if has_cycle(neighbor):
                    return True
            elif neighbor in rec_stack:
                return True

        rec_stack.remove(node)
        return False

    for node in graph:
        if node not in visited:
            if has_cycle(node):
                errors.append(f"Circular dependency detected involving action: {node}")
                break

    return errors


def format_validation_error(error: ValidationError) -> str:
    """Format a validation error for display."""
    path = " -> ".join(str(p) for p in error.absolute_path) if error.absolute_path else "root"
    return f"  [{path}] {error.message}"


def validate_message(
    message_path: Path,
    schema_path: Path | None = None,
    schema_type: str | None = None,
) -> tuple[bool, dict[str, Any]]:
    """
    Validate a message against a schema.

    Returns:
        Tuple of (is_valid, report)
    """
    report: dict[str, Any] = {
        "file": str(message_path),
        "timestamp": datetime.now().isoformat(),
        "valid": False,
        "errors": [],
        "warnings": [],
        "checks_performed": [],
    }

    # Load message
    try:
        message = load_file(message_path)
        report["checks_performed"].append("File loaded successfully")
    except Exception as e:
        report["errors"].append(f"Failed to load message file: {e}")
        return False, report

    # Determine schema
    if schema_path is None and schema_type is None:
        # Try to auto-detect based on content
        if "envelope" in message and "payload" in message:
            schema_type = "envelope"
        elif "action_plan" in message and "risk_assessment" in message:
            schema_type = "plan"
        elif "actions_summary" in message and "status" in message:
            schema_type = "report"
        elif "system_definitions" in message and "workflow_configuration" in message:
            schema_type = "config"
        else:
            report["errors"].append("Cannot auto-detect schema type. Please specify --type or --schema")
            return False, report

    if schema_path is None:
        schema_path = get_schema_path(schema_type)

    report["schema"] = str(schema_path)
    report["schema_type"] = schema_type

    # Load schema
    try:
        schema = load_file(schema_path)
        report["checks_performed"].append("Schema loaded successfully")
    except Exception as e:
        report["errors"].append(f"Failed to load schema: {e}")
        return False, report

    # JSON Schema validation
    if HAS_JSONSCHEMA:
        try:
            validator = Draft202012Validator(schema)
            errors = list(validator.iter_errors(message))

            if errors:
                report["errors"].extend(format_validation_error(e) for e in errors)
            else:
                report["checks_performed"].append("JSON Schema validation passed")
        except Exception as e:
            report["errors"].append(f"Schema validation error: {e}")
    else:
        report["warnings"].append("jsonschema not installed. Schema validation skipped.")

    # Checksum validation (for envelopes)
    if schema_type == "envelope":
        checksum_valid, checksum_msg = validate_checksum(message)
        if checksum_valid:
            report["checks_performed"].append(f"Checksum: {checksum_msg}")
        else:
            report["errors"].append(f"Checksum: {checksum_msg}")

    # Reference validation (for plans)
    if schema_type == "plan":
        ref_warnings = validate_references(message)
        report["warnings"].extend(ref_warnings)
        if not ref_warnings:
            report["checks_performed"].append("Internal references valid")

        cycle_errors = check_dependency_cycles(message)
        report["errors"].extend(cycle_errors)
        if not cycle_errors:
            report["checks_performed"].append("No dependency cycles detected")

    # Determine overall validity
    report["valid"] = len(report["errors"]) == 0

    return report["valid"], report


def print_report(report: dict[str, Any], verbose: bool = False) -> None:
    """Print validation report to stdout."""
    status = "VALID" if report["valid"] else "INVALID"
    print(f"\n{'=' * 60}")
    print(f"Validation Report: {status}")
    print(f"{'=' * 60}")
    print(f"File: {report['file']}")
    print(f"Schema: {report.get('schema', 'N/A')}")
    print(f"Type: {report.get('schema_type', 'N/A')}")
    print(f"Timestamp: {report['timestamp']}")

    if report["errors"]:
        print(f"\nErrors ({len(report['errors'])}):")
        for error in report["errors"]:
            print(f"  - {error}")

    if report["warnings"]:
        print(f"\nWarnings ({len(report['warnings'])}):")
        for warning in report["warnings"]:
            print(f"  - {warning}")

    if verbose and report["checks_performed"]:
        print(f"\nChecks Performed ({len(report['checks_performed'])}):")
        for check in report["checks_performed"]:
            print(f"  + {check}")

    print(f"\n{'=' * 60}\n")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate inter-agent messages against JSON schemas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s task_envelope.json --type envelope
  %(prog)s task_plan.json --type plan
  %(prog)s config.yaml --schema custom_schema.json
  %(prog)s message.json --verbose
        """,
    )
    parser.add_argument("message_file", type=Path, help="Path to the message file to validate")
    parser.add_argument(
        "--schema",
        type=Path,
        help="Path to custom schema file",
    )
    parser.add_argument(
        "--type",
        choices=list(SCHEMA_TYPES.keys()),
        help="Built-in schema type to use",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed validation report",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output report as JSON",
    )

    args = parser.parse_args()

    if not args.message_file.exists():
        print(f"Error: File not found: {args.message_file}", file=sys.stderr)
        return 1

    is_valid, report = validate_message(
        args.message_file,
        schema_path=args.schema,
        schema_type=args.type,
    )

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print_report(report, verbose=args.verbose)

    return 0 if is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
