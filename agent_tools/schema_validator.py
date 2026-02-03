#!/usr/bin/env python3
"""
JSON/YAML Schema Validator

Validates JSON and YAML files against defined schemas.
Supports both custom schemas and built-in agent protocol schemas.

Usage:
    python schema_validator.py <file> --schema <schema_file>
    python schema_validator.py <file> --type <task_plan|system_config|envelope|report>

Examples:
    python schema_validator.py task_plan.json --type task_plan
    python schema_validator.py config.yaml --schema custom_schema.yaml
    python schema_validator.py data.json --type envelope --verbose
"""

from __future__ import annotations

import argparse
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


# Built-in schema types
SCHEMA_TYPES = {
    "envelope": "task_envelope.schema.json",
    "task_plan": "task_plan.schema.json",
    "report": "execution_report.schema.json",
    "system_config": "system_config.schema.yaml",
}


def get_project_root() -> Path:
    """Get the project root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "agent").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parent.parent


def load_json(file_path: Path) -> dict[str, Any]:
    """Load JSON file."""
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


def load_yaml(file_path: Path) -> dict[str, Any]:
    """Load YAML file."""
    if not HAS_YAML:
        raise ImportError("PyYAML required. Install with: pip install pyyaml")
    with open(file_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_file(file_path: Path) -> dict[str, Any]:
    """Load JSON or YAML file based on extension."""
    suffix = file_path.suffix.lower()
    if suffix in (".yaml", ".yml"):
        return load_yaml(file_path)
    else:
        return load_json(file_path)


def get_builtin_schema_path(schema_type: str) -> Path:
    """Get path to built-in schema."""
    if schema_type not in SCHEMA_TYPES:
        valid_types = ", ".join(SCHEMA_TYPES.keys())
        raise ValueError(f"Unknown schema type: {schema_type}. Valid types: {valid_types}")

    project_root = get_project_root()
    schema_file = SCHEMA_TYPES[schema_type]
    return project_root / "agent" / "agent_protocol" / "schemas" / schema_file


def format_error(error: ValidationError, include_context: bool = True) -> str:
    """Format a validation error for display."""
    path = ".".join(str(p) for p in error.absolute_path) if error.absolute_path else "(root)"

    msg = f"[{path}] {error.message}"

    if include_context and error.context:
        for i, ctx in enumerate(error.context[:3], 1):  # Limit to 3 context items
            ctx_path = ".".join(str(p) for p in ctx.absolute_path) if ctx.absolute_path else "(root)"
            msg += f"\n    Context {i}: [{ctx_path}] {ctx.message}"

    return msg


def get_suggested_fix(error: ValidationError) -> str | None:
    """Generate a suggested fix for common errors."""
    message = error.message.lower()

    if "is a required property" in message:
        prop = error.message.split("'")[1] if "'" in error.message else "unknown"
        return f"Add the required property '{prop}' to your document"

    if "is not of type" in message:
        expected_type = error.message.split("type")[1].strip().strip("'\"")
        return f"Change the value to type: {expected_type}"

    if "is not one of" in message:
        return "Use one of the allowed values listed in the error"

    if "does not match" in message:
        return "Ensure the value matches the required pattern"

    return None


def validate_file(
    file_path: Path,
    schema_path: Path,
    verbose: bool = False,
) -> tuple[bool, list[str], list[str]]:
    """
    Validate a file against a schema.

    Returns:
        Tuple of (is_valid, errors, warnings)
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Load data file
    try:
        data = load_file(file_path)
    except json.JSONDecodeError as e:
        errors.append(f"JSON parse error: {e}")
        return False, errors, warnings
    except yaml.YAMLError as e:
        errors.append(f"YAML parse error: {e}")
        return False, errors, warnings
    except Exception as e:
        errors.append(f"Failed to load file: {e}")
        return False, errors, warnings

    # Load schema
    try:
        schema = load_file(schema_path)
    except Exception as e:
        errors.append(f"Failed to load schema: {e}")
        return False, errors, warnings

    # Validate
    if not HAS_JSONSCHEMA:
        warnings.append("jsonschema not installed. Full validation skipped.")
        warnings.append("Install with: pip install jsonschema")
        return True, errors, warnings

    try:
        validator = Draft202012Validator(schema)
        validation_errors = list(validator.iter_errors(data))

        for error in validation_errors:
            formatted = format_error(error, include_context=verbose)
            errors.append(formatted)

            fix = get_suggested_fix(error)
            if fix and verbose:
                errors.append(f"    Suggestion: {fix}")

    except jsonschema.SchemaError as e:
        errors.append(f"Schema error: {e.message}")
    except Exception as e:
        errors.append(f"Validation error: {e}")

    return len(errors) == 0, errors, warnings


def validate_syntax_only(file_path: Path) -> tuple[bool, list[str]]:
    """Validate only the syntax of a JSON/YAML file."""
    errors: list[str] = []

    try:
        load_file(file_path)
        return True, errors
    except json.JSONDecodeError as e:
        errors.append(f"JSON syntax error at line {e.lineno}, column {e.colno}: {e.msg}")
    except yaml.YAMLError as e:
        if hasattr(e, "problem_mark"):
            mark = e.problem_mark
            errors.append(f"YAML syntax error at line {mark.line + 1}, column {mark.column + 1}")
        else:
            errors.append(f"YAML syntax error: {e}")
    except Exception as e:
        errors.append(f"Parse error: {e}")

    return False, errors


def print_report(
    file_path: Path,
    schema_path: Path | None,
    is_valid: bool,
    errors: list[str],
    warnings: list[str],
) -> None:
    """Print validation report."""
    status = "VALID" if is_valid else "INVALID"

    print(f"\n{'=' * 60}")
    print(f"Schema Validation: {status}")
    print(f"{'=' * 60}")
    print(f"File: {file_path}")
    if schema_path:
        print(f"Schema: {schema_path}")
    print(f"Timestamp: {datetime.now().isoformat()}")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for error in errors:
            print(f"  - {error}")

    if warnings:
        print(f"\nWarnings ({len(warnings)}):")
        for warning in warnings:
            print(f"  - {warning}")

    if is_valid:
        print("\n  Document is valid!")

    print(f"\n{'=' * 60}\n")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate JSON/YAML files against schemas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Schema Types:
  envelope      - Task envelope (inter-agent messages)
  task_plan     - Inspector task plans
  report        - Executor execution reports
  system_config - System configuration files

Examples:
  %(prog)s task.json --type task_plan
  %(prog)s config.yaml --type system_config
  %(prog)s data.json --schema custom.schema.json
  %(prog)s file.json --syntax-only
        """,
    )
    parser.add_argument("file", type=Path, help="File to validate")
    parser.add_argument(
        "--schema",
        type=Path,
        help="Path to custom schema file",
    )
    parser.add_argument(
        "--type",
        choices=list(SCHEMA_TYPES.keys()),
        help="Built-in schema type",
    )
    parser.add_argument(
        "--syntax-only",
        action="store_true",
        help="Only check syntax, skip schema validation",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed error context and suggestions",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Only show errors, no formatting",
    )

    args = parser.parse_args()

    if not args.file.exists():
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        return 1

    # Syntax-only check
    if args.syntax_only:
        is_valid, errors = validate_syntax_only(args.file)
        if args.quiet:
            for error in errors:
                print(error)
        else:
            print_report(args.file, None, is_valid, errors, [])
        return 0 if is_valid else 1

    # Determine schema
    if args.schema:
        schema_path = args.schema
    elif args.type:
        try:
            schema_path = get_builtin_schema_path(args.type)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
    else:
        print("Error: Either --schema or --type must be specified", file=sys.stderr)
        return 1

    if not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}", file=sys.stderr)
        return 1

    is_valid, errors, warnings = validate_file(
        args.file,
        schema_path,
        verbose=args.verbose,
    )

    if args.quiet:
        for error in errors:
            print(error)
    else:
        print_report(args.file, schema_path, is_valid, errors, warnings)

    return 0 if is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
