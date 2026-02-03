#!/usr/bin/env python3
"""
Execution Plan Simulator

Simulates execution of a task plan without making any actual changes.
Useful for validating plans and previewing what would happen during execution.

Usage:
    python simulate_execution.py <task_plan.json> [--verbose]
    python simulate_execution.py <task_plan.json> --output report.json

Returns:
    Simulation report showing what would be changed
    No actual modifications are made

Examples:
    python simulate_execution.py task_plan.json
    python simulate_execution.py task_plan.json --verbose
    python simulate_execution.py task_plan.json --output simulation_report.json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class SimulationResult:
    """Result of simulating a single action."""

    action_id: str
    action_type: str
    target: str
    would_succeed: bool
    reason: str
    warnings: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SimulationReport:
    """Complete simulation report."""

    plan_id: str
    simulated_at: str
    overall_success: bool
    total_actions: int
    would_succeed: int
    would_fail: int
    would_skip: int
    files_affected: list[str]
    results: list[SimulationResult]
    warnings: list[str]
    errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "plan_id": self.plan_id,
            "simulated_at": self.simulated_at,
            "overall_success": self.overall_success,
            "summary": {
                "total_actions": self.total_actions,
                "would_succeed": self.would_succeed,
                "would_fail": self.would_fail,
                "would_skip": self.would_skip,
            },
            "files_affected": self.files_affected,
            "results": [
                {
                    "action_id": r.action_id,
                    "action_type": r.action_type,
                    "target": r.target,
                    "would_succeed": r.would_succeed,
                    "reason": r.reason,
                    "warnings": r.warnings,
                    "details": r.details,
                }
                for r in self.results
            ],
            "warnings": self.warnings,
            "errors": self.errors,
        }


def get_project_root() -> Path:
    """Get the project root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "agent").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parent.parent


def load_task_plan(plan_path: Path) -> dict[str, Any]:
    """Load a task plan from JSON file."""
    with open(plan_path, encoding="utf-8") as f:
        return json.load(f)


def check_file_exists(file_path: Path) -> bool:
    """Check if a file exists."""
    return file_path.exists()


def check_file_writable(file_path: Path) -> bool:
    """Check if a file is writable."""
    if file_path.exists():
        try:
            # Check if we can open for writing
            with open(file_path, "a"):
                pass
            return True
        except (PermissionError, OSError):
            return False
    else:
        # Check if parent directory is writable
        parent = file_path.parent
        return parent.exists() and parent.is_dir()


def check_protected_file(file_path: Path, project_root: Path) -> bool:
    """Check if a file is in the protected list."""
    protected_patterns = [
        ".git",
        ".env",
        "credentials",
        "secrets",
        "agent/agent_rules.md",
    ]
    relative_path = str(file_path.relative_to(project_root))
    return any(pattern in relative_path.lower() for pattern in protected_patterns)


def simulate_file_create(
    action: dict[str, Any],
    project_root: Path,
) -> SimulationResult:
    """Simulate FILE_CREATE action."""
    target = action.get("target", "")
    target_path = project_root / target
    warnings = []

    if target_path.exists():
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_CREATE",
            target=target,
            would_succeed=False,
            reason="File already exists",
            warnings=warnings,
            details={"existing_file": str(target_path)},
        )

    if not target_path.parent.exists():
        warnings.append(f"Parent directory does not exist: {target_path.parent}")

    if check_protected_file(target_path, project_root):
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_CREATE",
            target=target,
            would_succeed=False,
            reason="Target is a protected file",
            warnings=warnings,
        )

    return SimulationResult(
        action_id=action.get("action_id", ""),
        action_type="FILE_CREATE",
        target=target,
        would_succeed=True,
        reason="File can be created",
        warnings=warnings,
        details={"parent_exists": target_path.parent.exists()},
    )


def simulate_file_modify(
    action: dict[str, Any],
    project_root: Path,
) -> SimulationResult:
    """Simulate FILE_MODIFY action."""
    target = action.get("target", "")
    target_path = project_root / target
    warnings = []

    if not target_path.exists():
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_MODIFY",
            target=target,
            would_succeed=False,
            reason="File does not exist",
            warnings=warnings,
        )

    if not check_file_writable(target_path):
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_MODIFY",
            target=target,
            would_succeed=False,
            reason="File is not writable",
            warnings=warnings,
        )

    if check_protected_file(target_path, project_root):
        warnings.append("File is protected and requires elevated approval")

    # Get file info
    stat = target_path.stat()
    details = {
        "file_size": stat.st_size,
        "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
    }

    return SimulationResult(
        action_id=action.get("action_id", ""),
        action_type="FILE_MODIFY",
        target=target,
        would_succeed=True,
        reason="File can be modified",
        warnings=warnings,
        details=details,
    )


def simulate_file_delete(
    action: dict[str, Any],
    project_root: Path,
) -> SimulationResult:
    """Simulate FILE_DELETE action."""
    target = action.get("target", "")
    target_path = project_root / target
    warnings = []

    if not target_path.exists():
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_DELETE",
            target=target,
            would_succeed=False,
            reason="File does not exist",
            warnings=warnings,
        )

    if check_protected_file(target_path, project_root):
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_DELETE",
            target=target,
            would_succeed=False,
            reason="Cannot delete protected file",
            warnings=warnings,
        )

    return SimulationResult(
        action_id=action.get("action_id", ""),
        action_type="FILE_DELETE",
        target=target,
        would_succeed=True,
        reason="File can be deleted",
        warnings=["This action will permanently delete the file"],
        details={"file_size": target_path.stat().st_size},
    )


def simulate_file_rename(
    action: dict[str, Any],
    project_root: Path,
) -> SimulationResult:
    """Simulate FILE_RENAME action."""
    target = action.get("target", "")
    target_path = project_root / target
    operation = action.get("operation", {})
    new_name = operation.get("new_name", "")
    warnings = []

    if not target_path.exists():
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_RENAME",
            target=target,
            would_succeed=False,
            reason="Source file does not exist",
            warnings=warnings,
        )

    if not new_name:
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_RENAME",
            target=target,
            would_succeed=False,
            reason="New name not specified in operation",
            warnings=warnings,
        )

    new_path = project_root / new_name
    if new_path.exists():
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type="FILE_RENAME",
            target=target,
            would_succeed=False,
            reason=f"Destination already exists: {new_name}",
            warnings=warnings,
        )

    return SimulationResult(
        action_id=action.get("action_id", ""),
        action_type="FILE_RENAME",
        target=target,
        would_succeed=True,
        reason="File can be renamed",
        warnings=warnings,
        details={"new_name": new_name},
    )


def simulate_action(
    action: dict[str, Any],
    project_root: Path,
) -> SimulationResult:
    """Simulate a single action."""
    action_type = action.get("action_type", "UNKNOWN")

    simulators = {
        "FILE_CREATE": simulate_file_create,
        "FILE_MODIFY": simulate_file_modify,
        "FILE_DELETE": simulate_file_delete,
        "FILE_RENAME": simulate_file_rename,
    }

    if action_type in simulators:
        return simulators[action_type](action, project_root)
    else:
        return SimulationResult(
            action_id=action.get("action_id", ""),
            action_type=action_type,
            target=action.get("target", ""),
            would_succeed=True,
            reason=f"Action type {action_type} simulated (no detailed check)",
            warnings=[f"No specific simulator for action type: {action_type}"],
        )


def check_dependencies(
    actions: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """Check dependency validity and detect cycles."""
    errors = []
    warnings = []

    # Build ID set
    action_ids = {a.get("action_id") for a in actions}

    # Check references
    for action in actions:
        for dep_id in action.get("depends_on", []):
            if dep_id not in action_ids:
                errors.append(f"Action {action.get('action_id')} depends on unknown action: {dep_id}")

    # Simple cycle detection using DFS
    graph = {a.get("action_id"): a.get("depends_on", []) for a in actions}
    visited = set()
    rec_stack = set()

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

    return errors, warnings


def simulate_plan(plan: dict[str, Any], project_root: Path) -> SimulationReport:
    """Simulate execution of an entire task plan."""
    plan_id = plan.get("plan_id", "unknown")
    actions = plan.get("action_plan", [])

    results: list[SimulationResult] = []
    all_warnings: list[str] = []
    all_errors: list[str] = []
    files_affected: list[str] = []

    # Check dependencies first
    dep_errors, dep_warnings = check_dependencies(actions)
    all_errors.extend(dep_errors)
    all_warnings.extend(dep_warnings)

    # Simulate each action
    for action in actions:
        result = simulate_action(action, project_root)
        results.append(result)

        if result.target and result.target not in files_affected:
            files_affected.append(result.target)

        all_warnings.extend(result.warnings)

    # Calculate summary
    would_succeed = sum(1 for r in results if r.would_succeed)
    would_fail = sum(1 for r in results if not r.would_succeed)

    # Overall success: all actions would succeed and no errors
    overall_success = would_fail == 0 and len(all_errors) == 0

    return SimulationReport(
        plan_id=plan_id,
        simulated_at=datetime.now().isoformat(),
        overall_success=overall_success,
        total_actions=len(actions),
        would_succeed=would_succeed,
        would_fail=would_fail,
        would_skip=0,
        files_affected=files_affected,
        results=results,
        warnings=all_warnings,
        errors=all_errors,
    )


def print_report(report: SimulationReport, verbose: bool = False) -> None:
    """Print simulation report to stdout."""
    status = "SUCCESS" if report.overall_success else "WOULD FAIL"
    print(f"\n{'=' * 60}")
    print(f"Simulation Report: {status}")
    print(f"{'=' * 60}")
    print(f"Plan ID: {report.plan_id}")
    print(f"Simulated: {report.simulated_at}")
    print()
    print("Summary:")
    print(f"  Total Actions: {report.total_actions}")
    print(f"  Would Succeed: {report.would_succeed}")
    print(f"  Would Fail: {report.would_fail}")
    print()
    print(f"Files Affected ({len(report.files_affected)}):")
    for f in report.files_affected:
        print(f"  - {f}")

    if report.errors:
        print(f"\nErrors ({len(report.errors)}):")
        for error in report.errors:
            print(f"  [ERROR] {error}")

    if report.warnings:
        print(f"\nWarnings ({len(report.warnings)}):")
        for warning in report.warnings:
            print(f"  [WARN] {warning}")

    if verbose:
        print("\nDetailed Results:")
        for result in report.results:
            status_icon = "+" if result.would_succeed else "x"
            print(f"\n  [{status_icon}] {result.action_id}: {result.action_type}")
            print(f"      Target: {result.target}")
            print(f"      Status: {result.reason}")
            if result.details:
                print(f"      Details: {result.details}")

    print(f"\n{'=' * 60}\n")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Simulate execution of a task plan without making changes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("plan_file", type=Path, help="Path to the task plan JSON file")
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed simulation results",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Write report to JSON file",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        help="Project root directory (auto-detected if not specified)",
    )

    args = parser.parse_args()

    if not args.plan_file.exists():
        print(f"Error: Plan file not found: {args.plan_file}", file=sys.stderr)
        return 1

    project_root = args.project_root or get_project_root()

    try:
        plan = load_task_plan(args.plan_file)
    except Exception as e:
        print(f"Error loading plan: {e}", file=sys.stderr)
        return 1

    report = simulate_plan(plan, project_root)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2)
        print(f"Report written to: {args.output}")
    else:
        print_report(report, verbose=args.verbose)

    return 0 if report.overall_success else 1


if __name__ == "__main__":
    sys.exit(main())
