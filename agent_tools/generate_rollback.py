#!/usr/bin/env python3
"""
Rollback Checkpoint Generator

Creates rollback checkpoints for a task plan before execution.
Backs up all files that will be modified and generates a rollback manifest.

Usage:
    python generate_rollback.py <task_plan.json> --output <checkpoint_dir>
    python generate_rollback.py <task_plan.json> --output backups/ --dry-run

Returns:
    Rollback manifest and file backups in the output directory

Examples:
    python generate_rollback.py task_plan.json --output agent_outputs/reports/task_001/backups
    python generate_rollback.py task_plan.json --output backups/ --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


@dataclass
class Checkpoint:
    """A single file checkpoint."""

    checkpoint_id: str
    file_path: str
    backup_location: str
    original_hash: str
    original_size: int
    operation_to_reverse: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "file_path": self.file_path,
            "backup_location": self.backup_location,
            "original_hash": self.original_hash,
            "original_size": self.original_size,
            "operation_to_reverse": self.operation_to_reverse,
            "created_at": self.created_at,
        }


@dataclass
class RollbackManifest:
    """Complete rollback manifest."""

    manifest_id: str
    plan_id: str
    created_at: str
    status: str
    checkpoints: list[Checkpoint]
    rollback_order: list[str]
    files_backed_up: int
    total_backup_size: int
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "manifest_id": self.manifest_id,
            "plan_id": self.plan_id,
            "created_at": self.created_at,
            "status": self.status,
            "checkpoints": [c.to_dict() for c in self.checkpoints],
            "rollback_order": self.rollback_order,
            "summary": {
                "files_backed_up": self.files_backed_up,
                "total_backup_size": self.total_backup_size,
            },
            "notes": self.notes,
        }


def get_project_root() -> Path:
    """Get the project root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "agent").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parent.parent


def calculate_file_hash(file_path: Path) -> str:
    """Calculate SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def load_task_plan(plan_path: Path) -> dict[str, Any]:
    """Load a task plan from JSON file."""
    with open(plan_path, encoding="utf-8") as f:
        return json.load(f)


def get_files_to_backup(plan: dict[str, Any]) -> list[tuple[str, str]]:
    """
    Extract files that need to be backed up from the plan.

    Returns list of (file_path, operation_type) tuples.
    """
    files_to_backup = []
    actions = plan.get("action_plan", [])

    for action in actions:
        action_type = action.get("action_type", "")
        target = action.get("target", "")

        # Only backup for operations that modify existing files
        if action_type in ("FILE_MODIFY", "FILE_DELETE", "FILE_RENAME"):
            if target:
                files_to_backup.append((target, action_type))

    return files_to_backup


def create_backup_filename(file_path: str, checkpoint_id: str) -> str:
    """Create a unique backup filename."""
    original_name = Path(file_path).name
    return f"{checkpoint_id}_{original_name}.bak"


def backup_file(
    source: Path,
    backup_dir: Path,
    checkpoint_id: str,
    dry_run: bool = False,
) -> tuple[str, int]:
    """
    Backup a single file.

    Returns: (backup_location, file_size)
    """
    backup_filename = create_backup_filename(str(source), checkpoint_id)
    backup_path = backup_dir / backup_filename

    if not dry_run:
        shutil.copy2(source, backup_path)

    return str(backup_path.relative_to(backup_dir.parent)), source.stat().st_size


def generate_rollback_manifest(
    plan: dict[str, Any],
    output_dir: Path,
    project_root: Path,
    dry_run: bool = False,
) -> RollbackManifest:
    """Generate a rollback manifest for a task plan."""
    plan_id = plan.get("plan_id", str(uuid4()))
    manifest_id = str(uuid4())
    created_at = datetime.now().isoformat()

    # Get files to backup
    files_to_backup = get_files_to_backup(plan)

    checkpoints: list[Checkpoint] = []
    rollback_order: list[str] = []
    total_size = 0

    # Create backup directory
    backup_dir = output_dir / "backups"
    if not dry_run:
        backup_dir.mkdir(parents=True, exist_ok=True)

    for file_path, operation in files_to_backup:
        full_path = project_root / file_path

        if not full_path.exists():
            print(f"Warning: File does not exist, skipping backup: {file_path}")
            continue

        checkpoint_id = str(uuid4())[:8]

        # Calculate hash before backup
        file_hash = calculate_file_hash(full_path)

        # Create backup
        backup_location, file_size = backup_file(
            full_path,
            backup_dir,
            checkpoint_id,
            dry_run=dry_run,
        )

        total_size += file_size

        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            file_path=file_path,
            backup_location=backup_location,
            original_hash=file_hash,
            original_size=file_size,
            operation_to_reverse=operation,
            created_at=created_at,
        )

        checkpoints.append(checkpoint)
        rollback_order.append(checkpoint_id)

    # Reverse rollback order (last changed first to restore)
    rollback_order.reverse()

    manifest = RollbackManifest(
        manifest_id=manifest_id,
        plan_id=plan_id,
        created_at=created_at,
        status="ACTIVE",
        checkpoints=checkpoints,
        rollback_order=rollback_order,
        files_backed_up=len(checkpoints),
        total_backup_size=total_size,
        notes="Generated by generate_rollback.py" + (" (DRY RUN)" if dry_run else ""),
    )

    return manifest


def generate_rollback_script(manifest: RollbackManifest, output_dir: Path) -> Path:
    """Generate a shell script for manual rollback."""
    script_path = output_dir / "rollback.sh"

    lines = [
        "#!/bin/bash",
        "# Rollback script generated by generate_rollback.py",
        f"# Manifest ID: {manifest.manifest_id}",
        f"# Plan ID: {manifest.plan_id}",
        f"# Generated: {manifest.created_at}",
        "",
        "set -e",
        "",
        "echo 'Starting rollback...'",
        "",
    ]

    for checkpoint_id in manifest.rollback_order:
        checkpoint = next((c for c in manifest.checkpoints if c.checkpoint_id == checkpoint_id), None)
        if checkpoint:
            lines.append(f"# Restore {checkpoint.file_path}")
            lines.append(f"cp '{checkpoint.backup_location}' '{checkpoint.file_path}'")
            lines.append(f"echo 'Restored: {checkpoint.file_path}'")
            lines.append("")

    lines.append("echo 'Rollback complete!'")

    with open(script_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return script_path


def print_manifest_summary(manifest: RollbackManifest) -> None:
    """Print a summary of the rollback manifest."""
    print(f"\n{'=' * 60}")
    print("Rollback Manifest Generated")
    print(f"{'=' * 60}")
    print(f"Manifest ID: {manifest.manifest_id}")
    print(f"Plan ID: {manifest.plan_id}")
    print(f"Created: {manifest.created_at}")
    print(f"Status: {manifest.status}")
    print()
    print("Summary:")
    print(f"  Files Backed Up: {manifest.files_backed_up}")
    print(f"  Total Backup Size: {manifest.total_backup_size:,} bytes")
    print()

    if manifest.checkpoints:
        print("Checkpoints:")
        for cp in manifest.checkpoints:
            print(f"  - [{cp.checkpoint_id}] {cp.file_path}")
            print(f"    Operation: {cp.operation_to_reverse}")
            print(f"    Hash: {cp.original_hash[:16]}...")
            print(f"    Size: {cp.original_size:,} bytes")
            print()

    print(f"Rollback Order: {' -> '.join(manifest.rollback_order)}")
    print(f"\n{'=' * 60}\n")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate rollback checkpoints for a task plan",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("plan_file", type=Path, help="Path to the task plan JSON file")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        required=True,
        help="Output directory for backups and manifest",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be backed up without actually creating backups",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        help="Project root directory (auto-detected if not specified)",
    )
    parser.add_argument(
        "--generate-script",
        action="store_true",
        help="Generate a shell script for manual rollback",
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

    # Create output directory
    if not args.dry_run:
        args.output.mkdir(parents=True, exist_ok=True)

    manifest = generate_rollback_manifest(
        plan,
        args.output,
        project_root,
        dry_run=args.dry_run,
    )

    # Write manifest
    if not args.dry_run:
        manifest_path = args.output / "rollback_manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest.to_dict(), f, indent=2)
        print(f"Manifest written to: {manifest_path}")

        if args.generate_script:
            script_path = generate_rollback_script(manifest, args.output)
            print(f"Rollback script written to: {script_path}")

    print_manifest_summary(manifest)

    return 0


if __name__ == "__main__":
    sys.exit(main())
