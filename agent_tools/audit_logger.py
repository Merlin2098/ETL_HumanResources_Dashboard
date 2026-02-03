#!/usr/bin/env python3
"""
Audit Trail Logger

Generates and manages audit trails for agent operations.
Provides structured event logging with query and export capabilities.

Usage:
    python audit_logger.py log <event_type> <event_data>
    python audit_logger.py query [--from <date>] [--to <date>] [--type <type>]
    python audit_logger.py export [--format json|csv]

Examples:
    python audit_logger.py log PLAN_CREATED '{"plan_id": "abc123"}'
    python audit_logger.py query --type EXECUTION_COMPLETED --from 2026-01-01
    python audit_logger.py export --format csv --output audit_report.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


# Event types
EVENT_TYPES = [
    "PLAN_CREATED",
    "PLAN_VALIDATED",
    "PLAN_APPROVED",
    "PLAN_REJECTED",
    "EXECUTION_STARTED",
    "EXECUTION_COMPLETED",
    "EXECUTION_FAILED",
    "ROLLBACK_INITIATED",
    "ROLLBACK_COMPLETED",
    "FILE_MODIFIED",
    "FILE_CREATED",
    "FILE_DELETED",
    "ERROR_OCCURRED",
    "WARNING_GENERATED",
]


@dataclass
class AuditEntry:
    """A single audit log entry."""

    entry_id: str
    timestamp: str
    event_type: str
    agent: str
    data: dict[str, Any]
    previous_hash: str
    entry_hash: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "agent": self.agent,
            "data": self.data,
            "previous_hash": self.previous_hash,
            "entry_hash": self.entry_hash,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AuditEntry:
        """Create from dictionary."""
        return cls(
            entry_id=data["entry_id"],
            timestamp=data["timestamp"],
            event_type=data["event_type"],
            agent=data["agent"],
            data=data["data"],
            previous_hash=data["previous_hash"],
            entry_hash=data["entry_hash"],
        )


def get_project_root() -> Path:
    """Get the project root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "agent").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parent.parent


def get_log_dir() -> Path:
    """Get the audit log directory."""
    return get_project_root() / "agent" / "agent_logs" / "audit"


def get_current_log_file() -> Path:
    """Get the current day's log file."""
    log_dir = get_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    return log_dir / f"{date_str}_audit.jsonl"


def calculate_hash(data: str) -> str:
    """Calculate SHA-256 hash."""
    return hashlib.sha256(data.encode()).hexdigest()


def get_last_hash(log_file: Path) -> str:
    """Get the hash of the last entry in the log file."""
    if not log_file.exists():
        return "0" * 64  # Genesis hash

    last_hash = "0" * 64
    with open(log_file, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    entry = json.loads(line)
                    last_hash = entry.get("entry_hash", last_hash)
                except json.JSONDecodeError:
                    pass
    return last_hash


def create_entry(
    event_type: str,
    data: dict[str, Any],
    agent: str = "system",
) -> AuditEntry:
    """Create a new audit entry."""
    log_file = get_current_log_file()
    previous_hash = get_last_hash(log_file)

    entry_id = str(uuid4())
    timestamp = datetime.now().isoformat()

    # Calculate hash of this entry (excluding the hash itself)
    hash_content = json.dumps(
        {
            "entry_id": entry_id,
            "timestamp": timestamp,
            "event_type": event_type,
            "agent": agent,
            "data": data,
            "previous_hash": previous_hash,
        },
        sort_keys=True,
    )
    entry_hash = calculate_hash(hash_content)

    return AuditEntry(
        entry_id=entry_id,
        timestamp=timestamp,
        event_type=event_type,
        agent=agent,
        data=data,
        previous_hash=previous_hash,
        entry_hash=entry_hash,
    )


def append_entry(entry: AuditEntry) -> None:
    """Append an entry to the log file."""
    log_file = get_current_log_file()
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry.to_dict()) + "\n")


def log_event(event_type: str, data: dict[str, Any], agent: str = "system") -> AuditEntry:
    """Log an event and return the entry."""
    entry = create_entry(event_type, data, agent)
    append_entry(entry)
    return entry


def load_entries(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    event_type: str | None = None,
    agent: str | None = None,
    limit: int | None = None,
) -> list[AuditEntry]:
    """Load and filter audit entries."""
    log_dir = get_log_dir()
    entries: list[AuditEntry] = []

    if not log_dir.exists():
        return entries

    # Get all log files
    log_files = sorted(log_dir.glob("*_audit.jsonl"))

    for log_file in log_files:
        # Check if file is in date range
        try:
            file_date_str = log_file.stem.split("_")[0]
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")

            if from_date and file_date.date() < from_date.date():
                continue
            if to_date and file_date.date() > to_date.date():
                continue
        except (ValueError, IndexError):
            continue

        with open(log_file, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    data = json.loads(line)
                    entry = AuditEntry.from_dict(data)

                    # Apply filters
                    if event_type and entry.event_type != event_type:
                        continue
                    if agent and entry.agent != agent:
                        continue

                    entry_time = datetime.fromisoformat(entry.timestamp)
                    if from_date and entry_time < from_date:
                        continue
                    if to_date and entry_time > to_date:
                        continue

                    entries.append(entry)

                    if limit and len(entries) >= limit:
                        return entries

                except (json.JSONDecodeError, KeyError):
                    continue

    return entries


def verify_chain(entries: list[AuditEntry]) -> tuple[bool, list[str]]:
    """Verify the hash chain integrity."""
    errors: list[str] = []

    for i, entry in enumerate(entries):
        # Verify entry hash
        hash_content = json.dumps(
            {
                "entry_id": entry.entry_id,
                "timestamp": entry.timestamp,
                "event_type": entry.event_type,
                "agent": entry.agent,
                "data": entry.data,
                "previous_hash": entry.previous_hash,
            },
            sort_keys=True,
        )
        expected_hash = calculate_hash(hash_content)

        if entry.entry_hash != expected_hash:
            errors.append(f"Entry {entry.entry_id}: hash mismatch")

        # Verify chain link
        if i > 0:
            if entry.previous_hash != entries[i - 1].entry_hash:
                errors.append(f"Entry {entry.entry_id}: chain broken (previous hash mismatch)")

    return len(errors) == 0, errors


def export_entries(
    entries: list[AuditEntry],
    output_format: str = "json",
    output_file: Path | None = None,
) -> str:
    """Export entries to JSON or CSV format."""
    if output_format == "json":
        result = json.dumps([e.to_dict() for e in entries], indent=2)
    elif output_format == "csv":
        if not entries:
            result = "entry_id,timestamp,event_type,agent,data,entry_hash\n"
        else:
            output = []
            for entry in entries:
                output.append(
                    {
                        "entry_id": entry.entry_id,
                        "timestamp": entry.timestamp,
                        "event_type": entry.event_type,
                        "agent": entry.agent,
                        "data": json.dumps(entry.data),
                        "entry_hash": entry.entry_hash,
                    }
                )

            # Convert to CSV string
            import io

            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=output[0].keys())
            writer.writeheader()
            writer.writerows(output)
            result = buffer.getvalue()
    else:
        raise ValueError(f"Unknown format: {output_format}")

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(result)

    return result


def cmd_log(args: argparse.Namespace) -> int:
    """Handle the 'log' command."""
    event_type = args.event_type.upper()

    if event_type not in EVENT_TYPES:
        print(f"Warning: Unknown event type: {event_type}")
        print(f"Known types: {', '.join(EVENT_TYPES)}")

    try:
        if args.data.startswith("{"):
            data = json.loads(args.data)
        else:
            data = {"message": args.data}
    except json.JSONDecodeError:
        data = {"message": args.data}

    entry = log_event(event_type, data, agent=args.agent)

    print(f"Logged event: {entry.entry_id}")
    print(f"  Type: {entry.event_type}")
    print(f"  Time: {entry.timestamp}")
    print(f"  Hash: {entry.entry_hash[:16]}...")

    return 0


def cmd_query(args: argparse.Namespace) -> int:
    """Handle the 'query' command."""
    from_date = None
    to_date = None

    if args.from_date:
        from_date = datetime.strptime(args.from_date, "%Y-%m-%d")
    if args.to_date:
        to_date = datetime.strptime(args.to_date, "%Y-%m-%d")

    entries = load_entries(
        from_date=from_date,
        to_date=to_date,
        event_type=args.type,
        agent=args.agent,
        limit=args.limit,
    )

    print(f"\n{'=' * 60}")
    print(f"Audit Log Query Results: {len(entries)} entries")
    print(f"{'=' * 60}\n")

    for entry in entries:
        print(f"[{entry.timestamp}] {entry.event_type}")
        print(f"  ID: {entry.entry_id}")
        print(f"  Agent: {entry.agent}")
        print(f"  Data: {json.dumps(entry.data, indent=4)}")
        print()

    if args.verify:
        is_valid, errors = verify_chain(entries)
        if is_valid:
            print("Chain integrity: VERIFIED")
        else:
            print("Chain integrity: BROKEN")
            for error in errors:
                print(f"  - {error}")

    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Handle the 'export' command."""
    entries = load_entries()

    output_file = args.output
    if output_file:
        output_file = Path(output_file)

    result = export_entries(entries, output_format=args.format, output_file=output_file)

    if output_file:
        print(f"Exported {len(entries)} entries to: {output_file}")
    else:
        print(result)

    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Audit trail logger for agent operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Log command
    log_parser = subparsers.add_parser("log", help="Log a new event")
    log_parser.add_argument("event_type", help="Event type (e.g., PLAN_CREATED)")
    log_parser.add_argument("data", help="Event data (JSON or plain text)")
    log_parser.add_argument("--agent", default="system", help="Agent name")

    # Query command
    query_parser = subparsers.add_parser("query", help="Query audit log")
    query_parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)")
    query_parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)")
    query_parser.add_argument("--type", help="Filter by event type")
    query_parser.add_argument("--agent", help="Filter by agent")
    query_parser.add_argument("--limit", type=int, help="Maximum entries to return")
    query_parser.add_argument("--verify", action="store_true", help="Verify chain integrity")

    # Export command
    export_parser = subparsers.add_parser("export", help="Export audit log")
    export_parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format",
    )
    export_parser.add_argument("--output", "-o", help="Output file path")

    args = parser.parse_args()

    if args.command == "log":
        return cmd_log(args)
    elif args.command == "query":
        return cmd_query(args)
    elif args.command == "export":
        return cmd_export(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
